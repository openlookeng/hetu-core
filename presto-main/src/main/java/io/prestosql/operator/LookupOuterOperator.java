/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.prestosql.execution.Lifespan;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getDone;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"operatorContext", "lookupSourceFactory", "outerPositionsFuture", "probeOutputTypes", "onClose", "pageBuilder",
        "outerPositions", "closed", "snapshotState", "incomingMarkers", "outgoingMarkers", "blockFuture"})
public class LookupOuterOperator
        implements Operator
{
    private static final Logger LOG = Logger.get(LookupOuterOperator.class);

    public static class LookupOuterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> probeOutputTypes;
        private final List<Type> buildOutputTypes;
        private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;

        private final Set<Lifespan> createdLifespans = new HashSet<>();
        private boolean closed;

        public LookupOuterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> probeOutputTypes,
                List<Type> buildOutputTypes,
                JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.probeOutputTypes = ImmutableList.copyOf(requireNonNull(probeOutputTypes, "probeOutputTypes is null"));
            this.buildOutputTypes = ImmutableList.copyOf(requireNonNull(buildOutputTypes, "buildOutputTypes is null"));
            this.joinBridgeManager = joinBridgeManager;
        }

        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "LookupOuterOperatorFactory is closed");
            Lifespan lifespan = driverContext.getLifespan();
            if (createdLifespans.contains(lifespan)) {
                throw new IllegalStateException("Only one outer operator can be created per Lifespan");
            }
            createdLifespans.add(lifespan);

            LookupSourceFactory sourceFactory = joinBridgeManager.getJoinBridge(lifespan);
            ListenableFuture<OuterPositionIterator> positionsFuture = joinBridgeManager.getOuterPositionsFuture(lifespan);
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupOuterOperator.class.getSimpleName());
            joinBridgeManager.outerOperatorCreated(lifespan);
            LookupOuterOperator operator = new LookupOuterOperator(addOperatorContext, sourceFactory, positionsFuture, probeOutputTypes, buildOutputTypes, () -> joinBridgeManager.outerOperatorClosed(lifespan));
            // Snapshot: Offer this operator to the lookup-source-factory, so that when the corresponding lookup-join operator receives a marker,
            // it can forward the marker to this operator through the lookup-source-factory.
            sourceFactory.setLookupOuterOperator(operator);
            return operator;
        }

        @Override
        public void noMoreOperators(Lifespan lifespan)
        {
            joinBridgeManager.outerOperatorFactoryClosed(lifespan);
        }

        @Override
        public void noMoreOperators()
        {
            if (closed) {
                return;
            }
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException("Source operator factories can not be duplicated");
        }
    }

    private final OperatorContext operatorContext;
    private final LookupSourceFactory lookupSourceFactory;
    private ListenableFuture<OuterPositionIterator> outerPositionsFuture;

    private final List<Type> probeOutputTypes;
    private final Runnable onClose;

    private final PageBuilder pageBuilder;

    private OuterPositionIterator outerPositions;
    private boolean closed;

    private final SingleInputSnapshotState snapshotState;
    // There can be multiple input channels for markers, so keep track of input channels of each marker, to verify completeness
    private final Map<MarkerPage, Set<Integer>> incomingMarkers = new HashMap<>();
    // Markers that should be sent to downstream operators
    private final Queue<Page> outgoingMarkers = new LinkedList<>();
    // When lookup-join is not finished, the outerPositionsFuture is blocked, but we want to allow downstream operators to retrieve markers.
    // Use the following future to unblock when markers are available.
    private SettableFuture<?> blockFuture;

    public LookupOuterOperator(
            OperatorContext operatorContext,
            LookupSourceFactory lookupSourceFactory,
            ListenableFuture<OuterPositionIterator> outerPositionsFuture,
            List<Type> probeOutputTypes,
            List<Type> buildOutputTypes,
            Runnable onClose)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "joinBridge is null");
        this.outerPositionsFuture = requireNonNull(outerPositionsFuture, "outerPositionsFuture is null");

        List<Type> types = ImmutableList.<Type>builder()
                .addAll(requireNonNull(probeOutputTypes, "probeOutputTypes is null"))
                .addAll(requireNonNull(buildOutputTypes, "buildOutputTypes is null"))
                .build();
        this.probeOutputTypes = ImmutableList.copyOf(probeOutputTypes);
        this.pageBuilder = new PageBuilder(types);
        this.onClose = requireNonNull(onClose, "onClose is null");
        if (operatorContext.isSnapshotEnabled()) {
            snapshotState = SingleInputSnapshotState.forOperator(this, operatorContext);
            outerPositionsFuture.addListener(this::unblock, directExecutor());
        }
        else {
            snapshotState = null;
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (snapshotState == null) {
            return outerPositionsFuture;
        }
        if (!outgoingMarkers.isEmpty() || outerPositionsFuture.isDone()) {
            return NOT_BLOCKED;
        }
        // This future is updated by "unblock()" below, which is called
        // when either outerPositionsFuture is done, or a marker becomes available
        blockFuture = SettableFuture.create();
        return blockFuture;
    }

    private synchronized void unblock()
    {
        if (blockFuture != null) {
            blockFuture.set(null);
            blockFuture = null;
        }
    }

    @Override
    public void finish()
    {
        // this is a source operator, so we can just terminate the output now
        close();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && !outgoingMarkers.isEmpty()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return closed;
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page getOutput()
    {
        if (snapshotState != null) {
            if (!outgoingMarkers.isEmpty()) {
                return outgoingMarkers.poll();
            }

            if (closed) {
                return null;
            }
        }

        if (outerPositions == null) {
            if (!outerPositionsFuture.isDone()) {
                return null;
            }

            outerPositions = getDone(outerPositionsFuture);
            if (outerPositions == null) {
                close();
                return null;
            }
        }

        boolean outputPositionsFinished = false;
        while (!pageBuilder.isFull()) {
            // write build columns
            outputPositionsFinished = !outerPositions.appendToNext(pageBuilder, probeOutputTypes.size());
            if (outputPositionsFinished) {
                break;
            }
            pageBuilder.declarePosition();

            // write nulls into probe columns
            // todo use RLE blocks
            for (int probeChannel = 0; probeChannel < probeOutputTypes.size(); probeChannel++) {
                pageBuilder.getBlockBuilder(probeChannel).appendNull();
            }
        }

        // only flush full pages unless we are done
        Page page = null;
        if (pageBuilder.isFull() || (outputPositionsFinished && !pageBuilder.isEmpty())) {
            page = pageBuilder.build();
            pageBuilder.reset();
        }

        if (outputPositionsFinished) {
            outerPositionsFuture = outerPositions.getNextBatch();
            outerPositions = null;
            if (!outerPositionsFuture.isDone()) {
                return page;
            }

            outerPositions = getDone(outerPositionsFuture);
            if (outerPositions == null) {
                close();
                return page;
            }
        }
        return page;
    }

    @Override
    public Page pollMarker()
    {
        return outgoingMarkers.poll();
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        if (snapshotState != null) {
            snapshotState.close();
        }
        closed = true;
        pageBuilder.reset();
        onClose.run();
    }

    public synchronized void processMarkerForTableScan(MarkerPage marker)
    {
        if (closed) {
            return;
        }
        // See Gitee issue Checkpoint - handle LookupOuterOperator pipelines
        // https://gitee.com/open_lookeng/dashboard/issues?id=I2LMIW
        // This is for outer join with table-scan pipelines. Process marker without passing it on.
        snapshotState.processPage(marker);
    }

    public synchronized void processMarkerForExchange(MarkerPage marker, int totalDrivers, int driverId)
    {
        if (closed) {
            return;
        }

        LOG.debug("Received marker '%s' from source driver '%d' to target '%s'", marker.toString(), driverId, operatorContext.getUniqueId());
        // See Gitee issue Checkpoint - handle LookupOuterOperator pipelines
        // https://gitee.com/open_lookeng/dashboard/issues?id=I2LMIW
        // This is for outer join with non-table-scan pipelines.
        Set<Integer> drivers = incomingMarkers.get(marker);
        boolean toSend = false;
        if (drivers == null) {
            drivers = new HashSet<>();
            incomingMarkers.put(marker, drivers);
            if (marker.isResuming()) {
                // For resume, send out marker when the first is received
                toSend = true;
            }
        }
        if (!drivers.add(driverId)) {
            String message = String.format(Locale.ENGLISH, "Received duplicate marker '%s' from source driver '%d' to target '%s'", marker.toString(), driverId, operatorContext.getUniqueId());
            LOG.error(message);
        }
        if (drivers.size() == totalDrivers) {
            incomingMarkers.remove(marker);
            if (!marker.isResuming()) {
                // For snapshot,  Wait to receive marker from all drivers.
                // This may be a bit later, and capture more positions than it should, but this does no harm.
                // The captured positions is only used in the end to determine which rows to include.
                toSend = true;
            }
        }
        if (toSend) {
            snapshotState.processPage(marker);
            outgoingMarkers.add(marker);
            unblock();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        checkState(!closed, "Don't expect marker after operator is closed");
        // While this operator can still receive markers, the join pipeline has not finished,
        // and no data page can be received by this outer-join pipeline, so no need to capture
        // anything other than which join positions have been visited by the probe side.
        return lookupSourceFactory.captureJoinPositions();
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        lookupSourceFactory.restoreJoinPositions(state);
    }
}
