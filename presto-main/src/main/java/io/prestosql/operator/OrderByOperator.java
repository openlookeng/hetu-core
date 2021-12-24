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
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.snapshot.Spillable;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.Spiller;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.OrderingCompiler;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.transform;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.checkSuccess;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.util.MergeSortedPages.mergeSortedPages;
import static java.util.Objects.requireNonNull;

// Snapshot: Most of these fields are immutable objects, excpet for:
// - spillInProgress: must be "done" when markers are received (see needsInput)
// - finishMemoryRevoke: must be empty, because new input (including markers) can't be added until finishMemoryRevoke is called
// - sortedPages: only set after "finish" is called
@RestorableConfig(uncapturedFields = {"sortChannels", "sortOrder", "outputChannels", "sourceTypes", "spillerFactory",
        "orderingCompiler", "spillInProgress", "finishMemoryRevoke", "sortedPages", "state", "snapshotState"})
public class OrderByOperator
        implements Operator, Spillable
{
    public static class OrderByOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final int expectedPositions;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final PagesIndex.Factory pagesIndexFactory;
        private final boolean spillEnabled;
        private final Optional<SpillerFactory> spillerFactory;
        private final OrderingCompiler orderingCompiler;

        private boolean closed;

        public OrderByOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                int expectedPositions,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                Optional<SpillerFactory> spillerFactory,
                OrderingCompiler orderingCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.expectedPositions = expectedPositions;
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));

            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
            checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOperator.class.getSimpleName());
            return new OrderByOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new OrderByOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    expectedPositions,
                    sortChannels,
                    sortOrder,
                    pagesIndexFactory,
                    spillEnabled,
                    spillerFactory,
                    orderingCompiler);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private static final Logger LOG = Logger.get(OrderByOperator.class);
    private final OperatorContext operatorContext;
    private final List<Integer> sortChannels;
    private final List<SortOrder> sortOrder;
    private final int[] outputChannels;
    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final PagesIndex pageIndex;

    private final List<Type> sourceTypes;

    private final boolean spillEnabled;
    private final Optional<SpillerFactory> spillerFactory;
    private final OrderingCompiler orderingCompiler;

    private Optional<Spiller> spiller = Optional.empty();
    private ListenableFuture<?> spillInProgress = immediateFuture(null);
    private Runnable finishMemoryRevoke = () -> {};

    private Iterator<Optional<Page>> sortedPages;

    private State state = State.NEEDS_INPUT;

    private final SingleInputSnapshotState snapshotState;

    public OrderByOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            int expectedPositions,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            Optional<SpillerFactory> spillerFactory,
            OrderingCompiler orderingCompiler)
    {
        requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");

        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
        this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        this.pageIndex = pagesIndexFactory.newPagesIndex(sourceTypes, expectedPositions);
        this.spillEnabled = spillEnabled;
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.orderingCompiler = requireNonNull(orderingCompiler, "orderingCompiler is null");
        checkArgument(!spillEnabled || spillerFactory.isPresent(), "Spiller Factory is not present when spill is enabled");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (!spillInProgress.isDone()) {
            return;
        }
        checkSuccess(spillInProgress, "spilling failed");

        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // Convert revocable memory to user memory as sortedPages holds on to memory so we no longer can revoke.
            if (revocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = revocableMemoryContext.getBytes();
                revocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we don't
                    // have a proper way to atomically convert memory reservations
                    revocableMemoryContext.setBytes(currentRevocableBytes);
                    // spill since revocable memory could not be converted to user memory immediately
                    // TODO: this should be asynchronous
                    getFutureValue(spillToDisk());
                    finishMemoryRevoke.run();
                }
            }

            pageIndex.sort(sortChannels, sortOrder);
            Iterator<Page> sortedPagesIndex = pageIndex.getSortedPages();

            List<WorkProcessor<Page>> spilledPages = getSpilledPages();
            if (spilledPages.isEmpty()) {
                sortedPages = transform(sortedPagesIndex, Optional::of);
            }
            else {
                sortedPages = mergeSpilledAndMemoryPages(spilledPages, sortedPagesIndex).yieldingIterator();
            }
        }
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkSuccess(spillInProgress, "spilling failed");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        pageIndex.addPage(page);
        updateMemoryUsage();
    }

    @Override
    public Page getOutput()
    {
        checkSuccess(spillInProgress, "spilling failed");

        if (snapshotState != null) {
            Page marker = snapshotState.nextMarker();
            if (marker != null) {
                return marker;
            }
        }

        if (state != State.HAS_OUTPUT) {
            return null;
        }

        verify(sortedPages != null, "sortedPages is null");
        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (!next.isPresent()) {
            return null;
        }
        Page nextPage = next.get();
        Block[] blocks = new Block[outputChannels.length];
        for (int i = 0; i < outputChannels.length; i++) {
            blocks[i] = nextPage.getBlock(outputChannels[i]);
        }
        return new Page(nextPage.getPositionCount(), blocks);
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        verify(state == State.NEEDS_INPUT || revocableMemoryContext.getBytes() == 0, "Cannot spill in state: %s", state);
        return spillToDisk();
    }

    private ListenableFuture<?> spillToDisk()
    {
        checkSuccess(spillInProgress, "spilling failed");

        if (revocableMemoryContext.getBytes() == 0) {
            verify(pageIndex.getPositionCount() == 0 || state == State.HAS_OUTPUT);
            finishMemoryRevoke = () -> {};
            return immediateFuture(null);
        }

        // TODO try pageIndex.compact(); before spilling, as in com.facebook.presto.operator.HashBuilderOperator.startMemoryRevoke

        if (!spiller.isPresent()) {
            spiller = Optional.of(spillerFactory.get().create(
                    sourceTypes,
                    operatorContext.getSpillContext(),
                    operatorContext.newAggregateSystemMemoryContext()));
        }

        pageIndex.sort(sortChannels, sortOrder);
        spillInProgress = spiller.get().spill(pageIndex.getSortedPages());
        LOG.debug("spilling to disk initiated by Order by operator");
        finishMemoryRevoke = () -> {
            pageIndex.clear();
            updateMemoryUsage();
        };

        return spillInProgress;
    }

    @Override
    public void finishMemoryRevoke()
    {
        finishMemoryRevoke.run();
        finishMemoryRevoke = () -> {};
    }

    private List<WorkProcessor<Page>> getSpilledPages()
    {
        if (!spiller.isPresent()) {
            return ImmutableList.of();
        }

        return spiller.get().getSpills().stream()
                .map(WorkProcessor::fromIterator)
                .collect(toImmutableList());
    }

    private WorkProcessor<Page> mergeSpilledAndMemoryPages(List<WorkProcessor<Page>> spilledPages, Iterator<Page> sortedPagesIndex)
    {
        List<WorkProcessor<Page>> sortedStreams = ImmutableList.<WorkProcessor<Page>>builder()
                .addAll(spilledPages)
                .add(WorkProcessor.fromIterator(sortedPagesIndex))
                .build();

        return mergeSortedPages(
                sortedStreams,
                orderingCompiler.compilePageWithPositionComparator(sourceTypes, sortChannels, sortOrder),
                sourceTypes,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
    }

    private void updateMemoryUsage()
    {
        if (spillEnabled && state == State.NEEDS_INPUT) {
            if (pageIndex.getPositionCount() == 0) {
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
                revocableMemoryContext.setBytes(0L);
            }
            else {
                localUserMemoryContext.setBytes(0);
                revocableMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
        else {
            revocableMemoryContext.setBytes(0);
            if (!localUserMemoryContext.trySetBytes(pageIndex.getEstimatedSize().toBytes())) {
                pageIndex.compact();
                localUserMemoryContext.setBytes(pageIndex.getEstimatedSize().toBytes());
            }
        }
    }

    @Override
    public void close()
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
        pageIndex.clear();
        sortedPages = null;
        spiller.ifPresent(Spiller::close);
    }

    @Override
    public boolean isSpilled()
    {
        return spiller.isPresent();
    }

    @Override
    public List<Path> getSpilledFilePaths()
    {
        if (isSpilled()) {
            return spiller.get().getSpilledFilePaths();
        }
        return ImmutableList.of();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        OrderByOperatorState myState = new OrderByOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.revocableMemoryContext = revocableMemoryContext.getBytes();
        myState.localUserMemoryContext = localUserMemoryContext.getBytes();
        myState.pageIndex = pageIndex.capture(serdeProvider);

        // Capture spill related fields
        if (spiller.isPresent()) {
            myState.spiller = spiller.get().capture(serdeProvider);
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        OrderByOperatorState myState = (OrderByOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.revocableMemoryContext.setBytes(myState.revocableMemoryContext);
        this.localUserMemoryContext.setBytes(myState.localUserMemoryContext);
        this.pageIndex.restore(myState.pageIndex, serdeProvider);

        // Restore spill related fields
        if (myState.spiller != null) {
            if (!spiller.isPresent()) {
                spiller = Optional.of(spillerFactory.get().create(
                        sourceTypes,
                        operatorContext.getSpillContext(),
                        operatorContext.newAggregateSystemMemoryContext()));
            }
            this.spiller.get().restore(myState.spiller, serdeProvider);
        }
    }

    @Override
    public boolean supportsConsolidatedWrites()
    {
        return false;
    }

    private static class OrderByOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long revocableMemoryContext;
        private long localUserMemoryContext;

        private Object pageIndex;

        private Object spiller;
    }
}
