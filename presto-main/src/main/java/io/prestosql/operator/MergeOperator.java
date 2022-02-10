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

import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.metadata.Split;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.split.RemoteSplit;
import io.prestosql.sql.gen.OrderingCompiler;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.util.MergeSortedPages.mergeSortedPages;
import static io.prestosql.util.MoreLists.mappedCopy;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"sourceId", "exchangeClientSupplier", "comparator", "outputChannels", "outputTypes", "blockedOnSplits", "pageProducers", "closer", "closed",
        "clients", "snapshotState", "mergedPages", "inputChannels"})
public class MergeOperator
        implements SourceOperator, Closeable, MultiInputRestorable
{
    public static class MergeOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId sourceId;
        private final ExchangeClientSupplier exchangeClientSupplier;
        private final List<Type> types;
        private final List<Integer> outputChannels;
        private final List<Type> outputTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final OrderingCompiler orderingCompiler;
        private boolean closed;

        public MergeOperatorFactory(
                int operatorId,
                PlanNodeId sourceId,
                ExchangeClientSupplier exchangeClientSupplier,
                OrderingCompiler orderingCompiler,
                List<Type> types,
                List<Integer> outputChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
            this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
            this.types = requireNonNull(types, "types is null");
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.outputTypes = mappedCopy(outputChannels, types::get);
            this.sortChannels = requireNonNull(sortChannels, "sortChannels is null");
            this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
            this.orderingCompiler = requireNonNull(orderingCompiler, "mergeSortComparatorFactory is null");
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return sourceId;
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, sourceId, MergeOperator.class.getSimpleName());

            return new MergeOperator(
                    addOperatorContext,
                    sourceId,
                    exchangeClientSupplier,
                    orderingCompiler.compilePageWithPositionComparator(types, sortChannels, sortOrder),
                    outputChannels,
                    outputTypes);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId sourceId;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final PageWithPositionComparator comparator;
    private final List<Integer> outputChannels;
    private final List<Type> outputTypes;

    private final SettableFuture<Void> blockedOnSplits = SettableFuture.create();

    private final List<WorkProcessor<Page>> pageProducers = new ArrayList<>();
    private final Closer closer = Closer.create();

    private WorkProcessor<Page> mergedPages;
    private boolean closed;

    private final String id;
    private final List<ExchangeClient> clients = new ArrayList<>();
    private final MultiInputSnapshotState snapshotState;
    private Optional<Set<String>> inputChannels = Optional.empty();

    public MergeOperator(
            OperatorContext operatorContext,
            PlanNodeId sourceId,
            ExchangeClientSupplier exchangeClientSupplier,
            PageWithPositionComparator comparator,
            List<Integer> outputChannels,
            List<Type> outputTypes)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.sourceId = requireNonNull(sourceId, "sourceId is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.comparator = requireNonNull(comparator, "comparator is null");
        this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
        this.outputTypes = requireNonNull(outputTypes, "outputTypes is null");
        this.id = operatorContext.getUniqueId();
        this.snapshotState = operatorContext.isSnapshotEnabled() ? MultiInputSnapshotState.forOperator(this, operatorContext) : null;
    }

    @Override
    public PlanNodeId getSourceId()
    {
        return sourceId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        checkArgument(split.getConnectorSplit() instanceof RemoteSplit, "split is not a remote split");
        checkState(!blockedOnSplits.isDone(), "noMoreSplits has been called already");

        URI location = ((RemoteSplit) split.getConnectorSplit()).getLocation();
        String instanceId = ((RemoteSplit) split.getConnectorSplit()).getInstanceId();
        ExchangeClient exchangeClient = closer.register(exchangeClientSupplier.get(operatorContext.localSystemMemoryContext()));
        if (operatorContext.isSnapshotEnabled()) {
            exchangeClient.setSnapshotEnabled(operatorContext.getDriverContext().getPipelineContext().getTaskContext().getSnapshotManager().getQuerySnapshotManager());
            exchangeClient.setSnapshotState(snapshotState);
        }
        exchangeClient.addTarget(id);
        exchangeClient.noMoreTargets();
        exchangeClient.addLocation(new TaskLocation(location, instanceId));
        exchangeClient.noMoreLocations();
        clients.add(exchangeClient);
        pageProducers.add(exchangeClient.pages(id)
                .map(new WorkProcessor.RestorableFunction<SerializedPage, Page>()
                {
                    @Override
                    public Page apply(SerializedPage serializedPage)
                    {
                        operatorContext.recordNetworkInput(serializedPage.getSizeInBytes(), serializedPage.getPositionCount());
                        return operatorContext.getDriverContext().getSerde().deserialize(serializedPage);
                    }

                    @Override
                    public Object captureResult(Page result, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (result != null) {
                            return ((PagesSerde) serdeProvider).serialize(result).capture(serdeProvider);
                        }
                        return null;
                    }

                    @Override
                    public Page restoreResult(Object resultState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (resultState != null) {
                            SerializedPage serializedPage = SerializedPage.restoreSerializedPage(resultState);
                            return ((PagesSerde) serdeProvider).deserialize(serializedPage);
                        }
                        return null;
                    }

                    @Override
                    public Object captureInput(SerializedPage input, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (input != null) {
                            return input.capture(serdeProvider);
                        }
                        return null;
                    }

                    @Override
                    public SerializedPage restoreInput(Object inputState, BlockEncodingSerdeProvider serdeProvider)
                    {
                        if (inputState != null) {
                            return SerializedPage.restoreSerializedPage(inputState);
                        }
                        return null;
                    }

                    @Override
                    public Object capture(BlockEncodingSerdeProvider serdeProvider)
                    {
                        return 0;
                    }

                    @Override
                    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
                    {
                    }
                }));

        if (snapshotState != null) {
            // When inputChannels is not empty, then we should have received all locations
            checkState(!inputChannels.isPresent());
        }

        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        mergedPages = mergeSortedPages(
                pageProducers,
                comparator,
                outputChannels,
                outputTypes,
                (pageBuilder, pageWithPosition) -> pageBuilder.isFull(),
                false,
                operatorContext.aggregateUserMemoryContext(),
                operatorContext.getDriverContext().getYieldSignal());
        blockedOnSplits.set(null);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasPendingPages()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return closed || (mergedPages != null && mergedPages.isFinished());
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (!blockedOnSplits.isDone()) {
            return blockedOnSplits;
        }

        if (mergedPages.isBlocked()) {
            return mergedPages.getBlockedFuture();
        }

        return NOT_BLOCKED;
    }

    @Override
    public Page getOutput()
    {
        if (snapshotState != null) {
            Page marker = snapshotState.nextMarker();
            if (marker != null) {
                return marker;
            }
        }

        if (closed || mergedPages == null || !mergedPages.process() || mergedPages.isFinished()) {
            return null;
        }

        Page page = mergedPages.getResult();
        operatorContext.recordProcessedInput(page.getSizeInBytes(), page.getPositionCount());
        return page;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public Optional<Set<String>> getInputChannels()
    {
        if (inputChannels.isPresent()) {
            return inputChannels;
        }

        if (!blockedOnSplits.isDone()) {
            // Need to wait for all splits/locations/channels to be added
            return Optional.empty();
        }

        Set<String> channels = new HashSet<>();

        for (ExchangeClient client : clients) {
            channels.addAll(client.getAllClients());
        }
        // All channels have been added (i.e. blockedOnSplits is done) or have received expected number of input channels.
        // Because markers are sent to all potential table-scan tasks, expectedChannelCount should be the same as the final count,
        // so the input channel list won't change again, and can be cached.
        inputChannels = Optional.of(channels);
        return inputChannels;
    }

    @Override
    public void close()
    {
        try {
            closer.close();
            closed = true;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        return operatorContext.capture(serdeProvider);
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        operatorContext.restore(state, serdeProvider);
    }
}
