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
package io.prestosql.operator.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.PipelineExecutionStrategy;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.MultiInputRestorable;
import io.prestosql.snapshot.MultiInputSnapshotState;
import io.prestosql.snapshot.SnapshotStateId;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PartitioningHandle;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.operator.exchange.LocalExchangeSink.finishedLocalExchangeSink;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static java.util.Objects.requireNonNull;

@ThreadSafe
@RestorableConfig(uncapturedFields = {"sources", "memoryManager", "allSourcesFinished", "noMoreSinkFactories",
        "allSinkFactories", "openSinkFactories", "sinks", "nextSourceIndex", "snapshotState", "allInputChannels"})
public class LocalExchange
        implements MultiInputRestorable
{
    private static final Logger LOG = Logger.get(LocalExchange.class);

    private final Supplier<LocalExchanger> exchangerSupplier;

    private final List<LocalExchangeSource> sources;

    private final LocalExchangeMemoryManager memoryManager;

    @GuardedBy("this")
    private boolean allSourcesFinished;

    @GuardedBy("this")
    private boolean noMoreSinkFactories;

    @GuardedBy("this")
    private final List<LocalExchangeSinkFactory> allSinkFactories;

    @GuardedBy("this")
    private final Set<LocalExchangeSinkFactory> openSinkFactories = new HashSet<>();

    @GuardedBy("this")
    private final Set<LocalExchangeSink> sinks = new HashSet<>();

    @GuardedBy("this")
    private int nextSourceIndex;

    // Snapshot: distinguish between LocalExchangeSourceOperator and LocalMergeSourceOperator.
    //
    // For exchange, each source operator works with a single local-source.
    // Markers received from each local-sink are broadcasted to all local-sources, so source operator's local-source
    // receives markers from all local-sinks. Marker handling can happen entirely inside the source operator.
    //
    // Merge is different, in that n local-sink connects to n local-source using pass-through exchanger,
    // and a single merge operator reads from all n local-sources.
    // - Markers can't be broadcasted to local-sources, otherwise merge operator receives n*n markers
    // - Merge operator only reads from a local-source if it's its "turn", so markers from certain sources can be blocked,
    //   preventing it from being received by the operator, which in turn preventing the snapshot from being complete
    // This requires special handling in local-exchange for merge operator, to not broadcast marker,
    // and to process markers in local-exchange, instead of in merge operator.
    private final boolean isForMerge;
    // Instead of processing markers in merge operator, process them here. The snapshot state is made available to the merge operator,
    // so its markers can be sent to downstream operators. The does not affect total expected number of components that capture their states.
    // There is 1 less for the merge operator, but 1 more for local-exchange, so total stays the same.
    // The local-exchange component is identified using the plan-node-id, to ensure uniqueness within the task, and consistency across capture/restore.
    private final MultiInputSnapshotState snapshotState;
    // Total number of local-sink/local-source (they have the same number). Used to check if all sinks have been created,
    // which means we have the complete list of input channels for this component.
    private final int bufferCount;
    private Optional<Set<String>> allInputChannels = Optional.empty();

    public LocalExchange(
            int sinkFactoryCount,
            int bufferCount,
            PartitioningHandle partitioning,
            List<? extends Type> types,
            List<Integer> partitionChannels,
            Optional<Integer> partitionHashChannel,
            DataSize maxBufferedBytes,
            boolean isForMerge,
            TaskContext taskContext,
            String id,
            boolean snapshotEnabled,
            AggregationNode.AggregationType aggregationType)
    {
        this.bufferCount = bufferCount;
        this.isForMerge = isForMerge;
        this.snapshotState = isForMerge && snapshotEnabled ? MultiInputSnapshotState.forTaskComponent(this, taskContext, snapshotId -> SnapshotStateId.forTaskComponent(snapshotId, taskContext, id)) : null;
        this.allSinkFactories = Stream.generate(() -> new LocalExchangeSinkFactory(LocalExchange.this))
                .limit(sinkFactoryCount)
                .collect(toImmutableList());
        openSinkFactories.addAll(allSinkFactories);
        noMoreSinkFactories();

        ImmutableList.Builder<LocalExchangeSource> localExchangeSourceBuilder = ImmutableList.builder();
        for (int i = 0; i < bufferCount; i++) {
            // Snapshot state is given to all local-sources, so they can process markers when they are received.
            localExchangeSourceBuilder.add(new LocalExchangeSource(source -> checkAllSourcesFinished(), snapshotState));
        }
        this.sources = localExchangeSourceBuilder.build();

        List<BiConsumer<PageReference, String>> buffers = this.sources.stream()
                .map(buffer -> (BiConsumer<PageReference, String>) buffer::addPage)
                .collect(toImmutableList());

        this.memoryManager = new LocalExchangeMemoryManager(maxBufferedBytes.toBytes());
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            exchangerSupplier = () -> new BroadcastExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            exchangerSupplier = () -> new RandomExchanger(buffers, memoryManager);
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            if (!aggregationType.equals(AggregationNode.AggregationType.SORT_BASED)) {
                exchangerSupplier = () -> new PartitioningExchanger(buffers, memoryManager, types, partitionChannels, partitionHashChannel);
            }
            else {
                exchangerSupplier = () -> new SortBasedAggregationPartitioningExchanger(buffers, memoryManager, types,
                        partitionChannels, partitionHashChannel, taskContext.getSession());
            }
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            Iterator<LocalExchangeSource> sourceIterator = this.sources.iterator();
            exchangerSupplier = () -> {
                checkState(sourceIterator.hasNext(), "no more sources");
                return new PassthroughExchanger(sourceIterator.next(), maxBufferedBytes.toBytes() / bufferCount, memoryManager::updateMemoryUsage, isForMerge);
            };
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
    }

    public int getBufferCount()
    {
        return sources.size();
    }

    public long getBufferedBytes()
    {
        return memoryManager.getBufferedBytes();
    }

    public synchronized LocalExchangeSinkFactory createSinkFactory()
    {
        checkState(!noMoreSinkFactories, "No more sink factories already set");
        LocalExchangeSinkFactory newFactory = new LocalExchangeSinkFactory(this);
        openSinkFactories.add(newFactory);
        return newFactory;
    }

    public synchronized LocalExchangeSinkFactory getSinkFactory(LocalExchangeSinkFactoryId id)
    {
        return allSinkFactories.get(id.id);
    }

    public synchronized LocalExchangeSource getNextSource()
    {
        checkState(nextSourceIndex < sources.size(), "All operators already created");
        LocalExchangeSource result = sources.get(nextSourceIndex);
        nextSourceIndex++;
        return result;
    }

    @VisibleForTesting
    LocalExchangeSource getSource(int partitionIndex)
    {
        return sources.get(partitionIndex);
    }

    private void checkAllSourcesFinished()
    {
        checkNotHoldsLock(this);

        if (!sources.stream().allMatch(LocalExchangeSource::isFinished)) {
            return;
        }

        // all sources are finished, so finish the sinks
        ImmutableList<LocalExchangeSink> openSinks;
        synchronized (this) {
            allSourcesFinished = true;

            openSinks = ImmutableList.copyOf(sinks);
            sinks.clear();
        }

        // since all sources are finished there is no reason to allow new pages to be added
        // this can happen with a limit query
        openSinks.forEach(LocalExchangeSink::finish);
        checkAllSinksComplete();
    }

    private LocalExchangeSink createSink(LocalExchangeSinkFactory factory, String sinkId)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            checkState(openSinkFactories.contains(factory), "Factory is already closed");

            if (allSourcesFinished) {
                // all sources have completed so return a sink that is already finished
                return finishedLocalExchangeSink(isForMerge);
            }

            // Note: exchanger can be stateful so create a new one for each sink
            LocalExchanger exchanger = exchangerSupplier.get();
            if (isForMerge) {
                // Local merge always uses pass-through exchanger, so each source only needs 1 input channel
                ((PassthroughExchanger) exchanger).getLocalExchangeSource().addInputChannel(sinkId);
            }
            else {
                // Inform all LocalExchangeSourceOperator instances about this sink, so they all have a complete list of all sinks
                for (LocalExchangeSource source : sources) {
                    source.addInputChannel(sinkId);
                }
            }

            LocalExchangeSink sink = new LocalExchangeSink(this, exchanger, this::sinkFinished, isForMerge);
            sinks.add(sink);
            return sink;
        }
    }

    private void sinkFinished(LocalExchangeSink sink)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            sinks.remove(sink);
        }
        checkAllSinksComplete();
    }

    private void noMoreSinkFactories()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            noMoreSinkFactories = true;
        }
        checkAllSinksComplete();
    }

    private void sinkFactoryClosed(LocalExchangeSinkFactory sinkFactory)
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            openSinkFactories.remove(sinkFactory);
        }
        checkAllSinksComplete();
    }

    private void checkAllSinksComplete()
    {
        checkNotHoldsLock(this);

        synchronized (this) {
            if (!noMoreSinkFactories || !openSinkFactories.isEmpty() || !sinks.isEmpty()) {
                return;
            }
        }

        sources.forEach(LocalExchangeSource::finish);
    }

    private static void checkNotHoldsLock(Object lock)
    {
        checkState(!Thread.holdsLock(lock), "Can not execute this method while holding a lock");
    }

    @Override
    public Optional<Set<String>> getInputChannels()
    {
        if (allInputChannels.isPresent()) {
            return allInputChannels;
        }

        if (sinks.size() != bufferCount) {
            return Optional.empty();
        }

        checkState(isForMerge);

        Set<String> inputChannels = new HashSet<>();
        for (LocalExchangeSource source : sources) {
            Set<String> channels = source.getAllInputChannels();
            if (channels.size() != 1) {
                checkState(channels.size() == 0, "Local exchange source for LocalMergeOperator should only have 1 input channel");
                return Optional.empty();
            }
            inputChannels.addAll(channels);
        }
        allInputChannels = Optional.of(inputChannels);
        return allInputChannels;
    }

    public MultiInputSnapshotState getSnapshotState()
    {
        return snapshotState;
    }

    @ThreadSafe
    public static class LocalExchangeFactory
    {
        private final PartitioningHandle partitioning;
        private final List<Type> types;
        private final List<Integer> partitionChannels;
        private final Optional<Integer> partitionHashChannel;
        private final PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy;
        private final DataSize maxBufferedBytes;
        private final int bufferCount;
        private final boolean isForMerge;
        private final AggregationNode.AggregationType aggregationType;

        @GuardedBy("this")
        private boolean noMoreSinkFactories;
        // The number of total sink factories are tracked at planning time
        // so that the exact number of sink factory is known by the time execution starts.
        @GuardedBy("this")
        private int numSinkFactories;

        @GuardedBy("this")
        private final Map<Lifespan, LocalExchange> localExchangeMap = new HashMap<>();
        @GuardedBy("this")
        private final List<LocalExchangeSinkFactoryId> closedSinkFactories = new ArrayList<>();

        public LocalExchangeFactory(
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel,
                PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy,
                DataSize maxBufferedBytes)
        {
            this(partitioning, defaultConcurrency, types, partitionChannels, partitionHashChannel, exchangeSourcePipelineExecutionStrategy, maxBufferedBytes, false, AggregationNode.AggregationType.HASH);
        }

        public LocalExchangeFactory(
                PartitioningHandle partitioning,
                int defaultConcurrency,
                List<Type> types,
                List<Integer> partitionChannels,
                Optional<Integer> partitionHashChannel,
                PipelineExecutionStrategy exchangeSourcePipelineExecutionStrategy,
                DataSize maxBufferedBytes,
                boolean isForMerge,
                AggregationNode.AggregationType aggregationType)
        {
            this.partitioning = requireNonNull(partitioning, "partitioning is null");
            this.types = requireNonNull(types, "types is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitioningChannels is null");
            this.partitionHashChannel = requireNonNull(partitionHashChannel, "partitionHashChannel is null");
            this.exchangeSourcePipelineExecutionStrategy = requireNonNull(exchangeSourcePipelineExecutionStrategy, "exchangeSourcePipelineExecutionStrategy is null");
            this.maxBufferedBytes = requireNonNull(maxBufferedBytes, "maxBufferedBytes is null");

            this.bufferCount = computeBufferCount(partitioning, defaultConcurrency, partitionChannels);
            this.isForMerge = isForMerge;
            this.aggregationType = aggregationType;
        }

        public synchronized LocalExchangeSinkFactoryId newSinkFactoryId()
        {
            checkState(!noMoreSinkFactories);
            LocalExchangeSinkFactoryId result = new LocalExchangeSinkFactoryId(numSinkFactories);
            numSinkFactories++;
            return result;
        }

        public synchronized void noMoreSinkFactories()
        {
            noMoreSinkFactories = true;
        }

        public int getBufferCount()
        {
            return bufferCount;
        }

        public LocalExchange getLocalExchange(Lifespan lifespan)
        {
            return getLocalExchange(lifespan, null, null, false);
        }

        public LocalExchange getLocalExchange(Lifespan lifespan, TaskContext taskContext)
        {
            return getLocalExchange(lifespan, taskContext, null, false);
        }

        public synchronized LocalExchange getLocalExchange(Lifespan lifespan, TaskContext taskContext, String id, boolean snapshotEnabled)
        {
            if (exchangeSourcePipelineExecutionStrategy == UNGROUPED_EXECUTION) {
                checkArgument(lifespan.isTaskWide(), "LocalExchangeFactory is declared as UNGROUPED_EXECUTION. Driver-group exchange cannot be created.");
            }
            else {
                checkArgument(!lifespan.isTaskWide(), "LocalExchangeFactory is declared as GROUPED_EXECUTION. Task-wide exchange cannot be created.");
            }
            return localExchangeMap.computeIfAbsent(lifespan, ignored -> {
                checkState(noMoreSinkFactories);
                LocalExchange localExchange =
                        new LocalExchange(numSinkFactories, bufferCount, partitioning, types, partitionChannels, partitionHashChannel, maxBufferedBytes, isForMerge, taskContext, id, snapshotEnabled,
                                aggregationType);
                for (LocalExchangeSinkFactoryId closedSinkFactoryId : closedSinkFactories) {
                    localExchange.getSinkFactory(closedSinkFactoryId).close();
                }
                return localExchange;
            });
        }

        public synchronized void closeSinks(LocalExchangeSinkFactoryId sinkFactoryId)
        {
            closedSinkFactories.add(sinkFactoryId);
            for (LocalExchange localExchange : localExchangeMap.values()) {
                localExchange.getSinkFactory(sinkFactoryId).close();
            }
        }
    }

    private static int computeBufferCount(PartitioningHandle partitioning, int defaultConcurrency, List<Integer> partitionChannels)
    {
        int computeBufferCount;
        if (partitioning.equals(SINGLE_DISTRIBUTION)) {
            computeBufferCount = 1;
            checkArgument(partitionChannels.isEmpty(), "Gather exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_BROADCAST_DISTRIBUTION)) {
            computeBufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Broadcast exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_ARBITRARY_DISTRIBUTION)) {
            computeBufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Arbitrary exchange must not have partition channels");
        }
        else if (partitioning.equals(FIXED_HASH_DISTRIBUTION)) {
            computeBufferCount = defaultConcurrency;
            checkArgument(!partitionChannels.isEmpty(), "Partitioned exchange must have partition channels");
        }
        else if (partitioning.equals(FIXED_PASSTHROUGH_DISTRIBUTION)) {
            computeBufferCount = defaultConcurrency;
            checkArgument(partitionChannels.isEmpty(), "Passthrough exchange must not have partition channels");
        }
        else {
            throw new IllegalArgumentException("Unsupported local exchange partitioning " + partitioning);
        }
        return computeBufferCount;
    }

    public static class LocalExchangeSinkFactoryId
    {
        private final int id;

        public LocalExchangeSinkFactoryId(int id)
        {
            this.id = id;
        }
    }

    // Sink factory is entirely a pass thought to LocalExchange.
    // This class only exists as a separate entity to deal with the complex lifecycle caused
    // by operator factories (e.g., duplicate and noMoreSinkFactories).
    @ThreadSafe
    public static class LocalExchangeSinkFactory
            implements Closeable
    {
        private final LocalExchange exchange;

        private LocalExchangeSinkFactory(LocalExchange exchange)
        {
            this.exchange = requireNonNull(exchange, "exchange is null");
        }

        public LocalExchangeSink createSink(String sinkId)
        {
            return exchange.createSink(this, sinkId);
        }

        public LocalExchangeSinkFactory duplicate()
        {
            return exchange.createSinkFactory();
        }

        @Override
        public void close()
        {
            exchange.sinkFactoryClosed(this);
        }

        public void noMoreSinkFactories()
        {
            exchange.noMoreSinkFactories();
        }
    }

    public void broadcastMarker(MarkerPage page, String origin)
    {
        checkState(!isForMerge);

        memoryManager.updateMemoryUsage(page.getRetainedSizeInBytes() * sources.size());
        for (LocalExchangeSource source : sources) {
            PageReference pageReference = new PageReference(page, 1, () -> memoryManager.updateMemoryUsage(-page.getRetainedSizeInBytes()));
            source.addPage(pageReference, origin);
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        // No state needs to be captured
        checkState(isForMerge);
        return 0;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
    }
}
