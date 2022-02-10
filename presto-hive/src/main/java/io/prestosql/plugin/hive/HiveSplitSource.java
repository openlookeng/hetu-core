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
package io.prestosql.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.plugin.hive.util.AsyncQueue;
import io.prestosql.plugin.hive.util.ThrottledAsyncQueue;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.transformValues;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.failedFuture;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.plugin.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getMaxSplitSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.isDynamicFilteringSplitFilteringEnabled;
import static io.prestosql.plugin.hive.HiveSplitSource.StateKind.CLOSED;
import static io.prestosql.plugin.hive.HiveSplitSource.StateKind.FAILED;
import static io.prestosql.plugin.hive.HiveSplitSource.StateKind.INITIAL;
import static io.prestosql.plugin.hive.HiveSplitSource.StateKind.NO_MORE_SPLITS;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class HiveSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HiveSplitSource.class);

    private final String queryId;
    private final String databaseName;
    private final String tableName;
    private final PerBucket queues;
    private final AtomicInteger bufferedInternalSplitCount = new AtomicInteger();
    private final int maxOutstandingSplitsBytes;

    private final DataSize maxSplitSize;
    private final DataSize maxInitialSplitSize;
    private final AtomicInteger remainingInitialSplits;

    private final HiveSplitLoader splitLoader;
    private final AtomicReference<State> stateReference;

    private final AtomicLong estimatedSplitSizeInBytes = new AtomicLong();

    private final CounterStat highMemorySplitSourceCounter;
    private final AtomicBoolean loggedHighMemoryWarning = new AtomicBoolean();

    private final Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier;
    private final Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates;
    private final boolean isSplitFilteringEnabled;

    private final HiveConfig hiveConfig;

    private final TypeManager typeManager;
    private final HiveStorageFormat hiveStorageFormat;

    private HiveSplitSource(
            ConnectorSession session,
            String databaseName,
            String tableName,
            PerBucket queues,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            HiveSplitLoader splitLoader,
            AtomicReference<State> stateReference,
            CounterStat highMemorySplitSourceCounter,
            Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Set<TupleDomain<ColumnMetadata>> userDefinedCachedPredicates,
            TypeManager typeManager,
            HiveConfig hiveConfig,
            HiveStorageFormat hiveStorageFormat)
    {
        requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.databaseName = requireNonNull(databaseName, "databaseName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.queues = requireNonNull(queues, "queues is null");
        this.maxOutstandingSplitsBytes = toIntExact(maxOutstandingSplitsSize.toBytes());
        this.splitLoader = requireNonNull(splitLoader, "splitLoader is null");
        this.stateReference = requireNonNull(stateReference, "stateReference is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");

        this.maxSplitSize = getMaxSplitSize(session);
        this.maxInitialSplitSize = getMaxInitialSplitSize(session);
        this.remainingInitialSplits = new AtomicInteger(maxInitialSplits);

        this.dynamicFilterSupplier = dynamicFilterSupplier;
        this.isSplitFilteringEnabled = isDynamicFilteringSplitFilteringEnabled(session);
        this.userDefinedCachePredicates = userDefinedCachedPredicates;
        this.typeManager = typeManager;
        this.hiveConfig = hiveConfig;
        this.hiveStorageFormat = hiveStorageFormat;
    }

    public static HiveSplitSource allAtOnce(
            ConnectorSession session,
            String databaseName,
            String tableName,
            int maxInitialSplits,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int maxSplitsPerSecond,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter,
            Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates,
            TypeManager typeManager,
            HiveConfig hiveConfig,
            HiveStorageFormat hiveStorageFormat)
    {
        AtomicReference<State> localStateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                new PerBucket()
                {
                    private final AsyncQueue<InternalHiveSplit> queue = new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor);

                    @Override
                    public ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        // bucketNumber can be non-empty because BackgroundHiveSplitLoader does not have knowledge of execution plan
                        return queue.offer(connectorSplit);
                    }

                    @Override
                    public <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, AsyncQueue.BorrowResult<InternalHiveSplit, O>> function)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void finish()
                    {
                        queue.finish();
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        checkArgument(!bucketNumber.isPresent());
                        return queue.isFinished();
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                localStateReference,
                highMemorySplitSourceCounter,
                dynamicFilterSupplier,
                userDefinedCachePredicates,
                typeManager,
                hiveConfig,
                hiveStorageFormat);
    }

    public static HiveSplitSource bucketed(
            ConnectorSession session,
            String databaseName,
            String tableName,
            int estimatedOutstandingSplitsPerBucket,
            int maxInitialSplits,
            DataSize maxOutstandingSplitsSize,
            int maxSplitsPerSecond,
            HiveSplitLoader splitLoader,
            Executor executor,
            CounterStat highMemorySplitSourceCounter,
            Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates,
            TypeManager typeManager,
            HiveConfig hiveConfig,
            HiveStorageFormat hiveStorageFormat)
    {
        AtomicReference<State> localStateReference = new AtomicReference<>(State.initial());
        return new HiveSplitSource(
                session,
                databaseName,
                tableName,
                new PerBucket()
                {
                    private final Map<Integer, AsyncQueue<InternalHiveSplit>> queues = new ConcurrentHashMap<>();
                    private final AtomicBoolean finished = new AtomicBoolean();

                    @Override
                    public ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit connectorSplit)
                    {
                        AsyncQueue<InternalHiveSplit> queue = queueFor(bucketNumber);
                        queue.offer(connectorSplit);
                        // Do not block "offer" when running split discovery in bucketed mode.
                        // A limit is enforced on estimatedSplitSizeInBytes.
                        return immediateFuture(null);
                    }

                    @Override
                    public <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, AsyncQueue.BorrowResult<InternalHiveSplit, O>> function)
                    {
                        return queueFor(bucketNumber).borrowBatchAsync(maxSize, function);
                    }

                    @Override
                    public void finish()
                    {
                        if (finished.compareAndSet(false, true)) {
                            queues.values().forEach(AsyncQueue::finish);
                        }
                    }

                    @Override
                    public boolean isFinished(OptionalInt bucketNumber)
                    {
                        return queueFor(bucketNumber).isFinished();
                    }

                    public AsyncQueue<InternalHiveSplit> queueFor(OptionalInt bucketNumber)
                    {
                        checkArgument(bucketNumber.isPresent());
                        AtomicBoolean isNew = new AtomicBoolean();
                        AsyncQueue<InternalHiveSplit> queue = queues.computeIfAbsent(bucketNumber.getAsInt(), ignored -> {
                            isNew.set(true);
                            return new ThrottledAsyncQueue<>(maxSplitsPerSecond, estimatedOutstandingSplitsPerBucket, executor);
                        });
                        if (isNew.get() && finished.get()) {
                            // Check `finished` and invoke `queue.finish` after the `queue` is added to the map.
                            // Otherwise, `queue.finish` may not be invoked if `finished` is set while the lambda above is being evaluated.
                            queue.finish();
                        }
                        return queue;
                    }
                },
                maxInitialSplits,
                maxOutstandingSplitsSize,
                splitLoader,
                localStateReference,
                highMemorySplitSourceCounter,
                dynamicFilterSupplier,
                userDefinedCachePredicates,
                typeManager,
                hiveConfig,
                hiveStorageFormat);
    }

    /**
     * The upper bound of outstanding split count.
     * It might be larger than the actual number when called concurrently with other methods.
     */
    @VisibleForTesting
    int getBufferedInternalSplitCount()
    {
        return bufferedInternalSplitCount.get();
    }

    ListenableFuture<?> addToQueue(List<? extends InternalHiveSplit> splits)
    {
        ListenableFuture<?> lastResult = immediateFuture(null);
        for (InternalHiveSplit split : splits) {
            lastResult = addToQueue(split);
        }
        return lastResult;
    }

    ListenableFuture<?> addToQueue(InternalHiveSplit split)
    {
        if (stateReference.get().getKind() != INITIAL) {
            return immediateFuture(null);
        }
        if (estimatedSplitSizeInBytes.addAndGet(split.getEstimatedSizeInBytes()) > maxOutstandingSplitsBytes) {
            // TODO: investigate alternative split discovery strategies when this error is hit.
            // This limit should never be hit given there is a limit of maxOutstandingSplits.
            // If it's hit, it means individual splits are huge.
            if (loggedHighMemoryWarning.compareAndSet(false, true)) {
                highMemorySplitSourceCounter.update(1);
                log.warn("Split buffering for %s.%s in query %s exceeded memory limit (%s). %s splits are buffered.",
                        databaseName, tableName, queryId, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount());
            }
            throw new PrestoException(HiveErrorCode.HIVE_EXCEEDED_SPLIT_BUFFERING_LIMIT, format(
                    "Split buffering for %s.%s exceeded memory limit (%s). %s splits are buffered.",
                    databaseName, tableName, succinctBytes(maxOutstandingSplitsBytes), getBufferedInternalSplitCount()));
        }
        bufferedInternalSplitCount.incrementAndGet();
        OptionalInt bucketNumber = split.getBucketNumber();
        return queues.offer(bucketNumber, split);
    }

    void noMoreSplits()
    {
        if (setIf(stateReference, State.noMoreSplits(), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    void fail(Throwable e)
    {
        // The error must be recorded before setting the finish marker to make sure
        // isFinished will observe failure instead of successful completion.
        // Only record the first error message.
        if (setIf(stateReference, State.failed(e), state -> state.getKind() == INITIAL)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        boolean noMoreSplits;
        State state = stateReference.get();
        switch (state.getKind()) {
            case INITIAL:
                noMoreSplits = false;
                break;
            case NO_MORE_SPLITS:
                noMoreSplits = true;
                break;
            case FAILED:
                return failedFuture(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }

        OptionalInt bucketNumber = toBucketNumber(partitionHandle);
        ListenableFuture<List<ConnectorSplit>> future = queues.borrowBatchAsync(bucketNumber, maxSize, internalSplits -> {
            ImmutableList.Builder<InternalHiveSplit> splitsToInsertBuilder = ImmutableList.builder();
            ImmutableList.Builder<ConnectorSplit> resultBuilder = ImmutableList.builder();
            int removedEstimatedSizeInBytes = 0;
            for (InternalHiveSplit internalSplit : internalSplits) {
                long maxSplitBytes = getMaxSplitBytes();

                InternalHiveSplit.InternalHiveBlock block = internalSplit.currentBlock();
                long splitBytes;
                if (internalSplit.isSplittable()) {
                    splitBytes = min(maxSplitBytes, block.getEnd() - internalSplit.getStart());
                }
                else {
                    splitBytes = internalSplit.getEnd() - internalSplit.getStart();
                }
                boolean splitCacheable = matchesUserDefinedCachedPredicates(internalSplit.getPartitionKeys());

                resultBuilder.add(HiveSplitWrapper.wrap(new HiveSplit(
                        databaseName,
                        tableName,
                        internalSplit.getPartitionName(),
                        internalSplit.getPath(),
                        internalSplit.getStart(),
                        splitBytes,
                        internalSplit.getFileSize(),
                        internalSplit.getLastModifiedTime(),
                        internalSplit.getSchema(),
                        internalSplit.getPartitionKeys(),
                        block.getAddresses(),
                        internalSplit.getBucketNumber(),
                        internalSplit.isForceLocalScheduling(),
                        transformValues(internalSplit.getColumnCoercions(), HiveTypeName::toHiveType),
                        internalSplit.getBucketConversion(),
                        internalSplit.isS3SelectPushdownEnabled(),
                        internalSplit.getDeleteDeltaLocations(),
                        internalSplit.getStartRowOffsetOfFile(),
                        splitCacheable,
                        internalSplit.getCustomSplitInfo())));

                internalSplit.increaseStart(splitBytes);

                if (internalSplit.isDone()) {
                    removedEstimatedSizeInBytes += internalSplit.getEstimatedSizeInBytes();
                }
                else {
                    splitsToInsertBuilder.add(internalSplit);
                }
            }
            estimatedSplitSizeInBytes.addAndGet(-removedEstimatedSizeInBytes);

            List<InternalHiveSplit> splitsToInsert = splitsToInsertBuilder.build();
            List<ConnectorSplit> result = resultBuilder.build();
            bufferedInternalSplitCount.addAndGet(splitsToInsert.size() - result.size());

            return new AsyncQueue.BorrowResult<>(splitsToInsert, result);
        });

        ListenableFuture<ConnectorSplitBatch> transform = Futures.transform(future, splits -> {
            requireNonNull(splits, "splits is null");

            // Filter out splits if dynamic filter is available
            if (dynamicFilterSupplier != null && isSplitFilteringEnabled) {
                splits = splits.stream()
                        .filter(split -> !isPartitionFiltered(HiveSplitWrapper.getOnlyHiveSplit(split).getPartitionKeys(), dynamicFilterSupplier.get(), typeManager))
                        .collect(Collectors.toList());
            }

            if (noMoreSplits) {
                // Checking splits.isEmpty() here is required for thread safety.
                // Let's say there are 10 splits left, and max number of splits per batch is 5.
                // The futures constructed in two getNextBatch calls could each fetch 5, resulting in zero splits left.
                // After fetching the splits, both futures reach this line at the same time.
                // Without the isEmpty check, both will claim they are the last.
                // Side note 1: In such a case, it doesn't actually matter which one gets to claim it's the last.
                //              But having both claim they are the last would be a surprising behavior.
                // Side note 2: One could argue that the isEmpty check is overly conservative.
                //              The caller of getNextBatch will likely need to make an extra invocation.
                //              But an extra invocation likely doesn't matter.
                return new ConnectorSplitBatch(splits, splits.isEmpty() && queues.isFinished(bucketNumber));
            }
            else {
                return new ConnectorSplitBatch(splits, false);
            }
        }, directExecutor());

        return toCompletableFuture(transform);
    }

    /**
     * Validate the partitions key against all the user defined predicates
     * to determine whether or not that split should be cached.
     *
     * @return true if partition key matches the user defined cache predicates
     * false otherwise
     */
    private boolean matchesUserDefinedCachedPredicates(List<HivePartitionKey> partitionKeys)
    {
        if (userDefinedCachePredicates == null || userDefinedCachePredicates.isEmpty() || partitionKeys == null || partitionKeys.isEmpty()) {
            return false;
        }

        try {
            Map<String, HivePartitionKey> hivePartitionKeyMap = partitionKeys.stream().collect(Collectors.toMap(HivePartitionKey::getName, Function.identity()));
            for (TupleDomain<ColumnMetadata> tupleDomain : userDefinedCachePredicates) {
                if (!tupleDomain.getDomains().isPresent()) {
                    continue;
                }

                Map<ColumnMetadata, Domain> domainMap = tupleDomain.getDomains().get();
                Collection<String> columnsDefinedInPredicate = domainMap.keySet().stream().map(ColumnMetadata::getName).collect(Collectors.toList());
                if (!hivePartitionKeyMap.keySet().containsAll(columnsDefinedInPredicate)) {
                    continue;
                }

                boolean allMatches = domainMap.entrySet().stream().allMatch(entry -> {
                    ColumnMetadata columnMetadata = entry.getKey();
                    Domain domain = entry.getValue();
                    String partitionStringValue = hivePartitionKeyMap.get(columnMetadata.getName()).getValue();
                    NullableValue nullableValue;
                    if (partitionStringValue.equals("\\N")) {
                        nullableValue = NullableValue.asNull(columnMetadata.getType());
                    }
                    else {
                        nullableValue = HiveUtil.parsePartitionValue(columnMetadata.getName(), partitionStringValue, columnMetadata.getType());
                    }
                    return domain.includesNullableValue(nullableValue.getValue());
                });

                if (allMatches) {
                    return true;
                }
            }
        }
        catch (Exception ex) {
            log.warn(ex, "Unable to match partition keys %s with cached predicates. Ignoring this partition key. Error = %s", partitionKeys, ex.getMessage());
        }
        return false;
    }

    @Override
    public boolean isFinished()
    {
        State state = stateReference.get();

        switch (state.getKind()) {
            case INITIAL:
                return false;
            case NO_MORE_SPLITS:
                return bufferedInternalSplitCount.get() == 0;
            case FAILED:
                throw propagatePrestoException(state.getThrowable());
            case CLOSED:
                throw new IllegalStateException("HiveSplitSource is already closed");
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    public void close()
    {
        if (setIf(stateReference, State.closed(), state -> state.getKind() == INITIAL || state.getKind() == NO_MORE_SPLITS)) {
            // Stop the split loader before finishing the queue.
            // Once the queue is finished, it will always return a completed future to avoid blocking any caller.
            // This could lead to a short period of busy loop in splitLoader (although unlikely in general setup).
            splitLoader.stop();
            queues.finish();
        }
    }

    private static OptionalInt toBucketNumber(ConnectorPartitionHandle partitionHandle)
    {
        if (partitionHandle == NOT_PARTITIONED) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(((HivePartitionHandle) partitionHandle).getBucket());
    }

    private static <T> boolean setIf(AtomicReference<T> atomicReference, T newValue, Predicate<T> predicate)
    {
        while (true) {
            T current = atomicReference.get();
            if (!predicate.test(current)) {
                return false;
            }
            if (atomicReference.compareAndSet(current, newValue)) {
                return true;
            }
        }
    }

    static RuntimeException propagatePrestoException(Throwable throwable)
    {
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        if (throwable instanceof FileNotFoundException) {
            throw new PrestoException(HiveErrorCode.HIVE_FILE_NOT_FOUND, throwable);
        }
        throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR, throwable);
    }

    interface PerBucket
    {
        ListenableFuture<?> offer(OptionalInt bucketNumber, InternalHiveSplit split);

        <O> ListenableFuture<O> borrowBatchAsync(OptionalInt bucketNumber, int maxSize, Function<List<InternalHiveSplit>, AsyncQueue.BorrowResult<InternalHiveSplit, O>> function);

        void finish();

        boolean isFinished(OptionalInt bucketNumber);
    }

    static class State
    {
        private final StateKind kind;
        private final Throwable throwable;

        private State(StateKind kind, Throwable throwable)
        {
            this.kind = kind;
            this.throwable = throwable;
        }

        public StateKind getKind()
        {
            return kind;
        }

        public Throwable getThrowable()
        {
            checkState(throwable != null);
            return throwable;
        }

        public static State initial()
        {
            return new State(INITIAL, null);
        }

        public static State noMoreSplits()
        {
            return new State(NO_MORE_SPLITS, null);
        }

        public static State failed(Throwable throwable)
        {
            return new State(FAILED, throwable);
        }

        public static State closed()
        {
            return new State(CLOSED, null);
        }
    }

    enum StateKind
    {
        INITIAL,
        NO_MORE_SPLITS,
        FAILED,
        CLOSED,
    }

    @Override
    public List<ConnectorSplit> groupSmallSplits(List<ConnectorSplit> splitList, int maxGroupSize)
    {
        if (splitList.isEmpty()) {
            return splitList;
        }

        int maxSmallSplitsCanBeGrouped = Math.max(hiveConfig.getMaxSplitsToGroup(), maxGroupSize);
        if (maxSmallSplitsCanBeGrouped < 2) {
            return splitList;
        }

        if (hiveStorageFormat != HiveStorageFormat.ORC) {
            return splitList;
        }

        ImmutableList.Builder<ConnectorSplit> connectorSplitList = ImmutableList.builder();
        List<HiveSplitWrapper> hiveSplitWrappers = new ArrayList<>();
        splitList.forEach(pendingSplit -> hiveSplitWrappers.add((HiveSplitWrapper) pendingSplit));

        long maxSplitBytes = getMaxSplitBytes();

        /*
        1) selecting  splits that are less than MaxSplitSize and Not cached
        2) using MultiMap bucketNumToHiveSplitsMap separating splits based on bucked Id
        3) in every same bucket ID list, using MultiMap separating splits based on locations
        4) Sorting the List base on size, in descending order
        5) grouping first element from list (large file size) + last element  from list (small file size)
                i) till it reaches maxSplitSize and other conditions.
                ii) Number of files should not cross MinValue(max-splits-to-group, Total number of files with location / Number of selected locations).
         */

        Multimap<Integer, HiveSplit> bucketNumToHiveSplitsMap = HashMultimap.create();
        int replicationFactor = hiveSplitWrappers.get(0).getSplits().get(0).getAddresses().size();
        boolean bucketNumberPresent = hiveSplitWrappers.get(0).getBucketNumber().isPresent();

        //1> add to MultiMap bucketNumToHiveSplitsMap small files base of bucket number.
        if (false == getSmallerSplits(hiveSplitWrappers, bucketNumToHiveSplitsMap, maxSplitBytes, replicationFactor, connectorSplitList)) {
            return splitList;
        }

        // 3> in each and very Bucket, Multimap based on location, key first 3 location & value  List<HiveSplit>
        for (Integer bucketNumber : bucketNumToHiveSplitsMap.keySet()) {
            Multimap<String, HiveSplit> hostAddressHiveSplits = HashMultimap.create();

            Collection<HiveSplit> hiveSplits = bucketNumToHiveSplitsMap.get(bucketNumber);
            //Location base Multimap
            groupBaseOnLocation(hiveSplits, hostAddressHiveSplits);

            /*
            Number of files should not cross MinValue(max-splits-to-group, Total number of files with location / Number of selected locations).
             */

            for (String hostAddressText : hostAddressHiveSplits.keySet()) {
                List<HiveSplit> locationBaseHiveSplits = new ArrayList<>();
                hostAddressHiveSplits.get(hostAddressText).forEach(split1 -> locationBaseHiveSplits.add(split1));

                // sort in descending order by file size
                locationBaseHiveSplits.sort((split1, split2) -> ((int) (split2.getFileSize() - split1.getFileSize())));

                int numberOfSplitsPerLocation = locationBaseHiveSplits.size();

                // when number of flies are less than number of replication factor,splits are grouped in to single group.
                int avgSplitsPerNode = ((replicationFactor != 0) && (numberOfSplitsPerLocation >= replicationFactor)) ? numberOfSplitsPerLocation / replicationFactor : numberOfSplitsPerLocation;

                List<HiveSplit> groupedHiveSplit = new ArrayList<>();
                long totalSize = 0;
                int numberOfSplitsGrouped = 0;

                // to mean the size , add one big size file + one small size file present in the list
                while (!locationBaseHiveSplits.isEmpty()) {
                    int i = 0;
                    // add bigger file
                    totalSize += locationBaseHiveSplits.get(i).getFileSize();
                    numberOfSplitsGrouped += 1;
                    if ((maxSplitBytes < totalSize) || (avgSplitsPerNode < numberOfSplitsGrouped) || (maxSmallSplitsCanBeGrouped < numberOfSplitsGrouped)) {
                        connectorSplitList.add(HiveSplitWrapper.wrap(groupedHiveSplit, bucketNumberPresent ? OptionalInt.of(bucketNumber) : OptionalInt.empty()));
                        log.debug("info table %s,  groupedHiveSplit size %d, maxSplitBytes %d, totalSize %d, avgSplitsPerNode %d, numberOfSplitsGrouped %d, maxSmallSplitsCanBeGrouped %d, numberOfSplitsGrouped %d ",
                                groupedHiveSplit.get(0).getTable(), groupedHiveSplit.size(), maxSplitBytes, totalSize, avgSplitsPerNode, numberOfSplitsGrouped, maxSmallSplitsCanBeGrouped, numberOfSplitsGrouped);
                        totalSize = 0;
                        numberOfSplitsGrouped = 0;
                        groupedHiveSplit = new ArrayList<>();
                        continue;
                    }
                    groupedHiveSplit.add(locationBaseHiveSplits.get(i));
                    locationBaseHiveSplits.remove(i);
                    if (locationBaseHiveSplits.isEmpty()) {
                        break;
                    }

                    // add smaller file
                    int lastSplitLocation = locationBaseHiveSplits.size() - 1;
                    totalSize += locationBaseHiveSplits.get(lastSplitLocation).getFileSize();
                    numberOfSplitsGrouped += 1;
                    if ((maxSplitBytes < totalSize) || (avgSplitsPerNode < numberOfSplitsGrouped) || (maxSmallSplitsCanBeGrouped < numberOfSplitsGrouped)) {
                        connectorSplitList.add(HiveSplitWrapper.wrap(groupedHiveSplit, bucketNumberPresent ? OptionalInt.of(bucketNumber) : OptionalInt.empty()));
                        log.debug("info table %s,  groupedHiveSplit size %d, maxSplitBytes %d, totalSize %d, avgSplitsPerNode %d, numberOfSplitsGrouped %d, maxSmallSplitsCanBeGrouped %d, numberOfSplitsGrouped %d ",
                                groupedHiveSplit.get(0).getTable(), groupedHiveSplit.size(), maxSplitBytes, totalSize, avgSplitsPerNode, numberOfSplitsGrouped, maxSmallSplitsCanBeGrouped, numberOfSplitsGrouped);
                        totalSize = 0;
                        numberOfSplitsGrouped = 0;
                        groupedHiveSplit = new ArrayList<>();
                        continue;
                    }

                    groupedHiveSplit.add(locationBaseHiveSplits.get(lastSplitLocation));
                    locationBaseHiveSplits.remove(lastSplitLocation);
                }
                if (!groupedHiveSplit.isEmpty()) {
                    connectorSplitList.add(HiveSplitWrapper.wrap(groupedHiveSplit, bucketNumberPresent ? OptionalInt.of(bucketNumber) : OptionalInt.empty()));
                }
            }
        }
        List<ConnectorSplit> resultConnectorSplits = connectorSplitList.build();
        log.debug("info resultBuilder size %d", resultConnectorSplits.size());
        return resultConnectorSplits;
    }

    private boolean getSmallerSplits(List<HiveSplitWrapper> hiveSplitWrappers, Multimap<Integer, HiveSplit> bucketNumberHiveSplits,
            long maxSplitBytes, int replicationFactor, ImmutableList.Builder<ConnectorSplit> connectorSplitList)
    {
        int numSmallSplits = 0;
        for (HiveSplitWrapper hiveSplitWrapper : hiveSplitWrappers) {
            HiveSplit hiveSplit = hiveSplitWrapper.getSplits().get(0);
            long fileSize = hiveSplit.getFileSize();

            if (hiveSplit.isCacheable() || (replicationFactor != hiveSplit.getAddresses().size())) {
                //if different files have different replication factor or cache table, if will not be grouped.
                return false;
            }

            // 1) filtering small files.
            if (fileSize < maxSplitBytes) {
                //2) using MultiMap separating splits based on bucked Id.
                bucketNumberHiveSplits.put(hiveSplit.getBucketNumber().isPresent() ? hiveSplit.getBucketNumber().getAsInt() : 0, hiveSplit);
                numSmallSplits++;
            }
            else {
                connectorSplitList.add(HiveSplitWrapper.wrap(hiveSplit));
            }
        }

        if (0 == numSmallSplits) {
            // There are no small files to group
            return false;
        }

        log.info("info total Split %d,  numSmallSplits %d ", hiveSplitWrappers.size(), numSmallSplits);
        return true;
    }

    private void groupBaseOnLocation(Collection<HiveSplit> bucketBasedHiveSplits, Multimap<String, HiveSplit> hostAddressHiveSplits)
    {
        for (HiveSplit hiveSplit : bucketBasedHiveSplits) {
            List<HostAddress> hostAddresses = new ArrayList<>();
            hostAddresses.addAll(hiveSplit.getAddresses());
            hostAddresses.sort((host1, host2) -> (host1.getHostText().compareTo(host2.getHostText())));

            StringBuilder hostAddressText = new StringBuilder();
            hostAddresses.forEach(hostAddress -> hostAddressText.append(hostAddress.getHostText()));
            hostAddressHiveSplits.put(hostAddressText.toString(), hiveSplit);
        }
    }

    private long getMaxSplitBytes()
    {
        long maxSplitBytes = maxSplitSize.toBytes();
        if (remainingInitialSplits.get() > 0) {
            if (remainingInitialSplits.getAndDecrement() > 0) {
                maxSplitBytes = maxInitialSplitSize.toBytes();
            }
        }
        return maxSplitBytes;
    }
}
