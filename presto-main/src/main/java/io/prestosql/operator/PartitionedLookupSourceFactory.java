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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.BloomFilter;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.operator.LookupSourceProvider.LookupSourceLease;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.type.Type;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Futures.nonCancellationPropagating;
import static com.google.common.util.concurrent.Futures.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.operator.OuterLookupSource.createOuterLookupSourceSupplier;
import static io.prestosql.operator.PartitionedLookupSource.createPartitionedLookupSourceSupplier;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class PartitionedLookupSourceFactory
        implements LookupSourceFactory
{
    private final List<Type> types;
    private final List<Type> outputTypes;
    private final Map<Symbol, Integer> layout;
    private final List<Type> hashChannelTypes;
    private final boolean outer;
    private final SpilledLookupSource spilledLookupSource;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    @GuardedBy("lock")
    private final Supplier<LookupSource>[] partitions;

    private final SettableFuture<?> partitionsNoLongerNeeded = SettableFuture.create();

    @GuardedBy("lock")
    private final SettableFuture<?> destroyed = SettableFuture.create();

    @GuardedBy("lock")
    private int partitionsSet;

    @GuardedBy("lock")
    private SpillingInfo spillingInfo = new SpillingInfo(0, ImmutableSet.of(), ImmutableMap.of());

    @GuardedBy("lock")
    private final Map<Integer, SpilledLookupSourceHandle> spilledPartitions = new HashMap<>();

    @GuardedBy("lock")
    private TrackingLookupSourceSupplier lookupSourceSupplier;

    @GuardedBy("lock")
    private final List<SettableFuture<LookupSourceProvider>> lookupSourceFutures = new ArrayList<>();

    @GuardedBy("lock")
    private int finishedProbeOperators;

    @GuardedBy("lock")
    private OptionalInt partitionedConsumptionParticipants = OptionalInt.empty();

    @GuardedBy("lock")
    private final SettableFuture<PartitionedConsumption<Supplier<LookupSource>>> partitionedConsumption = SettableFuture.create();

    @GuardedBy("lock")
    private PartitionedLookupSource postConsumptionOuterSource;

    private final SettableFuture<?> probeInMemFinish = SettableFuture.create();

    PartitionedLookupSource.OuterPositionTrackerFactory outerPositions;

    /**
     * Cached LookupSource on behalf of LookupJoinOperator (represented by SpillAwareLookupSourceProvider). LookupSource instantiation has non-negligible cost.
     * <p>
     * Whole-sale modifications guarded by rwLock.writeLock(). Modifications (addition, update, removal) of entry for key K is confined to object K.
     * Important note: this cannot be replaced with regular map guarded by the read-write lock. This is because read lock is held in {@code withLease} for
     * the prolong time, and other threads would not be able to insert new (cached) lookup sources in this map, harming work concurrency.
     */
    private final ConcurrentHashMap<SpillAwareLookupSourceProvider, LookupSource> suppliedLookupSources = new ConcurrentHashMap<>();

    /**
     * Snapshot: For right/full outer join, the lookup-outer operator, to receive markers
     */
    private LookupOuterOperator lookupOuterOperator;

    /**
     * Snapshot: If markers are received before lookup-outer operator is set, then keep them here
     */
    private final List<Marker> pendingMarkers = new ArrayList<>();

    /**
     * Snapshot: If resuming happened before lookupSourceSupplier is created, then store it here, and use it to initialize lookupSourceSupplier
     */
    private Object restoredJoinPositions;

    public PartitionedLookupSourceFactory(List<Type> types, List<Type> outputTypes, List<Type> hashChannelTypes, int partitionCount, Map<Symbol, Integer> layout, boolean outer)
    {
        checkArgument(Integer.bitCount(partitionCount) == 1, "partitionCount must be a power of 2");

        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.hashChannelTypes = ImmutableList.copyOf(hashChannelTypes);
        this.layout = ImmutableMap.copyOf(layout);
        checkArgument(partitionCount > 0);
        this.partitions = (Supplier<LookupSource>[]) new Supplier<?>[partitionCount];
        this.outer = outer;
        spilledLookupSource = new SpilledLookupSource(outputTypes.size());
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public List<Type> getOutputTypes()
    {
        return outputTypes;
    }

    @Override
    public Map<Symbol, Integer> getLayout()
    {
        return layout;
    }

    // partitions is final, so we don't need a lock to read its length here
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int partitions()
    {
        return partitions.length;
    }

    @Override
    public ListenableFuture<LookupSourceProvider> createLookupSourceProvider()
    {
        lock.writeLock().lock();
        try {
            checkState(!destroyed.isDone(), "already destroyed");
            if (lookupSourceSupplier != null) {
                return immediateFuture(new SpillAwareLookupSourceProvider());
            }

            SettableFuture<LookupSourceProvider> lookupSourceFuture = SettableFuture.create();
            lookupSourceFutures.add(lookupSourceFuture);
            return lookupSourceFuture;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ListenableFuture<?> whenBuildFinishes()
    {
        return transform(
                this.createLookupSourceProvider(),
                lookupSourceProvider -> {
                    // Close the lookupSourceProvider we just created.
                    // The only reason we created it is to wait until lookup source is ready.
                    lookupSourceProvider.close();
                    return null;
                },
                directExecutor());
    }

    public ListenableFuture<?> lendPartitionLookupSource(int partitionIndex, Supplier<LookupSource> partitionLookupSource)
    {
        requireNonNull(partitionLookupSource, "partitionLookupSource is null");

        boolean completed;

        lock.writeLock().lock();
        try {
            if (destroyed.isDone()) {
                return immediateFuture(null);
            }

            checkState(partitions[partitionIndex] == null, "Partition already set");
            checkState(!spilledPartitions.containsKey(partitionIndex), "Partition already set as spilled");
            partitions[partitionIndex] = partitionLookupSource;
            partitionsSet++;
            completed = (partitionsSet == partitions.length);
        }
        finally {
            lock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }

        return partitionsNoLongerNeeded;
    }

    public void setPartitionSpilledLookupSourceHandle(int partitionIndex, SpilledLookupSourceHandle spilledLookupSourceHandle)
    {
        requireNonNull(spilledLookupSourceHandle, "spilledLookupSourceHandle is null");

        boolean completed;

        lock.writeLock().lock();
        try {
            if (partitionsNoLongerNeeded.isDone()) {
                spilledLookupSourceHandle.dispose();
                return;
            }

            checkState(!spilledPartitions.containsKey(partitionIndex), "Partition already set as spilled");
            spilledPartitions.put(partitionIndex, spilledLookupSourceHandle);
            Map<Integer, BloomFilter<Long>> spillBlooms = spilledPartitions.entrySet().stream()
                    .filter(e -> e.getValue().getSpillBloom().isPresent())
                    .collect(Collectors.toMap(x -> x.getKey(),
                            x -> x.getValue().getSpillBloom().get()));
            spillingInfo = new SpillingInfo(spillingInfo.spillEpoch() + 1, spilledPartitions.keySet(), spillBlooms);

            if (partitions[partitionIndex] != null) {
                // Was present and now it's spilled
                completed = false;
            }
            else {
                partitionsSet++;
                completed = (partitionsSet == partitions.length);
            }

            partitions[partitionIndex] = () -> spilledLookupSource;

            if (lookupSourceSupplier != null) {
                /*
                 * lookupSourceSupplier exists so the now-spilled partition is still referenced by it. Need to re-create lookupSourceSupplier to let the memory go
                 * and to prevent probe side accessing the partition.
                 */
                verify(!completed, "lookupSourceSupplier already exist when completing");
                verify(partitions.length > 1, "Spill occurred when only one partition");

                /* Reinitialize the spilled partition specific outerVisitedPositions */
                Object capturedStates = lookupSourceSupplier.captureJoinPositions();
                lookupSourceSupplier = createPartitionedLookupSourceSupplier(ImmutableList.copyOf(partitions),
                        hashChannelTypes, outer, capturedStates);
                closeCachedLookupSources();
            }
            else {
                verify(suppliedLookupSources.isEmpty(), "There are cached LookupSources even though lookupSourceSupplier does not exist");
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        if (completed) {
            supplyLookupSources();
        }
    }

    private void supplyLookupSources()
    {
        checkState(!lock.isWriteLockedByCurrentThread());

        List<SettableFuture<LookupSourceProvider>> lookupSourceFutureList;

        lock.writeLock().lock();
        try {
            checkState(partitionsSet == partitions.length, "Not all set yet");
            checkState(this.lookupSourceSupplier == null, "Already supplied");

            if (partitionsNoLongerNeeded.isDone()) {
                return;
            }

            if (partitionsSet != 1) {
                List<Supplier<LookupSource>> partitionList = ImmutableList.copyOf(this.partitions);
                Object capturedStates = restoredJoinPositions;
                if (lookupSourceSupplier != null) {
                    capturedStates = lookupSourceSupplier.captureJoinPositions();
                }
                this.lookupSourceSupplier = createPartitionedLookupSourceSupplier(partitionList, hashChannelTypes, outer, capturedStates);
            }
            else if (outer) {
                this.lookupSourceSupplier = createOuterLookupSourceSupplier(partitions[0], restoredJoinPositions);
            }
            else {
                checkState(!spillingInfo.hasSpilled(), "Spill not supported when there is single partition");
                this.lookupSourceSupplier = TrackingLookupSourceSupplier.nonTracking(partitions[0]);
            }

            // store futures into local variables so they can be used outside of the lock
            lookupSourceFutureList = ImmutableList.copyOf(this.lookupSourceFutures);
        }
        finally {
            lock.writeLock().unlock();
        }

        for (SettableFuture<LookupSourceProvider> lookupSourceFuture : lookupSourceFutureList) {
            lookupSourceFuture.set(new SpillAwareLookupSourceProvider());
        }
    }

    @Override
    public ListenableFuture<PartitionedConsumption<Supplier<LookupSource>>> finishProbeOperator(OptionalInt lookupJoinsCount)
    {
        lock.writeLock().lock();
        try {
            if (!spillingInfo.hasSpilled()) {
                finishedProbeOperators++;
                if (lookupJoinsCount.isPresent()) {
                    checkState(finishedProbeOperators <= lookupJoinsCount.getAsInt(), "%s probe operators finished out of %s declared", finishedProbeOperators, lookupJoinsCount.getAsInt());
                    if (finishedProbeOperators == lookupJoinsCount.getAsInt()) {
                        // We can dispose partitions now since right outer is not supported with spill and lookupJoinsCount should be absent
                        probeInMemFinish.set(null);
                    }
                }

                return immediateFuture(new PartitionedConsumption<>(
                        1,
                        emptyList(),
                        i -> {
                            throw new UnsupportedOperationException();
                        },
                        i -> {},
                        i -> {
                            throw new UnsupportedOperationException();
                        }));
            }

            int operatorsCount = lookupJoinsCount
                    .orElseThrow(() -> new IllegalStateException("A fixed distribution is required for JOIN when spilling is enabled"));
            checkState(finishedProbeOperators < operatorsCount, "%s probe operators finished out of %s declared", finishedProbeOperators + 1, operatorsCount);

            if (!partitionedConsumptionParticipants.isPresent()) {
                // This is the first probe to finish after anything has been spilled.
                partitionedConsumptionParticipants = OptionalInt.of(operatorsCount - finishedProbeOperators);
            }

            finishedProbeOperators++;
            if (finishedProbeOperators == operatorsCount) {
                // We can dispose partitions now since right outer is not supported with spill
                // Todo(Nitin) free partition after outer spill consumed

                verify(!partitionedConsumption.isDone());
                if (outer) {
                    postConsumptionOuterSource = (PartitionedLookupSource) this.lookupSourceSupplier.getLookupSource();

                    partitionedConsumption.set(new PartitionedConsumption<>(
                            partitionedConsumptionParticipants.getAsInt(),
                            spilledPartitions.keySet(),
                            this::loadSpilledLookupSource,
                            this::loadOuterIterator,
                            this::spilledLookupSourceDisposed));
                }
                else {
                    freePartitions();
                    partitionedConsumption.set(new PartitionedConsumption<>(
                            partitionedConsumptionParticipants.getAsInt(),
                            spilledPartitions.keySet(),
                            this::loadSpilledLookupSource,
                            this::disposeSpilledLookupSource,
                            this::spilledLookupSourceDisposed));
                }

                probeInMemFinish.set(null);
            }

            return partitionedConsumption;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private ListenableFuture<Supplier<LookupSource>> loadSpilledLookupSource(int partitionNumber)
    {
        if (outer) {
            verify(partitionsSet != 1);
            ListenableFuture<Supplier<LookupSource>> lookupSupplierFuture = getSpilledLookupSourceHandle(partitionNumber).getLookupSource();
            return Futures.transformAsync(lookupSupplierFuture,
                    lookupSource -> getUpdatedPartitionedLookupSourceSupplier(lookupSource, partitionNumber),
                    directExecutor());
        }
        return getSpilledLookupSourceHandle(partitionNumber).getLookupSource();
    }

    private ListenableFuture<Supplier<LookupSource>> getUpdatedPartitionedLookupSourceSupplier(Supplier<LookupSource> lookupSource, int partitionNumber)
    {
        postConsumptionOuterSource = (PartitionedLookupSource) this.lookupSourceSupplier.getLookupSource();
        postConsumptionOuterSource.setPartitionLookup(lookupSource.get(), partitionNumber);
        return immediateFuture(() -> postConsumptionOuterSource);
    }

    private void loadOuterIterator(int partitionNumber)
    {
        if (postConsumptionOuterSource != null) {
            /* commit all the partitions visited! */
            postConsumptionOuterSource.close();
            postConsumptionOuterSource = null;
        }

        this.lookupSourceSupplier.setOuterPartitionReady(partitionNumber)
                .addListener(() -> disposeSpilledLookupSource(partitionNumber), directExecutor());
    }

    private void disposeSpilledLookupSource(int partitionNumber)
    {
        getSpilledLookupSourceHandle(partitionNumber).dispose();
    }

    private ListenableFuture<?> spilledLookupSourceDisposed(int partitionNumber)
    {
        return getSpilledLookupSourceHandle(partitionNumber).getDisposeCompleted();
    }

    private SpilledLookupSourceHandle getSpilledLookupSourceHandle(int partitionNumber)
    {
        lock.readLock().lock();
        try {
            return requireNonNull(spilledPartitions.get(partitionNumber), "spilledPartitions.get(partitionNumber) is null");
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public ListenableFuture<?> whenMemProbeFinishes()
    {
        return probeInMemFinish;
    }

    @Override
    public OuterPositionIterator getOuterPositionIterator()
    {
        TrackingLookupSourceSupplier trackingLookupSourceSupplier;

        lock.writeLock().lock();
        try {
            checkState(this.lookupSourceSupplier != null, "lookup source not ready yet");
            trackingLookupSourceSupplier = this.lookupSourceSupplier;
        }
        finally {
            lock.writeLock().unlock();
        }

        return trackingLookupSourceSupplier.getOuterPositionIterator();
    }

    @Override
    public void destroy()
    {
        lock.writeLock().lock();
        try {
            freePartitions();
            postConsumptionOuterSource.close();
            spilledPartitions.values().forEach(SpilledLookupSourceHandle::dispose);

            // Setting destroyed must be last because it's a part of the state exposed by isDestroyed() without synchronization.
            destroyed.set(null);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void freePartitions()
    {
        // Let the HashBuilderOperators reduce their accounted memory
        partitionsNoLongerNeeded.set(null);

        lock.writeLock().lock();
        try {
            // Remove out references to partitions to actually free memory
            Arrays.fill(partitions, null);
            lookupSourceSupplier = null;
            closeCachedLookupSources();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void closeCachedLookupSources()
    {
        lock.writeLock().lock();
        try {
            suppliedLookupSources.values().forEach(LookupSource::close);
            suppliedLookupSources.clear();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public ListenableFuture<?> isDestroyed()
    {
        return nonCancellationPropagating(destroyed);
    }

    @NotThreadSafe
    private class SpillAwareLookupSourceProvider
            implements LookupSourceProvider
    {
        @Override
        public <R> R withLease(Function<LookupSourceLease, R> action)
        {
            lock.readLock().lock();
            try {
                LookupSource lookupSource = suppliedLookupSources.computeIfAbsent(this, k -> lookupSourceSupplier.getLookupSource());
                LookupSourceLease lease = new SpillAwareLookupSourceLease(lookupSource, spillingInfo);
                return action.apply(lease);
            }
            finally {
                lock.readLock().unlock();
            }
        }

        @Override
        public void close()
        {
            LookupSource lookupSource;
            lock.readLock().lock();
            try {
                lookupSource = suppliedLookupSources.remove(this);
            }
            finally {
                lock.readLock().unlock();
            }
            if (lookupSource != null) {
                lookupSource.close();
            }
        }
    }

    private static class SpillAwareLookupSourceLease
            implements LookupSourceLease
    {
        private final LookupSource lookupSource;
        private final SpillingInfo spillingInfo;

        public SpillAwareLookupSourceLease(LookupSource lookupSource, SpillingInfo spillingInfo)
        {
            this.lookupSource = requireNonNull(lookupSource, "lookupSource is null");
            this.spillingInfo = requireNonNull(spillingInfo, "spillingInfo is null");
        }

        @Override
        public LookupSource getLookupSource()
        {
            return lookupSource;
        }

        @Override
        public boolean hasSpilled()
        {
            return spillingInfo.hasSpilled();
        }

        @Override
        public long spillEpoch()
        {
            return spillingInfo.spillEpoch();
        }

        @Override
        public IntPredicate getSpillMask()
        {
            return spillingInfo.getSpillMask();
        }

        @Override
        public BiPredicate<Integer, Long> getSpillMatcher()
        {
            return spillingInfo.getSpillMatcher();
        }
    }

    protected static class SpilledLookupSource
            implements LookupSource
    {
        private final int channelCount;

        public SpilledLookupSource(int channelCount)
        {
            this.channelCount = channelCount;
        }

        @Override
        public boolean isEmpty()
        {
            return false;
        }

        @Override
        public int getChannelCount()
        {
            return channelCount;
        }

        @Override
        public long getInMemorySizeInBytes()
        {
            return 0;
        }

        @Override
        public long joinPositionWithinPartition(long joinPosition)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPositionCount()
        {
            // Will be counted after unspilling.
            return 0;
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appendTo(long position, PageBuilder pageBuilder, int outputChannelOffset)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }

    @Immutable
    private static final class SpillingInfo
    {
        private final long spillEpoch;
        private final Set<Integer> spilledPartitions;
        private final ImmutableMap<Integer, BloomFilter<Long>> spillBlooms;

        SpillingInfo(long spillEpoch, Set<Integer> spilledPartitions, Map<Integer, BloomFilter<Long>> spillBlooms)
        {
            this.spillEpoch = spillEpoch;
            this.spilledPartitions = ImmutableSet.copyOf(requireNonNull(spilledPartitions, "spilledPartitions is null"));
            this.spillBlooms = ImmutableMap.copyOf(spillBlooms);
        }

        boolean hasSpilled()
        {
            return !spilledPartitions.isEmpty();
        }

        long spillEpoch()
        {
            return spillEpoch;
        }

        IntPredicate getSpillMask()
        {
            return spilledPartitions::contains;
        }

        BiPredicate<Integer, Long> getSpillMatcher()
        {
            return (partition, rawHash) -> {
                if (!spillBlooms.containsKey(partition)) {
                    return false;
                }
                return spillBlooms.get(partition).mightContain(rawHash);
            };
        }
    }

    private static class Marker
    {
        private MarkerPage markerPage;
        private int totalDrivers;
        private Integer driverId;

        private Marker(MarkerPage markerPage, int totalDrivers, Integer driverId)
        {
            this.markerPage = markerPage;
            this.totalDrivers = totalDrivers;
            this.driverId = driverId;
        }
    }

    @Override
    public synchronized void setLookupOuterOperator(LookupOuterOperator operator)
    {
        checkState(outer && lookupOuterOperator == null, "Multiple lookup outer operators for same lookup source");
        lookupOuterOperator = operator;
        for (Marker marker : pendingMarkers) {
            if (marker.driverId == null) {
                operator.processMarkerForTableScan(marker.markerPage);
            }
            else {
                operator.processMarkerForExchange(marker.markerPage, marker.totalDrivers, marker.driverId);
            }
        }
    }

    @Override
    public synchronized void processMarkerForTableScanOuterJoin(MarkerPage markerPage)
    {
        if (lookupOuterOperator != null) {
            lookupOuterOperator.processMarkerForTableScan(markerPage);
        }
        else if (outer) {
            pendingMarkers.add(new Marker(markerPage, 0, null));
        }
    }

    @Override
    public synchronized void processMarkerForExchangeOuterJoin(MarkerPage markerPage, int totalDrivers, int driverId)
    {
        if (lookupOuterOperator != null) {
            lookupOuterOperator.processMarkerForExchange(markerPage, totalDrivers, driverId);
        }
        else if (outer) {
            pendingMarkers.add(new Marker(markerPage, totalDrivers, driverId));
        }
    }

    @Override
    public Object captureJoinPositions()
    {
        lock.readLock().lock();
        try {
            if (lookupSourceSupplier != null) {
                return lookupSourceSupplier.captureJoinPositions();
            }
        }
        finally {
            lock.readLock().unlock();
        }
        return 0;
    }

    @Override
    public void restoreJoinPositions(Object state)
    {
        if (state.getClass() == Integer.class) {
            return; // lookupSourceSupplier was not ready when the state was capture
        }

        lock.writeLock().lock();
        try {
            if (lookupSourceSupplier != null) {
                lookupSourceSupplier.restoreJoinPositions(state);
            }
            else {
                // Will use this to construct the supplier
                restoredJoinPositions = state;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }
}
