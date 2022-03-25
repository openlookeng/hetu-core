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
import com.google.common.io.Closer;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.prestosql.operator.exchange.LocalPartitionGenerator;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.type.Type;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static java.lang.Integer.numberOfTrailingZeros;
import static java.lang.Math.toIntExact;

@NotThreadSafe
public class PartitionedLookupSource
        implements LookupSource
{
    public static TrackingLookupSourceSupplier createPartitionedLookupSourceSupplier(List<Supplier<LookupSource>> partitions,
            List<Type> hashChannelTypes, boolean outer, Object restoredJoinPositions)
    {
        if (outer) {
            OuterPositionTrackerFactory outerPositionTrackerFactory = new OuterPositionTrackerFactory(partitions, restoredJoinPositions);

            return new TrackingLookupSourceSupplier()
            {
                @Override
                public LookupSource getLookupSource()
                {
                    return new PartitionedLookupSource(
                            partitions.stream()
                                    .map(Supplier::get)
                                    .collect(toImmutableList()),
                            hashChannelTypes,
                            Optional.of(outerPositionTrackerFactory.create()));
                }

                @Override
                public OuterPositionIterator getOuterPositionIterator()
                {
                    return outerPositionTrackerFactory.getOuterPositionIterator();
                }

                @Override
                public ListenableFuture<?> setOuterPartitionReady(int partition)
                {
                    return outerPositionTrackerFactory.setPartitionReady(partition);
                }

                @Override
                public void setUnspilledLookupSource(int partition, LookupSource lookupSource)
                {
                    outerPositionTrackerFactory.setUnspilledPartitionLookupSource(partition, lookupSource);
                }

                @Override
                public Object captureJoinPositions()
                {
                    try {
                        return outerPositionTrackerFactory.captureJoinPositions();
                    }
                    catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }

                @Override
                public void restoreJoinPositions(Object state)
                {
                    outerPositionTrackerFactory.restoreJoinPositions(state);
                }
            };
        }
        else {
            return TrackingLookupSourceSupplier.nonTracking(
                    () -> new PartitionedLookupSource(
                            partitions.stream()
                                    .map(Supplier::get)
                                    .collect(toImmutableList()),
                            hashChannelTypes,
                            Optional.empty()));
        }
    }

    private final LookupSource[] lookupSources;
    private final LocalPartitionGenerator partitionGenerator;
    private final int partitionMask;
    private final int shiftSize;
    @Nullable
    private final OuterPositionTracker outerPositionTracker;

    private boolean closed;

    private PartitionedLookupSource(List<? extends LookupSource> lookupSources, List<Type> hashChannelTypes, Optional<OuterPositionTracker> outerPositionTracker)
    {
        this.lookupSources = lookupSources.toArray(new LookupSource[lookupSources.size()]);

        // this generator is only used for getJoinPosition without a rawHash and in this case
        // the hash channels are always packed in a page without extra columns
        int[] hashChannels = new int[hashChannelTypes.size()];
        for (int i = 0; i < hashChannels.length; i++) {
            hashChannels[i] = i;
        }
        this.partitionGenerator = new LocalPartitionGenerator(new InterpretedHashGenerator(hashChannelTypes, hashChannels), lookupSources.size());

        this.partitionMask = lookupSources.size() - 1;
        this.shiftSize = numberOfTrailingZeros(lookupSources.size()) + 1;
        this.outerPositionTracker = outerPositionTracker.orElse(null);
    }

    @Override
    public boolean isEmpty()
    {
        return Arrays.stream(lookupSources).allMatch(LookupSource::isEmpty);
    }

    @Override
    public int getChannelCount()
    {
        return lookupSources[0].getChannelCount();
    }

    @Override
    public long getJoinPositionCount()
    {
        return Arrays.stream(lookupSources)
                .mapToLong(LookupSource::getJoinPositionCount)
                .sum();
    }

    @Override
    public long getInMemorySizeInBytes()
    {
        return Arrays.stream(lookupSources).mapToLong(LookupSource::getInMemorySizeInBytes).sum();
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage)
    {
        return getJoinPosition(position, hashChannelsPage, allChannelsPage, partitionGenerator.getRawHash(hashChannelsPage, position));
    }

    @Override
    public long getJoinPosition(int position, Page hashChannelsPage, Page allChannelsPage, long rawHash)
    {
        int partition = partitionGenerator.getPartition(rawHash);
        LookupSource lookupSource = lookupSources[partition];
        long joinPosition = lookupSource.getJoinPosition(position, hashChannelsPage, allChannelsPage, rawHash);
        if (joinPosition < 0) {
            return joinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(joinPosition));
    }

    @Override
    public long getNextJoinPosition(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        long nextJoinPosition = lookupSource.getNextJoinPosition(joinPosition, probePosition, allProbeChannelsPage);
        if (nextJoinPosition < 0) {
            return nextJoinPosition;
        }
        return encodePartitionedJoinPosition(partition, toIntExact(nextJoinPosition));
    }

    @Override
    public boolean isJoinPositionEligible(long currentJoinPosition, int probePosition, Page allProbeChannelsPage)
    {
        int partition = decodePartition(currentJoinPosition);
        long joinPosition = decodeJoinPosition(currentJoinPosition);
        LookupSource lookupSource = lookupSources[partition];
        return lookupSource.isJoinPositionEligible(joinPosition, probePosition, allProbeChannelsPage);
    }

    @Override
    public void appendTo(long partitionedJoinPosition, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int partition = decodePartition(partitionedJoinPosition);
        int joinPosition = decodeJoinPosition(partitionedJoinPosition);
        lookupSources[partition].appendTo(joinPosition, pageBuilder, outputChannelOffset);
        if (outerPositionTracker != null) {
            outerPositionTracker.positionVisited(partition, joinPosition);
        }
    }

    @Override
    public long joinPositionWithinPartition(long joinPosition)
    {
        return decodeJoinPosition(joinPosition);
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }

        try (Closer closer = Closer.create()) {
            if (outerPositionTracker != null) {
                closer.register(outerPositionTracker::commit);
            }
            Arrays.stream(lookupSources).forEach(closer::register);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        closed = true;
    }

    public void setPartitionLookup(LookupSource lookupSource, int partition)
    {
        verify(partition < lookupSources.length);
        verify(lookupSources[partition] instanceof PartitionedLookupSourceFactory.SpilledLookupSource);
        verify(!(lookupSource instanceof PartitionedLookupSourceFactory.SpilledLookupSource));

        this.lookupSources[partition] = lookupSource;
        if (outerPositionTracker != null) {
            lookupSource.getJoinPositionCount();
        }
    }

    private int decodePartition(long partitionedJoinPosition)
    {
        return (int) (partitionedJoinPosition & partitionMask);
    }

    private int decodeJoinPosition(long partitionedJoinPosition)
    {
        return toIntExact(partitionedJoinPosition >>> shiftSize);
    }

    private long encodePartitionedJoinPosition(int partition, int joinPosition)
    {
        return (((long) joinPosition) << shiftSize) | (partition);
    }

    private static class PartitionedLookupOuterPositionIterator
            implements OuterPositionIterator
    {
        private final LookupSource[] lookupSources;
        private final RoaringBitmap[] visitedPositions;
        private final OuterPositionTrackerFactory outerPositionTrackerFactory;
        private final int[] partitionNumbers;

        @GuardedBy("this")
        private int currentSource;

        @GuardedBy("this")
        private int currentPosition;

        public PartitionedLookupOuterPositionIterator(LookupSource[] lookupSources, RoaringBitmap[] visitedPositions,
                                                      int[] partitionNumbers, OuterPositionTrackerFactory outerPositionTrackerFactory)
        {
            this.lookupSources = lookupSources;
            this.visitedPositions = visitedPositions;
            this.partitionNumbers = partitionNumbers;
            this.outerPositionTrackerFactory = outerPositionTrackerFactory;
        }

        @Override
        public synchronized boolean appendToNext(PageBuilder pageBuilder, int outputChannelOffset)
        {
            while (currentSource < lookupSources.length) {
                long visitedPosCount = lookupSources[currentSource].getJoinPositionCount();
                while (currentPosition < visitedPosCount) {
                    if (!visitedPositions[currentSource].contains(currentPosition)) {
                        lookupSources[currentSource].appendTo(currentPosition, pageBuilder, outputChannelOffset);
                        currentPosition++;
                        return true;
                    }
                    currentPosition++;
                }
                currentPosition = 0;
                outerPositionTrackerFactory.setPartitionDone(partitionNumbers[currentSource]);
                currentSource++;
            }

            return false;
        }

        @Override
        public ListenableFuture<OuterPositionIterator> getNextBatch()
        {
            return outerPositionTrackerFactory.getNextReady();
        }
    }

    /**
     * Each LookupSource has it's own copy of OuterPositionTracker instance.
     * Each of those OuterPositionTracker must be committed after last write
     * and before first read.
     * <p>
     * All instances share visitedPositions array, but it is safe because each thread
     * starts with visitedPositions filled with false values and marks only some positions
     * to true. Since we don't care what will be the order of those writes to
     * visitedPositions, writes can be without synchronization.
     * <p>
     * Memory visibility between last writes in commit() and first read in
     * getVisitedPositions() is guaranteed by accessing AtomicLong referenceCount
     * variables in those two methods.
     */
    public static class OuterPositionTrackerFactory
    {
        private final List<LookupSource> lookupSources;
        private final List<RoaringBitmap> visitedPositions;
        private final ReentrantReadWriteLock[] locks;
        private final AtomicBoolean[] finished;
        private final AtomicLong[] referenceCount;
        private final List<SettableFuture<OuterPositionIterator>> partitionReady;
        private final List<SettableFuture<?>> partitionDone;

        public OuterPositionTrackerFactory(List<Supplier<LookupSource>> partitions, Object restoredJoinPositions)
        {
            this.lookupSources = partitions.stream()
                    .map(Supplier::get)
                    .collect(Collectors.toList());

            visitedPositions = new ArrayList<>();
            if (restoredJoinPositions != null) {
                restoreJoinPositions(restoredJoinPositions);
            }

            finished = new AtomicBoolean[lookupSources.size()];
            referenceCount = new AtomicLong[lookupSources.size()];

            partitionReady = new ArrayList<>();
            partitionDone = new ArrayList<>();
            locks = new ReentrantReadWriteLock[lookupSources.size()];
            for (int i = 0; i < partitions.size(); i++) {
                finished[i] = new AtomicBoolean();
                referenceCount[i] = new AtomicLong();

                partitionReady.add(SettableFuture.create());
                partitionDone.add(SettableFuture.create());
                if (!(partitions.get(i).get() instanceof PartitionedLookupSourceFactory.SpilledLookupSource)) {
                    partitionReady.get(i).set(null);
                }

                if (restoredJoinPositions == null) {
                    visitedPositions.add(new RoaringBitmap());
                }
                locks[i] = new ReentrantReadWriteLock();
            }
        }

        public OuterPositionTracker create()
        {
            return new InMemoryOuterPositionTracker(visitedPositions, locks, finished, referenceCount);
        }

        public OuterPositionIterator getOuterPositionIterator()
        {
            int[] selectedPartitions = new int[lookupSources.size()];
            int count = 0;
            for (int i = 0; i < lookupSources.size(); i++) {
                if (partitionReady.get(i).isDone()) {
                    if (!partitionDone.get(i).isDone()) {
                        if (lookupSources.get(i).getJoinPositionCount() <= 0
                                || lookupSources.get(i).getJoinPositionCount() <= visitedPositions.get(i).getCardinality()) {
                            setPartitionDone(i);
                            continue;
                        }
                        selectedPartitions[count++] = i;
                    }
                }
            }

            LookupSource[] ls = new LookupSource[count];
            RoaringBitmap[] rb = new RoaringBitmap[count];
            for (int i = 0; i < count; i++) {
                ls[i] = lookupSources.get(selectedPartitions[i]);
                rb[i] = visitedPositions.get(selectedPartitions[i]);

                // touching atomic values ensures memory visibility between commit and getVisitedPositions
                verify(referenceCount[selectedPartitions[i]].get() == 0);
                finished[selectedPartitions[i]].set(true);
            }

            return new PartitionedLookupOuterPositionIterator(ls, rb, selectedPartitions, this);
        }

        protected synchronized ListenableFuture<OuterPositionIterator> getNextReady()
        {
            ImmutableList.Builder<ListenableFuture<OuterPositionIterator>> builder = ImmutableList.builder();

            int objs = 0;
            for (int i = 0; i < lookupSources.size(); i++) {
                if (!partitionDone.get(i).isDone()) {
                    builder.add(partitionReady.get(i));
                    objs++;
                }
            }

            if (objs > 0) {
                return whenAnyComplete(builder.build());
            }

            return immediateFuture(null);
        }

        protected synchronized void setOuterPositionIterator(int partitionNumber)
        {
            verify(partitionNumber < lookupSources.size());
            verify(!partitionReady.get(partitionNumber).isDone());

            partitionReady.get(partitionNumber)
                    .set(new PartitionedLookupOuterPositionIterator(
                            new LookupSource[] {lookupSources.get(partitionNumber)},
                            new RoaringBitmap[] {visitedPositions.get(partitionNumber)},
                            new int[] {partitionNumber},
                            this));
        }

        public Object captureJoinPositions() throws IOException
        {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream dos = new DataOutputStream(bos);
            for (RoaringBitmap rr : visitedPositions) {
                rr.serialize(dos);
            }
            dos.close();
            return bos.toByteArray();
        }

        public void restoreJoinPositions(Object state)
        {
            ByteBuffer bb = ByteBuffer.wrap((byte[]) state);
            visitedPositions.clear();
            for (int i = 0; i < lookupSources.size(); i++) {
                ImmutableRoaringBitmap bm = new ImmutableRoaringBitmap(bb);
                visitedPositions.add(new RoaringBitmap(bm));
                bb.position(bb.position() + visitedPositions.get(i).serializedSizeInBytes());
            }
        }

        public void setPartitionDone(int partition)
        {
            verify(partition < partitionDone.size());
            partitionDone.get(partition).set(null);

            locks[partition].writeLock().lock();
            try {
                visitedPositions.get(partition).clear();
            }
            finally {
                locks[partition].writeLock().unlock();
            }
        }

        public ListenableFuture<?> setPartitionReady(int partition)
        {
            verify(partition < lookupSources.size());
            verify(!partitionReady.get(partition).isDone());

            locks[partition].writeLock().lock();
            try {
                if (lookupSources.get(partition).getJoinPositionCount() <= 0
                        || lookupSources.get(partition).getJoinPositionCount() <= visitedPositions.get(partition).getCardinality()) {
                    setPartitionDone(partition); /* all matched in this partition; skip it! */
                    partitionReady.get(partition)
                            .set(new PartitionedLookupOuterPositionIterator(
                                    new LookupSource[0],
                                    new RoaringBitmap[0],
                                    new int[0],
                                    this));
                }
                else {
                    partitionReady.get(partition)
                            .set(new PartitionedLookupOuterPositionIterator(
                                    new LookupSource[]{lookupSources.get(partition)},
                                    new RoaringBitmap[]{visitedPositions.get(partition)},
                                    new int[]{partition},
                                    this));
                }
                return partitionDone.get(partition);
            }
            finally {
                locks[partition].writeLock().unlock();
            }
        }

        public void setUnspilledPartitionLookupSource(int partition, LookupSource lookupSource)
        {
            verify(partition < lookupSources.size());
            verify(lookupSources.get(partition) instanceof PartitionedLookupSourceFactory.SpilledLookupSource);

            lookupSources.set(partition, lookupSource);
        }
    }

    public interface OuterPositionTracker
    {
        void positionVisited(int partitioned, int position);

        void commit();
    }

    private static class InMemoryOuterPositionTracker
            implements OuterPositionTracker
    {
        private final RoaringBitmap[] visitedPositions; // shared across multiple operators/drivers
        private final ReentrantReadWriteLock[] locks;
        private final AtomicBoolean[] finished; // shared across multiple operators/drivers
        private final AtomicLong[] referenceCount; // shared across multiple operators/drivers
        private boolean[] written; // unique per each operator/driver

        private InMemoryOuterPositionTracker(List<RoaringBitmap> visitedPositions, ReentrantReadWriteLock[] locks, AtomicBoolean[] finished, AtomicLong[] referenceCount)
        {
            this.visitedPositions = visitedPositions.toArray(new RoaringBitmap[visitedPositions.size()]);
            this.locks = locks;
            this.finished = finished;
            this.referenceCount = referenceCount;
            this.written = new boolean[visitedPositions.size()];
        }

        /**
         * No synchronization here, because it would be very expensive. Check comment above.
         */
        @Override
        public void positionVisited(int partition, int position)
        {
            verify(partition < referenceCount.length);
            if (!written[partition]) {
                written[partition] = true;
                verify(!finished[partition].get());
                referenceCount[partition].incrementAndGet();
            }

            locks[partition].writeLock().lock();
            try {
                visitedPositions[partition].add(position);
            }
            finally {
                locks[partition].writeLock().unlock();
            }
        }

        @Override
        public void commit()
        {
            for (int i = 0; i < written.length; i++) {
                if (written[i]) {
                    // touching atomic values ensures memory visibility between commit and getVisitedPositions
                    referenceCount[i].decrementAndGet();
                }
            }
        }
    }

    private static class SpillableOuterPositionTracker
            implements OuterPositionTracker
    {
        private final RoaringBitmap[] visitedPositions; // shared across multiple operators/drivers
        private final AtomicBoolean finished; // shared across multiple operators/drivers
        private final AtomicLong referenceCount; // shared across multiple operators/drivers
        private boolean written; // unique per each operator/driver

        private SpillableOuterPositionTracker(RoaringBitmap[] visitedPositions, AtomicBoolean finished, AtomicLong referenceCount)
        {
            this.visitedPositions = visitedPositions;
            this.finished = finished;
            this.referenceCount = referenceCount;
        }

        /**
         * No synchronization here, because it would be very expensive. Check comment above.
         */
        @Override
        public void positionVisited(int partitioned, int position)
        {
            if (!written) {
                written = true;
                verify(!finished.get());
                referenceCount.incrementAndGet();
            }
            visitedPositions[partitioned].add(position); /* Todo: Trigger spill if needed */
        }

        @Override
        public void commit()
        {
            if (written) {
                // touching atomic values ensures memory visibility between commit and getVisitedPositions
                referenceCount.decrementAndGet();
            }
        }
    }
}
