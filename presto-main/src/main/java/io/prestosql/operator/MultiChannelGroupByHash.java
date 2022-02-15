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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.operator.SyntheticAddress.decodePosition;
import static io.prestosql.operator.SyntheticAddress.decodeSliceIndex;
import static io.prestosql.operator.SyntheticAddress.encodeSyntheticAddress;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

// This implementation assumes arrays used in the hash are always a power of 2
@RestorableConfig(uncapturedFields = {"types", "hashTypes", "channels", "hashStrategy",
        "inputHashChannel", "processDictionary", "hashGenerator", "updateMemory"})
public class MultiChannelGroupByHash
        extends MultiChannelGroupBy implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupByHash.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;

    private PageBuilder currentPageBuilder;

    private long completedPagesMemorySize;

    private int hashCapacity;
    private int maxFill;
    private int mask;
    private long[] groupAddressByHash;
    private int[] groupIdsByHash;
    private byte[] rawHashByHashPosition;

    private final LongBigArray groupAddressByGroupId;

    private int nextGroupId;
    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;

    public MultiChannelGroupByHash(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory)
    {
        super(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler);
        startNewPage();

        // reserve memory for the arrays
        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        groupAddressByHash = new long[hashCapacity];
        Arrays.fill(groupAddressByHash, -1);

        rawHashByHashPosition = new byte[hashCapacity];

        groupIdsByHash = new int[hashCapacity];

        groupAddressByGroupId = new LongBigArray();
        groupAddressByGroupId.ensureCapacity(maxFill);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public long getRawHash(int groupId)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        return hashStrategy.hashPosition(blockIndex, position);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes() +
                sizeOf(groupAddressByHash) +
                sizeOf(groupIdsByHash) +
                groupAddressByGroupId.sizeOf() +
                sizeOf(rawHashByHashPosition) +
                preallocatedMemoryInBytes;
    }

    @Override
    public long getHashCollisions()
    {
        return hashCollisions;
    }

    @Override
    public double getExpectedHashCollisions()
    {
        return expectedHashCollisions + estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        long address = groupAddressByGroupId.get(groupId);
        int blockIndex = decodeSliceIndex(address);
        int position = decodePosition(address);
        hashStrategy.appendTo(blockIndex, position, pageBuilder, outputChannelOffset);
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new AddRunLengthEncodedPageWork(page, this);
        }
        if (canProcessDictionary(page)) {
            return new AddDictionaryPageWork(page, this);
        }

        return new AddNonDictionaryPageWork(page, this);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        if (isRunLengthEncoded(page)) {
            return new GetRunLengthEncodedGroupIdsWork(page, this);
        }
        if (canProcessDictionary(page)) {
            return new GetDictionaryGroupIdsWork(page, this);
        }

        return new GetNonDictionaryGroupIdsWork(page, this);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        long rawHash = hashStrategy.hashRow(position, page);
        return contains(position, page, hashChannels, rawHash);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for a slot containing this key
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, hashChannels)) {
                // found an existing slot for this key
                return true;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }

        return false;
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    public int putIfAbsent(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return putIfAbsent(position, page, rawHash);
    }

    public int putIfAbsent(int position, Page page, long rawHash)
    {
        int hashPosition = (int) getHashPosition(rawHash, mask);

        // look for an empty slot or a slot containing this key
        int groupId = -1;
        while (groupAddressByHash[hashPosition] != -1) {
            if (positionNotDistinctFromCurrentRow(groupAddressByHash[hashPosition], hashPosition, position, page, (byte) rawHash, channels)) {
                // found an existing slot for this key
                groupId = groupIdsByHash[hashPosition];

                break;
            }
            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        // did we find an existing group?
        if (groupId < 0) {
            groupId = addNewGroup(hashPosition, position, page, rawHash);
        }
        return groupId;
    }

    private int addNewGroup(int hashPosition, int position, Page page, long rawHash)
    {
        // add the row to the open page
        for (int i = 0; i < channels.length; i++) {
            int hashChannel = channels[i];
            Type type = types.get(i);
            type.appendTo(page.getBlock(hashChannel), position, currentPageBuilder.getBlockBuilder(i));
        }
        if (precomputedHashChannel.isPresent()) {
            BIGINT.writeLong(currentPageBuilder.getBlockBuilder(precomputedHashChannel.getAsInt()), rawHash);
        }
        currentPageBuilder.declarePosition();
        int pageIndex = channelBuilders.get(0).size() - 1;
        int pagePosition = currentPageBuilder.getPositionCount() - 1;
        long address = encodeSyntheticAddress(pageIndex, pagePosition);

        // record group id in hash
        int groupId = nextGroupId++;

        groupAddressByHash[hashPosition] = address;
        rawHashByHashPosition[hashPosition] = (byte) rawHash;
        groupIdsByHash[hashPosition] = groupId;
        groupAddressByGroupId.set(groupId, address);

        // create new page builder if this page is full
        if (currentPageBuilder.isFull()) {
            startNewPage();
        }

        // increase capacity, if necessary
        if (needMoreCapacity()) {
            tryToIncreaseCapacity();
        }
        return groupId;
    }

    public boolean needMoreCapacity()
    {
        return nextGroupId >= maxFill;
    }

    private void startNewPage()
    {
        if (currentPageBuilder != null) {
            completedPagesMemorySize += currentPageBuilder.getRetainedSizeInBytes();
            currentPageBuilder = currentPageBuilder.newPageBuilderLike();
        }
        else {
            currentPageBuilder = new PageBuilder(types);
        }

        for (int i = 0; i < types.size(); i++) {
            channelBuilders.get(i).add(currentPageBuilder.getBlockBuilder(i));
        }
    }

    public boolean tryToIncreaseCapacity()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for groupAddressByHash, rawHashByHashPosition, groupIdsByHash, and groupAddressByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES + Byte.BYTES) +
                (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES +
                currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        int newMask = newCapacity - 1;
        long[] newKey = new long[newCapacity];
        byte[] rawHashes = new byte[newCapacity];
        Arrays.fill(newKey, -1);
        int[] newValue = new int[newCapacity];

        int oldIndex = 0;
        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            // seek to the next used slot
            while (groupAddressByHash[oldIndex] == -1) {
                oldIndex++;
            }

            // get the address for this slot
            long address = groupAddressByHash[oldIndex];

            long rawHash = hashPosition(address);
            // find an empty slot for the address
            int pos = (int) getHashPosition(rawHash, newMask);
            while (newKey[pos] != -1) {
                pos = (pos + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newKey[pos] = address;
            rawHashes[pos] = (byte) rawHash;
            newValue[pos] = groupIdsByHash[oldIndex];
            oldIndex++;
        }

        this.mask = newMask;
        this.hashCapacity = newCapacity;
        this.maxFill = calculateMaxFill(newCapacity);
        this.groupAddressByHash = newKey;
        this.rawHashByHashPosition = rawHashes;
        this.groupIdsByHash = newValue;
        groupAddressByGroupId.ensureCapacity(maxFill);
        return true;
    }

    private long hashPosition(long sliceAddress)
    {
        int sliceIndex = decodeSliceIndex(sliceAddress);
        int position = decodePosition(sliceAddress);
        if (precomputedHashChannel.isPresent()) {
            return getRawHash(sliceIndex, position);
        }
        return hashStrategy.hashPosition(sliceIndex, position);
    }

    private long getRawHash(int sliceIndex, int position)
    {
        return channelBuilders.get(precomputedHashChannel.getAsInt()).get(sliceIndex).getLong(position, 0);
    }

    private boolean positionNotDistinctFromCurrentRow(long address, int hashPosition, int position, Page page, byte rawHash, int[] hashChannels)
    {
        if (rawHashByHashPosition[hashPosition] != rawHash) {
            return false;
        }
        return hashStrategy.positionNotDistinctFromRow(decodeSliceIndex(address), decodePosition(address), position, page, hashChannels);
    }

    private static long getHashPosition(long rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int calculateFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (calculateFill == hashSize) {
            calculateFill--;
        }
        checkArgument(hashSize > calculateFill, "hashSize must be larger than calculateFill");
        return calculateFill;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        MultiChannelGroupByHashState myState = new MultiChannelGroupByHashState();
        myState.currentPageBuilder = currentPageBuilder.capture(serdeProvider);

        myState.completedPagesMemorySize = completedPagesMemorySize;

        myState.hashCapacity = hashCapacity;
        myState.maxFill = maxFill;
        myState.mask = mask;
        myState.groupAddressByHash = Arrays.copyOf(groupAddressByHash, groupAddressByHash.length);
        myState.groupIdsByHash = Arrays.copyOf(groupIdsByHash, groupIdsByHash.length);
        myState.rawHashByHashPosition = Arrays.copyOf(rawHashByHashPosition, rawHashByHashPosition.length);
        myState.groupAddressByGroupId = groupAddressByGroupId.capture(serdeProvider);

        myState.nextGroupId = nextGroupId;
        if (dictionaryLookBack != null) {
            myState.dictionaryLookBack = dictionaryLookBack.capture(serdeProvider);
        }
        myState.hashCollisions = hashCollisions;
        myState.expectedHashCollisions = expectedHashCollisions;
        myState.preallocatedMemoryInBytes = preallocatedMemoryInBytes;
        myState.currentPageSizeInBytes = currentPageSizeInBytes;

        myState.channelBuilders = new byte[channelBuilders.size()][][];
        for (int i = 0; i < channelBuilders.size(); i++) {
            if (channelBuilders.get(i).size() > 0) {
                // The last block in channelBuilder[i] is always in currentPageBuilder
                myState.channelBuilders[i] = new byte[channelBuilders.get(i).size() - 1][];
                for (int j = 0; j < channelBuilders.get(i).size() - 1; j++) {
                    SliceOutput sliceOutput = new DynamicSliceOutput(1);
                    serdeProvider.getBlockEncodingSerde().writeBlock(sliceOutput, channelBuilders.get(i).get(j));
                    myState.channelBuilders[i][j] = sliceOutput.getUnderlyingSlice().getBytes();
                }
            }
        }
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MultiChannelGroupByHashState myState = (MultiChannelGroupByHashState) state;
        this.currentPageBuilder.restore(myState.currentPageBuilder, serdeProvider);

        this.completedPagesMemorySize = myState.completedPagesMemorySize;

        this.hashCapacity = myState.hashCapacity;
        this.maxFill = myState.maxFill;
        this.mask = myState.mask;
        this.groupAddressByHash = myState.groupAddressByHash;
        this.groupIdsByHash = myState.groupIdsByHash;
        this.rawHashByHashPosition = myState.rawHashByHashPosition;

        this.groupAddressByGroupId.restore(myState.groupAddressByGroupId, serdeProvider);

        this.nextGroupId = myState.nextGroupId;
        if (myState.dictionaryLookBack != null) {
            Slice input = Slices.wrappedBuffer(((DictionaryLookBack.DictionaryLookBackState) myState.dictionaryLookBack).dictionary);
            this.dictionaryLookBack = new DictionaryLookBack(serdeProvider.getBlockEncodingSerde().readBlock(input.getInput()));
            this.dictionaryLookBack.restore(myState.dictionaryLookBack, serdeProvider);
        }
        else {
            this.dictionaryLookBack = null;
        }
        this.hashCollisions = myState.hashCollisions;
        this.expectedHashCollisions = myState.expectedHashCollisions;
        this.preallocatedMemoryInBytes = myState.preallocatedMemoryInBytes;
        this.currentPageSizeInBytes = myState.currentPageSizeInBytes;

        checkState(myState.channelBuilders.length == this.channelBuilders.size());
        for (int i = 0; i < myState.channelBuilders.length; i++) {
            if (myState.channelBuilders[i] != null) {
                this.channelBuilders.get(i).clear();
                for (int j = 0; j < myState.channelBuilders[i].length; j++) {
                    Slice input = Slices.wrappedBuffer(myState.channelBuilders[i][j]);
                    this.channelBuilders.get(i).add(serdeProvider.getBlockEncodingSerde().readBlock(input.getInput()));
                }
                this.channelBuilders.get(i).add(this.currentPageBuilder.getBlockBuilder(i));
            }
        }
    }

    private static class MultiChannelGroupByHashState
            implements Serializable
    {
        private Object currentPageBuilder;

        private long completedPagesMemorySize;

        private int hashCapacity;
        private int maxFill;
        private int mask;
        private long[] groupAddressByHash;
        private int[] groupIdsByHash;
        private byte[] rawHashByHashPosition;

        private Object groupAddressByGroupId;

        private int nextGroupId;
        private Object dictionaryLookBack;
        private long hashCollisions;
        private double expectedHashCollisions;
        private long preallocatedMemoryInBytes;
        private long currentPageSizeInBytes;

        private byte[][][] channelBuilders;
    }
}
