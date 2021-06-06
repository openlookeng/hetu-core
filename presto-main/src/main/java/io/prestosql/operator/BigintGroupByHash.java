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
import io.prestosql.array.IntBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.type.BigintOperators;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static io.prestosql.util.HashCollisionsEstimator.estimateNumberOfHashCollisions;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"updateMemory"})
public class BigintGroupByHash
        extends BigintGroupBy implements GroupByHash
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupByHash.class).instanceSize();

    private int hashCapacity;
    private int mask;

    // the hash table from values to groupIds
    private LongBigArray values;
    private IntBigArray groupIds;

    // reverse index from the groupId back to the value
    private final LongBigArray valuesByGroupId;

    private long hashCollisions;
    private double expectedHashCollisions;

    // reserve enough memory before rehash
    private final UpdateMemory updateMemory;
    private long preallocatedMemoryInBytes;

    public BigintGroupByHash(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        super(hashChannel, outputRawHash);
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");

        hashCapacity = arraySize(expectedSize, FILL_RATIO);

        maxFill = calculateMaxFill(hashCapacity);
        mask = hashCapacity - 1;
        values = new LongBigArray();
        values.ensureCapacity(hashCapacity);
        groupIds = new IntBigArray(-1);
        groupIds.ensureCapacity(hashCapacity);

        valuesByGroupId = new LongBigArray();
        valuesByGroupId.ensureCapacity(hashCapacity);

        // This interface is used for actively reserving memory (push model) for rehash.
        // The caller can also query memory usage on this object (pull model)
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");
    }

    @Override
    public List<Type> getTypes()
    {
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                groupIds.sizeOf() +
                values.sizeOf() +
                valuesByGroupId.sizeOf() +
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
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        checkArgument(groupId >= 0, "groupId is negative");
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset);
        if (groupId == nullGroupId) {
            blockBuilder.appendNull();
        }
        else {
            BIGINT.writeLong(blockBuilder, valuesByGroupId.get(groupId));
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, BigintOperators.hashCode(valuesByGroupId.get(groupId)));
            }
        }
    }

    @Override
    public Work<?> addPage(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new AddPageWork(page.getBlock(hashChannel), this);
    }

    @Override
    public Work<GroupByIdBlock> getGroupIds(Page page)
    {
        currentPageSizeInBytes = page.getRetainedSizeInBytes();
        return new GetGroupIdsWork(page.getBlock(hashChannel), this);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        Block block = page.getBlock(hashChannel);
        if (block.isNull(position)) {
            return nullGroupId >= 0;
        }

        long value = BIGINT.getLong(block, position);
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                return false;
            }
            if (value == values.get(hashPosition)) {
                return true;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
        }
    }

    @Override
    public long getRawHash(int groupId)
    {
        return BigintType.hash(valuesByGroupId.get(groupId));
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return hashCapacity;
    }

    public int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = nextGroupId++;
            }

            // increase capacity, if necessary. after nextGroupId++, it maybe equals maxFill, so need to check whether need rehash
            if (needMoreCapacity()) {
                tryToIncreaseCapacity();
            }
            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        long hashPosition = getHashPosition(value, mask);

        // look for an empty slot or a slot containing this key
        while (true) {
            int groupId = groupIds.get(hashPosition);
            if (groupId == -1) {
                break;
            }

            if (value == values.get(hashPosition)) {
                return groupId;
            }

            // increment position and mask to handle wrap around
            hashPosition = (hashPosition + 1) & mask;
            hashCollisions++;
        }

        return addNewGroup(hashPosition, value);
    }

    private int addNewGroup(long hashPosition, long value)
    {
        // record group id in hash
        int groupId = nextGroupId++;

        values.set(hashPosition, value);
        valuesByGroupId.set(groupId, value);
        groupIds.set(hashPosition, groupId);

        // increase capacity, if necessary
        if (needMoreCapacity()) {
            tryToIncreaseCapacity();
        }
        return groupId;
    }

    public boolean tryToIncreaseCapacity()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        // An estimate of how much extra memory is needed before we can go ahead and expand the hash table.
        // This includes the new capacity for values, groupIds, and valuesByGroupId as well as the size of the current page
        preallocatedMemoryInBytes = (newCapacity - hashCapacity) * (long) (Long.BYTES + Integer.BYTES) + (calculateMaxFill(newCapacity) - maxFill) * Long.BYTES + currentPageSizeInBytes;
        if (!updateMemory.update()) {
            // reserved memory but has exceeded the limit
            return false;
        }
        preallocatedMemoryInBytes = 0;

        expectedHashCollisions += estimateNumberOfHashCollisions(getGroupCount(), hashCapacity);

        int newMask = newCapacity - 1;
        LongBigArray newValues = new LongBigArray();
        newValues.ensureCapacity(newCapacity);
        IntBigArray newGroupIds = new IntBigArray(-1);
        newGroupIds.ensureCapacity(newCapacity);

        for (int groupId = 0; groupId < nextGroupId; groupId++) {
            if (groupId == nullGroupId) {
                continue;
            }
            long value = valuesByGroupId.get(groupId);

            // find an empty slot for the address
            long hashPosition = getHashPosition(value, newMask);
            while (newGroupIds.get(hashPosition) != -1) {
                hashPosition = (hashPosition + 1) & newMask;
                hashCollisions++;
            }

            // record the mapping
            newValues.set(hashPosition, value);
            newGroupIds.set(hashPosition, groupId);
        }

        mask = newMask;
        hashCapacity = newCapacity;
        maxFill = calculateMaxFill(hashCapacity);
        values = newValues;
        groupIds = newGroupIds;

        this.valuesByGroupId.ensureCapacity(maxFill);
        return true;
    }

    private static long getHashPosition(long rawHash, int mask)
    {
        return murmurHash3(rawHash) & mask;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        BigintGroupByHashState myState = new BigintGroupByHashState();
        myState.hashCapacity = hashCapacity;
        myState.maxFill = maxFill;
        myState.mask = mask;
        myState.values = values.capture(serdeProvider);
        myState.groupIds = groupIds.capture(serdeProvider);
        myState.nullGroupId = nullGroupId;
        myState.valuesByGroupId = valuesByGroupId.capture(serdeProvider);
        myState.nextGroupId = nextGroupId;
        myState.hashCollisions = hashCollisions;
        myState.expectedHashCollisions = expectedHashCollisions;
        myState.preallocatedMemoryInBytes = preallocatedMemoryInBytes;
        myState.currentPageSizeInBytes = currentPageSizeInBytes;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BigintGroupByHashState myState = (BigintGroupByHashState) state;
        this.hashCapacity = myState.hashCapacity;
        this.maxFill = myState.maxFill;
        this.mask = myState.mask;
        this.values.restore(myState.values, serdeProvider);
        this.groupIds.restore(myState.groupIds, serdeProvider);
        this.nullGroupId = myState.nullGroupId;
        this.valuesByGroupId.restore(myState.valuesByGroupId, serdeProvider);
        this.nextGroupId = myState.nextGroupId;
        this.hashCollisions = myState.hashCollisions;
        this.expectedHashCollisions = myState.expectedHashCollisions;
        this.preallocatedMemoryInBytes = myState.preallocatedMemoryInBytes;
        this.currentPageSizeInBytes = myState.currentPageSizeInBytes;
    }

    private static class BigintGroupByHashState
            implements Serializable
    {
        private int hashCapacity;
        private int maxFill;
        private int mask;
        private Object values;
        private Object groupIds;
        private int nullGroupId;
        private Object valuesByGroupId;
        private int nextGroupId;
        private long hashCollisions;
        private double expectedHashCollisions;
        private long preallocatedMemoryInBytes;
        private long currentPageSizeInBytes;
    }
}
