/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.type.BigintOperators;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static java.lang.Math.toIntExact;

@RestorableConfig(uncapturedFields = {"sortedValuesByGroupId", "prevValue", "isNoGroupAdded", "addNewGroup"})
public class BigintGroupBySort
        extends BigintGroupBy implements GroupBySort
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BigintGroupBySort.class).instanceSize();

    // reverse index from the groupId back to the value
    private final LongArrayList sortedValuesByGroupId;

    private long prevValue;
    boolean addNewGroup;

    public BigintGroupBySort(int hashChannel, boolean outputRawHash, int expectedSize, UpdateMemory updateMemory)
    {
        super(hashChannel, outputRawHash);
        checkArgument(hashChannel >= 0, "hashChannel must be at least zero");
        checkArgument(expectedSize > 0, "expectedSize must be greater than zero");
        maxFill = expectedSize;
        sortedValuesByGroupId = new LongArrayList(expectedSize);
        this.prevValue = -1;
        this.addNewGroup = true;
        this.nullGroupId = -1;
    }

    @Override
    public List<Type> getTypes()
    {
        return outputRawHash ? TYPES_WITH_RAW_HASH : TYPES;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + sortedValuesByGroupId.size();
    }

    @Override
    public int getGroupCount()
    {
        return nextGroupId;
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
            BIGINT.writeLong(blockBuilder, sortedValuesByGroupId.get(groupId));
        }

        if (outputRawHash) {
            BlockBuilder hashBlockBuilder = pageBuilder.getBlockBuilder(outputChannelOffset + 1);
            if (groupId == nullGroupId) {
                BIGINT.writeLong(hashBlockBuilder, NULL_HASH_CODE);
            }
            else {
                BIGINT.writeLong(hashBlockBuilder, BigintOperators.hashCode(sortedValuesByGroupId.get(groupId)));
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
        return false;
    }

    @VisibleForTesting
    @Override
    public int getCapacity()
    {
        return maxFill;
    }

    public int putIfAbsent(int position, Block block)
    {
        if (block.isNull(position)) {
            if (nullGroupId < 0) {
                // set null group id
                nullGroupId = addNewGroupForNullValue();
            }
            prevValue = 0;
            addNewGroup = true;
            return nullGroupId;
        }

        long value = BIGINT.getLong(block, position);
        //1) there is chance 1st value is -1, prevValue is also -1 which is assigned in constructor, & addNewGroup will be true.
        //2) previous values is null group and next value is 0. in this case both prevValue & value will be 0 ,
        // but addNewGroup will be true which is set in isNull if block
        if ((false == addNewGroup) && (prevValue == value)) {
            return nextGroupId - 1;
        }
        addNewGroup = false;
        prevValue = value;

        return addNewGroup(value);
    }

    private int addNewGroupForNullValue()
    {
        //store -1 in sortedValuesByGroupId, this not read. For null values it will call blockBuilder.appendNull();
        sortedValuesByGroupId.add(-1);
        return addNewGroup();
    }

    private int addNewGroup(long value)
    {
        sortedValuesByGroupId.add(value);
        return addNewGroup();
    }

    private int addNewGroup()
    {
        // record group id in hash
        int groupId = nextGroupId++;
        // increase capacity, if necessary
        if (needMoreCapacity()) {
            tryToIncreaseCapacity();
        }
        return groupId;
    }

    public boolean tryToIncreaseCapacity()
    {
        long newCapacityLong = maxFill * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of Array cannot exceed 1 billion entries");
        }
        int newCapacity = toIntExact(newCapacityLong);

        maxFill = newCapacity;
        this.sortedValuesByGroupId.ensureCapacity(maxFill);
        return true;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        BigintGroupBySortState myState = new BigintGroupBySortState();
        myState.maxFill = maxFill;
        myState.nextGroupId = nextGroupId;
        myState.currentPageSizeInBytes = currentPageSizeInBytes;
        myState.nullGroupId = nullGroupId;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BigintGroupBySortState myState = (BigintGroupBySortState) state;
        this.maxFill = myState.maxFill;
        this.nextGroupId = myState.nextGroupId;
        this.currentPageSizeInBytes = myState.currentPageSizeInBytes;
        this.nullGroupId = myState.nullGroupId;
    }

    protected static class BigintGroupBySortState
            implements Serializable
    {
        protected int maxFill;
        protected int nextGroupId;
        protected long currentPageSizeInBytes;
        protected int nullGroupId;
    }
}
