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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.operator.scalar.CombineHashFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.optimizations.HashGenerationOptimizer;
import io.prestosql.type.TypeUtils;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.IntFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.type.BigintType.BIGINT;

// This implementation assumes arrays used in the hash are always a power of 2
@RestorableConfig(uncapturedFields = {"types", "hashTypes", "channels", "hashStrategy",
        "inputHashChannel", "hashGenerator", "updateMemory", "processDictionary"})
public class MultiChannelGroupBySort
        extends MultiChannelGroupBy implements GroupBySort
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MultiChannelGroupBySort.class).instanceSize();
    private static final long NULL_HASH_VALUE = 1;

    private PageBuilder currentPageBuilder;
    private long completedPagesMemorySize;
    private long rawPrevHash;
    private int nextSortBasedGroupId;
    private List<Integer> maxGroupId;
    private int nextGroupIdStartingRange;
    private int currentGroupIdStartingRange;
    private int newGroupId;
    private int sliceIndex;
    private long rawPrevNullHash;

    public MultiChannelGroupBySort(
            List<? extends Type> hashTypes,
            int[] hashChannels,
            Optional<Integer> inputHashChannel,
            int expectedSize,
            boolean processDictionary,
            JoinCompiler joinCompiler)
    {
        super(hashTypes, hashChannels, inputHashChannel, expectedSize, processDictionary, joinCompiler);
        startNewPage();

        this.rawPrevHash = -1;
        this.rawPrevNullHash = -1;
        this.maxGroupId = new ArrayList<>();
        this.currentGroupIdStartingRange = Integer.MAX_VALUE;
        this.nextGroupIdStartingRange = Integer.MAX_VALUE;
        this.newGroupId = 0;
        this.sliceIndex = 0;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                (sizeOf(channelBuilders.get(0).elements()) * channelBuilders.size()) +
                completedPagesMemorySize +
                currentPageBuilder.getRetainedSizeInBytes();
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public int getGroupCount()
    {
        return nextSortBasedGroupId;
    }

    @Override
    public void appendValuesTo(int groupId, PageBuilder pageBuilder, int outputChannelOffset)
    {
        int currentSliceIndex = sliceIndex;
        if (groupId == nextGroupIdStartingRange) {
            currentGroupIdStartingRange = nextGroupIdStartingRange;
            if (maxGroupId.size() != 0) {
                nextGroupIdStartingRange = maxGroupId.get(0);
                maxGroupId.remove(0);
            }
            else {
                nextGroupIdStartingRange = Integer.MAX_VALUE;
            }
            sliceIndex++;
            groupId = newGroupId == 0 ? groupId : newGroupId;
            newGroupId = 0;
        }
        else if (groupId > currentGroupIdStartingRange) {
            groupId = newGroupId++;
        }

        int blockIndex = currentSliceIndex;
        int position = groupId;
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
            return new MultiChannelGroupBySort.GetRunLengthEncodedGroupIdsWork(page, this);
        }
        if (canProcessDictionary(page)) {
            return new MultiChannelGroupBySort.GetDictionaryGroupIdsWork(page, this);
        }

        return new MultiChannelGroupBySort.GetNonDictionaryGroupIdsWork(page, this);
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels)
    {
        //This is used for only for aggregation not for hashing
        return false;
    }

    @Override
    public boolean contains(int position, Page page, int[] hashChannels, long rawHash)
    {
        //This is used for only for aggregation not for hashing
        return false;
    }

    public boolean isNullHashPosition(int position, Page page)
    {
        for (int i = 0; i < channels.length; i++) {
            if (page.getBlock(channels[i]).isNull(position)) {
                return true;
            }
        }
        return false;
    }

    public long getHashValueForNullAndZeroPosition(int position, Page page)
    {
        return getHashValueForNullAndZeroPosition(position, page::getBlock);
    }

    public long getHashValueForNullAndZeroPosition(int position, IntFunction<Block> blockProvider)
    {
        // this method is called when hash channel have only Null and Zero values
        long result = HashGenerationOptimizer.INITIAL_HASH_VALUE;
        for (int i = 0; i < channels.length; i++) {
            Type type = hashTypes.get(i);
            Block block = blockProvider.apply(channels[i]);
            if (block.isNull(position)) {
                // for null we will set 1. So that we can differentiate between Null and 0
                result = CombineHashFunction.getHash(result, NULL_HASH_VALUE);
            }
            result = CombineHashFunction.getHash(result, TypeUtils.hashPosition(type, blockProvider.apply(channels[i]), position));
        }
        return result;
    }

    public int putIfAbsent(int position, Page page)
    {
        long rawHash = hashGenerator.hashPosition(position, page);
        return putIfAbsent(position, page, rawHash);
    }

    public int putIfAbsent(int position, Page page, long rawHash)
    {
        int groupId;
        if (rawHash == 0 && isNullHashPosition(position, page)) {
            long rawNullHash = getHashValueForNullAndZeroPosition(position, page);
            if (rawPrevNullHash == rawNullHash) {
                return nextSortBasedGroupId - 1;
            }
            else {
                rawPrevNullHash = rawNullHash;
                groupId = addNewGroup(position, page, rawHash);
                return groupId;
            }
        }

        if (rawPrevHash == -1) {
            groupId = addNewGroup(position, page, rawHash);
            rawPrevHash = rawHash;
            return groupId;
        }

        if (rawPrevHash == rawHash) {
            return nextSortBasedGroupId - 1;
        }

        rawPrevHash = rawHash;
        groupId = addNewGroup(position, page, rawHash);
        return groupId;
    }

    private int addNewGroup(int position, Page page, long rawHash)
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

        if (currentPageBuilder.isFull()) {
            startNewPage();
        }

        int groupId = nextSortBasedGroupId++;
        return groupId;
    }

    private void startNewPage()
    {
        if (currentPageBuilder != null) {
            completedPagesMemorySize += currentPageBuilder.getRetainedSizeInBytes();
            currentPageBuilder = currentPageBuilder.newPageBuilderLike();
            if (nextGroupIdStartingRange == Integer.MAX_VALUE) {
                nextGroupIdStartingRange = nextSortBasedGroupId;
            }
            else {
                maxGroupId.add(nextSortBasedGroupId);
            }
        }
        else {
            currentPageBuilder = new PageBuilder(types);
        }

        for (int i = 0; i < types.size(); i++) {
            channelBuilders.get(i).add(currentPageBuilder.getBlockBuilder(i));
        }
    }

    public boolean needMoreCapacity()
    {
        return false;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        MultiChannelGroupBySortState myState = new MultiChannelGroupBySortState();
        myState.currentPageBuilder = currentPageBuilder.capture(serdeProvider);
        myState.completedPagesMemorySize = completedPagesMemorySize;
        myState.nextSortBasedGroupId = nextSortBasedGroupId;
        if (dictionaryLookBack != null) {
            myState.dictionaryLookBack = dictionaryLookBack.capture(serdeProvider);
        }

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
        myState.rawPrevHash = rawPrevHash;
        myState.maxGroupId = maxGroupId;
        myState.nextGroupIdStartingRange = nextGroupIdStartingRange;
        myState.currentGroupIdStartingRange = currentGroupIdStartingRange;
        myState.sliceIndex = sliceIndex;
        myState.rawPrevNullHash = rawPrevNullHash;
        myState.newGroupId = newGroupId;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MultiChannelGroupBySortState myState = (MultiChannelGroupBySortState) state;
        this.currentPageBuilder.restore(myState.currentPageBuilder, serdeProvider);
        this.completedPagesMemorySize = myState.completedPagesMemorySize;
        myState.currentPageSizeInBytes = currentPageSizeInBytes;
        this.nextSortBasedGroupId = myState.nextSortBasedGroupId;
        if (myState.dictionaryLookBack != null) {
            Slice input = Slices.wrappedBuffer(((DictionaryLookBack.DictionaryLookBackState) myState.dictionaryLookBack).dictionary);
            this.dictionaryLookBack = new DictionaryLookBack(serdeProvider.getBlockEncodingSerde().readBlock(input.getInput()));
            this.dictionaryLookBack.restore(myState.dictionaryLookBack, serdeProvider);
        }
        else {
            this.dictionaryLookBack = null;
        }
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
        this.rawPrevHash = myState.rawPrevHash;
        this.maxGroupId = myState.maxGroupId;
        this.nextGroupIdStartingRange = myState.nextGroupIdStartingRange;
        this.currentGroupIdStartingRange = myState.currentGroupIdStartingRange;
        this.sliceIndex = myState.sliceIndex;
        this.rawPrevNullHash = myState.rawPrevNullHash;
        this.newGroupId = myState.newGroupId;
    }

    private static class MultiChannelGroupBySortState
            implements Serializable
    {
        private Object currentPageBuilder;
        private long completedPagesMemorySize;
        private int nextSortBasedGroupId;
        private Object dictionaryLookBack;
        private long currentPageSizeInBytes;
        private byte[][][] channelBuilders;
        private long rawPrevHash;
        private List<Integer> maxGroupId;
        private int nextGroupIdStartingRange;
        private int currentGroupIdStartingRange;
        private int sliceIndex;
        private long rawPrevNullHash;
        private int newGroupId;
    }
}
