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
package io.prestosql.operator.aggregation;

import io.prestosql.array.IntBigArray;
import io.prestosql.array.ShortBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Verify.verify;

/**
 * Instances of this state use a single PageBuilder for all groups.
 */
@RestorableConfig(uncapturedFields = {"currentPageBuilder"})
public abstract class AbstractGroupCollectionAggregationState<T>
        extends AbstractGroupedAccumulatorState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractGroupCollectionAggregationState.class).instanceSize();
    private static final int MAX_NUM_BLOCKS = 30000;
    private static final short NULL = -1;

    private final ShortBigArray headBlockIndex;
    private final IntBigArray headPosition;

    private final ShortBigArray nextBlockIndex;
    private final IntBigArray nextPosition;

    private final ShortBigArray tailBlockIndex;
    private final IntBigArray tailPosition;

    private final List<PageBuilder> values;
    private final LongList sumPositions;
    private final IntBigArray groupEntryCount;
    private PageBuilder currentPageBuilder;

    private long valueBlocksRetainedSizeInBytes;
    private long totalPositions;
    private long capacity;

    protected AbstractGroupCollectionAggregationState(PageBuilder pageBuilder)
    {
        this.headBlockIndex = new ShortBigArray(NULL);
        this.headPosition = new IntBigArray(NULL);
        this.nextBlockIndex = new ShortBigArray(NULL);
        this.nextPosition = new IntBigArray(NULL);
        this.tailBlockIndex = new ShortBigArray(NULL);
        this.tailPosition = new IntBigArray(NULL);

        this.currentPageBuilder = pageBuilder;
        this.values = new ArrayList<>();
        this.sumPositions = new LongArrayList();
        this.groupEntryCount = new IntBigArray();
        values.add(currentPageBuilder);
        sumPositions.add(0L);
        valueBlocksRetainedSizeInBytes = 0;

        totalPositions = 0;
        capacity = 1024;
        nextBlockIndex.ensureCapacity(capacity);
        nextPosition.ensureCapacity(capacity);
        groupEntryCount.ensureCapacity(capacity);
    }

    @Override
    public void ensureCapacity(long size)
    {
        headBlockIndex.ensureCapacity(size);
        headPosition.ensureCapacity(size);
        tailBlockIndex.ensureCapacity(size);
        tailPosition.ensureCapacity(size);
        groupEntryCount.ensureCapacity(size);
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE +
                headBlockIndex.sizeOf() +
                headPosition.sizeOf() +
                tailBlockIndex.sizeOf() +
                tailPosition.sizeOf() +
                nextBlockIndex.sizeOf() +
                nextPosition.sizeOf() +
                groupEntryCount.sizeOf() +
                valueBlocksRetainedSizeInBytes +
                // valueBlocksRetainedSizeInBytes doesn't contain the current block builder
                currentPageBuilder.getRetainedSizeInBytes();
    }

    /**
     * This method should be called before {@link #appendAtChannel(int, Block, int)} to update the internal linked list, where
     * {@link #appendAtChannel(int, Block, int)} is called for each channel that has a new entry to be added.
     */
    protected final void prepareAdd()
    {
        if (currentPageBuilder.isFull()) {
            valueBlocksRetainedSizeInBytes += currentPageBuilder.getRetainedSizeInBytes();
            sumPositions.add(totalPositions);
            currentPageBuilder = currentPageBuilder.newPageBuilderLike();
            values.add(currentPageBuilder);

            verify(values.size() <= MAX_NUM_BLOCKS);
        }

        long currentGroupId = getGroupId();
        short insertedBlockIndex = (short) (values.size() - 1);
        int insertedPosition = currentPageBuilder.getPositionCount();

        if (totalPositions == capacity) {
            capacity *= 1.5;
            nextBlockIndex.ensureCapacity(capacity);
            nextPosition.ensureCapacity(capacity);
        }

        if (isEmpty()) {
            // new linked list, set up the header pointer
            headBlockIndex.set(currentGroupId, insertedBlockIndex);
            headPosition.set(currentGroupId, insertedPosition);
        }
        else {
            // existing linked list, link the new entry to the tail
            long absoluteTailAddress = toAbsolutePosition(tailBlockIndex.get(currentGroupId), tailPosition.get(currentGroupId));
            nextBlockIndex.set(absoluteTailAddress, insertedBlockIndex);
            nextPosition.set(absoluteTailAddress, insertedPosition);
        }
        tailBlockIndex.set(currentGroupId, insertedBlockIndex);
        tailPosition.set(currentGroupId, insertedPosition);
        groupEntryCount.increment(currentGroupId);
        currentPageBuilder.declarePosition();
        totalPositions++;
    }

    protected final void appendAtChannel(int channel, Block block, int position)
    {
        currentPageBuilder.getType(channel).appendTo(block, position, currentPageBuilder.getBlockBuilder(channel));
    }

    public void forEach(T consumer)
    {
        short currentBlockId = headBlockIndex.get(getGroupId());
        int currentPosition = headPosition.get(getGroupId());
        while (currentBlockId != NULL) {
            accept(consumer, values.get(currentBlockId), currentPosition);

            long absoluteCurrentAddress = toAbsolutePosition(currentBlockId, currentPosition);
            currentBlockId = nextBlockIndex.get(absoluteCurrentAddress);
            currentPosition = nextPosition.get(absoluteCurrentAddress);
        }
    }

    public boolean isEmpty()
    {
        return headBlockIndex.get(getGroupId()) == NULL;
    }

    public final int getEntryCount()
    {
        return groupEntryCount.get(getGroupId());
    }

    private long toAbsolutePosition(short blockId, int position)
    {
        return sumPositions.get(blockId) + position;
    }

    protected abstract void accept(T consumer, PageBuilder pageBuilder, int currentPosition);

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        AbstractGroupCollectionAggregationStateState myState = new AbstractGroupCollectionAggregationStateState();
        myState.headBlockIndex = headBlockIndex.capture(serdeProvider);
        myState.headPosition = headPosition.capture(serdeProvider);
        myState.nextBlockIndex = nextBlockIndex.capture(serdeProvider);
        myState.nextPosition = nextPosition.capture(serdeProvider);
        myState.tailBlockIndex = tailBlockIndex.capture(serdeProvider);
        myState.tailPosition = tailPosition.capture(serdeProvider);
        myState.values = new Object[values.size()];
        for (int i = 0; i < values.size(); i++) {
            myState.values[i] = values.get(i).capture(serdeProvider);
        }
        myState.sumPositions = sumPositions;
        myState.groupEntryCount = groupEntryCount.capture(serdeProvider);
        myState.valueBlocksRetainedSizeInBytes = valueBlocksRetainedSizeInBytes;
        myState.totalPositions = totalPositions;
        myState.capacity = capacity;
        myState.baseState = super.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        AbstractGroupCollectionAggregationStateState myState = (AbstractGroupCollectionAggregationStateState) state;
        this.headBlockIndex.restore(myState.headBlockIndex, serdeProvider);
        this.headPosition.restore(myState.headPosition, serdeProvider);
        this.nextBlockIndex.restore(myState.nextBlockIndex, serdeProvider);
        this.nextPosition.restore(myState.nextPosition, serdeProvider);
        this.tailBlockIndex.restore(myState.tailBlockIndex, serdeProvider);
        this.tailPosition.restore(myState.tailPosition, serdeProvider);
        this.values.clear();
        for (int i = 0; i < myState.values.length; i++) {
            values.add(currentPageBuilder.newPageBuilderLike());
            values.get(i).restore(myState.values[i], serdeProvider);
        }
        this.currentPageBuilder = values.get(values.size() - 1);
        this.sumPositions.clear();
        this.sumPositions.addAll(myState.sumPositions);
        this.groupEntryCount.restore(myState.groupEntryCount, serdeProvider);
        this.valueBlocksRetainedSizeInBytes = myState.valueBlocksRetainedSizeInBytes;
        this.totalPositions = myState.totalPositions;
        this.capacity = myState.capacity;
        super.restore(myState.baseState, serdeProvider);
    }

    private static class AbstractGroupCollectionAggregationStateState
            implements Serializable
    {
        private Object headBlockIndex;
        private Object headPosition;
        private Object nextBlockIndex;
        private Object nextPosition;
        private Object tailBlockIndex;
        private Object tailPosition;
        private Object[] values;
        private LongList sumPositions;
        private Object groupEntryCount;
        private long valueBlocksRetainedSizeInBytes;
        private long totalPositions;
        private long capacity;
        private Object baseState;
    }
}
