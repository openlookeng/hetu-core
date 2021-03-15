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
package io.prestosql.operator.aggregation.state;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.aggregation.BlockComparator;
import io.prestosql.operator.aggregation.TypedHeap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.function.Function;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.lang.Math.toIntExact;

public class MinMaxNStateSerializer
        implements AccumulatorStateSerializer<MinMaxNState>
{
    private final BlockComparator blockComparator;
    private final Type elementType;
    private final ArrayType arrayType;
    private final Type serializedType;

    public MinMaxNStateSerializer(BlockComparator blockComparator, Type elementType)
    {
        this.blockComparator = blockComparator;
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
        this.serializedType = RowType.anonymous(ImmutableList.of(BIGINT, arrayType));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MinMaxNState state, BlockBuilder out)
    {
        TypedHeap heap = state.getTypedHeap();
        if (heap == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();
        BIGINT.writeLong(blockBuilder, heap.getCapacity());
        BlockBuilder elements = blockBuilder.beginBlockEntry();
        heap.writeAll(elements);
        blockBuilder.closeEntry();

        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, MinMaxNState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        int capacity = toIntExact(BIGINT.getLong(currentBlock, 0));
        Block heapBlock = arrayType.getObject(currentBlock, 1);
        TypedHeap heap = new TypedHeap(blockComparator, elementType, capacity);
        heap.addAll(heapBlock);
        state.setTypedHeap(heap);
    }

    @Override
    public Object serializeCapture(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MinMaxNState minMaxNState = (MinMaxNState) state;
        boolean grouped = minMaxNState instanceof MinMaxNStateFactory.GroupedMinMaxNState;
        if (!grouped) {
            if (minMaxNState.getTypedHeap() != null) {
                return minMaxNState.getTypedHeap().capture(serdeProvider);
            }
            return null;
        }
        GroupedMinMaxNStateState myState = new GroupedMinMaxNStateState();
        MinMaxNStateFactory.GroupedMinMaxNState groupedState = (MinMaxNStateFactory.GroupedMinMaxNState) minMaxNState;
        myState.size = groupedState.getSize();
        myState.groupId = groupedState.getStateGroupId();
        Function<Object, Object> heapsCapture = content -> ((TypedHeap) content).capture(serdeProvider);
        myState.heaps = groupedState.getHeaps().capture(heapsCapture);
        return myState;
    }

    @Override
    public void deserializeRestore(Object snapshot, Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MinMaxNState minMaxNState = (MinMaxNState) state;
        boolean grouped = minMaxNState instanceof MinMaxNStateFactory.GroupedMinMaxNState;
        if (!grouped) {
            if (snapshot != null) {
                minMaxNState.setTypedHeap(TypedHeap.restoreTypedHeap(blockComparator, elementType, snapshot, serdeProvider));
            }
            else {
                minMaxNState.setTypedHeap(null);
            }
        }
        else {
            GroupedMinMaxNStateState myState = (GroupedMinMaxNStateState) snapshot;
            MinMaxNStateFactory.GroupedMinMaxNState groupedState = (MinMaxNStateFactory.GroupedMinMaxNState) minMaxNState;
            groupedState.setSize(myState.size);
            groupedState.setStateGroupId(myState.groupId);
            Function<Object, Object> heapsRestore = content -> TypedHeap.restoreTypedHeap(blockComparator, elementType, content, serdeProvider);
            groupedState.getHeaps().restore(heapsRestore, myState.heaps);
        }
    }

    private static class GroupedMinMaxNStateState
            implements Serializable
    {
        private Object heaps;
        private long size;
        private long groupId;
    }
}
