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
package io.prestosql.operator.aggregation.minmaxby;

import io.prestosql.operator.aggregation.BlockComparator;
import io.prestosql.operator.aggregation.TypedKeyValueHeap;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.function.Function;

public class MinMaxByNStateSerializer
        implements AccumulatorStateSerializer<MinMaxByNState>
{
    private final BlockComparator blockComparator;
    private final Type keyType;
    private final Type valueType;
    private final Type serializedType;

    public MinMaxByNStateSerializer(BlockComparator blockComparator, Type keyType, Type valueType)
    {
        this.blockComparator = blockComparator;
        this.keyType = keyType;
        this.valueType = valueType;
        this.serializedType = TypedKeyValueHeap.getSerializedType(keyType, valueType);
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MinMaxByNState state, BlockBuilder out)
    {
        TypedKeyValueHeap heap = state.getTypedKeyValueHeap();
        if (heap == null) {
            out.appendNull();
            return;
        }

        heap.serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, MinMaxByNState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        state.setTypedKeyValueHeap(TypedKeyValueHeap.deserialize(currentBlock, keyType, valueType, blockComparator));
    }

    @Override
    public Object serializeCapture(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MinMaxByNState minMaxByNState = (MinMaxByNState) state;
        boolean grouped = minMaxByNState instanceof MinMaxByNStateFactory.GroupedMinMaxByNState;
        if (!grouped) {
            if (minMaxByNState.getTypedKeyValueHeap() != null) {
                return minMaxByNState.getTypedKeyValueHeap().capture(serdeProvider);
            }
            return null;
        }
        GroupedMinMaxByNStateState myState = new GroupedMinMaxByNStateState();
        MinMaxByNStateFactory.GroupedMinMaxByNState groupedState = (MinMaxByNStateFactory.GroupedMinMaxByNState) minMaxByNState;
        myState.size = groupedState.getSize();
        myState.groupId = groupedState.getStateGroupId();
        Function<Object, Object> heapsCapture = content -> ((TypedKeyValueHeap) content).capture(serdeProvider);
        myState.heaps = groupedState.getHeaps().capture(heapsCapture);
        return myState;
    }

    @Override
    public void deserializeRestore(Object snapshot, Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        MinMaxByNState minMaxByNState = (MinMaxByNState) state;
        boolean grouped = minMaxByNState instanceof MinMaxByNStateFactory.GroupedMinMaxByNState;
        if (!grouped) {
            if (snapshot != null) {
                minMaxByNState.setTypedKeyValueHeap(TypedKeyValueHeap.restoreTypedKeyValueHeap(blockComparator, keyType, valueType, snapshot, serdeProvider));
            }
            else {
                minMaxByNState.setTypedKeyValueHeap(null);
            }
        }
        else {
            GroupedMinMaxByNStateState myState = (GroupedMinMaxByNStateState) snapshot;
            MinMaxByNStateFactory.GroupedMinMaxByNState groupedState = (MinMaxByNStateFactory.GroupedMinMaxByNState) minMaxByNState;
            groupedState.setSize(myState.size);
            groupedState.setStateGroupId(myState.groupId);
            Function<Object, Object> heapsRestore = content -> TypedKeyValueHeap.restoreTypedKeyValueHeap(blockComparator, keyType, valueType, content, serdeProvider);
            groupedState.getHeaps().restore(heapsRestore, myState.heaps);
        }
    }

    private static class GroupedMinMaxByNStateState
            implements Serializable
    {
        private Object heaps;
        private long size;
        private long groupId;
    }
}
