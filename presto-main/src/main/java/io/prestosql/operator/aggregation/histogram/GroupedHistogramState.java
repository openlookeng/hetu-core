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

package io.prestosql.operator.aggregation.histogram;

import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;

/**
 * state object that uses a single histogram for all groups. See {@link GroupedTypedHistogram}
 */
public class GroupedHistogramState
        extends AbstractGroupedAccumulatorState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedHistogramState.class).instanceSize();
    private TypedHistogram typedHistogram;
    private long size;

    public GroupedHistogramState(Type keyType, int expectedEntriesCount)
    {
        typedHistogram = new GroupedTypedHistogram(keyType, expectedEntriesCount);
    }

    @Override
    public void ensureCapacity(long size)
    {
        typedHistogram.ensureCapacity(size);
    }

    @Override
    public TypedHistogram get()
    {
        return typedHistogram.setGroupId(getGroupId());
    }

    @Override
    public void deserialize(Block block, Type type, int expectedSize)
    {
        typedHistogram = new GroupedTypedHistogram(getGroupId(), block, type, expectedSize);
    }

    @Override
    public void addMemoryUsage(long memory)
    {
        size += memory;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + typedHistogram.getEstimatedSize();
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        GroupedHistogramStateState myState = new GroupedHistogramStateState();
        myState.baseState = super.capture(serdeProvider);
        myState.typedHistogram = typedHistogram.capture(serdeProvider);
        myState.size = size;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        GroupedHistogramStateState myState = (GroupedHistogramStateState) state;
        super.restore(myState.baseState, serdeProvider);
        this.typedHistogram.restore(myState.typedHistogram, serdeProvider);
        this.size = myState.size;
    }

    private static class GroupedHistogramStateState
            implements Serializable
    {
        private Object baseState;
        private Object typedHistogram;
        private long size;
    }
}
