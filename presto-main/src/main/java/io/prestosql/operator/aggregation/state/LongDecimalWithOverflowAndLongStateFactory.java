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

import io.prestosql.array.LongBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;

public class LongDecimalWithOverflowAndLongStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowAndLongState>
{
    @Override
    public LongDecimalWithOverflowAndLongState createSingleState()
    {
        return new SingleLongDecimalWithOverflowAndLongState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowAndLongState> getSingleStateClass()
    {
        return SingleLongDecimalWithOverflowAndLongState.class;
    }

    @Override
    public LongDecimalWithOverflowAndLongState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowAndLongState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowAndLongState> getGroupedStateClass()
    {
        return GroupedLongDecimalWithOverflowAndLongState.class;
    }

    public static class GroupedLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.GroupedLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongDecimalWithOverflowAndLongState.class).instanceSize();
        private final LongBigArray longs = new LongBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            longs.ensureCapacity(size);
            super.ensureCapacity(size);
        }

        @Override
        public long getLong()
        {
            return longs.get(getGroupId());
        }

        @Override
        public void setLong(long value)
        {
            longs.set(getGroupId(), value);
        }

        @Override
        public void addLong(long value)
        {
            longs.add(getGroupId(), value);
        }

        @Override
        public void reset()
        {
            super.reset();
            longs.reset(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + isNotNull.sizeOf() + unscaledDecimals.sizeOf() + (overflows == null ? 0 : overflows.sizeOf());
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowAndLongStateState myState = new GroupedLongDecimalWithOverflowAndLongStateState();
            myState.longs = longs.capture(serdeProvider);
            myState.baseState = super.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowAndLongStateState myState = (GroupedLongDecimalWithOverflowAndLongStateState) state;
            this.longs.restore(myState.longs, serdeProvider);
            super.restore(myState.baseState, serdeProvider);
        }

        private static class GroupedLongDecimalWithOverflowAndLongStateState
                implements Serializable
        {
            private Object longs;
            private Object baseState;
        }
    }

    public static class SingleLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongDecimalWithOverflowAndLongState.class).instanceSize();

        protected long longValue;

        @Override
        public long getLong()
        {
            return longValue;
        }

        @Override
        public void setLong(long longValue)
        {
            this.longValue = longValue;
        }

        @Override
        public void addLong(long value)
        {
            longValue += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowAndLongStateState myState = new SingleLongDecimalWithOverflowAndLongStateState();
            myState.baseState = super.capture(serdeProvider);
            myState.longValue = longValue;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowAndLongStateState myState = (SingleLongDecimalWithOverflowAndLongStateState) state;
            super.restore(myState.baseState, serdeProvider);
            this.longValue = myState.longValue;
        }

        private static class SingleLongDecimalWithOverflowAndLongStateState
                implements Serializable
        {
            private Object baseState;
            private long longValue;
        }
    }
}
