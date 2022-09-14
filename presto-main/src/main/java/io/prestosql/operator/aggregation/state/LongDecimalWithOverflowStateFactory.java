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

import io.airlift.slice.Slices;
import io.prestosql.array.BooleanBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;

public class LongDecimalWithOverflowStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowState>
{
    @Override
    public LongDecimalWithOverflowState createSingleState()
    {
        return new SingleLongDecimalWithOverflowState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowState> getSingleStateClass()
    {
        return SingleLongDecimalWithOverflowState.class;
    }

    @Override
    public LongDecimalWithOverflowState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowState> getGroupedStateClass()
    {
        return GroupedLongDecimalWithOverflowState.class;
    }

    public static class GroupedLongDecimalWithOverflowState
            extends AbstractGroupedAccumulatorState
            implements LongDecimalWithOverflowState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongDecimalWithOverflowState.class).instanceSize();
        protected final BooleanBigArray isNotNull = new BooleanBigArray();
        /**
         * Stores 128-bit decimals as pairs of longs
         */
        protected final LongBigArray unscaledDecimals = new LongBigArray();
        @Nullable
        protected LongBigArray overflows; // lazily initialized on the first overflow

        @Override
        public void ensureCapacity(long size)
        {
            isNotNull.ensureCapacity(size);
            unscaledDecimals.ensureCapacity(size * 2);
            if (overflows != null) {
                overflows.ensureCapacity(size);
            }
        }

        @Override
        public boolean isNotNull()
        {
            return isNotNull.get(getGroupId());
        }

        @Override
        public void setNotNull()
        {
            isNotNull.set(getGroupId(), true);
        }

        @Override
        public long[] getDecimalArray()
        {
            return unscaledDecimals.getSegment(getGroupId() * 2);
        }

        @Override
        public int getDecimalArrayOffset()
        {
            return unscaledDecimals.getOffset(getGroupId() * 2);
        }

        @Override
        public long getOverflow()
        {
            if (overflows == null) {
                return 0;
            }
            return overflows.get(getGroupId());
        }

        @Override
        public void setOverflow(long overflow)
        {
            // setOverflow(0) must overwrite any existing overflow value
            if (overflow == 0 && overflows == null) {
                return;
            }
            long groupId = getGroupId();
            if (overflows == null) {
                overflows = new LongBigArray();
                overflows.ensureCapacity(isNotNull.getCapacity());
            }
            overflows.set(groupId, overflow);
        }

        @Override
        public void addOverflow(long overflow)
        {
            if (overflow != 0) {
                long groupId = getGroupId();
                if (overflows == null) {
                    overflows = new LongBigArray();
                    overflows.ensureCapacity(isNotNull.getCapacity());
                }
                overflows.add(groupId, overflow);
            }
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + isNotNull.sizeOf() + unscaledDecimals.sizeOf() + (overflows == null ? 0 : overflows.sizeOf());
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowStateState myState = new GroupedLongDecimalWithOverflowStateState();
            myState.unscaledDecimals = unscaledDecimals.capture(serdeProvider);
            myState.overflows = overflows.capture(serdeProvider);
            myState.baseState = super.capture(serdeProvider);
            myState.isNotNull = isNotNull.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowStateState myState = (GroupedLongDecimalWithOverflowStateState) state;
            Function<Object, Object> unscaledDecimalsRestore = content -> Slices.wrappedBuffer((byte[]) content);
            this.unscaledDecimals.restore(myState.unscaledDecimals, serdeProvider);
            this.overflows.restore(myState.overflows, serdeProvider);
            this.isNotNull.restore(myState.isNotNull, serdeProvider);
            super.restore(myState.baseState, serdeProvider);
        }

        private static class GroupedLongDecimalWithOverflowStateState
                implements Serializable
        {
            private Object unscaledDecimals;
            private Object overflows;
            private Object baseState;
            private Object isNotNull;
        }
    }

    public static class SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongDecimalWithOverflowState.class).instanceSize();
        protected static final int SIZE = (int) sizeOf(new long[2]);

        protected long[] unscaledDecimal = new long[2];
        protected boolean isNotNull;
        protected long overflow;

        @Override
        public boolean isNotNull()
        {
            return isNotNull;
        }

        @Override
        public void setNotNull()
        {
            isNotNull = true;
        }

        @Override
        public long[] getDecimalArray()
        {
            return unscaledDecimal;
        }

        @Override
        public int getDecimalArrayOffset()
        {
            return 0;
        }

        @Override
        public long getOverflow()
        {
            return overflow;
        }

        @Override
        public void setOverflow(long overflow)
        {
            this.overflow = overflow;
        }

        @Override
        public void addOverflow(long overflow)
        {
            this.overflow += overflow;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowStateState myState = new SingleLongDecimalWithOverflowStateState();
            myState.unscaledDecimal = unscaledDecimal;
            myState.overflow = overflow;
            myState.isNotNull = isNotNull;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowStateState myState = (SingleLongDecimalWithOverflowStateState) state;
            this.unscaledDecimal = myState.unscaledDecimal;
            this.overflow = myState.overflow;
            this.isNotNull = myState.isNotNull;
        }

        private static class SingleLongDecimalWithOverflowStateState
                implements Serializable
        {
            private long[] unscaledDecimal;
            private long overflow;
            private boolean isNotNull;
        }
    }
}
