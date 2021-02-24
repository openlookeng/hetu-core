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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.array.LongBigArray;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.function.Function;

import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static java.util.Objects.requireNonNull;

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
        protected final ObjectBigArray<Slice> unscaledDecimals = new ObjectBigArray<>();
        protected final LongBigArray overflows = new LongBigArray();
        protected long numberOfElements;

        @Override
        public void ensureCapacity(long size)
        {
            unscaledDecimals.ensureCapacity(size);
            overflows.ensureCapacity(size);
        }

        @Override
        public Slice getLongDecimal()
        {
            return unscaledDecimals.get(getGroupId());
        }

        @Override
        public void setLongDecimal(Slice value)
        {
            requireNonNull(value, "value is null");
            if (getLongDecimal() == null) {
                numberOfElements++;
            }
            unscaledDecimals.set(getGroupId(), value);
        }

        @Override
        public long getOverflow()
        {
            return overflows.get(getGroupId());
        }

        @Override
        public void setOverflow(long overflow)
        {
            overflows.set(getGroupId(), overflow);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + unscaledDecimals.sizeOf() + overflows.sizeOf() + numberOfElements * SingleLongDecimalWithOverflowState.SIZE;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowStateState myState = new GroupedLongDecimalWithOverflowStateState();
            Function<Object, Object> unscaledDecimalsCapture = content -> ((Slice) content).getBytes();
            myState.unscaledDecimals = unscaledDecimals.capture(unscaledDecimalsCapture);
            myState.overflows = overflows.capture(serdeProvider);
            myState.numberOfElements = numberOfElements;
            myState.baseState = super.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLongDecimalWithOverflowStateState myState = (GroupedLongDecimalWithOverflowStateState) state;
            Function<Object, Object> unscaledDecimalsRestore = content -> Slices.wrappedBuffer((byte[]) content);
            this.unscaledDecimals.restore(unscaledDecimalsRestore, myState.unscaledDecimals);
            this.overflows.restore(myState.overflows, serdeProvider);
            this.numberOfElements = myState.numberOfElements;
            super.restore(myState.baseState, serdeProvider);
        }

        private static class GroupedLongDecimalWithOverflowStateState
                implements Serializable
        {
            private Object unscaledDecimals;
            private Object overflows;
            private long numberOfElements;
            private Object baseState;
        }
    }

    public static class SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongDecimalWithOverflowState.class).instanceSize();
        public static final int SIZE = ClassLayout.parseClass(Slice.class).instanceSize() + UNSCALED_DECIMAL_128_SLICE_LENGTH;

        protected Slice unscaledDecimal;
        protected long overflow;

        @Override
        public Slice getLongDecimal()
        {
            return unscaledDecimal;
        }

        @Override
        public void setLongDecimal(Slice unscaledDecimal)
        {
            this.unscaledDecimal = unscaledDecimal;
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
        public long getEstimatedSize()
        {
            if (getLongDecimal() == null) {
                return INSTANCE_SIZE;
            }
            return INSTANCE_SIZE + SIZE;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowStateState myState = new SingleLongDecimalWithOverflowStateState();
            if (this.unscaledDecimal != null) {
                myState.unscaledDecimal = unscaledDecimal.getBytes();
            }
            myState.overflow = overflow;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLongDecimalWithOverflowStateState myState = (SingleLongDecimalWithOverflowStateState) state;
            if (myState.unscaledDecimal == null) {
                this.unscaledDecimal = null;
            }
            else {
                this.unscaledDecimal = Slices.wrappedBuffer(myState.unscaledDecimal);
            }
            this.overflow = myState.overflow;
        }

        private static class SingleLongDecimalWithOverflowStateState
                implements Serializable
        {
            private byte[] unscaledDecimal;
            private long overflow;
        }
    }
}
