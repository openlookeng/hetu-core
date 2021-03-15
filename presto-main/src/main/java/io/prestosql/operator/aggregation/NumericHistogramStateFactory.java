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

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;

import java.io.Serializable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class NumericHistogramStateFactory
        implements AccumulatorStateFactory<DoubleHistogramAggregation.State>
{
    @Override
    public DoubleHistogramAggregation.State createSingleState()
    {
        return new SingleState();
    }

    @Override
    public Class<? extends DoubleHistogramAggregation.State> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public DoubleHistogramAggregation.State createGroupedState()
    {
        return new GroupedState();
    }

    @Override
    public Class<? extends DoubleHistogramAggregation.State> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements DoubleHistogramAggregation.State
    {
        private final ObjectBigArray<NumericHistogram> histograms = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            histograms.ensureCapacity(size);
        }

        @Override
        public NumericHistogram get()
        {
            return histograms.get(getGroupId());
        }

        @Override
        public void set(NumericHistogram value)
        {
            requireNonNull(value, "value is null");

            NumericHistogram previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            histograms.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public long getEstimatedSize()
        {
            return size + histograms.sizeOf();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedStateState myState = new GroupedStateState();
            myState.size = size;
            myState.baseState = super.capture(serdeProvider);
            Function<Object, Object> histogramsCapture = content -> content;
            myState.histograms = histograms.capture(histogramsCapture);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedStateState myState = (GroupedStateState) state;
            this.size = myState.size;
            super.restore(myState.baseState, serdeProvider);
            Function<Object, Object> histogramsRestore = content -> content;
            this.histograms.restore(histogramsRestore, myState.histograms);
        }

        private static class GroupedStateState
                implements Serializable
        {
            private long size;
            private Object baseState;
            private Object histograms;
        }
    }

    public static class SingleState
            implements DoubleHistogramAggregation.State, Restorable
    {
        private NumericHistogram histogram;

        @Override
        public NumericHistogram get()
        {
            return histogram;
        }

        @Override
        public void set(NumericHistogram value)
        {
            histogram = value;
        }

        @Override
        public long getEstimatedSize()
        {
            if (histogram == null) {
                return 0;
            }
            return histogram.estimatedInMemorySize();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            if (histogram != null) {
                return histogram;
            }
            return null;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            if (state != null) {
                this.histogram = (NumericHistogram) state;
            }
            else {
                this.histogram = null;
            }
        }
    }
}
