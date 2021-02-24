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
import io.airlift.stats.QuantileDigest;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class QuantileDigestStateFactory
        implements AccumulatorStateFactory<QuantileDigestState>
{
    @Override
    public QuantileDigestState createSingleState()
    {
        return new SingleQuantileDigestState();
    }

    @Override
    public Class<? extends QuantileDigestState> getSingleStateClass()
    {
        return SingleQuantileDigestState.class;
    }

    @Override
    public QuantileDigestState createGroupedState()
    {
        return new GroupedQuantileDigestState();
    }

    @Override
    public Class<? extends QuantileDigestState> getGroupedStateClass()
    {
        return GroupedQuantileDigestState.class;
    }

    public static class GroupedQuantileDigestState
            extends AbstractGroupedAccumulatorState
            implements QuantileDigestState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedQuantileDigestState.class).instanceSize();
        private final ObjectBigArray<QuantileDigest> qdigests = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            qdigests.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getQuantileDigest()
        {
            return qdigests.get(getGroupId());
        }

        @Override
        public void setQuantileDigest(QuantileDigest value)
        {
            requireNonNull(value, "value is null");
            qdigests.set(getGroupId(), value);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + qdigests.sizeOf();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedQuantileDigestStateState myState = new GroupedQuantileDigestStateState();
            myState.size = size;
            myState.baseState = super.capture(serdeProvider);
            Function<Object, Object> qdigestsCapture = content -> ((QuantileDigest) content).serialize().getBytes();
            myState.qdigests = qdigests.capture(qdigestsCapture);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedQuantileDigestStateState myState = (GroupedQuantileDigestStateState) state;
            this.size = myState.size;
            super.restore(myState.baseState, serdeProvider);
            Function<Object, Object> qdigestsRestore = content -> new QuantileDigest(Slices.wrappedBuffer((byte[]) content));
            this.qdigests.restore(qdigestsRestore, myState.qdigests);
        }

        private static class GroupedQuantileDigestStateState
                implements Serializable
        {
            private long size;
            private Object baseState;
            private Object qdigests;
        }
    }

    public static class SingleQuantileDigestState
            implements QuantileDigestState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleQuantileDigestState.class).instanceSize();
        private QuantileDigest qdigest;

        @Override
        public QuantileDigest getQuantileDigest()
        {
            return qdigest;
        }

        @Override
        public void setQuantileDigest(QuantileDigest value)
        {
            qdigest = value;
        }

        @Override
        public void addMemoryUsage(int value)
        {
            // noop
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (qdigest != null) {
                estimatedSize += qdigest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            if (this.qdigest != null) {
                return qdigest.serialize().getBytes();
            }
            return null;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            if (state != null) {
                this.qdigest = new QuantileDigest(Slices.wrappedBuffer((byte[]) state));
            }
            else {
                this.qdigest = null;
            }
        }
    }
}
