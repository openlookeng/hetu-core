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
import io.prestosql.array.DoubleBigArray;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class DigestAndPercentileStateFactory
        implements AccumulatorStateFactory<DigestAndPercentileState>
{
    @Override
    public DigestAndPercentileState createSingleState()
    {
        return new SingleDigestAndPercentileState();
    }

    @Override
    public Class<? extends DigestAndPercentileState> getSingleStateClass()
    {
        return SingleDigestAndPercentileState.class;
    }

    @Override
    public DigestAndPercentileState createGroupedState()
    {
        return new GroupedDigestAndPercentileState();
    }

    @Override
    public Class<? extends DigestAndPercentileState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileState.class;
    }

    public static class GroupedDigestAndPercentileState
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedDigestAndPercentileState.class).instanceSize();
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final DoubleBigArray percentiles = new DoubleBigArray();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentiles.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            requireNonNull(digest, "value is null");
            digests.set(getGroupId(), digest);
        }

        @Override
        public double getPercentile()
        {
            return percentiles.get(getGroupId());
        }

        @Override
        public void setPercentile(double percentile)
        {
            percentiles.set(getGroupId(), percentile);
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentiles.sizeOf();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedDigestAndPercentileStateState myState = new GroupedDigestAndPercentileStateState();
            Function<Object, Object> digestsCapture = content -> ((QuantileDigest) content).serialize().getBytes();
            myState.digests = digests.capture(digestsCapture);
            myState.percentiles = percentiles.capture(serdeProvider);
            myState.size = size;
            myState.baseState = super.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedDigestAndPercentileStateState myState = (GroupedDigestAndPercentileStateState) state;
            Function<Object, Object> digestsRestore = content -> new QuantileDigest(Slices.wrappedBuffer((byte[]) content));
            this.digests.restore(digestsRestore, myState.digests);
            this.percentiles.restore(myState.percentiles, serdeProvider);
            this.size = myState.size;
            super.restore(myState.baseState, serdeProvider);
        }

        private static class GroupedDigestAndPercentileStateState
                implements Serializable
        {
            private Object digests;
            private Object percentiles;
            private long size;
            private Object baseState;
        }
    }

    public static class SingleDigestAndPercentileState
            implements DigestAndPercentileState, Restorable
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleDigestAndPercentileState.class).instanceSize();
        private QuantileDigest digest;
        private double percentile;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = digest;
        }

        @Override
        public double getPercentile()
        {
            return percentile;
        }

        @Override
        public void setPercentile(double percentile)
        {
            this.percentile = percentile;
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
            if (digest != null) {
                estimatedSize += digest.estimatedInMemorySizeInBytes();
            }
            return estimatedSize;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleDigestAndPercentileStateState myState = new SingleDigestAndPercentileStateState();
            if (this.digest != null) {
                myState.digest = digest.serialize().getBytes();
            }
            myState.percentile = percentile;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleDigestAndPercentileStateState myState = (SingleDigestAndPercentileStateState) state;
            if (myState.digest != null) {
                this.digest = new QuantileDigest(Slices.wrappedBuffer(myState.digest));
            }
            else {
                this.digest = null;
            }
            this.percentile = myState.percentile;
        }

        private static class SingleDigestAndPercentileStateState
                implements Serializable
        {
            private byte[] digest;
            private double percentile;
        }
    }
}
