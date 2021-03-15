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

import io.airlift.slice.SizeOf;
import io.airlift.slice.Slices;
import io.airlift.stats.QuantileDigest;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class DigestAndPercentileArrayStateFactory
        implements AccumulatorStateFactory<DigestAndPercentileArrayState>
{
    @Override
    public DigestAndPercentileArrayState createSingleState()
    {
        return new SingleDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getSingleStateClass()
    {
        return SingleDigestAndPercentileArrayState.class;
    }

    @Override
    public DigestAndPercentileArrayState createGroupedState()
    {
        return new GroupedDigestAndPercentileArrayState();
    }

    @Override
    public Class<? extends DigestAndPercentileArrayState> getGroupedStateClass()
    {
        return GroupedDigestAndPercentileArrayState.class;
    }

    public static class GroupedDigestAndPercentileArrayState
            extends AbstractGroupedAccumulatorState
            implements DigestAndPercentileArrayState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedDigestAndPercentileArrayState.class).instanceSize();
        private final ObjectBigArray<QuantileDigest> digests = new ObjectBigArray<>();
        private final ObjectBigArray<List<Double>> percentilesArray = new ObjectBigArray<>();
        private long size;

        @Override
        public void ensureCapacity(long size)
        {
            digests.ensureCapacity(size);
            percentilesArray.ensureCapacity(size);
        }

        @Override
        public QuantileDigest getDigest()
        {
            return digests.get(getGroupId());
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            digests.set(getGroupId(), requireNonNull(digest, "digest is null"));
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentilesArray.get(getGroupId());
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            percentilesArray.set(getGroupId(), requireNonNull(percentiles, "percentiles is null"));
        }

        @Override
        public void addMemoryUsage(int value)
        {
            size += value;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + digests.sizeOf() + percentilesArray.sizeOf();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedDigestAndPercentileArrayStateState myState = new GroupedDigestAndPercentileArrayStateState();
            Function<Object, Object> digestsCapture = content -> ((QuantileDigest) content).serialize().getBytes();
            myState.digests = digests.capture(digestsCapture);
            Function<Object, Object> percentilesArrayCapture = content -> ((List<Double>) content).stream().toArray(Double[]::new);
            myState.percentilesArray = percentilesArray.capture(percentilesArrayCapture);
            myState.size = size;
            myState.baseState = super.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedDigestAndPercentileArrayStateState myState = (GroupedDigestAndPercentileArrayStateState) state;
            Function<Object, Object> digestsRestore = content -> new QuantileDigest(Slices.wrappedBuffer((byte[]) content));
            this.digests.restore(digestsRestore, myState.digests);
            Function<Object, Object> percentilesArrayRestore = content -> Arrays.stream((double[]) content).boxed().collect(Collectors.toList());
            this.percentilesArray.restore(percentilesArrayRestore, myState.percentilesArray);
            this.size = myState.size;
            super.restore(myState.baseState, serdeProvider);
        }

        private static class GroupedDigestAndPercentileArrayStateState
                implements Serializable
        {
            private Object digests;
            private Object percentilesArray;
            private long size;
            private Object baseState;
        }
    }

    public static class SingleDigestAndPercentileArrayState
            implements DigestAndPercentileArrayState, Restorable
    {
        public static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleDigestAndPercentileArrayState.class).instanceSize();
        private QuantileDigest digest;
        private List<Double> percentiles;

        @Override
        public QuantileDigest getDigest()
        {
            return digest;
        }

        @Override
        public void setDigest(QuantileDigest digest)
        {
            this.digest = requireNonNull(digest, "digest is null");
        }

        @Override
        public List<Double> getPercentiles()
        {
            return percentiles;
        }

        @Override
        public void setPercentiles(List<Double> percentiles)
        {
            this.percentiles = requireNonNull(percentiles, "percentiles is null");
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
            if (percentiles != null) {
                estimatedSize += SizeOf.sizeOfDoubleArray(percentiles.size());
            }
            return estimatedSize;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleDigestAndPercentileArrayStateState myState = new SingleDigestAndPercentileArrayStateState();
            if (this.digest != null) {
                myState.digest = digest.serialize().getBytes();
            }
            if (this.percentiles != null) {
                myState.percentiles = percentiles.stream().toArray(Double[]::new);
            }
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleDigestAndPercentileArrayStateState myState = (SingleDigestAndPercentileArrayStateState) state;
            if (myState.digest != null) {
                this.digest = new QuantileDigest(Slices.wrappedBuffer(myState.digest));
            }
            else {
                this.digest = null;
            }
            if (myState.percentiles != null) {
                this.percentiles = Arrays.asList(myState.percentiles);
            }
            else {
                this.percentiles = null;
            }
        }

        private static class SingleDigestAndPercentileArrayStateState
                implements Serializable
        {
            private byte[] digest;
            private Double[] percentiles;
        }
    }
}
