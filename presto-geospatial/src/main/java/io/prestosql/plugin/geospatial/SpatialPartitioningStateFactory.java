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
package io.prestosql.plugin.geospatial;

import com.esri.core.geometry.Envelope;
import io.prestosql.array.IntBigArray;
import io.prestosql.array.LongBigArray;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.geospatial.Rectangle;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static java.lang.Math.toIntExact;

public class SpatialPartitioningStateFactory
        implements AccumulatorStateFactory<SpatialPartitioningState>
{
    @Override
    public SpatialPartitioningState createSingleState()
    {
        return new SingleSpatialPartitioningState();
    }

    @Override
    public Class<SpatialPartitioningState> getSingleStateClass()
    {
        return SpatialPartitioningState.class;
    }

    @Override
    public SpatialPartitioningState createGroupedState()
    {
        return new GroupedSpatialPartitioningState();
    }

    @Override
    public Class<GroupedSpatialPartitioningState> getGroupedStateClass()
    {
        return GroupedSpatialPartitioningState.class;
    }

    public static final class GroupedSpatialPartitioningState
            implements GroupedAccumulatorState, SpatialPartitioningState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedSpatialPartitioningState.class).instanceSize();
        private static final int ENVELOPE_SIZE = toIntExact(new Envelope(1, 2, 3, 4).estimateMemorySize());

        private long groupId;
        private final IntBigArray partitionCounts = new IntBigArray();
        private final LongBigArray counts = new LongBigArray();
        private final ObjectBigArray<Rectangle> envelopes = new ObjectBigArray<>();
        private final ObjectBigArray<List<Rectangle>> samples = new ObjectBigArray<>();
        private int envelopeCount;
        private int samplesCount;

        @Override
        public int getPartitionCount()
        {
            return partitionCounts.get(groupId);
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCounts.set(groupId, partitionCount);
        }

        @Override
        public long getCount()
        {
            return counts.get(groupId);
        }

        @Override
        public void setCount(long count)
        {
            counts.set(groupId, count);
        }

        @Override
        public Rectangle getExtent()
        {
            return envelopes.get(groupId);
        }

        @Override
        public void setExtent(Rectangle envelope)
        {
            if (envelopes.get(groupId) == null) {
                envelopeCount++;
            }
            envelopes.set(groupId, envelope);
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples.get(groupId);
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            List<Rectangle> currentSamples = this.samples.get(groupId);
            if (currentSamples != null) {
                samplesCount -= currentSamples.size();
            }
            samplesCount += samples.size();
            this.samples.set(groupId, samples);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + partitionCounts.sizeOf() + counts.sizeOf() + envelopes.sizeOf() + samples.sizeOf() + ENVELOPE_SIZE * (envelopeCount + samplesCount);
        }

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            partitionCounts.ensureCapacity(size);
            counts.ensureCapacity(size);
            envelopes.ensureCapacity(size);
            samples.ensureCapacity(size);
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedSpatialPartitioningStateState myState = new GroupedSpatialPartitioningStateState();
            myState.groupId = groupId;
            myState.partitionCounts = partitionCounts.capture(serdeProvider);
            myState.counts = counts.capture(serdeProvider);
            Function<Object, Object> envelopesCapture = content -> content;
            myState.envelopes = envelopes.capture(envelopesCapture);
            Function<Object, Object> samplesCapture = content -> {
                List<Rectangle> list = (List<Rectangle>) content;
                List<Rectangle> capturedList = new ArrayList<>(list);
                return capturedList;
            };
            myState.samples = samples.capture(samplesCapture);
            myState.envelopeCount = envelopeCount;
            myState.samplesCount = samplesCount;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedSpatialPartitioningStateState myState = (GroupedSpatialPartitioningStateState) state;
            this.groupId = myState.groupId;
            this.partitionCounts.restore(myState.partitionCounts, serdeProvider);
            this.counts.restore(myState.counts, serdeProvider);
            Function<Object, Object> envelopesRestore = content -> content;
            this.envelopes.restore(envelopesRestore, myState.envelopes);
            Function<Object, Object> samplesRestore = content -> content;
            this.samples.restore(samplesRestore, myState.samples);
            this.envelopeCount = myState.envelopeCount;
            this.samplesCount = myState.samplesCount;
        }

        private static class GroupedSpatialPartitioningStateState
                implements Serializable
        {
            private long groupId;
            private Object partitionCounts;
            private Object counts;
            private Object envelopes;
            private Object samples;
            private int envelopeCount;
            private int samplesCount;
        }
    }

    public static final class SingleSpatialPartitioningState
            implements SpatialPartitioningState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleSpatialPartitioningState.class).instanceSize();

        private int partitionCount;
        private long count;
        private Rectangle envelope;
        private List<Rectangle> samples = new ArrayList<>();

        @Override
        public int getPartitionCount()
        {
            return partitionCount;
        }

        @Override
        public void setPartitionCount(int partitionCount)
        {
            this.partitionCount = partitionCount;
        }

        @Override
        public long getCount()
        {
            return count;
        }

        @Override
        public void setCount(long count)
        {
            this.count = count;
        }

        @Override
        public Rectangle getExtent()
        {
            return envelope;
        }

        @Override
        public void setExtent(Rectangle envelope)
        {
            this.envelope = envelope;
        }

        @Override
        public List<Rectangle> getSamples()
        {
            return samples;
        }

        @Override
        public void setSamples(List<Rectangle> samples)
        {
            this.samples = samples;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + (envelope != null ? envelope.estimateMemorySize() * (1 + samples.size()) : 0);
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleSpatialPartitioningStateState myState = new SingleSpatialPartitioningStateState();
            myState.partitionCount = partitionCount;
            myState.count = count;
            if (envelope != null) {
                myState.envelope = envelope;
            }
            myState.samples = samples;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleSpatialPartitioningStateState myState = (SingleSpatialPartitioningStateState) state;
            this.partitionCount = myState.partitionCount;
            this.count = myState.count;
            this.envelope = myState.envelope;
            this.samples = myState.samples;
        }

        private static class SingleSpatialPartitioningStateState
                implements Serializable
        {
            private int partitionCount;
            private long count;
            private Rectangle envelope;
            private List<Rectangle> samples;
        }
    }
}
