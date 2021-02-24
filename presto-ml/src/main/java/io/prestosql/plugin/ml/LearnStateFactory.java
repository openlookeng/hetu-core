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
package io.prestosql.plugin.ml;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.array.ObjectBigArray;
import io.prestosql.array.SliceBigArray;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.function.GroupedAccumulatorState;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import libsvm.svm_parameter;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LearnStateFactory
        implements AccumulatorStateFactory<LearnState>
{
    private static final long ARRAY_LIST_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    private static final long SVM_PARAMETERS_SIZE = ClassLayout.parseClass(svm_parameter.class).instanceSize();

    @Override
    public LearnState createSingleState()
    {
        return new SingleLearnState();
    }

    @Override
    public Class<? extends LearnState> getSingleStateClass()
    {
        return SingleLearnState.class;
    }

    @Override
    public LearnState createGroupedState()
    {
        return new GroupedLearnState();
    }

    @Override
    public Class<? extends LearnState> getGroupedStateClass()
    {
        return GroupedLearnState.class;
    }

    public static class GroupedLearnState
            implements GroupedAccumulatorState, LearnState, Restorable
    {
        private final ObjectBigArray<List<Double>> labelsArray = new ObjectBigArray<>();
        private final ObjectBigArray<List<FeatureVector>> featureVectorsArray = new ObjectBigArray<>();
        private final SliceBigArray parametersArray = new SliceBigArray();
        private final BiMap<String, Integer> labelEnumeration = HashBiMap.create();
        private long groupId;
        private int nextLabel;
        private long size;

        @Override
        public void setGroupId(long groupId)
        {
            this.groupId = groupId;
        }

        @Override
        public void ensureCapacity(long size)
        {
            labelsArray.ensureCapacity(size);
            featureVectorsArray.ensureCapacity(size);
            parametersArray.ensureCapacity(size);
        }

        @Override
        public long getEstimatedSize()
        {
            return size + labelsArray.sizeOf() + featureVectorsArray.sizeOf();
        }

        @Override
        public BiMap<String, Integer> getLabelEnumeration()
        {
            return labelEnumeration;
        }

        @Override
        public int enumerateLabel(String label)
        {
            if (!labelEnumeration.containsKey(label)) {
                labelEnumeration.put(label, nextLabel);
                nextLabel++;
            }
            return labelEnumeration.get(label);
        }

        @Override
        public List<Double> getLabels()
        {
            List<Double> labels = labelsArray.get(groupId);
            if (labels == null) {
                labels = new ArrayList<>();
                size += ARRAY_LIST_SIZE;
                // Assume that one parameter will be set for each group of labels
                size += SVM_PARAMETERS_SIZE;
                labelsArray.set(groupId, labels);
            }
            return labels;
        }

        @Override
        public List<FeatureVector> getFeatureVectors()
        {
            List<FeatureVector> featureVectors = featureVectorsArray.get(groupId);
            if (featureVectors == null) {
                featureVectors = new ArrayList<>();
                size += ARRAY_LIST_SIZE;
                featureVectorsArray.set(groupId, featureVectors);
            }
            return featureVectors;
        }

        @Override
        public Slice getParameters()
        {
            return parametersArray.get(groupId);
        }

        @Override
        public void setParameters(Slice parameters)
        {
            parametersArray.set(groupId, parameters);
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLearnStateState myState = new GroupedLearnStateState();
            Function<Object, Object> labelsArrayCapture = content -> ((List<Double>) content).stream().toArray(Double[]::new);
            myState.labelsArray = labelsArray.capture(labelsArrayCapture);
            Function<Object, Object> featureVectorsArrayCapture = content -> ((List<FeatureVector>) content).stream().map(vector -> vector.capture(serdeProvider)).toArray();
            myState.featureVectorsArray = featureVectorsArray.capture(featureVectorsArrayCapture);
            myState.parametersArray = parametersArray.capture(serdeProvider);
            myState.labelEnumeration = labelEnumeration;
            myState.groupId = groupId;
            myState.nextLabel = nextLabel;
            myState.size = size;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedLearnStateState myState = (GroupedLearnStateState) state;
            Function<Object, Object> labelsArrayRestore = content -> Arrays.stream((Double[]) content).collect(Collectors.toList());
            this.labelsArray.restore(labelsArrayRestore, myState.labelsArray);
            Function<Object, Object> featureVectorsArrayRestore = content -> Arrays.stream(((Map<Integer, Double>[]) content)).map(FeatureVector::new).collect(Collectors.toList());
            this.featureVectorsArray.restore(featureVectorsArrayRestore, myState.featureVectorsArray);
            this.parametersArray.restore(myState.parametersArray, serdeProvider);
            this.labelEnumeration.clear();
            this.labelEnumeration.putAll(myState.labelEnumeration);
            this.groupId = myState.groupId;
            this.nextLabel = myState.nextLabel;
            this.size = myState.size;
        }

        private static class GroupedLearnStateState
                implements Serializable
        {
            private Object labelsArray;
            private Object featureVectorsArray;
            private Object parametersArray;
            private BiMap<String, Integer> labelEnumeration;
            private long groupId;
            private int nextLabel;
            private long size;
        }
    }

    public static class SingleLearnState
            implements LearnState, Restorable
    {
        private final List<Double> labels = new ArrayList<>();
        private final List<FeatureVector> featureVectors = new ArrayList<>();
        private final BiMap<String, Integer> labelEnumeration = HashBiMap.create();
        private int nextLabel;
        private Slice parameters;
        private long size;

        @Override
        public long getEstimatedSize()
        {
            return size + 2 * ARRAY_LIST_SIZE;
        }

        @Override
        public BiMap<String, Integer> getLabelEnumeration()
        {
            return labelEnumeration;
        }

        @Override
        public int enumerateLabel(String label)
        {
            if (!labelEnumeration.containsKey(label)) {
                labelEnumeration.put(label, nextLabel);
                nextLabel++;
            }
            return labelEnumeration.get(label);
        }

        @Override
        public List<Double> getLabels()
        {
            return labels;
        }

        @Override
        public List<FeatureVector> getFeatureVectors()
        {
            return featureVectors;
        }

        @Override
        public Slice getParameters()
        {
            return parameters;
        }

        @Override
        public void setParameters(Slice parameters)
        {
            this.parameters = parameters;
        }

        @Override
        public void addMemoryUsage(long value)
        {
            size += value;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLearnStateState myState = new SingleLearnStateState();
            myState.labels = labels.stream().toArray(Double[]::new);
            myState.featureVectors = featureVectors.stream().map(vector -> vector.capture(serdeProvider)).toArray();
            myState.labelEnumeration = labelEnumeration;
            myState.nextLabel = nextLabel;
            if (this.parameters != null) {
                myState.parameters = parameters.getBytes();
            }
            myState.size = size;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            SingleLearnStateState myState = (SingleLearnStateState) state;
            this.labels.clear();
            labels.addAll(Arrays.asList(myState.labels));
            this.featureVectors.clear();
            for (int i = 0; i < myState.featureVectors.length; i++) {
                this.featureVectors.add(new FeatureVector((Map<Integer, Double>) myState.featureVectors[i]));
            }
            this.labelEnumeration.clear();
            this.labelEnumeration.putAll(myState.labelEnumeration);
            this.nextLabel = myState.nextLabel;
            if (myState.parameters == null) {
                this.parameters = null;
            }
            else {
                this.parameters = Slices.wrappedBuffer(myState.parameters);
            }
            this.size = myState.size;
        }

        private static class SingleLearnStateState
                implements Serializable
        {
            private Double[] labels;
            private Object[] featureVectors;
            private BiMap<String, Integer> labelEnumeration;
            private int nextLabel;
            private byte[] parameters;
            private long size;
        }
    }
}
