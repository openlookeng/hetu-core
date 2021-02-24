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

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.KeyValuePairs;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class KeyValuePairsStateFactory
        implements AccumulatorStateFactory<KeyValuePairsState>
{
    private final Type keyType;
    private final Type valueType;

    public KeyValuePairsStateFactory(Type keyType, Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public KeyValuePairsState createSingleState()
    {
        return new SingleState(keyType, valueType);
    }

    @Override
    public Class<? extends KeyValuePairsState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public KeyValuePairsState createGroupedState()
    {
        return new GroupedState(keyType, valueType);
    }

    @Override
    public Class<? extends KeyValuePairsState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    @RestorableConfig(uncapturedFields = {"keyType", "valueType"})
    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements KeyValuePairsState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final Type keyType;
        private final Type valueType;
        private final ObjectBigArray<KeyValuePairs> pairs = new ObjectBigArray<>();
        private long size;

        public GroupedState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public void ensureCapacity(long size)
        {
            pairs.ensureCapacity(size);
        }

        @Override
        public KeyValuePairs get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(KeyValuePairs value)
        {
            requireNonNull(value, "value is null");

            KeyValuePairs previous = get();
            if (previous != null) {
                size -= previous.estimatedInMemorySize();
            }

            pairs.set(getGroupId(), value);
            size += value.estimatedInMemorySize();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + pairs.sizeOf();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedStateState myState = new GroupedStateState();
            myState.size = size;
            myState.baseState = super.capture(serdeProvider);
            Function<Object, Object> pairsCapture = content -> ((KeyValuePairs) content).capture(serdeProvider);
            myState.pairs = pairs.capture(pairsCapture);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            GroupedStateState myState = (GroupedStateState) state;
            this.size = myState.size;
            super.restore(myState.baseState, serdeProvider);
            Function<Object, Object> pairsRestore = content -> {
                KeyValuePairs pair = new KeyValuePairs(keyType, valueType);
                pair.restore(content, serdeProvider);
                return pair;
            };
            this.pairs.restore(pairsRestore, myState.pairs);
        }

        private static class GroupedStateState
                implements Serializable
        {
            private long size;
            private Object baseState;
            private Object pairs;
        }
    }

    @RestorableConfig(uncapturedFields = {"keyType", "valueType"})
    public static class SingleState
            implements KeyValuePairsState, Restorable
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private final Type keyType;
        private final Type valueType;
        private KeyValuePairs pair;

        public SingleState(Type keyType, Type valueType)
        {
            this.keyType = keyType;
            this.valueType = valueType;
        }

        @Override
        public KeyValuePairs get()
        {
            return pair;
        }

        @Override
        public void set(KeyValuePairs value)
        {
            pair = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (pair != null) {
                estimatedSize += pair.estimatedInMemorySize();
            }
            return estimatedSize;
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            if (this.pair != null) {
                return pair.capture(serdeProvider);
            }
            return null;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            if (state != null) {
                if (this.pair == null) {
                    this.pair = new KeyValuePairs(keyType, valueType);
                }
                this.pair.restore(state, serdeProvider);
            }
            else {
                this.pair = null;
            }
        }
    }
}
