/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql;

import io.prestosql.operator.aggregation.BlockComparator;
import io.prestosql.operator.aggregation.TypedKeyValueHeap;
import io.prestosql.operator.aggregation.minmaxby.MinMaxByNState;
import io.prestosql.operator.aggregation.minmaxby.MinMaxByNStateFactory;
import io.prestosql.operator.aggregation.minmaxby.MinMaxByNStateSerializer;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestMinMaxByNState
{
    private static final int INPUT_SIZE = 1_000_000; // larger than COMPACT_THRESHOLD_* to guarantee coverage of compact
    private static final int OUTPUT_SIZE = 1_000;

    private static final BlockComparator MAX_ELEMENTS_COMPARATOR = BIGINT::compareTo;

    private static final IntStream keyInputStream = IntStream.range(0, INPUT_SIZE);
    private static final Stream<String> valueInputStream = IntStream.range(0, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2));
    private static final Iterator<String> outputIterator = IntStream.range(INPUT_SIZE - OUTPUT_SIZE, INPUT_SIZE).mapToObj(key -> Integer.toString(key * 2)).iterator();

    @Test
    public void testGroupedMinMaxByNState()
    {
        MinMaxByNStateFactory factory = new MinMaxByNStateFactory();
        MinMaxByNState groupedState = factory.createGroupedState();
        TypedKeyValueHeap heap0 = new TypedKeyValueHeap(MAX_ELEMENTS_COMPARATOR, BIGINT, VARCHAR, OUTPUT_SIZE);
        groupedState.setTypedKeyValueHeap(heap0);

        BlockBuilder keysBlockBuilder = BIGINT.createBlockBuilder(null, INPUT_SIZE);
        BlockBuilder valuesBlockBuilder = VARCHAR.createBlockBuilder(null, INPUT_SIZE);
        keyInputStream.forEach(x -> BIGINT.writeLong(keysBlockBuilder, x));
        valueInputStream.forEach(x -> VARCHAR.writeString(valuesBlockBuilder, x));

        heap0.addAll(keysBlockBuilder, valuesBlockBuilder);

        MinMaxByNStateSerializer serializer = new MinMaxByNStateSerializer(MAX_ELEMENTS_COMPARATOR, BIGINT, VARCHAR);
        Object snapshot = serializer.serializeCapture(groupedState, null);
        MinMaxByNState newGroupedState = factory.createGroupedState();
        serializer.deserializeRestore(snapshot, newGroupedState, null);

        BlockBuilder resultBlockBuilder = VARCHAR.createBlockBuilder(null, OUTPUT_SIZE);
        newGroupedState.getTypedKeyValueHeap().popAll(resultBlockBuilder);

        Block resultBlock = resultBlockBuilder.build();
        assertEquals(resultBlock.getPositionCount(), OUTPUT_SIZE);
        for (int i = 0; i < OUTPUT_SIZE; i++) {
            assertEquals(VARCHAR.getSlice(resultBlock, i).toStringUtf8(), outputIterator.next());
        }
    }
}
