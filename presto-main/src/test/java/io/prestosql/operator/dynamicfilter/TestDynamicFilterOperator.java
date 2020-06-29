/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.operator.dynamicfilter;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.prestosql.operator.DriverContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.statestore.MockStateMap;
import io.prestosql.statestore.StateStoreProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterOperator
{
    private AtomicInteger operatorId = new AtomicInteger();
    private PlanNodeId planNodeId = new PlanNodeId("123456");
    private String queryId = "query-123456";
    private ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-bloom-filter-operator-%s"));

    @Test
    public void testBloomFilterWithoutValue()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        StateStoreProvider stateStoreProvider = createStateStore();
        DynamicFilterOperator operator = createBloomFilterOperator(stateStoreProvider);
        assertFalse(operator.isFinished());

        List<Page> pages = rowPagesBuilder(types)
                .row("10001", "0001")
                .row("10002", "0002")
                .pageBreak()
                .row("10003", "0003")
                .build();

        assertNull(operator.getOutput());
        assertFalse(operator.isFinished());

        operator.addInput(pages.get(0));
        assertFalse(operator.needsInput());

        Page page1 = operator.getOutput();
        assertEquals(page1, pages.get(0));

        assertTrue(operator.needsInput());
        operator.addInput(pages.get(1));
        Page page2 = operator.getOutput();
        assertEquals(page2, pages.get(1));
    }

    @Test
    public void testBloomFilter()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        StateStoreProvider stateStoreProvider = createStateStore();
        DynamicFilterOperator operator = createBloomFilterOperator(stateStoreProvider);

        StateMap<String, byte[]> mockStateMap = (StateMap<String, byte[]>) stateStoreProvider.getStateStore().createStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION, StateCollection.Type.MAP);
        addBloomFilter("orderId", ImmutableList.of("10001"), mockStateMap);

        List<Page> pages = rowPagesBuilder(types)
                .row("10001", "0001")
                .row("10002", "0002")
                .row("10003", "0003")
                .build();

        operator.addInput(pages.get(0));

        Page page = operator.getOutput();
        assertEquals(page.getPositionCount(), 1);
        Block block = page.getBlock(0).getLoadedBlock();
        String nativeValue = TypeUtils.readNativeValueForDynamicFilter(types.get(0), block, 0);
        assertEquals(nativeValue, "10001");
    }

    @Test
    public void testMultipleFilters()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        StateStoreProvider stateStoreProvider = createStateStore();
        DynamicFilterOperator operator = createBloomFilterOperator(stateStoreProvider);

        StateMap<String, byte[]> mockStateMap = (StateMap<String, byte[]>) stateStoreProvider.getStateStore().createStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION, StateCollection.Type.MAP);
        addBloomFilter("orderId", ImmutableList.of("10003", "10004"), mockStateMap);
        addBloomFilter("custKey", ImmutableList.of("0001", "0002"), mockStateMap);

        List<Page> pages = rowPagesBuilder(types)
                .row("10001", "0001")
                .row("10002", "0002")
                .row("10003", "0003")
                .build();

        operator.addInput(pages.get(0));

        Page page = operator.getOutput();
        assertEquals(page.getPositionCount(), 0);
    }

    private DynamicFilterOperator createBloomFilterOperator(StateStoreProvider stateStoreProvider)
    {
        int filterOperatorId = operatorId.getAndIncrement();
        DynamicFilterOperator.DynamicFilterOperatorFactory factory = new DynamicFilterOperator.DynamicFilterOperatorFactory(
                filterOperatorId,
                planNodeId,
                queryId,
                createSymbolList(),
                createTypeProvider(),
                stateStoreProvider);
        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        return (DynamicFilterOperator) factory.createOperator(driverContext);
    }

    private StateStoreProvider createStateStore()
    {
        StateCollection mockStateMap = new MockStateMap<Integer, byte[]>("test", new HashMap<>());
        StateStoreProvider stateStoreProvider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        when(stateStore.getStateCollection(anyString())).thenReturn(mockStateMap);
        when(stateStore.createStateCollection(anyString(), any())).thenReturn(mockStateMap);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);

        return stateStoreProvider;
    }

    private static List<Symbol> createSymbolList()
    {
        List<Symbol> symbols = new ArrayList<>();
        Symbol symbol = new Symbol("orderId");
        symbols.add(symbol);

        symbol = new Symbol("custKey");
        symbols.add(symbol);

        return symbols;
    }

    private static TypeProvider createTypeProvider()
    {
        Map<Symbol, Type> types = new HashMap<>();
        types.put(new Symbol("orderId"), VarcharType.VARCHAR);
        types.put(new Symbol("custKey"), VarcharType.VARCHAR);

        return TypeProvider.viewOf(types);
    }

    private static void addBloomFilter(String column, List<Object> values, StateMap map)
    {
        BloomFilter bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1024 * 1024, 0.005);
        values.forEach(value -> bloomFilter.put(value));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            bloomFilter.writeTo(out);
            map.put(column, out.toByteArray());
        }
        catch (IOException e) {
            throw new RuntimeException("error to write bloom filter into byte");
        }
    }
}
