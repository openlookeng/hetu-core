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
package io.prestosql.operator.dynamicfilter;

import com.google.common.collect.ImmutableList;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.operator.DriverContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.planner.TypeProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter.convertBloomFilterToByteArray;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestCrossRegionDynamicFilterOperator
{
    private AtomicInteger operatorId = new AtomicInteger();
    private PlanNodeId planNodeId = new PlanNodeId("123456");
    private ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-bloom-filter-operator-%s"));

    @Test
    public void testBloomFilterWithoutValue()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        String queryId = "query-123456";
        DynamicFilterCacheManager dynamicFilterCacheManager = new DynamicFilterCacheManager();
        CrossRegionDynamicFilterOperator operator = createBloomFilterOperator(queryId, dynamicFilterCacheManager);
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
        operator.close();
    }

    @Test
    public void testBloomFilter()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        String queryId = "query-123456-2";
        DynamicFilterCacheManager dynamicFilterCacheManager = new DynamicFilterCacheManager();
        CrossRegionDynamicFilterOperator operator = createBloomFilterOperator(queryId, dynamicFilterCacheManager);

        addBloomFilter("orderId", ImmutableList.of("10001"), dynamicFilterCacheManager, queryId);

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
        operator.close();
    }

    @Test
    public void testMultipleFilters()
    {
        List<Type> types = ImmutableList.of(VarcharType.VARCHAR, VarcharType.VARCHAR);
        String queryId = "query-123456-3";
        DynamicFilterCacheManager dynamicFilterCacheManager = new DynamicFilterCacheManager();
        CrossRegionDynamicFilterOperator operator = createBloomFilterOperator(queryId, dynamicFilterCacheManager);

        addBloomFilter("orderId", ImmutableList.of("10003", "10004"), dynamicFilterCacheManager, queryId);
        addBloomFilter("custkey", ImmutableList.of("0001", "0002"), dynamicFilterCacheManager, queryId);

        List<Page> pages = rowPagesBuilder(types)
                .row("10001", "0001")
                .row("10002", "0002")
                .row("10003", "0003")
                .build();

        operator.addInput(pages.get(0));

        Page page = operator.getOutput();
        assertEquals(page.getPositionCount(), 0);
        operator.close();
    }

    private CrossRegionDynamicFilterOperator createBloomFilterOperator(String queryId, DynamicFilterCacheManager dynamicFilterCacheManager)
    {
        int filterOperatorId = operatorId.getAndIncrement();
        CrossRegionDynamicFilterOperator.CrossRegionDynamicFilterOperatorFactory factory = new CrossRegionDynamicFilterOperator.CrossRegionDynamicFilterOperatorFactory(
                filterOperatorId,
                planNodeId,
                queryId,
                createSymbolList(),
                createTypeProvider(),
                dynamicFilterCacheManager,
                createColumns(),
                createSymbolList());
        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        return (CrossRegionDynamicFilterOperator) factory.createOperator(driverContext);
    }

    private static List<Symbol> createSymbolList()
    {
        List<Symbol> symbols = new ArrayList<>();
        Symbol symbol = new Symbol("orderId_1");
        symbols.add(symbol);

        symbol = new Symbol("custKey_1");
        symbols.add(symbol);

        return symbols;
    }

    private static List<String> createColumns()
    {
        List<String> columns = new ArrayList<>();
        columns.add("orderId");
        columns.add("custkey");
        return columns;
    }

    private static TypeProvider createTypeProvider()
    {
        Map<Symbol, Type> types = new HashMap<>();
        types.put(new Symbol("orderId_1"), VarcharType.VARCHAR);
        types.put(new Symbol("custKey_1"), VarcharType.VARCHAR);

        return TypeProvider.viewOf(types);
    }

    private void addBloomFilter(String column, List<String> values, DynamicFilterCacheManager dynamicFilterCacheManager, String queryId)
    {
        BloomFilter bloomFilter = new BloomFilter(1024 * 1024, 0.005);
        values.forEach(value -> bloomFilter.add(value.getBytes()));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            bloomFilter.writeTo(out);
            Map<String, byte[]> bloomFilters = dynamicFilterCacheManager.getBloomFitler(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);
            if (bloomFilters == null) {
                bloomFilters = new HashMap<>();
            }
            bloomFilters.put(column, convertBloomFilterToByteArray(bloomFilter));

            dynamicFilterCacheManager.cacheBloomFilters(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION, bloomFilters);
        }
        catch (IOException e) {
            throw new RuntimeException("error to write bloom filter into byte");
        }
    }
}
