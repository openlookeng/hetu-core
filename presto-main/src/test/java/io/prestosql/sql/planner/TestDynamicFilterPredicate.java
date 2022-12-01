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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.dynamicfilter.DynamicFilterListener;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.statestore.MockStateMap;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.testing.assertions.Assert;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_DATA_TYPE;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.utils.DynamicFilterUtils.MERGED_DYNAMIC_FILTERS;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static io.prestosql.utils.TestDynamicFilterUtil.registerDf;
import static io.prestosql.utils.TestDynamicFilterUtil.setupMockStateStore;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterPredicate
{
    private DynamicFilterService dynamicFilterService;
    private StateStoreProvider stateStoreProvider;
    private String filterId;
    private Session session;

    @BeforeTest
    private void setUpHashSet()
    {
        filterId = "df2";
        session = testSessionBuilder()
                .setQueryId(QueryId.valueOf("qq2"))
                .setSystemProperty(DYNAMIC_FILTERING_DATA_TYPE, "HASHSET")
                .build();

        StateStore stateStore = setupMockStateStore(new HashMap<>(), new HashMap<>(),
                new HashSet<>(), new HashSet<>(), session.getQueryId().toString(), filterId);

        stateStoreProvider = mock(StateStoreProvider.class);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);

        dynamicFilterService = new DynamicFilterService(stateStoreProvider);
        dynamicFilterService.start();
    }

    @Test
    public void testDynamicFilters()
            throws InterruptedException
    {
        final QueryId queryId = new QueryId("test_query");
        filterId = "1";
        final String columnName = "column";
        final TestingColumnHandle columnHandle = new TestingColumnHandle(columnName);
        final Set<Long> valueSet = ImmutableSet.of(1L, 2L, 3L);

        TaskContext taskContext = mock(TaskContext.class);
        session = testSessionBuilder().setQueryId(queryId).setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true").setSystemProperty(DYNAMIC_FILTERING_DATA_TYPE, "HASHSET").build();
        when(taskContext.getSession()).thenReturn(session);

        Map mockMap = new HashMap<>();
        stateStoreProvider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        StateMap stateMap = new MockStateMap<>("test-map", mockMap);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);
        when(stateStore.getStateCollection(any())).thenReturn(stateMap);
        when(stateStore.createStateMap(any())).thenReturn(stateMap);
        when(stateStore.getOrCreateStateCollection(any(), any())).thenReturn(stateMap);

        StateStoreListenerManager stateStoreListenerManager = new StateStoreListenerManager(stateStoreProvider);
        DynamicFilterCacheManager dynamicFilterCacheManager = new DynamicFilterCacheManager();
        stateStoreListenerManager.addStateStoreListener(new DynamicFilterListener(dynamicFilterCacheManager), MERGED_DYNAMIC_FILTERS);
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(taskContext, Optional.empty(), dynamicFilterCacheManager);
        TableScanNode tableScan = mock(TableScanNode.class);
        when(tableScan.getAssignments()).thenReturn(ImmutableMap.of(new Symbol(columnName), columnHandle));
        List<DynamicFilters.Descriptor> dynamicFilterDescriptors = ImmutableList.of(new DynamicFilters.Descriptor(filterId, new VariableReferenceExpression(columnName, BIGINT)));
        collector.initContext(ImmutableList.of(dynamicFilterDescriptors), SymbolUtils.toLayOut(tableScan.getOutputSymbols()));

        assertTrue(collector.getDynamicFilters(tableScan).isEmpty(), "there should be no dynamic filter available");

        stateMap.put(createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId.getId()), valueSet);
        TimeUnit.MILLISECONDS.sleep(100);

        List<Map<ColumnHandle, DynamicFilter>> dynamicFilters = collector.getDynamicFilters(tableScan);
        assertEquals(dynamicFilters.size(), 1, "there should be a new dynamic filter");
        assertEquals(dynamicFilters.size(), 1);
        DynamicFilter dynamicFilter = dynamicFilters.get(0).get(columnHandle);
        assertTrue(dynamicFilter instanceof HashSetDynamicFilter, "new dynamic filter should be hashset");
        assertEquals(dynamicFilter.getSize(), valueSet.size(), "new dynamic filter should have correct size");
        for (Long value : valueSet) {
            assertTrue(dynamicFilter.contains(value), "new dynamic filter should contain correct values");
        }

        collector.removeDynamicFilter(true);
        DynamicFilter cachedFilter = dynamicFilterCacheManager.getDynamicFilter(DynamicFilterCacheManager.createCacheKey(filterId, queryId.getId()));
        assertNull(cachedFilter, "cached dynamic filter should have been removed");

        assertTrue(dynamicFilter.hasMinMaxStats());
        assertEquals(dynamicFilter.getMax(), 3L);
        assertEquals(dynamicFilter.getMin(), 1L);
    }

    @Test
    public void testDynamicFiltersMulti()
            throws InterruptedException
    {
        setUpHashSet();
        filterId = "df2";
        registerDf(filterId, session, PARTITIONED, dynamicFilterService);

        // Test getDynamicFilterSupplier
        VariableReferenceExpression mockExpression = mock(VariableReferenceExpression.class);
        when(mockExpression.getName()).thenReturn("name");
        ColumnHandle mockColumnHandle = mock(ColumnHandle.class);
        Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression))),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set when dynamic filters are not available");

        mockLocalDynamicFilterHashSet("task1.0", filterId, session.getQueryId().toString(), Arrays.asList(11L, 12L, 13L, 14L));
        mockLocalDynamicFilterHashSet("task1.1", filterId, session.getQueryId().toString(), Arrays.asList(15L, 16L, 17L, 18L));

        Thread.sleep(2000);
        Set hs = fetchDynamicFilterHashSet(filterId, session.getQueryId().toString());
        for (long i = 11; i < 19; i++) {
            Assert.assertEquals(true, hs.contains(i));
        }
        Assert.assertEquals(false, hs.contains("10"));

        // Test getDynamicFilterSupplier
        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression))),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        List<Set<DynamicFilter>> dynamicFilters = dynamicFilterSupplier.get();
        assertFalse(dynamicFilters == null, "dynamic filters should be ready");
        assertEquals(dynamicFilters.size(), 1, "there should be 1 dynamic filter in supplier");

        HashSetDynamicFilter hsDF = ((HashSetDynamicFilter) dynamicFilters.get(0).toArray()[0]);
        assertEquals(hs, hsDF.getSetValues(), "dynamic filter in supplier should be the same as the one merged");

        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(new QueryId("invalid"),
                ImmutableList.of(ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression))),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set for invalid or non-existing queryId");

        String queryId = session.getQueryId().getId();
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId)).size(), 2);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId)).size(), 2);
        dynamicFilterService.clearDynamicFiltersForQuery(queryId);
        Thread.sleep(1000);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId)).size(), 0);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId)).size(), 0);

        assertTrue(hsDF.hasMinMaxStats());
        assertEquals(hsDF.getMax(), 18L);
        assertEquals(hsDF.getMin(), 11L);
    }

    private Set fetchDynamicFilterHashSet(String filterId, String queryId)
    {
        Set hashSet = (Set) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS))
                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
        Assert.assertNotNull(hashSet);

        return hashSet;
    }

    private void mockLocalDynamicFilterHashSet(String taskId, String filterId, String queryId, List<Long> values)
    {
        HashSet filter = new HashSet();
        for (Long val : values) {
            filter.add(val);
        }

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId);

        try {
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key)).add(filter);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId))).add(taskId);
        }

        catch (Exception e) {
            Assert.fail("could not register finish filter, Exception happened:" + e.getMessage());
        }
    }
}
