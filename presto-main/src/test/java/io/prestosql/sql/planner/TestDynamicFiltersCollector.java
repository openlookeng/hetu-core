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
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.statestore.MockStateMap;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_DATA_TYPE;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.utils.DynamicFilterUtils.MERGED_DYNAMIC_FILTERS;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicFiltersCollector
{
    @Test
    public void TestCollectingGlobalDynamicFilters()
            throws InterruptedException
    {
        final QueryId queryId = new QueryId("test_query");
        final String filterId = "1";
        final String columnName = "column";
        final TestingColumnHandle columnHandle = new TestingColumnHandle(columnName);
        final Set<String> valueSet = ImmutableSet.of("1", "2", "3");

        TaskContext taskContext = mock(TaskContext.class);
        Session session = testSessionBuilder()
                .setQueryId(queryId)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, "true")
                .setSystemProperty(DYNAMIC_FILTERING_DATA_TYPE, "HASHSET")
                .build();
        when(taskContext.getSession()).thenReturn(session);

        // set up state store and merged dynamic filters map
        Map mockMap = new HashMap<>();
        StateStoreProvider stateStoreProvider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        StateMap stateMap = new MockStateMap<>("test-map", mockMap);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);
        when(stateStore.getStateCollection(any())).thenReturn(stateMap);
        when(stateStore.createStateMap(any())).thenReturn(stateMap);
        when(stateStore.getOrCreateStateCollection(any(), any())).thenReturn(stateMap);

        // set up state store listener and dynamic filter cache
        StateStoreListenerManager stateStoreListenerManager = new StateStoreListenerManager(stateStoreProvider);
        DynamicFilterCacheManager dynamicFilterCacheManager = new DynamicFilterCacheManager();
        stateStoreListenerManager.addStateStoreListener(new DynamicFilterListener(dynamicFilterCacheManager), MERGED_DYNAMIC_FILTERS);
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(
                taskContext,
                Optional.empty(),
                dynamicFilterCacheManager);
        TableScanNode tableScan = mock(TableScanNode.class);
        when(tableScan.getAssignments()).thenReturn(ImmutableMap.of(new Symbol(columnName), columnHandle));
        List<DynamicFilters.Descriptor> dynamicFilterDescriptors = ImmutableList.of(new DynamicFilters.Descriptor(filterId, new VariableReferenceExpression(columnName, BIGINT)));
        collector.initContext(ImmutableList.of(dynamicFilterDescriptors), SymbolUtils.toLayOut(tableScan.getOutputSymbols()));

        assertTrue(collector.getDynamicFilters(tableScan).isEmpty(), "there should be no dynamic filter available");

        // put some values in state store as a new dynamic filter
        // and wait for the listener to process the event
        stateMap.put(createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId.getId()), valueSet);
        TimeUnit.MILLISECONDS.sleep(100);

        // get available dynamic filter and verify it
        List<Map<ColumnHandle, DynamicFilter>> dynamicFilters = collector.getDynamicFilters(tableScan);
        assertEquals(dynamicFilters.size(), 1, "there should be a new dynamic filter");
        assertEquals(dynamicFilters.size(), 1);
        DynamicFilter dynamicFilter = dynamicFilters.get(0).get(columnHandle);
        assertTrue(dynamicFilter instanceof HashSetDynamicFilter, "new dynamic filter should be hashset");
        assertEquals(dynamicFilter.getSize(), valueSet.size(), "new dynamic filter should have correct size");
        for (String value : valueSet) {
            assertTrue(dynamicFilter.contains(value), "new dynamic filter should contain correct values");
        }

        // clean up when task finishes
        collector.removeDynamicFilter(true);
        DynamicFilter cachedFilter = dynamicFilterCacheManager.getDynamicFilter(DynamicFilterCacheManager.createCacheKey(filterId, queryId.getId()));
        assertNull(cachedFilter, "cached dynamic filter should have been removed");
    }
}
