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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterListenerService;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.TestingColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.MockStateMap;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.listener.StateStoreListenerManager;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_DATA_TYPE;
import static io.prestosql.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.utils.DynamicFilterUtils.MERGEMAP;
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
    public void TestCollectingGlobalDynamicFilters() throws InterruptedException
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

        Map mockMap = new HashMap<>();
        StateStoreProvider stateStoreProvider = mock(StateStoreProvider.class);
        StateStore stateStore = mock(StateStore.class);
        StateMap stateMap = new MockStateMap<>("test-map", mockMap);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);
        when(stateStore.getStateCollection(any())).thenReturn(stateMap);
        when(stateStore.createStateMap(any())).thenReturn(stateMap);

        StateStoreListenerManager stateStoreListenerManager = new StateStoreListenerManager(stateStoreProvider);
        DynamicFilterListenerService dynamicFilterListenerService = new DynamicFilterListenerService(stateStoreProvider);
        stateStoreListenerManager.addStateStoreListener(dynamicFilterListenerService, MERGEMAP);
        LocalDynamicFiltersCollector collector = new LocalDynamicFiltersCollector(
                taskContext,
                stateStoreProvider,
                dynamicFilterListenerService);
        TableScanNode tableScan = mock(TableScanNode.class);
        when(tableScan.getAssignments()).thenReturn(ImmutableMap.of(new Symbol(columnName), columnHandle));
        List<DynamicFilters.Descriptor> dynamicFilterDescriptors = ImmutableList.of(new DynamicFilters.Descriptor(filterId, new SymbolReference(columnName)));
        collector.initContext(tableScan.getAssignments(), dynamicFilterDescriptors);

        assertTrue(collector.getDynamicFilters(tableScan).isEmpty());
        stateMap.put(createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId.getId()), valueSet);
        Map<ColumnHandle, DynamicFilter> dynamicFilters = collector.getDynamicFilters(tableScan);
        assertEquals(dynamicFilters.size(), 1);
        DynamicFilter dynamicFilter = dynamicFilters.get(columnHandle);
        assertTrue(dynamicFilter instanceof HashSetDynamicFilter);
        assertEquals(dynamicFilter.getSize(), valueSet.size());
        for (String value : valueSet) {
            assertTrue(dynamicFilter.contains(value));
        }
        stateMap.remove(createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId.getId()));
        String cacheKey = filterId + "-" + queryId.getId();
        dynamicFilterListenerService.removeDynamicFilter(cacheKey);
        Thread.sleep(10);
        assertNull(dynamicFilterListenerService.getDynamicFilter(cacheKey));
    }
}
