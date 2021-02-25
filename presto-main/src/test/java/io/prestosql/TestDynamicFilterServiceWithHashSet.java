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
package io.prestosql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.testing.assertions.Assert;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import static io.prestosql.SystemSessionProperties.DYNAMIC_FILTERING_DATA_TYPE;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static io.prestosql.utils.TestDynamicFilterUtil.registerDf;
import static io.prestosql.utils.TestDynamicFilterUtil.setupMockStateStore;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDynamicFilterServiceWithHashSet
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
    public void testRegisterAndMergeDynamicFiltersHashSet()
            throws InterruptedException
    {
        setUpHashSet();
        filterId = "df2";
        registerDf(filterId, session, PARTITIONED, dynamicFilterService);

        // Test getDynamicFilterSupplier
        VariableReferenceExpression mockExpression = mock(VariableReferenceExpression.class);
        when(mockExpression.getName()).thenReturn("name");
        ColumnHandle mockColumnHandle = mock(ColumnHandle.class);
        Supplier<Set<DynamicFilter>> dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set when dynamic filters are not available");

        mockLocalDynamicFilterHashSet("task1.0", filterId, session.getQueryId().toString(), Arrays.asList("11", "12", "13", "14"));
        mockLocalDynamicFilterHashSet("task1.1", filterId, session.getQueryId().toString(), Arrays.asList("15", "16", "17", "18"));

        Thread.sleep(2000);
        Set hs = fetchDynamicFilterHashSet(filterId, session.getQueryId().toString());
        for (int i = 11; i < 19; i++) {
            Assert.assertEquals(true, hs.contains(i + ""));
        }
        Assert.assertEquals(false, hs.contains("10"));

        // Test getDynamicFilterSupplier
        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        Set<DynamicFilter> dynamicFilters = dynamicFilterSupplier.get();
        assertFalse(dynamicFilters == null, "dynamic filters should be ready");
        assertEquals(dynamicFilters.size(), 1, "there should be 1 dynamic filter in supplier");

        HashSetDynamicFilter hsDF = ((HashSetDynamicFilter) dynamicFilters.toArray()[0]);
        assertEquals(hs, hsDF.getSetValues(), "dynamic filter in supplier should be the same as the one merged");

        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(new QueryId("invalid"),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set for invalid or non-existing queryId");

        String queryId = session.getQueryId().getId();
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId)).size(), 2);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId)).size(), 2);
        dynamicFilterService.clearDynamicFiltersForQuery(queryId);
        Thread.sleep(1000);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId)).size(), 0);
        assertEquals(stateStoreProvider.getStateStore().getStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId)).size(), 0);
    }

    private Set fetchDynamicFilterHashSet(String filterId, String queryId)
    {
        Set hashSet = (Set) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS))
                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
        Assert.assertNotNull(hashSet);

        return hashSet;
    }

    private void mockLocalDynamicFilterHashSet(String taskId, String filterId, String queryId, List<String> values)
    {
        HashSet filter = new HashSet();
        for (String val : values) {
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
