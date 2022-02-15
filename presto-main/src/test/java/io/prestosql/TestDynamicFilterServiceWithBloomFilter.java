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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.testing.assertions.Assert;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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

public class TestDynamicFilterServiceWithBloomFilter
{
    private DynamicFilterService dynamicFilterService;
    private StateStoreProvider stateStoreProvider;
    private String filterId;
    private Session session;

    // setup for test provider
    @BeforeTest
    private void setUp()
    {
        filterId = "df1";
        session = testSessionBuilder()
                .setQueryId(QueryId.valueOf("qq1"))
                .setSystemProperty(DYNAMIC_FILTERING_DATA_TYPE, "BLOOM_FILTER")
                .build();

        StateStore stateStore = setupMockStateStore(new HashMap<>(), new HashMap<>(),
                new HashSet<>(), new HashSet<>(), session.getQueryId().toString(), filterId);

        stateStoreProvider = mock(StateStoreProvider.class);
        when(stateStoreProvider.getStateStore()).thenReturn(stateStore);

        dynamicFilterService = new DynamicFilterService(stateStoreProvider);
        dynamicFilterService.start();
    }

    @Test
    public void testRegisterAndMergeDynamicFilters()
            throws InterruptedException
    {
        registerDf(filterId, session, PARTITIONED, dynamicFilterService);

        // Test getDynamicFilterSupplier
        VariableReferenceExpression mockExpression = mock(VariableReferenceExpression.class);
        when(mockExpression.getName()).thenReturn("name");
        ColumnHandle mockColumnHandle = mock(ColumnHandle.class);
        Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression))),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set when dynamic filters are not available");

        mockLocalDynamicFilter("task1.0", filterId, session.getQueryId().toString(), Arrays.asList("1", "2", "3", "4"));
        mockLocalDynamicFilter("task1.1", filterId, session.getQueryId().toString(), Arrays.asList("5", "6", "7", "8"));

        Thread.sleep(3000);
        BloomFilter bf = fetchDynamicFilter(filterId, session.getQueryId().toString());
        for (int i = 1; i < 9; i++) {
            assertTrue(bf.test((String.valueOf(i).getBytes(StandardCharsets.UTF_8))));
        }
        assertFalse(bf.test("10".getBytes(StandardCharsets.UTF_8)));

        // Test getDynamicFilterSupplier
        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression))),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        List<Set<DynamicFilter>> dynamicFilters = dynamicFilterSupplier.get();
        assertFalse(dynamicFilters == null, "dynamic filters should be ready");
        assertEquals(dynamicFilters.size(), 1, "there should be 1 dynamic filter in supplier");

        DynamicFilter dynamicFilter = dynamicFilters.get(0).iterator().next();
        for (int i = 1; i < 9; i++) {
            assertTrue(dynamicFilter.contains(String.valueOf(i)));
        }
        assertFalse(dynamicFilter.contains("10"));

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
    }

    private BloomFilter fetchDynamicFilter(String filterId, String queryId)
    {
        byte[] bloomFilter = (byte[]) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS))
                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
        Assert.assertNotNull(bloomFilter);

        return deserializeBloomFilter(bloomFilter);
    }

    private BloomFilter deserializeBloomFilter(byte[] serializedBloomFilter)
    {
        try (java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(serializedBloomFilter)) {
            return BloomFilter.readFrom(bis);
        }
        catch (IOException e) {
            Assert.fail("Dynamic Filter cannot be created from byte array");
        }
        return null;
    }

    private void mockLocalDynamicFilter(String taskId, String filterId, String queryId, List<String> values)
    {
        BloomFilter bloomFilter = new BloomFilter(1024 * 1024, 0.1);
        for (String val : values) {
            bloomFilter.add(val.getBytes(StandardCharsets.UTF_8));
        }

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId);
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            bloomFilter.writeTo(out);
            byte[] finalOutput = out.toByteArray();
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key)).add(finalOutput);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId))).add(taskId);
        }

        catch (IOException e) {
            Assert.fail("could not register finish filter, Exception happened:" + e.getMessage());
        }
    }
}
