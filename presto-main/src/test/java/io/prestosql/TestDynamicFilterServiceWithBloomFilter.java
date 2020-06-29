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
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.StageStateMachine;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.JoinNode.DistributionType;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.testing.assertions.Assert;
import io.prestosql.utils.DynamicFilterUtils;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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
        session = testSessionBuilder().setQueryId(QueryId.valueOf("qq1")).build();

        StateStore stateStore = setupMockStateStore(new HashMap<>(), new HashMap<>(), new HashSet<>(), new HashSet<>(),
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
        registerDf(filterId, session, PARTITIONED);

        // Test getDynamicFilterSupplier
        SymbolReference mockExpression = mock(SymbolReference.class);
        when(mockExpression.getName()).thenReturn("name");
        ColumnHandle mockColumnHandle = mock(ColumnHandle.class);
        Supplier<Set<DynamicFilter>> dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set when dynamic filters are not available");

        mockDynamicFilterSourceOperator("w1", "d1", filterId, session.getQueryId().toString(), Arrays.asList("1", "2"));
        mockDynamicFilterSourceOperator("w1", "d2", filterId, session.getQueryId().toString(), Arrays.asList("3", "4"));
        mockDynamicFilterSourceOperator("w2", "d3", filterId, session.getQueryId().toString(), Arrays.asList("5", "6"));
        mockDynamicFilterSourceOperator("w2", "d4", filterId, session.getQueryId().toString(), Arrays.asList("7", "8"));

        Assert.assertEquals(stateStoreProvider.getStateStore()
                .getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, session.getQueryId().toString())).size(), 4);

        Thread.sleep(2000);
        BloomFilter bf = fetchDynamicFilter(filterId, session.getQueryId().toString());
        for (int i = 1; i < 9; i++) {
            Assert.assertEquals(true, bf.mightContain(i + ""));
        }
        Assert.assertEquals(false, bf.mightContain("10"));

        // Test getDynamicFilterSupplier
        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        Set<DynamicFilter> dynamicFilters = dynamicFilterSupplier.get();
        assertFalse(dynamicFilters == null, "dynamic filters should be ready");
        assertEquals(dynamicFilters.size(), 1, "there should be 1 dynamic filter in supplier");

        BloomFilter bloomFilter = ((BloomFilterDynamicFilter) dynamicFilters.toArray()[0]).getBloomFilterDeserialized();
        assertEquals(bf, bloomFilter, "dynamic filter in supplier should be the same as the one merged");

        dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(new QueryId("invalid"),
                ImmutableList.of(new DynamicFilters.Descriptor(filterId, mockExpression)),
                ImmutableMap.of(new Symbol("name"), mockColumnHandle));
        assertTrue(dynamicFilterSupplier.get().isEmpty(), "should return empty dynamic filter set for invalid or non-existing queryId");
    }

    private void registerDf(String filterId, Session session, DistributionType joinType)
    {
        JoinNode node = mock(JoinNode.class);
        HashMap<String, Symbol> dfs = new HashMap<>();

        List<JoinNode.EquiJoinClause> criteria = new ArrayList<JoinNode.EquiJoinClause>();
        Symbol right = new Symbol("rightCol");
        Symbol left = new Symbol("leftCol");
        JoinNode.EquiJoinClause clause = new JoinNode.EquiJoinClause(left, right);
        criteria.add(clause);
        dfs.put(filterId, right);

        when(node.getCriteria()).thenReturn(criteria);
        when(node.getDynamicFilters()).thenReturn(dfs);
        when(node.getDistributionType()).thenReturn(Optional.of(joinType));

        TaskId taskid = mock(TaskId.class);
        HashSet<TaskId> tasks = new HashSet<>();
        tasks.add(taskid);
        StageStateMachine stateMachine = mock(StageStateMachine.class);

        when(stateMachine.getSession()).thenReturn(session);

        InternalNode worker = mock(InternalNode.class);
        InternalNode worker2 = mock(InternalNode.class);
        HashSet<InternalNode> workers = new HashSet<>();
        when(worker.getNodeIdentifier()).thenReturn("w1");
        when(worker2.getNodeIdentifier()).thenReturn("w2");

        workers.add(worker);
        workers.add(worker2);

        dynamicFilterService.registerTasks(node, tasks, workers, stateMachine);
    }

    private BloomFilter fetchDynamicFilter(String filterId, String queryId)
    {
        byte[] bloomFilter = (byte[]) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP))
                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
        Assert.assertNotNull(bloomFilter);

        return deserializeBloomFilter(bloomFilter);
    }

    private BloomFilter deserializeBloomFilter(byte[] serializedBloomFilter)
    {
        try (java.io.ByteArrayInputStream bis = new java.io.ByteArrayInputStream(serializedBloomFilter)) {
            return BloomFilter.readFrom(bis, Funnels.stringFunnel(Charset.defaultCharset()));
        }
        catch (IOException e) {
            Assert.fail("Dynamic Filter cannot be created from byte array");
        }
        return null;
    }

    private void mockDynamicFilterSourceOperator(String workerId, String driverId, String filterId, String queryId, List<String> values)
    {
        ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId))).add(driverId);

        BloomFilter bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1024 * 1024, 0.1);
        for (String val : values) {
            bloomFilter.put(val);
        }

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId);
        String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, filterId, queryId);
        ((StateMap) stateStoreProvider.getStateStore()
                .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).put(typeKey, DynamicFilterUtils.BLOOMFILTERTYPEGLOBAL);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            bloomFilter.writeTo(out);
            byte[] finalOutput = out.toByteArray();
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key)).add(finalOutput);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId))).add(driverId);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId))).add(workerId);
        }

        catch (IOException e) {
            Assert.fail("could not register finish filter, Exception happened:" + e.getMessage());
        }
    }

    private StateStore setupMockStateStore(Map<String, byte[]> mergeMap, Map<String, String> dfTypeMap, Set<String> registerSet, Set<String> finishSet, Set<String> workers, Set<byte[]> partial, String queryId, String filterId)
    {
        StateMap mockMergeMap = mock(StateMap.class);
        StateMap mockDFTypeMap = mock(StateMap.class);
        StateSet mockPartialSet = mock(StateSet.class);
        StateSet mockRegisterSet = mock(StateSet.class);
        StateSet mockWorkersSet = mock(StateSet.class);
        StateSet mockFinishSet = mock(StateSet.class);
        StateStore stateStore = mock(StateStore.class);

        when(mockMergeMap.put(anyString(), any(byte[].class))).thenAnswer(i -> mergeMap.put((String) i.getArguments()[0], (byte[]) i.getArguments()[1]));
        when(mockDFTypeMap.put(anyString(), anyString())).thenAnswer(i -> dfTypeMap.put((String) i.getArguments()[0], (String) i.getArguments()[1]));
        when(mockWorkersSet.add(anyString())).thenAnswer(i -> workers.add((String) i.getArguments()[0]));
        when(mockRegisterSet.add(anyString())).thenAnswer(i -> registerSet.add((String) i.getArguments()[0]));
        when(mockFinishSet.add(anyString())).thenAnswer(i -> finishSet.add((String) i.getArguments()[0]));
        when(mockPartialSet.add(any(byte[].class))).thenAnswer(i -> partial.add((byte[]) i.getArguments()[0]));

        when(mockMergeMap.get(anyString())).thenAnswer(i -> mergeMap.get(i.getArguments()[0]));
        when(mockDFTypeMap.get(anyString())).thenAnswer(i -> dfTypeMap.get(i.getArguments()[0]));

        when(mockMergeMap.getAll()).thenReturn(mergeMap);
        when(mockDFTypeMap.getAll()).thenReturn(dfTypeMap);

        when(mockPartialSet.getAll()).thenReturn(partial);
        when(mockRegisterSet.size()).thenAnswer(i -> registerSet.size());
        when(mockPartialSet.size()).thenAnswer(i -> partial.size());
        when(mockWorkersSet.size()).thenAnswer(i -> workers.size());
        when(mockFinishSet.size()).thenAnswer(i -> finishSet.size());

        when(stateStore.getStateCollection(DynamicFilterUtils.MERGEMAP)).thenReturn(mockMergeMap);
        when(stateStore.createStateCollection(DynamicFilterUtils.MERGEMAP, StateCollection.Type.MAP)).thenReturn(mockMergeMap);

        when(stateStore.getStateCollection(DynamicFilterUtils.DFTYPEMAP)).thenReturn(mockDFTypeMap);
        when(stateStore.createStateCollection(DynamicFilterUtils.DFTYPEMAP, StateCollection.Type.MAP)).thenReturn(mockDFTypeMap);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId))).thenReturn(mockWorkersSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockWorkersSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId))).thenReturn(mockFinishSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockFinishSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId))).thenReturn(mockPartialSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockPartialSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId))).thenReturn(mockRegisterSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockRegisterSet);

        return stateStore;
    }

    private void mockDynamicFilterSourceOperatorHashSet(String workerId, String driverId, String filterId, String queryId, List<String> values)
    {
        ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId))).add(driverId);

        HashSet filter = new HashSet();
        for (String val : values) {
            filter.add(val);
        }

        String key = DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId);
        String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, filterId, queryId);
        ((StateMap) stateStoreProvider.getStateStore()
                .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).put(typeKey, DynamicFilterUtils.HASHSETTYPEGLOBAL);

        try {
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(key)).add(filter);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId))).add(driverId);
            ((StateSet) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId))).add(workerId);
        }

        catch (Exception e) {
            Assert.fail("could not register finish filter, Exception happened:" + e.getMessage());
        }
    }

    private StateStore setupMockStateStoreHashSet(Map<String, Set> mergeMap, Map<String, String> dfTypeMap, Set<String> registerSet, Set<String> finishSet, Set<String> workers, Set<Set> partial, String queryId, String filterId)
    {
        StateMap mockMergeMap = mock(StateMap.class);
        StateMap mockDFTypeMap = mock(StateMap.class);
        StateSet mockPartialSet = mock(StateSet.class);
        StateSet mockRegisterSet = mock(StateSet.class);
        StateSet mockWorkersSet = mock(StateSet.class);
        StateSet mockFinishSet = mock(StateSet.class);
        StateStore stateStore = mock(StateStore.class);

        when(mockMergeMap.put(anyString(), any(Set.class))).thenAnswer(i -> mergeMap.put((String) i.getArguments()[0], (Set) i.getArguments()[1]));
        when(mockDFTypeMap.put(anyString(), anyString())).thenAnswer(i -> dfTypeMap.put((String) i.getArguments()[0], (String) i.getArguments()[1]));
        when(mockWorkersSet.add(anyString())).thenAnswer(i -> workers.add((String) i.getArguments()[0]));
        when(mockRegisterSet.add(anyString())).thenAnswer(i -> registerSet.add((String) i.getArguments()[0]));
        when(mockFinishSet.add(anyString())).thenAnswer(i -> finishSet.add((String) i.getArguments()[0]));
        when(mockPartialSet.add(any(Set.class))).thenAnswer(i -> partial.add((Set) i.getArguments()[0]));

        when(mockMergeMap.get(anyString())).thenAnswer(i -> mergeMap.get(i.getArguments()[0]));
        when(mockDFTypeMap.get(anyString())).thenAnswer(i -> dfTypeMap.get(i.getArguments()[0]));

        when(mockMergeMap.getAll()).thenReturn(mergeMap);
        when(mockDFTypeMap.getAll()).thenReturn(dfTypeMap);

        when(mockPartialSet.getAll()).thenReturn(partial);
        when(mockRegisterSet.size()).thenAnswer(i -> registerSet.size());
        when(mockPartialSet.size()).thenAnswer(i -> partial.size());
        when(mockWorkersSet.size()).thenAnswer(i -> workers.size());
        when(mockFinishSet.size()).thenAnswer(i -> finishSet.size());

        when(stateStore.getStateCollection(DynamicFilterUtils.MERGEMAP)).thenReturn(mockMergeMap);
        when(stateStore.createStateCollection(DynamicFilterUtils.MERGEMAP, StateCollection.Type.MAP)).thenReturn(mockMergeMap);

        when(stateStore.getStateCollection(DynamicFilterUtils.DFTYPEMAP)).thenReturn(mockDFTypeMap);
        when(stateStore.createStateCollection(DynamicFilterUtils.DFTYPEMAP, StateCollection.Type.MAP)).thenReturn(mockDFTypeMap);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId))).thenReturn(mockWorkersSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockWorkersSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId))).thenReturn(mockFinishSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockFinishSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId))).thenReturn(mockPartialSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockPartialSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId))).thenReturn(mockRegisterSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockRegisterSet);

        return stateStore;
    }
}
