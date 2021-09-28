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
package io.prestosql.utils;

import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.StageStateMachine;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.planner.plan.RemoteSourceNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestDynamicFilterUtil
{
    private TestDynamicFilterUtil() {}

    public static void registerDf(String filterId, Session session, JoinNode.DistributionType joinType, DynamicFilterService dynamicFilterService)
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
        RemoteSourceNode leftNode = mock(RemoteSourceNode.class);
        when(node.getLeft()).thenReturn(leftNode);

        HashSet<TaskId> tasks = new HashSet<>();
        tasks.add(new TaskId("task1.0"));
        tasks.add(new TaskId("task1.1"));
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

    public static StateStore setupMockStateStore(Map mergeMap, Map<String, String> dfTypeMap, Set<String> tasks, Set partial, String queryId, String filterId)
    {
        StateMap mockMergeMap = mock(StateMap.class);
        StateMap mockDFTypeMap = mock(StateMap.class);
        StateSet mockPartialSet = mock(StateSet.class);
        StateSet mockTasksSet = mock(StateSet.class);
        StateStore stateStore = mock(StateStore.class);

        when(mockMergeMap.put(anyString(), any())).thenAnswer(i -> mergeMap.put(i.getArguments()[0], i.getArguments()[1]));
        when(mockDFTypeMap.put(anyString(), anyString())).thenAnswer(i -> dfTypeMap.put((String) i.getArguments()[0], (String) i.getArguments()[1]));
        when(mockTasksSet.add(anyString())).thenAnswer(i -> tasks.add((String) i.getArguments()[0]));
        when(mockPartialSet.add(any())).thenAnswer(i -> partial.add(i.getArguments()[0]));

        when(mockMergeMap.get(anyString())).thenAnswer(i -> mergeMap.get(i.getArguments()[0]));
        when(mockDFTypeMap.get(anyString())).thenAnswer(i -> dfTypeMap.get(i.getArguments()[0]));

        when(mockMergeMap.getAll()).thenReturn(mergeMap);
        when(mockDFTypeMap.getAll()).thenReturn(dfTypeMap);

        when(mockPartialSet.getAll()).thenReturn(partial);
        when(mockPartialSet.size()).thenAnswer(i -> partial.size());
        when(mockTasksSet.size()).thenAnswer(i -> tasks.size());

        when(stateStore.getStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS)).thenReturn(mockMergeMap);
        when(stateStore.createStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS, StateCollection.Type.MAP)).thenReturn(mockMergeMap);
        when(stateStore.getOrCreateStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS, StateCollection.Type.MAP)).thenReturn(mockMergeMap);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId))).thenReturn(mockTasksSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockTasksSet);

        when(stateStore.getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId))).thenReturn(mockPartialSet);
        when(stateStore.createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId), StateCollection.Type.SET)).thenReturn(mockPartialSet);

        // In statestore, set and map are destroyed and set to null after query finishes, however, in the UT we just assume the set and map to be empty.
        doAnswer(i -> {
            tasks.clear();
            return null;
        }).when(mockTasksSet).destroy();
        doAnswer(i -> {
            partial.clear();
            return null;
        }).when(mockPartialSet).destroy();

        return stateStore;
    }
}
