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

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.MockManagedQueryExecution;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.SharedResourceGroupState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test DistributedResourceGroupUtils
 *
 * @since 2019-11-29
 */
public class TestDistributedResourceGroupUtils
{
    // To ensure that test cases are run sequentially
    private final Object lock = new Object();

    @Test
    public void testMapCachedStates()
    {
        synchronized (lock) {
            ResourceGroupId root = new ResourceGroupId("root");
            // set up queryStates
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
            query1.setResourceGroupId(root);
            query2.setResourceGroupId(root);
            query3.setResourceGroupId(root);
            // query1 running, query2 queued, query3 completed
            query1.startWaitingForResources();
            query3.complete();
            SharedQueryState queryState1 = getSharedQueryState(query1);
            SharedQueryState queryState2 = getSharedQueryState(query2);
            SharedQueryState queryState3 = getSharedQueryState(query3);
            Map<String, SharedQueryState> queryStates = ImmutableMap.of(query1.toString(), queryState1,
                    query2.toString(), queryState2, query3.toString(), queryState3);
            StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);

            // set up cpu usage states
            Map<String, Long> cpuUsageStates = ImmutableMap.of(root.toString(), 1000L);
            StateCacheStore.get().setCachedStates(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME, cpuUsageStates);

            // map QueryStates and cpuUsageStates to ResourceGroups
            DistributedResourceGroupUtils.mapCachedStates();
            Map resourceGroupStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);

            // check ResourceGroupState
            assertTrue(resourceGroupStates.containsKey(root));
            SharedResourceGroupState resourceGroupState = (SharedResourceGroupState) resourceGroupStates.get(root);
            assertEquals(resourceGroupState.getRunningQueries().size(), 1);
            assertTrue(resourceGroupState.getRunningQueries().contains(queryState1));
            assertEquals(resourceGroupState.getQueuedQueries().size(), 1);
            assertEquals(resourceGroupState.getQueuedQueries().peek(), queryState2);
            assertEquals(resourceGroupState.getFinishedQueries().size(), 1);
            assertEquals(resourceGroupState.getFinishedQueries().peek(), queryState3);
            StateCacheStore.get().resetCachedStates();
        }
    }

    @Test
    public void testUpdateResourceGroupLastExecutionTime()
    {
        synchronized (lock) {
            ResourceGroupId root = new ResourceGroupId("root");
            // set up queryStates
            Map<String, SharedQueryState> queryStates = new HashMap<>();
            MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
            query1.setResourceGroupId(root);
            query1.startWaitingForResources(); // query1 running
            SharedQueryState queryState1 = getSharedQueryState(query1);
            queryStates.put(query1.toString(), queryState1);
            Optional<DateTime> queryStartTime1 = queryState1.getExecutionStartTime();
            StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);

            // map QueryStates to ResourceGroups and update ResourceGroup LastExecutionTime
            DistributedResourceGroupUtils.mapCachedStates();
            Map resourceGroupStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
            // lastExecutionTime should be updated to query1's start execution time
            assertTrue(resourceGroupStates.containsKey(root));
            SharedResourceGroupState resourceGroupState = (SharedResourceGroupState) resourceGroupStates.get(root);
            assertEquals(queryStartTime1, resourceGroupState.getLastExecutionTime());

            // add new queries: query2 (finished) and query3(queued)
            MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
            query2.setResourceGroupId(root);
            query2.complete();
            SharedQueryState queryState2 = getSharedQueryState(query2);
            MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
            query3.setResourceGroupId(root);
            SharedQueryState queryState3 = getSharedQueryState(query3);
            queryStates.put(query2.toString(), queryState2);
            queryStates.put(query3.toString(), queryState3);
            Optional<DateTime> queryStartTime2 = queryState2.getExecutionStartTime();
            StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);
            DistributedResourceGroupUtils.mapCachedStates();

            // check ResourceGroupState lastExecutionTime
            resourceGroupState = (SharedResourceGroupState) StateCacheStore.get()
                    .getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME).get(root);
            // lastExecutionTime should be updated to query2's start execution time
            assertEquals(queryStartTime2, resourceGroupState.getLastExecutionTime());
            StateCacheStore.get().resetCachedStates();
        }
    }

    private static SharedQueryState getSharedQueryState(MockManagedQueryExecution query)
    {
        return new SharedQueryState(
                query.getBasicQueryInfo(),
                query.getErrorCode(),
                query.getUserMemoryReservation(),
                query.getTotalMemoryReservation(),
                query.getTotalCpuTime(),
                new DateTime(),
                Optional.of(new DateTime()));
    }
}
