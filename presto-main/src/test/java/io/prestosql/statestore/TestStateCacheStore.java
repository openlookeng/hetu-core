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
package io.prestosql.statestore;

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.MockManagedQueryExecution;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test class for StateCacheStore
 *
 * @since 2019-11-29
 */
public class TestStateCacheStore
{
    @AfterMethod
    public void clear()
    {
        StateCacheStore.get().resetCachedStates();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "cacheName is null")
    public void testGetCachedStatesNull()
    {
        StateCacheStore.get().getCachedStates(null);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "states is null")
    public void testSetCachedStatesNull()
    {
        StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, null);
    }

    @Test
    public void testQueryCachedStates()
    {
        Map<String, SharedQueryState> queryStates = new HashMap<>();
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        queryStates.put(query1.toString(), getSharedQueryState(query1));
        queryStates.put(query2.toString(), getSharedQueryState(query2));
        StateCacheStore.get().setCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, queryStates);
        Map resultQueryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        assertQueryStatesEquals(resultQueryStates, queryStates);
    }

    @Test
    public void testResourceGroupCachedStates()
    {
        MockManagedQueryExecution query1 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query2 = new MockManagedQueryExecution(0);
        MockManagedQueryExecution query3 = new MockManagedQueryExecution(0);
        ResourceGroupId group1 = new ResourceGroupId("group1");
        ResourceGroupId group2 = new ResourceGroupId("group2");
        ResourceGroupId group3 = new ResourceGroupId("group3");
        SharedResourceGroupState resourceGroupState1 = new SharedResourceGroupState(group1);
        resourceGroupState1.addQueryState(getSharedQueryState(query1));
        SharedResourceGroupState resourceGroupState2 = new SharedResourceGroupState(group2);
        resourceGroupState2.addQueryState(getSharedQueryState(query2));
        SharedResourceGroupState resourceGroupState3 = new SharedResourceGroupState(group3);
        resourceGroupState3.addQueryState(getSharedQueryState(query3));
        Map<ResourceGroupId, SharedResourceGroupState> resourceGroupStates = ImmutableMap.of(
                group1, resourceGroupState1,
                group2, resourceGroupState2,
                group3, resourceGroupState3);
        StateCacheStore.get().setCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME, resourceGroupStates);
        Map<ResourceGroupId, SharedResourceGroupState> result = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
        assertResourceGroupStateEquals(result, resourceGroupStates);
    }

    private static SharedQueryState getSharedQueryState(MockManagedQueryExecution query)
    {
        return new SharedQueryState(
                query.getBasicQueryInfo(),
                query.getSession().toSessionRepresentation(),
                query.getErrorCode(),
                query.getUserMemoryReservation(),
                query.getTotalMemoryReservation(),
                query.getTotalCpuTime(),
                new DateTime(DateTimeZone.UTC),
                Optional.of(new DateTime(DateTimeZone.UTC)));
    }

    private void assertQueryStatesEquals(Map<String, SharedQueryState> actual, Map<String, SharedQueryState> expected)
    {
        assertEquals(expected.size(), actual.size());
        for (String queryId : actual.keySet()) {
            SharedQueryState actualQuery = actual.get(queryId);
            SharedQueryState expectedQuery = expected.get(queryId);
            assertTrue(Objects.equals(actualQuery.getBasicQueryInfo(), expectedQuery.getBasicQueryInfo()) &&
                    Objects.equals(actualQuery.getSession(), expectedQuery.getSession()) &&
                    Objects.equals(actualQuery.getErrorCode(), expectedQuery.getErrorCode()) &&
                    Objects.equals(actualQuery.getUserMemoryReservation(), expectedQuery.getUserMemoryReservation()) &&
                    Objects.equals(actualQuery.getTotalMemoryReservation(), expectedQuery.getTotalMemoryReservation()) &&
                    Objects.equals(actualQuery.getTotalCpuTime(), expectedQuery.getTotalCpuTime()) &&
                    Objects.equals(actualQuery.getStateUpdateTime(), expectedQuery.getStateUpdateTime()) &&
                    Objects.equals(actualQuery.getExecutionStartTime(), expectedQuery.getExecutionStartTime()));
        }
    }

    private void assertResourceGroupStateEquals(Map<ResourceGroupId, SharedResourceGroupState> actual,
                                                Map<ResourceGroupId, SharedResourceGroupState> expected)
    {
        assertEquals(expected.size(), actual.size());
        for (ResourceGroupId queryId : actual.keySet()) {
            SharedResourceGroupState actualState = actual.get(queryId);
            SharedResourceGroupState expectedState = expected.get(queryId);
            assertTrue(Objects.equals(actualState.getId(), expectedState.getId()) &&
                    actualState.getQueuedQueries() == expectedState.getQueuedQueries() &&
                    actualState.getRunningQueries() == expectedState.getRunningQueries() &&
                    actualState.getFinishedQueries() == expectedState.getFinishedQueries() &&
                    Objects.equals(actualState.getLastExecutionTime(), expectedState.getLastExecutionTime()));
        }
    }
}
