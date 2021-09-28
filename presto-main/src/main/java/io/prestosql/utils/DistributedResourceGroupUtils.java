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

import io.airlift.log.Logger;
import io.prestosql.execution.QueryState;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.SharedResourceGroupState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Util functions for DistributedResourceGroup
 *
 * @since 2019-11-29
 */
public class DistributedResourceGroupUtils
{
    private static Logger log = Logger.get(StateCacheStore.class);

    private DistributedResourceGroupUtils()
    {
    }

    /**
     * Map cached states to resource group states in StateCacheStore
     */
    public static synchronized void mapCachedStates()
    {
        mapQueryStatesToResourceGroups();
    }

    /**
     * Map cached query states to resource groups
     */
    private static void mapQueryStatesToResourceGroups()
    {
        Map<String, SharedQueryState> queryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        if (queryStates == null) {
            return;
        }

        Map<ResourceGroupId, SharedResourceGroupState> resourceGroupStates = new HashMap<>();

        for (SharedQueryState state : queryStates.values()) {
            Optional<ResourceGroupId> resourceGroupId = state.getBasicQueryInfo().getResourceGroupId();
            if (!resourceGroupId.isPresent()) {
                continue;
            }

            if (!resourceGroupStates.containsKey(resourceGroupId.get())) {
                resourceGroupStates.put(resourceGroupId.get(), new SharedResourceGroupState(resourceGroupId.get()));
            }
            SharedResourceGroupState resourceGroupState = resourceGroupStates.get(resourceGroupId.get());
            resourceGroupState.addQueryState(state);
            updateResourceGroupLastExecutionTime(state, resourceGroupState);
            logQueryState(state);
        }

        StateCacheStore.get().setCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME, resourceGroupStates);
    }

    private static void updateResourceGroupLastExecutionTime(SharedQueryState query, SharedResourceGroupState resourceGroupState)
    {
        if (query.getBasicQueryInfo().getState() != QueryState.QUEUED) {
            if (!query.getBasicQueryInfo().getResourceGroupId().isPresent()) {
                return;
            }

            Optional<DateTime> executionStartTime = query.getExecutionStartTime();
            executionStartTime.ifPresent(time -> resourceGroupState.updateLastExecutionTime(time));
        }
    }

    private static void logQueryState(SharedQueryState state)
    {
        log.debug(String.format("Id: %s, state: %s, uri: %s, query: %s",
                state.getBasicQueryInfo().getQueryId().getId(),
                state.getBasicQueryInfo().getState().toString(),
                state.getBasicQueryInfo().getSelf().toString(),
                state.getBasicQueryInfo().getQuery()));
    }
}
