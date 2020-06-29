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
package io.prestosql.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.resourcegroups.BaseResourceGroup;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.SharedResourceGroupState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.math.LongMath.saturatedAdd;
import static com.google.common.math.LongMath.saturatedMultiply;
import static com.google.common.math.LongMath.saturatedSubtract;
import static io.prestosql.spi.ErrorType.USER_ERROR;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Util functions for DistributedResourceGroup
 *
 * @since 2019-11-29
 */
public class DistributedResourceGroupUtils
{
    private static Logger log = Logger.get(StateCacheStore.class);
    private static DateTime lastUpdateTime = new DateTime();
    private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();
    private static final AtomicLong LAST_CPU_QUOTA_GENERATION_NANOS = new AtomicLong(System.nanoTime());

    private static final Long NANO_SECONDS_PER_SECOND = 1_000_000_000L;

    private DistributedResourceGroupUtils()
    {
    }

    /**
     * Map cached states to resource group states in StateCacheStore
     */
    public static synchronized void mapCachedStates()
    {
        mapQueryStatesToResourceGroups();
        mapCpuUsageStateToResourceGroups();
    }

    /**
     * Update cpu usage StateCollection. Calculate cpu usage for each resource group and generate new cpu quota.
     *
     * @param stateStore where cpu usage StateCollection is stored
     * @param groups existing resource groups
     */
    public static void generateCpuQuotaForAllGroups(StateStore stateStore, Map<ResourceGroupId, BaseResourceGroup> groups)
    {
        accumulateCpuUsage(stateStore);
        updateCpuQuota(stateStore, groups);
    }

    /**
     * Accumulate cpu usage for eligible queries' resource group.
     *
     * @param stateStore StateStore to save the CPU usage info
     */
    public static void accumulateCpuUsage(StateStore stateStore)
    {
        StateCollection queryStateCollection = stateStore.getStateCollection(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        if (queryStateCollection == null) {
            return;
        }

        // Update cpu usage for each resource group based on queryStates for finished queries
        StateMap cpuUsageCollection = (StateMap) stateStore.getStateCollection(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME);
        Map<String, String> queryStates = ((StateMap<String, String>) queryStateCollection).getAll();
        for (Map.Entry<String, String> entry : queryStates.entrySet()) {
            try {
                SharedQueryState queryState = MAPPER.readerFor(SharedQueryState.class).readValue(entry.getValue());
                if (queryEligibleForCpuUpdate(queryState)) {
                    String id = queryState.getBasicQueryInfo().getResourceGroupId().get().toString();
                    long cpuUsageMillis = cpuUsageCollection.get(id) == null ? 0 : (long) cpuUsageCollection.get(id);
                    cpuUsageMillis = saturatedAdd(cpuUsageMillis, queryState.getTotalCpuTime().toMillis());
                    cpuUsageCollection.put(id, cpuUsageMillis);
                }
            }
            catch (Exception e) {
                log.error(e);
            }
        }

        lastUpdateTime = new DateTime();
    }

    /**
     * Check whether a query's cpu time should be added to the resource group's cpu usage
     */
    private static Boolean queryEligibleForCpuUpdate(SharedQueryState query)
    {
        // skip queries that have no resourceGroupId or already checked in the previous state update
        if (!query.getBasicQueryInfo().getResourceGroupId().isPresent() || query.getStateUpdateTime().isBefore(lastUpdateTime)) {
            return false;
        }

        // only count queries that succeeded or the error caused by user
        return (query.getBasicQueryInfo().getState() == QueryState.FINISHED && !query.getErrorCode().isPresent()) || (
                query.getBasicQueryInfo().getState() == QueryState.FAILED && query.getErrorCode().get().getType() == USER_ERROR);
    }

    /**
     * Generate new cpu quota for all resource groups and update in the cpu usage StateCollection.
     *
     * @param stateStore StateStore that saves the cpu quota
     * @param groups existing resource groups
     */
    public static void updateCpuQuota(StateStore stateStore, Map<ResourceGroupId, BaseResourceGroup> groups)
    {
        StateMap cpuUsageCollection = (StateMap) stateStore.getStateCollection(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME);
        Map<String, Long> cpuUsages = ((StateMap<String, Long>) cpuUsageCollection).getAll();
        long elapsedSeconds = getElapsedSeconds();
        // Only advance our clock on second boundaries to avoid generating cpu quota too frequently,
        // and because it would be a no-op for zero seconds.
        if (elapsedSeconds > 0) {
            for (Map.Entry<String, Long> entry : cpuUsages.entrySet()) {
                long cpuQuotaGenerationMillisPerSecond = groups.containsKey(entry.getKey()) ?
                        groups.get(entry.getKey()).getCpuQuotaGenerationMillisPerSecond() : Long.MAX_VALUE;
                long newCpuQuota = saturatedMultiply(elapsedSeconds, cpuQuotaGenerationMillisPerSecond);
                long cpuUsageMillis = saturatedSubtract(entry.getValue(), newCpuQuota);
                if (cpuUsageMillis < 0 || cpuUsageMillis == Long.MAX_VALUE) {
                    cpuUsageMillis = 0;
                }
                cpuUsageCollection.put(entry.getKey(), cpuUsageMillis);
            }
        }
    }

    /**
     * Calculate the elapsed seconds between current time and last state update
     */
    private static long getElapsedSeconds()
    {
        long nanoTime = System.nanoTime();
        long elapsedSeconds = NANOSECONDS.toSeconds(nanoTime - LAST_CPU_QUOTA_GENERATION_NANOS.get());
        if (elapsedSeconds > 0) {
            LAST_CPU_QUOTA_GENERATION_NANOS.addAndGet(elapsedSeconds * NANO_SECONDS_PER_SECOND);
        }
        else if (elapsedSeconds < 0) {
            // nano time has overflowed
            LAST_CPU_QUOTA_GENERATION_NANOS.set(nanoTime);
        }
        return elapsedSeconds;
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

    /**
     * Update cpuUsage for each SharedResourceGroupState based on cpuUsage StateCollection cache
     */
    private static void mapCpuUsageStateToResourceGroups()
    {
        Map<ResourceGroupId, Long> cpuUsageMap = StateCacheStore.get().getCachedStates(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME);
        if (cpuUsageMap == null) {
            return;
        }

        Map<ResourceGroupId, SharedResourceGroupState> resourceGroupStates = StateCacheStore.get().getCachedStates(StateStoreConstants.RESOURCE_GROUP_STATE_COLLECTION_NAME);
        if (resourceGroupStates != null) {
            for (SharedResourceGroupState resourceGroupState : resourceGroupStates.values()) {
                String id = resourceGroupState.getId().toString();
                if (cpuUsageMap.get(id) != null) {
                    resourceGroupState.setCpuUsageMillis(cpuUsageMap.get(id));
                }
            }
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

    public static DateTime getLastUpdateTime()
    {
        return lastUpdateTime;
    }
}
