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

import io.prestosql.execution.QueryState;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * SharedResourceGroupState created based on query states from StateStore and contains aggregated info of corresponding resource group
 *
 * @since 2019-11-29
 */
public class SharedResourceGroupState
{
    private final ResourceGroupId id;
    private final PriorityQueue<SharedQueryState> queuedQueries = new PriorityQueue<>(
            Comparator.comparing(state -> state.getBasicQueryInfo().getQueryStats().getQueuedTime(), Comparator.reverseOrder()));
    private final Set<SharedQueryState> runningQueries = new HashSet<>();
    private final PriorityQueue<SharedQueryState> finishedQueries = new PriorityQueue<>(Comparator.comparing(state -> state.getStateUpdateTime()));
    private Optional<DateTime> lastExecutionTime = Optional.empty();
    private long cpuUsageMillis;

    public SharedResourceGroupState(ResourceGroupId id)
    {
        this.id = id;
    }

    public ResourceGroupId getId()
    {
        return id;
    }

    public PriorityQueue<SharedQueryState> getQueuedQueries()
    {
        return queuedQueries;
    }

    public Set<SharedQueryState> getRunningQueries()
    {
        return runningQueries;
    }

    public PriorityQueue<SharedQueryState> getFinishedQueries()
    {
        return finishedQueries;
    }

    public Optional<DateTime> getLastExecutionTime()
    {
        return lastExecutionTime;
    }

    public long getCpuUsageMillis()
    {
        return cpuUsageMillis;
    }

    public void setCpuUsageMillis(long cpuUsage)
    {
        cpuUsageMillis = cpuUsage;
    }

    /**
     * Add the current query state to corresponding query state collections
     *
     * @param state query state to be added
     */
    public void addQueryState(SharedQueryState state)
    {
        if (state.getBasicQueryInfo().getState() == QueryState.QUEUED) {
            queuedQueries.add(state);
        }
        else if (state.getBasicQueryInfo().getState() == QueryState.FINISHED || state.getBasicQueryInfo().getState() == QueryState.FAILED) {
            finishedQueries.add(state);
        }
        else {
            runningQueries.add(state);
        }
    }

    /**
     * Update the execution timestamp
     *
     * @param executionStartTime current execution timestamp
     */
    public void updateLastExecutionTime(DateTime executionStartTime)
    {
        // Only update lastExecutionTime if it's newer
        if (!lastExecutionTime.isPresent() || executionStartTime.isAfter(lastExecutionTime.get())) {
            lastExecutionTime = Optional.ofNullable(executionStartTime);
        }
    }
}
