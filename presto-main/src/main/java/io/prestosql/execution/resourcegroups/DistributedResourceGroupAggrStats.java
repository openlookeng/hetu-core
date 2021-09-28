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
package io.prestosql.execution.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.resourcegroups.ResourceGroupId;

import java.util.Objects;

public class DistributedResourceGroupAggrStats
{
    private final ResourceGroupId resourceGroupId;
    private final int runningQueries;
    private final int queuedQueries;
    private final int descendantRunningQueries;
    private final int descendantQueuedQueries;
    private final long cpuUsageMillis;
    private final long cachedMemoryUsageBytes;

    @JsonCreator
    public DistributedResourceGroupAggrStats(
            @JsonProperty("resourceGroupId") ResourceGroupId resourceGroupId,
            @JsonProperty("runningQueries") int runningQueries,
            @JsonProperty("queuedQueries") int queuedQueries,
            @JsonProperty("descendantRunningQueries") int descendantRunningQueries,
            @JsonProperty("descendantQueuedQueries") int descendantQueuedQueries,
            @JsonProperty("cpuUsageMillis") long cpuUsageMillis,
            @JsonProperty("cachedMemoryUsageBytes") long cachedMemoryUsageBytes)
    {
        this.runningQueries = runningQueries;
        this.queuedQueries = queuedQueries;
        this.descendantRunningQueries = descendantRunningQueries;
        this.descendantQueuedQueries = descendantQueuedQueries;
        this.cpuUsageMillis = cpuUsageMillis;
        this.cachedMemoryUsageBytes = cachedMemoryUsageBytes;
        this.resourceGroupId = resourceGroupId;
    }

    @JsonProperty
    public int getRunningQueries()
    {
        return runningQueries;
    }

    @JsonProperty
    public int getQueuedQueries()
    {
        return queuedQueries;
    }

    @JsonProperty
    public int getDescendantRunningQueries()
    {
        return descendantRunningQueries;
    }

    @JsonProperty
    public int getDescendantQueuedQueries()
    {
        return descendantQueuedQueries;
    }

    @JsonProperty
    public long getCpuUsageMillis()
    {
        return cpuUsageMillis;
    }

    @JsonProperty
    public long getCachedMemoryUsageBytes()
    {
        return cachedMemoryUsageBytes;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DistributedResourceGroupAggrStats)) {
            return false;
        }
        DistributedResourceGroupAggrStats that = (DistributedResourceGroupAggrStats) o;
        return resourceGroupId.equals(that.resourceGroupId)
                && runningQueries == that.runningQueries
                && queuedQueries == that.queuedQueries
                && descendantRunningQueries == that.descendantRunningQueries
                && descendantQueuedQueries == that.descendantQueuedQueries
                && cpuUsageMillis == that.cpuUsageMillis
                && cachedMemoryUsageBytes == that.cachedMemoryUsageBytes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(resourceGroupId, runningQueries, queuedQueries, descendantRunningQueries, descendantQueuedQueries, cpuUsageMillis, cachedMemoryUsageBytes);
    }

    @Override
    public String toString()
    {
        return "DistributedResourceGroupAggrStats{" +
                "runningQueries=" + runningQueries +
                ", queuedQueries=" + queuedQueries +
                ", descendantRunningQueries=" + descendantRunningQueries +
                ", descendantQueuedQueries=" + descendantQueuedQueries +
                ", cpuUsageMillis=" + cpuUsageMillis +
                ", cachedMemoryUsageBytes=" + cachedMemoryUsageBytes +
                '}';
    }
}
