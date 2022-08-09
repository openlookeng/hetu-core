/*
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
package io.prestosql.execution.scheduler;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.DefunctConfig;
import io.airlift.configuration.LegacyConfig;
import io.airlift.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

import static java.util.Locale.ENGLISH;

@DefunctConfig({"node-scheduler.location-aware-scheduling-enabled", "node-scheduler.multiple-tasks-per-node-enabled"})
public class NodeSchedulerConfig
{
    public static class NetworkTopologyType
    {
        public static final String LEGACY = "legacy";
        public static final String FLAT = "flat";
        public static final String BENCHMARK = "benchmark";
    }

    private int minCandidates = 10;
    private boolean includeCoordinator = true;
    private int maxSplitsPerNode = 100;
    private int maxPendingSplitsPerTask = 10;
    private String networkTopology = NetworkTopologyType.LEGACY;
    private boolean optimizedLocalScheduling = true;

    private Duration allowedNoMatchingNodePeriod = new Duration(2, TimeUnit.MINUTES);
    private NodeAllocatorType nodeAllocatorType = NodeAllocatorType.BIN_PACKING;

    @NotNull
    public String getNetworkTopology()
    {
        return networkTopology;
    }

    @Config("node-scheduler.network-topology")
    public NodeSchedulerConfig setNetworkTopology(String networkTopology)
    {
        this.networkTopology = networkTopology;
        return this;
    }

    @Min(1)
    public int getMinCandidates()
    {
        return minCandidates;
    }

    @Config("node-scheduler.min-candidates")
    public NodeSchedulerConfig setMinCandidates(int candidates)
    {
        this.minCandidates = candidates;
        return this;
    }

    public boolean isIncludeCoordinator()
    {
        return includeCoordinator;
    }

    @Config("node-scheduler.include-coordinator")
    public NodeSchedulerConfig setIncludeCoordinator(boolean includeCoordinator)
    {
        this.includeCoordinator = includeCoordinator;
        return this;
    }

    @Config("node-scheduler.max-pending-splits-per-task")
    @LegacyConfig({"node-scheduler.max-pending-splits-per-node-per-task", "node-scheduler.max-pending-splits-per-node-per-stage"})
    public NodeSchedulerConfig setMaxPendingSplitsPerTask(int maxPendingSplitsPerTask)
    {
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        return this;
    }

    public int getMaxPendingSplitsPerTask()
    {
        return maxPendingSplitsPerTask;
    }

    public int getMaxSplitsPerNode()
    {
        return maxSplitsPerNode;
    }

    @Config("node-scheduler.max-splits-per-node")
    public NodeSchedulerConfig setMaxSplitsPerNode(int maxSplitsPerNode)
    {
        this.maxSplitsPerNode = maxSplitsPerNode;
        return this;
    }

    public boolean getOptimizedLocalScheduling()
    {
        return optimizedLocalScheduling;
    }

    @Config("node-scheduler.optimized-local-scheduling")
    public NodeSchedulerConfig setOptimizedLocalScheduling(boolean optimizedLocalScheduling)
    {
        this.optimizedLocalScheduling = optimizedLocalScheduling;
        return this;
    }

    public Duration getAllowedNoMatchingNodePeriod()
    {
        return allowedNoMatchingNodePeriod;
    }

    @Config("node-scheduler.allowed-no-matching-node-period")
    @ConfigDescription("How long scheduler should wait before failing a query for which hard task requirements (e.g. node exposing specific catalog) cannot be satisfied")
    public NodeSchedulerConfig setAllowedNoMatchingNodePeriod(Duration allowedNoMatchingNodePeriod)
    {
        this.allowedNoMatchingNodePeriod = allowedNoMatchingNodePeriod;
        return this;
    }

    public enum NodeAllocatorType
    {
        FIXED_COUNT,
        BIN_PACKING
    }

    @NotNull
    public NodeAllocatorType getNodeAllocatorType()
    {
        return nodeAllocatorType;
    }

    @Config("node-scheduler.allocator-type")
    public NodeSchedulerConfig setNodeAllocatorType(String nodeAllocatorType)
    {
        this.nodeAllocatorType = toNodeAllocatorType(nodeAllocatorType);
        return this;
    }

    private static NodeAllocatorType toNodeAllocatorType(String nodeAllocatorType)
    {
        switch (nodeAllocatorType.toLowerCase(ENGLISH)) {
            case "fixed_count":
                return NodeAllocatorType.FIXED_COUNT;
            case "bin_packing":
                return NodeAllocatorType.BIN_PACKING;
            default:
                throw new IllegalArgumentException("Unknown node allocator type: " + nodeAllocatorType);
        }
    }
}
