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

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.service.PropertyService;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.whenAnyCompleteCancelOthers;
import static io.prestosql.execution.scheduler.NodeSchedulerConfig.NetworkTopologyType;
import static io.prestosql.metadata.NodeState.ACTIVE;
import static java.util.Objects.requireNonNull;

public class NodeScheduler
{
    private static final Logger LOG = Logger.get(NodeScheduler.class);

    private final Cache<InternalNode, Boolean> inaccessibleNodeLogCache = CacheBuilder.newBuilder()
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();

    private final NetworkLocationCache networkLocationCache;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final InternalNodeManager nodeManager;
    private final int minCandidates;
    private final boolean includeCoordinator;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final boolean optimizedLocalScheduling;
    private final NodeTaskMap nodeTaskMap;
    private final boolean useNetworkTopology;

    @Inject
    public NodeScheduler(NetworkTopology networkTopology, InternalNodeManager nodeManager, NodeSchedulerConfig config, NodeTaskMap nodeTaskMap)
    {
        this(new NetworkLocationCache(networkTopology), networkTopology, nodeManager, config, nodeTaskMap);
    }

    public NodeScheduler(
            NetworkLocationCache networkLocationCache,
            NetworkTopology networkTopology,
            InternalNodeManager nodeManager,
            NodeSchedulerConfig config,
            NodeTaskMap nodeTaskMap)
    {
        this.networkLocationCache = networkLocationCache;
        this.nodeManager = nodeManager;
        this.minCandidates = config.getMinCandidates();
        this.includeCoordinator = config.isIncludeCoordinator();
        this.maxSplitsPerNode = config.getMaxSplitsPerNode();
        this.maxPendingSplitsPerTask = config.getMaxPendingSplitsPerTask();
        this.optimizedLocalScheduling = config.getOptimizedLocalScheduling();
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        checkArgument(maxSplitsPerNode >= maxPendingSplitsPerTask, "maxSplitsPerNode must be > maxPendingSplitsPerTask");
        this.useNetworkTopology = !config.getNetworkTopology().equals(NetworkTopologyType.LEGACY);

        ImmutableList.Builder<CounterStat> builder = ImmutableList.builder();
        if (useNetworkTopology) {
            networkLocationSegmentNames = ImmutableList.copyOf(networkTopology.getLocationSegmentNames());
            for (int i = 0; i < networkLocationSegmentNames.size() + 1; i++) {
                builder.add(new CounterStat());
            }
        }
        else {
            networkLocationSegmentNames = ImmutableList.of();
        }
        topologicalSplitCounters = builder.build();
    }

    @PreDestroy
    public void stop()
    {
        networkLocationCache.stop();
    }

    public Map<String, CounterStat> getTopologicalSplitCounters()
    {
        ImmutableMap.Builder<String, CounterStat> counters = ImmutableMap.builder();
        for (int i = 0; i < topologicalSplitCounters.size(); i++) {
            counters.put(i == 0 ? "all" : networkLocationSegmentNames.get(i - 1), topologicalSplitCounters.get(i));
        }
        return counters.build();
    }

    public NodeSelector createNodeSelector(CatalogName catalogName, boolean keepConsumerOnFeederNodes, Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes)
    {
        // this supplier is thread-safe. TODO: this logic should probably move to the scheduler since the choice of which node to run in should be
        // done as close to when the the split is about to be scheduled
        Supplier<NodeMap> nodeMap = Suppliers.memoizeWithExpiration(() -> {
            ImmutableSetMultimap.Builder<HostAddress, InternalNode> byHostAndPort = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<InetAddress, InternalNode> byHost = ImmutableSetMultimap.builder();
            ImmutableSetMultimap.Builder<NetworkLocation, InternalNode> workersByNetworkPath = ImmutableSetMultimap.builder();

            Set<InternalNode> nodes;
            if (catalogName != null) {
                nodes = nodeManager.getActiveConnectorNodes(catalogName);
            }
            else {
                nodes = nodeManager.getNodes(ACTIVE);
            }

            Set<String> coordinatorNodeIds = nodeManager.getCoordinators().stream()
                    .map(InternalNode::getNodeIdentifier)
                    .collect(toImmutableSet());

            for (InternalNode node : nodes) {
                if (useNetworkTopology && (includeCoordinator || !coordinatorNodeIds.contains(node.getNodeIdentifier()))) {
                    NetworkLocation location = networkLocationCache.get(node.getHostAndPort());
                    for (int i = 0; i <= location.getSegments().size(); i++) {
                        workersByNetworkPath.put(location.subLocation(0, i), node);
                    }
                }
                try {
                    byHostAndPort.put(node.getHostAndPort(), node);

                    InetAddress host = InetAddress.getByName(node.getInternalUri().getHost());
                    byHost.put(host, node);
                }
                catch (UnknownHostException e) {
                    if (inaccessibleNodeLogCache.getIfPresent(node) == null) {
                        inaccessibleNodeLogCache.put(node, true);
                        LOG.warn(e, "Unable to resolve host name for node: %s", node);
                    }
                }
            }

            ////TODO duration will be reverted to 5 seconds after implementing discovery Service AA mode(now with AP mode)
            return new NodeMap(byHostAndPort.build(), byHost.build(), workersByNetworkPath.build(), coordinatorNodeIds);
        }, 2, TimeUnit.SECONDS);

        if (keepConsumerOnFeederNodes) {
            return new SimpleFixedNodeSelector(nodeManager, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, optimizedLocalScheduling, feederScheduledNodes);
        }

        NodeSelector defaultNodeSelector = null;
        if (useNetworkTopology) {
            defaultNodeSelector = new TopologyAwareNodeSelector(
                    nodeManager,
                    nodeTaskMap,
                    includeCoordinator,
                    nodeMap,
                    minCandidates,
                    maxSplitsPerNode,
                    maxPendingSplitsPerTask,
                    topologicalSplitCounters,
                    networkLocationSegmentNames,
                    networkLocationCache,
                    feederScheduledNodes);
        }
        else {
            defaultNodeSelector = new SimpleNodeSelector(nodeManager, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, optimizedLocalScheduling, feederScheduledNodes);
        }

        if (PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED)) {
            return new SplitCacheAwareNodeSelector(
                    nodeManager,
                    nodeTaskMap,
                    includeCoordinator,
                    nodeMap,
                    minCandidates,
                    maxSplitsPerNode,
                    maxPendingSplitsPerTask,
                    defaultNodeSelector,
                    feederScheduledNodes);
        }
        else {
            return defaultNodeSelector;
        }
    }

    public static List<InternalNode> selectNodes(int limit, Iterator<InternalNode> candidates)
    {
        checkArgument(limit > 0, "limit must be at least 1");

        List<InternalNode> selected = new ArrayList<>(limit);
        while (selected.size() < limit && candidates.hasNext()) {
            selected.add(candidates.next());
        }

        return selected;
    }

    public static ResettableRandomizedIterator<InternalNode> randomizedNodes(NodeMap nodeMap, Set<InternalNode> excludedNodes)
    {
        ImmutableList<InternalNode> nodes = nodeMap.getNodesByHostAndPort().values().stream()
                .filter(InternalNode::isWorker)
                .filter(node -> !excludedNodes.contains(node))
                .collect(toImmutableList());
        return new ResettableRandomizedIterator<>(nodes);
    }

    public static List<InternalNode> selectExactNodes(NodeMap nodeMap, List<HostAddress> hosts, boolean includeCoordinator)
    {
        Set<InternalNode> chosen = new LinkedHashSet<>();

        for (HostAddress host : hosts) {
            nodeMap.getNodesByHostAndPort().get(host).stream()
                    .filter(InternalNode::isWorker)
                    .forEach(chosen::add);

            InetAddress address;
            try {
                address = host.toInetAddress();
            }
            catch (UnknownHostException e) {
                // skip hosts that don't resolve
                continue;
            }

            // consider a split with a host without a port as being accessible by all nodes in that host
            if (!host.hasPort()) {
                nodeMap.getNodesByHost().get(address).stream()
                        .filter(InternalNode::isWorker)
                        .forEach(chosen::add);
            }
        }

        // if the chosen set is empty and the host is the coordinator, force pick the coordinator
        if (chosen.isEmpty() && !includeCoordinator) {
            for (HostAddress host : hosts) {
                // In the code below, before calling `chosen::add`, it could have been checked that
                // `coordinatorIds.contains(node.getNodeIdentifier())`. But checking the condition isn't necessary
                // because every node satisfies it. Otherwise, `chosen` wouldn't have been empty.

                chosen.addAll(nodeMap.getNodesByHostAndPort().get(host));

                InetAddress address;
                try {
                    address = host.toInetAddress();
                }
                catch (UnknownHostException e) {
                    // skip hosts that don't resolve
                    continue;
                }

                // consider a split with a host without a port as being accessible by all nodes in that host
                if (!host.hasPort()) {
                    chosen.addAll(nodeMap.getNodesByHost().get(address));
                }
            }
        }

        return ImmutableList.copyOf(chosen);
    }

    public static SplitPlacementResult selectDistributionNodes(
            NodeMap nodeMap,
            NodeTaskMap nodeTaskMap,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            Set<Split> splits,
            List<RemoteTask> existingTasks,
            BucketNodeMap bucketNodeMap)
    {
        Multimap<InternalNode, Split> assignments = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        Set<InternalNode> blockedNodes = new HashSet<>();
        for (Split split : splits) {
            // node placement is forced by the bucket to node map
            InternalNode node = bucketNodeMap.getAssignedNode(split).get();

            // if node is full, don't schedule now, which will push back on the scheduling of splits
            if (assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode ||
                    assignmentStats.getQueuedSplitCountForStage(node) < maxPendingSplitsPerTask) {
                assignments.put(node, split);
                assignmentStats.addAssignedSplit(node);
            }
            else {
                blockedNodes.add(node);
            }
        }

        ListenableFuture<?> blocked = toWhenHasSplitQueueSpaceFuture(blockedNodes, existingTasks, calculateLowWatermark(maxPendingSplitsPerTask));
        return new SplitPlacementResult(blocked, ImmutableMultimap.copyOf(assignments));
    }

    public static int calculateLowWatermark(int maxPendingSplitsPerTask)
    {
        return (int) Math.ceil(maxPendingSplitsPerTask / 2.0);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(Set<InternalNode> blockedNodes, List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (blockedNodes.isEmpty()) {
            return immediateFuture(null);
        }
        Map<String, RemoteTask> nodeToTaskMap = new HashMap<>();
        for (RemoteTask task : existingTasks) {
            nodeToTaskMap.put(task.getNodeId(), task);
        }
        List<ListenableFuture<?>> blockedFutures = blockedNodes.stream()
                .map(InternalNode::getNodeIdentifier)
                .map(nodeToTaskMap::get)
                .filter(Objects::nonNull)
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        if (blockedFutures.isEmpty()) {
            return immediateFuture(null);
        }
        return whenAnyCompleteCancelOthers(blockedFutures);
    }

    public static ListenableFuture<?> toWhenHasSplitQueueSpaceFuture(List<RemoteTask> existingTasks, int spaceThreshold)
    {
        if (existingTasks.isEmpty()) {
            return immediateFuture(null);
        }
        List<ListenableFuture<?>> stateChangeFutures = existingTasks.stream()
                .map(remoteTask -> remoteTask.whenSplitQueueHasSpace(spaceThreshold))
                .collect(toImmutableList());
        return whenAnyCompleteCancelOthers(stateChangeFutures);
    }

    public void refreshNodeStates()
    {
        nodeManager.refreshWorkerStates();
    }
}
