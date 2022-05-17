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
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.PlanNodeId;

import javax.annotation.Nullable;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static io.prestosql.execution.scheduler.NetworkLocation.ROOT_LOCATION;
import static io.prestosql.execution.scheduler.NodeScheduler.calculateLowWatermark;
import static io.prestosql.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectExactNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.toWhenHasSplitQueueSpaceFuture;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Objects.requireNonNull;

public class TopologyAwareNodeSelector
        implements NodeSelector
{
    private static final Logger LOG = Logger.get(TopologyAwareNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private final List<CounterStat> topologicalSplitCounters;
    private final List<String> networkLocationSegmentNames;
    private final NetworkLocationCache networkLocationCache;
    private final Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes;

    public TopologyAwareNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            List<CounterStat> topologicalSplitCounters,
            List<String> networkLocationSegmentNames,
            NetworkLocationCache networkLocationCache,
            Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.topologicalSplitCounters = requireNonNull(topologicalSplitCounters, "topologicalSplitCounters is null");
        this.networkLocationSegmentNames = requireNonNull(networkLocationSegmentNames, "networkLocationSegmentNames is null");
        this.networkLocationCache = requireNonNull(networkLocationCache, "networkLocationCache is null");
        this.feederScheduledNodes = feederScheduledNodes;
    }

    @Override
    public void lockDownNodes()
    {
        nodeMap.set(Suppliers.ofInstance(nodeMap.get().get()));
    }

    @Override
    public List<InternalNode> allNodes()
    {
        return ImmutableList.copyOf(nodeMap.get().get().getNodesByHostAndPort().values());
    }

    @Override
    public int selectableNodeCount()
    {
        NodeMap map = nodeMap.get().get();
        if (includeCoordinator) {
            return map.getNodesByHostAndPort().size();
        }
        return (int) map.getNodesByHostAndPort().values().stream()
                .filter(node -> !map.getCoordinatorNodeIds().contains(node.getNodeIdentifier()))
                .count();
    }

    @Override
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, Optional<SqlStageExecution> stage)
    {
        NodeMap nodeMapSlice = this.nodeMap.get().get();
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMapSlice, existingTasks);

        int[] topologicCounters = new int[topologicalSplitCounters.size()];
        Set<NetworkLocation> filledLocations = new HashSet<>();
        Set<InternalNode> blockedExactNodes = new HashSet<>();
        boolean splitWaitingForAnyNode = false;
        for (Split split : splits) {
            if (!split.isRemotelyAccessible()) {
                List<InternalNode> candidateNodes = selectExactNodes(nodeMapSlice, split.getAddresses(), includeCoordinator);
                if (candidateNodes.isEmpty()) {
                    LOG.debug("No nodes available to schedule %s. Available nodes %s", split, nodeMapSlice.getNodesByHost().keys());
                    throw new PrestoException(NO_NODES_AVAILABLE, "No nodes available to run query");
                }
                InternalNode chosenNode = bestNodeSplitCount(candidateNodes.iterator(), minCandidates, maxPendingSplitsPerTask, assignmentStats);
                if (chosenNode != null) {
                    assignment.put(chosenNode, split);
                    assignmentStats.addAssignedSplit(chosenNode);
                }
                // Exact node set won't matter, if a split is waiting for any node
                else if (!splitWaitingForAnyNode) {
                    blockedExactNodes.addAll(candidateNodes);
                }
                continue;
            }

            InternalNode chosenNode = null;
            int depth = networkLocationSegmentNames.size();
            int chosenDepth = 0;
            Set<NetworkLocation> locations = new HashSet<>();
            for (HostAddress host : split.getAddresses()) {
                locations.add(networkLocationCache.get(host));
            }
            if (locations.isEmpty()) {
                // Add the root location
                locations.add(ROOT_LOCATION);
                depth = 0;
            }
            // Try each address at progressively shallower network locations
            for (int i = depth; i >= 0 && chosenNode == null; i--) {
                for (NetworkLocation location : locations) {
                    // Skip locations which are only shallower than this level
                    // For example, locations which couldn't be located will be at the "root" location
                    if (location.getSegments().size() < i) {
                        continue;
                    }
                    location = location.subLocation(0, i);
                    if (filledLocations.contains(location)) {
                        continue;
                    }
                    Set<InternalNode> nodes = nodeMapSlice.getWorkersByNetworkPath().get(location);
                    chosenNode = bestNodeSplitCount(new ResettableRandomizedIterator<>(nodes), minCandidates, calculateMaxPendingSplits(i, depth), assignmentStats);
                    if (chosenNode != null) {
                        chosenDepth = i;
                        break;
                    }
                    filledLocations.add(location);
                }
            }
            if (chosenNode != null) {
                assignment.put(chosenNode, split);
                assignmentStats.addAssignedSplit(chosenNode);
                topologicCounters[chosenDepth]++;
            }
            else {
                splitWaitingForAnyNode = true;
            }
        }
        for (int i = 0; i < topologicCounters.length; i++) {
            if (topologicCounters[i] > 0) {
                topologicalSplitCounters.get(i).update(topologicCounters[i]);
            }
        }

        ListenableFuture<?> blocked;
        int maxPendingForWildcardNetworkAffinity = calculateMaxPendingSplits(0, networkLocationSegmentNames.size());
        if (splitWaitingForAnyNode) {
            blocked = toWhenHasSplitQueueSpaceFuture(existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }
        else {
            blocked = toWhenHasSplitQueueSpaceFuture(blockedExactNodes, existingTasks, calculateLowWatermark(maxPendingForWildcardNetworkAffinity));
        }

        // Check if its CTE node and its feeder
        if (stage.isPresent() && stage.get().getFragment().getFeederCTEId().isPresent()) {
            updateFeederNodeAndSplitCount(stage.get(), assignment);
        }

        return new SplitPlacementResult(blocked, assignment);
    }

    private void updateFeederNodeAndSplitCount(SqlStageExecution stage, Multimap<InternalNode, Split> assignment)
    {
        FixedNodeScheduleData data;
        if (feederScheduledNodes.containsKey(stage.getFragment().getFeederCTEParentId().get())) {
            data = feederScheduledNodes.get(stage.getFragment().getFeederCTEParentId().get());
            data.updateSplitCount(assignment.size());
            data.updateAssignedNodes(assignment.keys().stream().collect(Collectors.toSet()));
        }
        else {
            data = new FixedNodeScheduleData(assignment.size(), assignment.keys().stream().collect(Collectors.toSet()));
        }

        feederScheduledNodes.put(stage.getFragment().getFeederCTEParentId().get(), data);
    }

    /**
     * Computes how much of the queue can be filled by splits with the network topology distance to a node given by
     * splitAffinity. A split with zero affinity can only fill half the queue, whereas one that matches
     * exactly can fill the entire queue.
     */
    private int calculateMaxPendingSplits(int splitAffinity, int totalDepth)
    {
        if (totalDepth == 0) {
            return maxPendingSplitsPerTask;
        }
        // Use half the queue for any split
        // Reserve the other half for splits that have some amount of network affinity
        double queueFraction = 0.5 * (1.0 + splitAffinity / (double) totalDepth);
        return (int) Math.ceil(maxPendingSplitsPerTask * queueFraction);
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }

    @Nullable
    private InternalNode bestNodeSplitCount(Iterator<InternalNode> candidates, int minCandidatesWhenFull, int maxPendingSplitsPerTask, NodeAssignmentStats assignmentStats)
    {
        InternalNode bestQueueNotFull = null;
        int min = Integer.MAX_VALUE;
        int fullCandidatesConsidered = 0;

        while (candidates.hasNext() && (fullCandidatesConsidered < minCandidatesWhenFull || bestQueueNotFull == null)) {
            InternalNode node = candidates.next();
            if (assignmentStats.getTotalSplitCount(node) < maxSplitsPerNode) {
                return node;
            }
            fullCandidatesConsidered++;
            int totalSplitCount = assignmentStats.getQueuedSplitCountForStage(node);
            if (totalSplitCount < min && totalSplitCount < maxPendingSplitsPerTask) {
                min = totalSplitCount;
                bestQueueNotFull = node;
            }
        }
        return bestQueueNotFull;
    }
}
