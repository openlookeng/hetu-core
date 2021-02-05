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
package io.prestosql.execution.scheduler;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SplitCacheMap;
import io.prestosql.execution.SplitKey;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.connector.CatalogName;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.execution.scheduler.NodeScheduler.randomizedNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectDistributionNodes;
import static io.prestosql.execution.scheduler.NodeScheduler.selectNodes;
import static java.util.Objects.requireNonNull;

public class SplitCacheAwareNodeSelector
        implements NodeSelector
{
    private static final Logger log = Logger.get(SplitCacheAwareNodeSelector.class);

    private final InternalNodeManager nodeManager;
    private final NodeTaskMap nodeTaskMap;
    private final boolean includeCoordinator;
    private final AtomicReference<Supplier<NodeMap>> nodeMap;
    private final int minCandidates;
    private final int maxSplitsPerNode;
    private final int maxPendingSplitsPerTask;
    private NodeSelector defaultNodeSelector;

    public SplitCacheAwareNodeSelector(
            InternalNodeManager nodeManager,
            NodeTaskMap nodeTaskMap,
            boolean includeCoordinator,
            Supplier<NodeMap> nodeMap,
            int minCandidates,
            int maxSplitsPerNode,
            int maxPendingSplitsPerTask,
            NodeSelector defaultNodeSelector)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
        this.includeCoordinator = includeCoordinator;
        this.nodeMap = new AtomicReference<>(nodeMap);
        this.minCandidates = minCandidates;
        this.maxSplitsPerNode = maxSplitsPerNode;
        this.maxPendingSplitsPerTask = maxPendingSplitsPerTask;
        this.defaultNodeSelector = defaultNodeSelector;
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
    public InternalNode selectCurrentNode()
    {
        // TODO: this is a hack to force scheduling on the coordinator
        return nodeManager.getCurrentNode();
    }

    @Override
    public List<InternalNode> selectRandomNodes(int limit, Set<InternalNode> excludedNodes)
    {
        return selectNodes(limit, randomizedNodes(nodeMap.get().get(), includeCoordinator, excludedNodes));
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, Optional<SqlStageExecution> stage)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        Map<CatalogName, Map<String, InternalNode>> activeNodesByCatalog = new HashMap<>();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);

        Set<Split> uncacheableSplits = new HashSet<>();
        Set<Split> newCacheableSplits = new HashSet<>();
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();
        for (Split split : splits) {
            Optional<String> assignedNodeId = Optional.empty();
            SplitKey splitKey = createSplitKey(split);
            if (splitKey != null) {
                assignedNodeId = splitCacheMap.getCachedNodeId(splitKey);
            }

            if (!split.getConnectorSplit().isCacheable() || splitKey == null) {
                //uncacheable splits will be scheduled using default node selector
                uncacheableSplits.add(split);
                continue;
            }

            Map<String, InternalNode> activeNodes = activeNodesByCatalog.computeIfAbsent(split.getCatalogName(),
                    catalogName -> nodeManager.getActiveConnectorNodes(catalogName)
                    .stream()
                    .collect(Collectors.toMap(InternalNode::getNodeIdentifier, Function.identity())));
            InternalNode assignedNode = assignedNodeId.map(activeNodes::get).orElse(null);
            // check if a node has been assigned and ensure it is still active before scheduling
            if (assignedNode != null) {
                // split has been previously assigned to a node
                // assign the split to the same node as before
                assignment.put(assignedNode, split);
                assignmentStats.addAssignedSplit(assignedNode);
            }
            else {
                //splits that have not be previously cached or the assigned node is now inactive
                newCacheableSplits.add(split);
            }
        }

        log.info("%d out of %d splits already cached. %d new splits to be cached. %d splits cannot be cached.", assignment.size(), splits.size(), newCacheableSplits.size(), uncacheableSplits.size());

        Set<Split> unassignedSplits = new HashSet<>();
        unassignedSplits.addAll(newCacheableSplits);
        unassignedSplits.addAll(uncacheableSplits);

        // Compute split assignments for splits that cannot be cached, newly cacheable, and already cached but cached worker is inactive now.
        SplitPlacementResult defaultSplitPlacementResult = defaultNodeSelector.computeAssignments(unassignedSplits, existingTasks, stage);
        defaultSplitPlacementResult.getAssignments().forEach(((internalNode, split) -> {
            //Set or Update cached node id only if split is cacheable
            if (newCacheableSplits.contains(split)) {
                SplitKey splitKey = createSplitKey(split);
                if (splitKey != null) {
                    splitCacheMap.addCachedNode(splitKey, internalNode.getNodeIdentifier());
                }
            }
            assignmentStats.addAssignedSplit(internalNode);
        }));
        assignment.putAll(defaultSplitPlacementResult.getAssignments());

        return new SplitPlacementResult(defaultSplitPlacementResult.getBlocked(), assignment);
    }

    private SplitKey createSplitKey(Split split)
    {
        SplitKey splitKey = null;
        Object splitInfo = split.getConnectorSplit().getInfo();
        if (splitInfo instanceof Map) {
            Map<String, Object> splitInfoMap = (Map) splitInfo;
            String schema = (String) splitInfoMap.getOrDefault("database", splitInfoMap.get("schema"));
            if (schema != null) {
                splitKey = new SplitKey(
                        split,
                        split.getCatalogName().getCatalogName(),
                        schema,
                        splitInfoMap.get("table").toString());
            }
        }
        return splitKey;
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, BucketNodeMap bucketNodeMap)
    {
        return selectDistributionNodes(nodeMap.get().get(), nodeTaskMap, maxSplitsPerNode, maxPendingSplitsPerTask, splits, existingTasks, bucketNodeMap);
    }
}
