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

package io.prestosql.execution.scheduler;

import com.google.common.base.Supplier;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.InternalNodeManager;
import io.prestosql.metadata.Split;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.plan.PlanNodeId;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class SimpleFixedNodeSelector
        extends SimpleNodeSelector
{
    private static final Logger log = Logger.get(SimpleFixedNodeSelector.class);
    private final Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes;
    private final FixedNodeScheduleData consumedNodes;

    public SimpleFixedNodeSelector(InternalNodeManager nodeManager,
                                   NodeTaskMap nodeTaskMap,
                                   boolean includeCoordinator,
                                   Supplier<NodeMap> nodeMap,
                                   int minCandidates,
                                   int maxSplitsPerNode,
                                   int maxPendingSplitsPerTask,
                                   boolean optimizedLocalScheduling,
                                   Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes)
    {
        super(nodeManager, nodeTaskMap, includeCoordinator, nodeMap, minCandidates, maxSplitsPerNode, maxPendingSplitsPerTask, optimizedLocalScheduling, feederScheduledNodes);
        this.feederScheduledNodes = feederScheduledNodes;
        this.consumedNodes = new FixedNodeScheduleData(0, new HashSet<>());
    }

    @Override
    public SplitPlacementResult computeAssignments(Set<Split> splits, List<RemoteTask> existingTasks, Optional<SqlStageExecution> stage)
    {
        Multimap<InternalNode, Split> assignment = HashMultimap.create();
        NodeMap nodeMap = this.nodeMap.get().get();
        NodeAssignmentStats assignmentStats = new NodeAssignmentStats(nodeTaskMap, nodeMap, existingTasks);
        List<InternalNode> candidateNodes = new ArrayList<>();

        if (!stage.isPresent()) {
            log.error("Cant schedule as stage missing");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "stage is empty");
        }

        PlanNodeId planNodeId = stage.get().getFragment().getFeederCTEParentId().get();

        // if still feeder has not been scheduled then no point in scheduling this also
        if (!feederScheduledNodes.containsKey(planNodeId)) {
            return new SplitPlacementResult(immediateFuture(null), assignment);
        }

        // Find max number of splits consumer can schedule in current cycle.
        int maxSplitsToSchedule = feederScheduledNodes.get(planNodeId).getSplitCount() - consumedNodes.getSplitCount();

        // find list of nodes where still consumer has not been scheduled.
        if (feederScheduledNodes.get(planNodeId).getAssignedNodes().equals(consumedNodes.getAssignedNodes())) {
            candidateNodes = new ArrayList<>(consumedNodes.getAssignedNodes());
        }
        else {
            for (InternalNode node : feederScheduledNodes.get(planNodeId).getAssignedNodes()) {
                if (!consumedNodes.getAssignedNodes().contains(node)) {
                    candidateNodes.add(node);
                    consumedNodes.getAssignedNodes().add(node);
                }
            }
        }

        // schedule derived number of splits on derived list of nodes.
        // It is expected that splits count should be at-least equal to number of nodes so that each node gets at-least
        // one split.
        int index = 0;
        int totalNodes = candidateNodes.size();
        for (Split split : Iterables.limit(splits, maxSplitsToSchedule)) {
            InternalNode chosenNode = candidateNodes.get(index % totalNodes);
            assignment.put(chosenNode, split);
            assignmentStats.addAssignedSplit(chosenNode);
            index++;
        }

        consumedNodes.updateSplitCount(maxSplitsToSchedule);
        return new SplitPlacementResult(immediateFuture(null), assignment);
    }
}
