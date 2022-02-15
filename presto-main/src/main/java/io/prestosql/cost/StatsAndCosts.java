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

package io.prestosql.cost;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.prestosql.execution.StageInfo;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.PlanFragment;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StatsAndCosts
{
    private static final StatsAndCosts EMPTY = new StatsAndCosts(ImmutableMap.of(), ImmutableMap.of());

    private final Map<PlanNodeId, PlanNodeStatsEstimate> stats;
    private final Map<PlanNodeId, PlanCostEstimate> costs;

    public static StatsAndCosts empty()
    {
        return EMPTY;
    }

    @JsonCreator
    public StatsAndCosts(
            @JsonProperty("stats") Map<PlanNodeId, PlanNodeStatsEstimate> stats,
            @JsonProperty("costs") Map<PlanNodeId, PlanCostEstimate> costs)
    {
        this.stats = ImmutableMap.copyOf(requireNonNull(stats, "stats is null"));
        this.costs = ImmutableMap.copyOf(requireNonNull(costs, "costs is null"));
    }

    @JsonProperty
    public Map<PlanNodeId, PlanNodeStatsEstimate> getStats()
    {
        return stats;
    }

    @JsonProperty
    public Map<PlanNodeId, PlanCostEstimate> getCosts()
    {
        return costs;
    }

    public StatsAndCosts getForSubplan(PlanNode root)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> filteredStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> filteredCosts = ImmutableMap.builder();
        Set<PlanNodeId> visitedPlanNodeId = new HashSet<>();
        for (PlanNode node : planIterator) {
            PlanNodeId id = node.getId();
            if (visitedPlanNodeId.contains(id)) {
                // This can happen only incase of CTE reuse optimization.
                continue;
            }

            visitedPlanNodeId.add(id);
            if (stats.containsKey(id)) {
                filteredStats.put(id, stats.get(id));
            }
            if (costs.containsKey(id)) {
                filteredCosts.put(id, costs.get(id));
            }
        }
        return new StatsAndCosts(filteredStats.build(), filteredCosts.build());
    }

    public static StatsAndCosts create(PlanNode root, StatsProvider statsProvider, CostProvider costProvider)
    {
        Iterable<PlanNode> planIterator = Traverser.forTree(PlanNode::getSources)
                .depthFirstPreOrder(root);
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> tmpStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> tmpCosts = ImmutableMap.builder();
        Set<PlanNodeId> visitedPlanNodeId = new HashSet<>();
        for (PlanNode node : planIterator) {
            PlanNodeId id = node.getId();
            if (visitedPlanNodeId.contains(id)) {
                // This can happen only incase of CTE reuse optimization.
                continue;
            }

            visitedPlanNodeId.add(id);
            tmpStats.put(id, statsProvider.getStats(node));
            tmpCosts.put(id, costProvider.getCost(node));
        }
        return new StatsAndCosts(tmpStats.build(), tmpCosts.build());
    }

    public static StatsAndCosts create(StageInfo stageInfo)
    {
        ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> planNodeStats = ImmutableMap.builder();
        ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> planNodeCosts = ImmutableMap.builder();
        Set<PlanNodeId> visitedPlanNodeId = new HashSet<>();
        reconstructStatsAndCosts(stageInfo, planNodeStats, planNodeCosts, visitedPlanNodeId);
        return new StatsAndCosts(planNodeStats.build(), planNodeCosts.build());
    }

    private static void reconstructStatsAndCosts(
            StageInfo stage,
            ImmutableMap.Builder<PlanNodeId, PlanNodeStatsEstimate> planNodeStats,
            ImmutableMap.Builder<PlanNodeId, PlanCostEstimate> planNodeCosts,
            Set<PlanNodeId> visitedPlanNodeId)
    {
        PlanFragment planFragment = stage.getPlan();
        if (planFragment != null) {
            if (!visitedPlanNodeId.contains(planFragment.getRoot().getId())) {
                visitedPlanNodeId.add(planFragment.getRoot().getId());
                planNodeStats.putAll(planFragment.getStatsAndCosts().getStats());
                planNodeCosts.putAll(planFragment.getStatsAndCosts().getCosts());
            }
        }
        for (StageInfo subStage : stage.getSubStages()) {
            reconstructStatsAndCosts(subStage, planNodeStats, planNodeCosts, visitedPlanNodeId);
        }
    }
}
