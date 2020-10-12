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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsProvider;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxSize;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.sql.planner.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.JoinNode.Type.RIGHT;
import static io.prestosql.sql.planner.plan.Patterns.join;

public class CreateDynamicFilters
        implements Rule<JoinNode>
{
    private final Metadata metadata;

    private static final Pattern<JoinNode> PATTERN = join().matching(joinNode -> (joinNode.getType() == INNER || joinNode.getType() == RIGHT) && !joinNode.isDynamicFiltersCreated());
    private static final double DEFAULT_SELECTIVITY_THRESHOLD = 0.5D;

    public CreateDynamicFilters(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnableDynamicFiltering(session);
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        StatsProvider statsProvider = context.getStatsProvider();
        PlanNode right = lookup.resolve(node.getRight());
        Optional<PlanNode> buildSideTableScanNode = Optional.empty();

        // Only handle the case that build side of JoinNode is
        // TableScanNode or FilterNode above TableScanNode
        // as the estimates will be more accurate
        if (right instanceof TableScanNode) {
            // If no predicate at all, the selectivity will be high
            if (((TableScanNode) right).getEnforcedConstraint().isAll()) {
                return Result.ofPlanNode(joinNodeWithoutDynamicFilters(node));
            }
            buildSideTableScanNode = Optional.of(right);
        }

        if (right instanceof FilterNode) {
            PlanNode sourceNode = lookup.resolve(((FilterNode) right).getSource());
            if (sourceNode instanceof TableScanNode) {
                buildSideTableScanNode = Optional.of(sourceNode);
            }
        }

        if (buildSideTableScanNode.isPresent()) {
            Estimate totalRowCount = metadata.getTableStatistics(context.getSession(), ((TableScanNode) buildSideTableScanNode.get()).getTable(), Constraint.alwaysTrue()).getRowCount();
            PlanNodeStatsEstimate filteredStats = statsProvider.getStats(right);

            if (!filteredStats.isOutputRowCountUnknown() && !totalRowCount.isUnknown()) {
                // If filtered row count is too big, no need to create Dynamic Filter
                if (filteredStats.getOutputRowCount() > getDynamicFilteringMaxSize(context.getSession())) {
                    return Result.ofPlanNode(joinNodeWithoutDynamicFilters(node));
                }

                // If selectivity too low, no need to create Dynamic Filter
                double selectivity = filteredStats.getOutputRowCount() / totalRowCount.getValue();
                if (selectivity > DEFAULT_SELECTIVITY_THRESHOLD) {
                    return Result.ofPlanNode(joinNodeWithoutDynamicFilters(node));
                }
            }
        }

        Map<String, Symbol> dynamicFilters = createDynamicFilters(node.getCriteria(), context.getIdAllocator());

        return Result.ofPlanNode(joinNodeWithDynamicFilters(node, dynamicFilters));
    }

    private Map<String, Symbol> createDynamicFilters(List<JoinNode.EquiJoinClause> equiJoinClauses, PlanNodeIdAllocator idAllocator)
    {
        ImmutableMap.Builder<String, Symbol> dynamicFiltersBuilder = ImmutableMap.builder();
        for (JoinNode.EquiJoinClause clause : equiJoinClauses) {
            Symbol buildSymbol = clause.getRight();
            String id = idAllocator.getNextId().toString();
            dynamicFiltersBuilder.put(id, buildSymbol);
        }
        return dynamicFiltersBuilder.build();
    }

    private static JoinNode joinNodeWithDynamicFilters(JoinNode node, Map<String, Symbol> dynamicFilters)
    {
        return new JoinNode(
                node.getId(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getOutputSymbols(),
                node.getFilter(),
                node.getLeftHashSymbol(),
                node.getRightHashSymbol(),
                node.getDistributionType(),
                node.isSpillable(),
                dynamicFilters,
                true);
    }

    private static JoinNode joinNodeWithoutDynamicFilters(JoinNode node)
    {
        return new JoinNode(
                node.getId(),
                node.getType(),
                node.getLeft(),
                node.getRight(),
                node.getCriteria(),
                node.getOutputSymbols(),
                node.getFilter(),
                node.getLeftHashSymbol(),
                node.getRightHashSymbol(),
                node.getDistributionType(),
                node.isSpillable(),
                ImmutableMap.of(),
                true);
    }
}
