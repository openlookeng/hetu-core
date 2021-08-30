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

package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.PartialAndFinalAggregationType;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Memo;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.SystemSessionProperties.isSortBasedAggregationEnabled;
import static io.prestosql.spi.plan.TableScanNode.getActualColName;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;

public class AddSortBasedAggregation
        implements PlanOptimizer
{
    private static final Logger LOG = Logger.get(AddSortBasedAggregation.class);
    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final CostComparator costComparator;

    public AddSortBasedAggregation(Metadata metadata, StatsCalculator statsCalculator, CostCalculator costCalculator, CostComparator costComparator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = costCalculator;
        this.costComparator = costComparator;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        if (!isSortBasedAggregationEnabled(session)) {
            return plan;
        }

        OptimizedPlanRewriter optimizedPlanRewriter = new OptimizedPlanRewriter(session, metadata, planSymbolAllocator, idAllocator, statsCalculator, costCalculator, costComparator);
        PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan);
        return newNode;
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<TableHandleInfo>
    {
        private final Session session;
        private final Metadata metadata;
        private final PlanSymbolAllocator planSymbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final CostComparator costComparator;

        private Map<Symbol, RowExpression> symbolMappings;
        private Map<Symbol, ColumnHandle> columnMappings;

        private OptimizedPlanRewriter(Session session, Metadata metadata, PlanSymbolAllocator planSymbolAllocator,
                                      PlanNodeIdAllocator idAllocator, StatsCalculator statsCalculator, CostCalculator costCalculator, CostComparator costComparator)
        {
            this.session = session;
            this.metadata = metadata;
            this.planSymbolAllocator = planSymbolAllocator;
            this.idAllocator = idAllocator;
            this.statsCalculator = statsCalculator;
            this.costCalculator = costCalculator;
            this.costComparator = costComparator;
            this.symbolMappings = new HashMap<>();
            this.columnMappings = new HashMap<>();
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<TableHandleInfo> context)
        {
            PartialAndFinalAggregationType partialAndFinalAggregationType = null;
            List<String> groupingKeyNames = new ArrayList<>();
            List<String> groupingKeyNamesTemp = groupingKeyNames;
            node.getGroupingKeys().forEach(symbol -> groupingKeyNamesTemp.add(symbol.getName()));
            groupingKeyNames = groupingKeyNames.stream().map(x -> getActualColName(x)).collect(Collectors.toList());

            if (0 == groupingKeyNames.size()) {
                return visitPlan(node, context);
            }

            //send groupingKeyNames info to below nodes so that, Ex: join nodes can validate it
            TableHandleInfo tableHandleInfo = new TableHandleInfo(groupingKeyNames);
            node = (AggregationNode) context.defaultRewrite(node, tableHandleInfo);

            if ((null != tableHandleInfo.tableHandles) && (tableHandleInfo.isJoinCriteriaOrdered())) {
                partialAndFinalAggregationType = metadata.validateAndGetSortAggregationType(session, tableHandleInfo.tableHandles, groupingKeyNames);
            }

            if ((null != partialAndFinalAggregationType) && (partialAndFinalAggregationType.isSortAggregation() || partialAndFinalAggregationType.isPartialAsSortAndFinalAsHashAggregation())) {
                Optional<Symbol> symbol = Optional.empty();
                if (node.getStep().equals(AggregationNode.Step.SINGLE) && partialAndFinalAggregationType.isSortAggregation()) {
                    if (planSymbolAllocator.getSymbols().containsKey(new Symbol("$finalizevalue"))) {
                        // if $finalizevalue already present in planSymbolAllocator don't add once again
                        symbol = Optional.of(new Symbol("$finalizevalue"));
                    }
                    else {
                        symbol = Optional.of(planSymbolAllocator.newSymbol("$finalizevalue", BooleanType.BOOLEAN));
                    }
                }

                PlanNode sortAggregateNode = new AggregationNode(
                        node.getId(),
                        node.getSource(),
                        node.getAggregations(),
                        node.getGroupingSets(),
                        node.getPreGroupedSymbols(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol(),
                        AggregationNode.AggregationType.SORT_BASED,
                        symbol);

                Memo memo = new Memo(idAllocator, node);
                Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));
                StatsProvider statsProvider = new CachingStatsProvider(statsCalculator, Optional.of(memo), lookup, session, planSymbolAllocator.getTypes());
                CostProvider costProvider = new CachingCostProvider(costCalculator, statsProvider, Optional.of(memo), session, planSymbolAllocator.getTypes());

                if (costOptimized(node, sortAggregateNode, costProvider)) {
                    return sortAggregateNode;
                }
            }
            else {
                LOG.debug(" Node not matched for sort based aggregation ");
            }
            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScanNode, RewriteContext<TableHandleInfo> context)
        {
            if (!tableScanNode.getTable().getConnectorHandle().isSortBasedAggregationSupported()) {
                return tableScanNode;
            }

            columnMappings.putAll(tableScanNode.getAssignments());

            // only at probe side we select tables for sort aggregation
            if (context.get() != null && context.get().isProbeSide()) {
                TableHandle tableHandle = tableScanNode.getTable();
                context.get().setTableHandle(tableHandle);
            }
            return tableScanNode;
        }

        /*
        1) Only probe side tables will selected.
        2) grouping keys and join criteria order should match
        3) join criteria can't be more than grouping keys
            Ex:  grouping keys are : A,B,C
            Join criteria can be
                    1) A
                    2) A,B
                    3) A,B,C
             Join criteria can't be
                   1) B
                   2) B,C
                   3) A,C
                   4) C
                   5) A,B,C,D
                     ......
         */
        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<TableHandleInfo> context)
        {
            if (context.get() == null || (context.get().getGroupingKeyNames().size() == 0)) {
                return visitPlan(node, context);
            }

            TableHandleInfo tableHandleInfo = context.get();
            List<String> groupingKeyNames = tableHandleInfo.getGroupingKeyNames();

            // only at probe side we select tables for sort aggregation
            PlanNode probe = context.rewrite(node.getLeft(), tableHandleInfo);

            // Checking Group keys with JoinCriteriaOrdered
            if (groupingKeyNames.size() < node.getCriteria().size()) {
                LOG.debug("number of Group keys " + groupingKeyNames.size() + "are less JoinCriteriaOrdered size " + node.getCriteria().size());
                tableHandleInfo.setJoinCriteriaOrdered(false);
            }
            else {
                for (int j = 0; j < node.getCriteria().size(); j++) {
                    if (!groupingKeyNames.get(j).equals(node.getCriteria().get(j).getLeft().getName())) {
                        tableHandleInfo.setJoinCriteriaOrdered(false);
                        LOG.debug("GroupingKeys are different from node Criteria");
                        break;
                    }
                }
            }

            // Checking Sorted columns with JoinCriteriaOrdered
            List<String> leftCriteriaNames = new ArrayList<>();
            node.getCriteria().forEach(c -> leftCriteriaNames.add(c.getLeft().getName()));

            //This is build Side we will not select table
            context.get().setProbeSide(false);
            PlanNode build = context.rewrite(node.getRight(), context.get());

            context.get().setProbeSide(true);
            PlanNode planNode = replaceChildren(node, ImmutableList.of(probe, build));
            return planNode;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<TableHandleInfo> context)
        {
            if (context.get() == null || (context.get().getGroupingKeyNames().size() == 0)) {
                return visitPlan(node, context);
            }

            TableHandleInfo tableHandleInfo = context.get();
            List<String> groupingKeyNames = tableHandleInfo.getGroupingKeyNames();

            // only at Source side we select tables for sort aggregation
            PlanNode probe = context.rewrite(node.getSource(), tableHandleInfo);
            if (!groupingKeyNames.get(0).equals(node.getSourceJoinSymbol().getName())) {
                tableHandleInfo.setJoinCriteriaOrdered(false);
                LOG.debug("GroupingKeys are different from node Criteria");
            }

            //This is FilteringSource Side we will not select table
            context.get().setProbeSide(false);
            PlanNode build = context.rewrite(node.getFilteringSource(), context.get());
            context.get().setProbeSide(true);
            node = (SemiJoinNode) replaceChildren(node, ImmutableList.of(probe, build));
            return node;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<TableHandleInfo> context)
        {
            symbolMappings.putAll(node.getAssignments().getMap());
            return super.visitProject(node, context);
        }

        private boolean costOptimized(PlanNode node, PlanNode rewrittenNode, CostProvider costProvider)
        {
            PlanCostEstimate left = costProvider.getCost(node);
            PlanCostEstimate right = costProvider.getCost(rewrittenNode);
            if (left.hasUnknownComponents() || right.hasUnknownComponents()) {
                return true;
            }
            if (left.hasUnknownComponents() || right.hasUnknownComponents()) {
                return true;
            }
            if (costComparator.compare(session, left, right) >= 0) {
                return true;
            }
            return false;
        }
    }

    public static class TableHandleInfo
    {
        private TableHandle tableHandles;
        private boolean isProbeSide;
        private boolean isJoinCriteriaOrdered;
        private List<String> orgGroupingKeyNames;

        public TableHandleInfo(List<String> orgGroupingKeyNames)
        {
            this.tableHandles = null;
            this.isProbeSide = true;
            this.isJoinCriteriaOrdered = true;
            this.orgGroupingKeyNames = new ArrayList<>(orgGroupingKeyNames);
        }

        public TableHandle getTableHandle()
        {
            return tableHandles;
        }

        public void setTableHandle(TableHandle tableHandle)
        {
            this.tableHandles = tableHandle;
        }

        public void setProbeSide(boolean probeSide)
        {
            this.isProbeSide = probeSide;
        }

        public boolean isProbeSide()
        {
            return isProbeSide;
        }

        public void setJoinCriteriaOrdered(boolean isJoinCriteriaOrdered)
        {
            this.isJoinCriteriaOrdered = isJoinCriteriaOrdered;
        }

        public boolean isJoinCriteriaOrdered()
        {
            return isJoinCriteriaOrdered;
        }

        public List<String> getGroupingKeyNames()
        {
            return orgGroupingKeyNames;
        }
    }
}
