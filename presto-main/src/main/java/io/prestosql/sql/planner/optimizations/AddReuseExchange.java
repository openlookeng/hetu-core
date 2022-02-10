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

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.ScanTableIdAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.getSpillOperatorThresholdReuseExchange;
import static io.prestosql.SystemSessionProperties.isColocatedJoinEnabled;
import static io.prestosql.SystemSessionProperties.isReuseTableScanEnabled;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
import static java.util.Objects.requireNonNull;

/**
 * Legacy Optimizer for creating new TableScanNode for queries satisfying ReuseExchange requirements.
 */
public class AddReuseExchange
        implements PlanOptimizer
{
    private final Metadata metadata;

    public AddReuseExchange(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isReuseTableScanEnabled(session) || isColocatedJoinEnabled(session)) {
            return plan;
        }
        else {
            OptimizedPlanRewriter optimizedPlanRewriter = new OptimizedPlanRewriter(session, metadata, types, false);
            PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan);
            optimizedPlanRewriter.setSecondVisit();

            if (!optimizedPlanRewriter.isPlanNodeListHashMapEmpty()) {
                return SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, newNode);
            }
            else {
                return plan;
            }
        }
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;

        private final Metadata metadata;

        private final TypeProvider typeProvider;

        private Boolean isNodeAlreadyVisited;

        private Map<WrapperScanNode, Integer> planNodeListHashMap = new HashMap<>();
        private final Map<WrapperScanNode, TableHandle> nodeToTempHandleMapping = new HashMap<>();
        private final Map<WrapperScanNode, UUID> track = new HashMap<>();
        private final Map<WrapperScanNode, Integer> reuseTableScanMappingIdConsumerTableScanNodeCount = new HashMap<>();

        private OptimizedPlanRewriter(Session session, Metadata metadata, TypeProvider typeProvider, Boolean isNodeAlreadyVisited)
        {
            this.session = session;
            this.metadata = metadata;
            this.typeProvider = typeProvider;
            this.isNodeAlreadyVisited = isNodeAlreadyVisited;
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            Optional<RowExpression> filterExpression;
            if (node.getSource() instanceof TableScanNode) {
                filterExpression = Optional.of(node.getPredicate());
                if (!filterExpression.equals(Optional.empty())) {
                    TableScanNode scanNode = (TableScanNode) node.getSource();
                    scanNode.setFilterExpr(filterExpression.get());
                }
            }

            PlanNode planNode = context.defaultRewrite(node, context.get());
            if (node.getSource() instanceof TableScanNode
                    && ((TableScanNode) node.getSource()).getTable().getConnectorHandle().isReuseTableScanSupported()) {
                TableScanNode scanNode = (TableScanNode) node.getSource();
                RowExpressionDomainTranslator rowExpressionDomainTranslator = new RowExpressionDomainTranslator(metadata);
                RowExpressionDomainTranslator.ExtractionResult decomposedPredicate =
                        rowExpressionDomainTranslator.fromPredicate(session.toConnectorSession(), node.getPredicate());

                TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                        .transform(scanNode.getAssignments()::get)
                        .intersect(scanNode.getEnforcedConstraint());
                visitTableScanInternal(scanNode, newDomain);
            }

            return planNode;
        }

        private PlanNode getTableScanNode(PlanNode node)
        {
            if (node instanceof ProjectNode) {
                if (((ProjectNode) node).getSource() instanceof FilterNode) {
                    if (((FilterNode) ((ProjectNode) node).getSource()).getSource() instanceof TableScanNode) {
                        return ((FilterNode) ((ProjectNode) node).getSource()).getSource();
                    }
                }
                else if (((ProjectNode) node).getSource() instanceof TableScanNode) {
                    return ((ProjectNode) node).getSource();
                }
            }
            else if (node instanceof TableScanNode) {
                return node;
            }
            else if (node instanceof ExchangeNode) {
                PlanNode child = node.getSources().get(0);
                while (child != null && child instanceof ExchangeNode) {
                    child = child.getSources().get(0);
                }

                if (child instanceof TableScanNode) {
                    return child;
                }
            }

            return null;
        }

        public void setSecondVisit()
        {
            this.isNodeAlreadyVisited = true;
        }

        @Override
        public PlanNode visitJoin(JoinNode inputNode, RewriteContext<Void> context)
        {
            JoinNode node = inputNode;
            node = (JoinNode) visitPlan(node, context);
            // verify right side
            TableScanNode left = (TableScanNode) getTableScanNode(node.getLeft());
            TableScanNode right = (TableScanNode) getTableScanNode(node.getRight());
            if (left != null && right != null && WrapperScanNode.of(left).equals(WrapperScanNode.of(right))) {
                WrapperScanNode leftNode = WrapperScanNode.of(left);
                if (planNodeListHashMap.get(leftNode) != null) {
                    // These nodes are part of reuse exchange, adjust them.
                    planNodeListHashMap.remove(leftNode);
                }
            }

            return node;
        }

        private double getMaxTableSizeToEnableReuseExchange(TableStatistics stats, Map<Symbol, ColumnHandle> assignments)
        {
            return assignments.values().stream().map(x -> stats.getColumnStatistics().get(x)).filter(x -> x != null)
                    .map(x -> x.getDataSize().getValue())
                    .map(x -> x.isNaN() ? 4.0 * stats.getRowCount().getValue() : x)
                    .collect(Collectors.summingDouble(Double::doubleValue));
        }

        private boolean isPlanNodeListHashMapEmpty()
        {
            return planNodeListHashMap.isEmpty();
        }

        private void visitTableScanInternal(TableScanNode node, TupleDomain<ColumnHandle> newDomain)
        {
            if (!isNodeAlreadyVisited && node.getTable().getConnectorHandle().isReuseTableScanSupported()) {
                TableStatistics stats = metadata.getTableStatistics(session, node.getTable(), (newDomain != null) ? new Constraint(newDomain) : Constraint.alwaysTrue(), true);
                if (isMaxTableSizeGreaterThanSpillThreshold(node, stats)) {
                    planNodeListHashMap.remove(WrapperScanNode.of(node));
                }
            }
        }

        private boolean isMaxTableSizeGreaterThanSpillThreshold(TableScanNode node, TableStatistics stats)
        {
            if (getMaxTableSizeToEnableReuseExchange(stats, node.getAssignments()) / 1024 / 1024 > getSpillOperatorThresholdReuseExchange(session) * 3) {
                return true;
            }
            return false;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode tableScanNode, RewriteContext<Void> context)
        {
            if (!tableScanNode.getTable().getConnectorHandle().isReuseTableScanSupported()) {
                return tableScanNode;
            }

            TableScanNode rewrittenNode = tableScanNode;
            WrapperScanNode node = WrapperScanNode.of(tableScanNode);
            if (!isNodeAlreadyVisited) {
                Integer pos = planNodeListHashMap.get(node);
                if (pos == null) {
                    planNodeListHashMap.put(node, 1);
                }
                else {
                    planNodeListHashMap.put(node, ++pos);
                }
            }
            else {
                Integer pos = planNodeListHashMap.get(node);
                if (pos != null && pos != 0) {
                    UUID reuseTableScanMappingId = track.get(node);
                    if (pos > 1) {
                        if (nodeToTempHandleMapping.get(node) == null) {
                            nodeToTempHandleMapping.put(node, tableScanNode.getTable());
                            track.put(node, ScanTableIdAllocator.getNextId());
                            reuseTableScanMappingIdConsumerTableScanNodeCount.put(node, pos - 1);
                        }

                        rewrittenNode = new TableScanNode(tableScanNode.getId(), nodeToTempHandleMapping.get(node), tableScanNode.getOutputSymbols(),
                                tableScanNode.getAssignments(), tableScanNode.getEnforcedConstraint(), tableScanNode.getPredicate(), REUSE_STRATEGY_CONSUMER, track.get(node), 0, false);
                        planNodeListHashMap.put(node, --pos);
                        return rewrittenNode;
                    }
                    else if (reuseTableScanMappingId != null) {
                        rewrittenNode = new TableScanNode(tableScanNode.getId(), nodeToTempHandleMapping.get(node), tableScanNode.getOutputSymbols(),
                                tableScanNode.getAssignments(), tableScanNode.getEnforcedConstraint(), tableScanNode.getPredicate(), REUSE_STRATEGY_PRODUCER, reuseTableScanMappingId, reuseTableScanMappingIdConsumerTableScanNodeCount.get(node), false);
                    }

                    planNodeListHashMap.remove(node);
                    nodeToTempHandleMapping.remove(node);
                    track.remove(node);
                }
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode inputNode, RewriteContext<Void> context)
        {
            ProjectNode node = (ProjectNode) visitPlan(inputNode, context);

            // Incase of reuse exchange both consumer and producer should have assigments in same order.
            Assignments.Builder newAssignments = Assignments.builder();
            TableScanNode scanNode = null;
            if (node.getSource() instanceof FilterNode && ((FilterNode) node.getSource()).getSource() instanceof TableScanNode) {
                scanNode = (TableScanNode) ((FilterNode) node.getSource()).getSource();
            }
            else if (node.getSource() instanceof TableScanNode) {
                scanNode = (TableScanNode) node.getSource();
                visitTableScanInternal(scanNode, null);
            }

            // Store projection in the same order for reuse case.
            if (scanNode != null && scanNode.getStrategy() != REUSE_STRATEGY_DEFAULT) {
                newAssignments.putAllSorted(node.getAssignments());
                return new ProjectNode(node.getId(), node.getSource(), newAssignments.build());
            }

            return node;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode node, RewriteContext<Void> context)
        {
            planNodeListHashMap.clear();
            return node;
        }

        @Override
        public PlanNode visitExchange(ExchangeNode inputNode, RewriteContext<Void> context)
        {
            ExchangeNode node = (ExchangeNode) visitPlan(inputNode, context);
            node.getSources().stream().forEach(x -> {
                if (x instanceof TableScanNode) {
                    visitTableScanInternal((TableScanNode) x, null);
                }
            });

            return node;
        }
    }

    static class WrapperScanNode
    {
        private final TableScanNode node;

        private WrapperScanNode(TableScanNode node)
        {
            this.node = node;
        }

        public static WrapperScanNode of(TableScanNode node)
        {
            return new WrapperScanNode(node);
        }

        @Override
        public int hashCode()
        {
            TableHandle tableHandle = node.getTable();
            List<String> columnNames = node.getOutputSymbols().stream().map(x -> node.getActualColName(x.getName())).collect(Collectors.toList());
            return Objects.hash(tableHandle.getConnectorHandle(), tableHandle.getCatalogName().toString(), columnNames);
        }

        private TableScanNode getNode()
        {
            return node;
        }

        @Override
        public boolean equals(Object o)
        {
            WrapperScanNode curr = (WrapperScanNode) (o);
            if (curr == this) {
                return true;
            }

            TableScanNode tableScanNode = curr.getNode();

            if (tableScanNode == this.node) {
                return true;
            }

            if (!(tableScanNode instanceof TableScanNode)) {
                return false;
            }

            if (tableScanNode.getTable().getCatalogName().equals(this.node.getTable().getCatalogName())
                    && tableEqualsTo(tableScanNode.getTable(), this.node.getTable())
                    && this.node.isSourcesEqual(tableScanNode.getSources(), this.node.getSources())
                    && this.node.isSymbolsEqual(tableScanNode.getOutputSymbols(), this.node.getOutputSymbols())
                    && this.node.isPredicateSame(tableScanNode)) {
                return true;
            }

            return false;
        }

        private boolean tableEqualsTo(Object o1, Object o2)
        {
            if (o1 == o2) {
                return true;
            }
            if (!(o2 instanceof TableHandle)) {
                return false;
            }
            TableHandle thisTableHandle = (TableHandle) o1;
            TableHandle otherTableHandle = (TableHandle) (o2);

            if (otherTableHandle.getConnectorHandle().equals(thisTableHandle.getConnectorHandle())
                    && thisTableHandle.getCatalogName().equals(otherTableHandle.getCatalogName())) {
                return true;
            }
            return false;
        }
    }
}
