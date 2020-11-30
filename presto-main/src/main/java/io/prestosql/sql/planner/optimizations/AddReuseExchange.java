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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableHandle;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.ScanTableIdAllocator;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

import static io.prestosql.SystemSessionProperties.isReuseTableScanEnabled;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
import static java.util.Objects.requireNonNull;

/**
 * Legacy Optimizer for creating new TableScanNode for queries satisfying ReuseExchange requirements.
 */
public class AddReuseExchange
        implements PlanOptimizer
{
    private final Metadata metadata;

    private SqlQueryBuilder sqlQueryBuilder;
    private OptimizedPlanRewriter optimizedPlanRewriter;

    public AddReuseExchange(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, SymbolAllocator symbolAllocator,
            PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isReuseTableScanEnabled(session)) {
            return plan;
        }
        else {
            optimizedPlanRewriter = new OptimizedPlanRewriter(session, metadata, symbolAllocator, idAllocator, types, false);
            PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan);
            optimizedPlanRewriter.setSecondVisit();

            return SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, newNode);
//            PlanNode newNode = SimplePlanRewriter.rewriteWith(new OptimizedPlanRewriter(session, metadata, symbolAllocator, idAllocator, types, false),
//                    plan);
//            return SimplePlanRewriter.rewriteWith(new OptimizedPlanRewriter(session, metadata, symbolAllocator, idAllocator, types, true),
//                    newNode);
        }
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<Void>
    {
        private final Session session;

        private final Metadata metadata;

        private final SqlQueryBuilder sqlQueryBuilder;

        private final Map<PlanNode, PlanNode> cache = new WeakHashMap<>();

        private final SymbolAllocator symbolAllocator;

        private final PlanNodeIdAllocator idAllocator;

        private final TypeProvider typeProvider;

        private Boolean second;

//        private ConcurrentMap<QueryId, Map<PlanNode, Integer>> planNodeListHashMapList;
//        private ConcurrentMap<RewriteContext, Map<PlanNode, Integer>> planNodeListHashMapListContext;
        private Map<PlanNode, Integer> planNodeListHashMap;
        private final Map<PlanNode, TableHandle> nodeToTempHandleMapping = new HashMap<>();
        private final Map<PlanNodeId, Expression> planNodeFilter = new HashMap<>();
        private final Map<PlanNode, Integer> track = new HashMap<>();
        private boolean isReuseEnabled;
        private final Map<PlanNode, Integer> slotConsumerCount = new HashMap<>();

        private OptimizedPlanRewriter(Session session, Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator,
                TypeProvider typeProvider, Boolean second)
        {
            this.session = session;
            this.metadata = metadata;
            this.sqlQueryBuilder = new SqlQueryBuilder(metadata, session);
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
            this.typeProvider = typeProvider;
            this.second = second;
            this.isReuseEnabled = isReuseTableScanEnabled(session);
            this.planNodeListHashMap = new HashMap<>();
//            if (isReuseEnabled) {
//                this.planNodeListHashMapList = planNodeListHashMapList;
//                if (planNodeListHashMapList.get(session.getQueryId()) != null) {
//                    this.planNodeListHashMap = planNodeListHashMapList.get(session.getQueryId());
//                }
//                else {
//
//                    planNodeListHashMapList.put(session.getQueryId(), this.planNodeListHashMap);
//                }
//            }
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            if (isReuseEnabled) {
                Optional<Expression> filterExpression;
                if (node.getSource() instanceof TableScanNode) {
                    filterExpression = Optional.of(node.getPredicate());
                    if (!filterExpression.equals(Optional.empty())) {
                        TableScanNode scanNode = (TableScanNode) node.getSource();
                        scanNode.setFilterExpr(filterExpression.get());
                    }
                }
            }

            return context.defaultRewrite(node, context.get());
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
            this.second = true;
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            if (!isReuseEnabled) {
                return node;
            }

            node = (JoinNode) visitPlan(node, context);
            // verify right side
            PlanNode left = getTableScanNode(node.getLeft());
            PlanNode right = getTableScanNode(node.getRight());
            if (left != null && right != null && left.equals(right)) {
                if (planNodeListHashMap.get(left) != null) {
                    // These nodes are part of reuse exchange, adjust them.
                    planNodeListHashMap.remove(left);
                }
            }

            return node;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            if (!isReuseEnabled) {
                return node;
            }

            TableScanNode rewrittenNode = node;
            if (!second) {
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
                    Integer slot = track.get(node);
                    if (pos > 1) {
                        if (nodeToTempHandleMapping.get(node) == null) {
                            nodeToTempHandleMapping.put(node, node.getTable());
                            track.put(node, ScanTableIdAllocator.getNextId());
                            slotConsumerCount.put(node, pos - 1);
                        }

                        rewrittenNode = new TableScanNode(node.getId(), nodeToTempHandleMapping.get(node), node.getOutputSymbols(),
                                node.getAssignments(), node.getEnforcedConstraint(), node.getPredicate(), REUSE_STRATEGY_CONSUMER, track.get(node), 0);
                        planNodeListHashMap.put(node, --pos);
                        return rewrittenNode;
                    }
                    else if (slot != null) {
                        rewrittenNode = new TableScanNode(node.getId(), nodeToTempHandleMapping.get(node), node.getOutputSymbols(),
                                node.getAssignments(), node.getEnforcedConstraint(), node.getPredicate(), REUSE_STRATEGY_PRODUCER, slot, slotConsumerCount.get(node));
                    }

                    planNodeListHashMap.remove(node);
//                    if (planNodeListHashMap.size() == 0) {
//                        planNodeListHashMapList.remove(session.getQueryId());
//                    }
                    nodeToTempHandleMapping.remove(node);
                    track.remove(node);
                }
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            if (!isReuseEnabled) {
                return node;
            }

            node = (ProjectNode) visitPlan(node, context);

            // Incase of reuse exchange both consumer and producer should have assigments in same order.
            Assignments.Builder newAssignments = Assignments.builder();
            TableScanNode scanNode = null;
            if (node.getSource() instanceof FilterNode && ((FilterNode) node.getSource()).getSource() instanceof TableScanNode) {
                scanNode = (TableScanNode) ((FilterNode) node.getSource()).getSource();
            }
            else if (node.getSource() instanceof TableScanNode) {
                scanNode = (TableScanNode) node.getSource();
            }

            // Store projection in the same order.
            if (scanNode != null && scanNode.getStrategy() != REUSE_STRATEGY_DEFAULT) {
                newAssignments.putAllSorted(node.getAssignments());
            }
            else {
                newAssignments.putAll(node.getAssignments());
            }

            return new ProjectNode(node.getId(), node.getSource(), newAssignments.build());
        }
    }
}
