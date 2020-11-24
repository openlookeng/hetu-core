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
import io.prestosql.spi.QueryId;
import io.prestosql.sql.builder.SqlQueryBuilder;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.ScanTableIdAllocator;
import io.prestosql.sql.planner.SymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
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
import static io.prestosql.operator.ReuseExchangeOperator.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.operator.ReuseExchangeOperator.REUSE_STRATEGY_DEFAULT;
import static io.prestosql.operator.ReuseExchangeOperator.REUSE_STRATEGY_PRODUCER;
import static java.util.Objects.requireNonNull;

/**
 * Legacy optimizer to push sub-query with join down to the connector.
 */
public class AddReuseExchange
        implements PlanOptimizer
{
    private final Metadata metadata;

    private SqlQueryBuilder sqlQueryBuilder;

    private final Boolean second;
    private final Map<QueryId, Map<PlanNode, Integer>> planNodeListHashMapList;

    public AddReuseExchange(Metadata metadata, Boolean second, Map<QueryId, Map<PlanNode, Integer>> planNodeListHashMapList)
    {
        this.metadata = metadata;
        this.second = second;
        this.planNodeListHashMapList = planNodeListHashMapList;
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

        return SimplePlanRewriter.rewriteWith(new OptimizedPlanRewriter(session, metadata, symbolAllocator, idAllocator, types, second, planNodeListHashMapList),
                plan);
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

        private final Boolean second;

        private Map<QueryId, Map<PlanNode, Integer>> planNodeListHashMapList;
        private Map<PlanNode, Integer> planNodeListHashMap;
        private final Map<PlanNode, TableHandle> nodeToTempHandleMapping = new HashMap<>();
        private final Map<PlanNodeId, Expression> planNodeFilter = new HashMap<>();
        private final Map<PlanNode, Integer> track = new HashMap<>();
        private boolean isReuseEnabled;

        private OptimizedPlanRewriter(Session session, Metadata metadata, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator,
                TypeProvider typeProvider, Boolean second, Map<QueryId, Map<PlanNode, Integer>> planNodeListHashMapList)
        {
            this.session = session;
            this.metadata = metadata;
            this.sqlQueryBuilder = new SqlQueryBuilder(metadata, session);
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
            this.typeProvider = typeProvider;
            this.second = second;
            this.isReuseEnabled = isReuseTableScanEnabled(session);
            if (isReuseEnabled) {
                this.planNodeListHashMapList = planNodeListHashMapList;
                if (planNodeListHashMapList.get(session.getQueryId()) != null) {
                    this.planNodeListHashMap = planNodeListHashMapList.get(session.getQueryId());
                }
                else {
                    this.planNodeListHashMap = new HashMap<>();
                    planNodeListHashMapList.put(session.getQueryId(), this.planNodeListHashMap);
                }
            }
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
                        }

                        rewrittenNode = new TableScanNode(node.getId(), nodeToTempHandleMapping.get(node), node.getOutputSymbols(),
                                node.getAssignments(), node.getEnforcedConstraint(), node.getPredicate(), REUSE_STRATEGY_CONSUMER, track.get(node));
                        planNodeListHashMap.put(node, --pos);
                        return rewrittenNode;
                    }
                    else if (slot != null) {
                        rewrittenNode = new TableScanNode(node.getId(), nodeToTempHandleMapping.get(node), node.getOutputSymbols(),
                                node.getAssignments(), node.getEnforcedConstraint(), node.getPredicate(), REUSE_STRATEGY_PRODUCER, slot);
                    }

                    planNodeListHashMap.remove(node);
                    if (planNodeListHashMap.size() == 0) {
                        planNodeListHashMapList.remove(session.getQueryId());
                    }
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
