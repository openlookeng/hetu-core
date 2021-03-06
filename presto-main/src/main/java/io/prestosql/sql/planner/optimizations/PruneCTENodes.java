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
package io.prestosql.sql.planner.optimizations;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static java.util.Objects.requireNonNull;

/*
 * If any specific CTE is being used only once, then remove the CTE node from the plan.
 */
public class PruneCTENodes
        implements PlanOptimizer
{
    private final boolean pruneCTEWithFilter;
    private final boolean pruneCTEWithCrossJoin;
    private final boolean pruneCTEWithDynFilter;

    public PruneCTENodes(boolean pruneCTEWithFilter, boolean pruneCTEWithCrossJoin, boolean pruneCTEWithDynFilter)
    {
        this.pruneCTEWithFilter = pruneCTEWithFilter;
        this.pruneCTEWithCrossJoin = pruneCTEWithCrossJoin;
        this.pruneCTEWithDynFilter = pruneCTEWithDynFilter;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator symbolAllocator,
                             PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(symbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");

        if (!isCTEReuseEnabled(session)) {
            return plan;
        }
        else {
            OptimizedPlanRewriter optimizedPlanRewriter = new OptimizedPlanRewriter(false, pruneCTEWithFilter, pruneCTEWithCrossJoin, pruneCTEWithDynFilter);
            PlanNode newNode = SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, plan);
            if (optimizedPlanRewriter.isSecondTraverseRequired()) {
                return SimplePlanRewriter.rewriteWith(optimizedPlanRewriter, newNode);
            }

            return newNode;
        }
    }

    private static class OptimizedPlanRewriter
            extends SimplePlanRewriter<ExpressionDetails>
    {
        private boolean isNodeAlreadyVisited;
        private final boolean pruneCTEWithFilter;
        private final boolean pruneCTEWithCrossJoin;
        private final boolean pruneCTEWithDynFilter;
        private Set<Integer> cTEWithCrossJoinList = new HashSet<>();

        private final Map<Integer, Integer> cteUsageMap;
        private final Map<Integer, Integer> cteJoinDynMap;
        private final Set<Integer> cteToPrune; //because of dynamic filter not matching

        private OptimizedPlanRewriter(Boolean isNodeAlreadyVisited, boolean pruneCTEWithFilter, boolean pruneCTEWithCrossJoin, boolean pruneCTEWithDynFilter)
        {
            this.isNodeAlreadyVisited = isNodeAlreadyVisited;
            this.cteUsageMap = new HashMap<>();
            this.pruneCTEWithFilter = pruneCTEWithFilter;
            this.pruneCTEWithCrossJoin = pruneCTEWithCrossJoin;
            this.pruneCTEWithDynFilter = pruneCTEWithDynFilter;
            cteJoinDynMap = new HashMap<>();
            cteToPrune = new HashSet<>();
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<ExpressionDetails> context)
        {
            return context.defaultRewrite(node, new ExpressionDetails(context.get() != null ? context.get().getDynFilCount() : 0, node.getPredicate()));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<ExpressionDetails> context)
        {
            if (pruneCTEWithCrossJoin && node.isCrossJoin()) {
                Integer left = getChildCTERefNum(node.getLeft());
                Integer right = getChildCTERefNum(node.getRight());
                if (left != null && right != null && left.equals(right)) {
                    cTEWithCrossJoinList.add(left);
                }
            }

            int currentCount = context.get() != null ? context.get().getDynFilCount() : 0;
            if (pruneCTEWithDynFilter) {
                Integer left = getChildCTERefNum(node.getLeft());
                if (left != null && !cteToPrune.contains(left)) {
                    if (cteJoinDynMap.containsKey(left)) {
                        if (cteJoinDynMap.get(left) != currentCount + node.getDynamicFilters().size()) {
                            cteToPrune.add(left);
                        }
                    }
                    else {
                        cteJoinDynMap.put(left, currentCount + node.getDynamicFilters().size());
                    }
                }
            }
            return context.defaultRewrite(node, new ExpressionDetails(currentCount + node.getDynamicFilters().size(),
                                                                        context.get() != null ? context.get().getPredicate() : null));
        }

        private Integer getChildCTERefNum(PlanNode node)
        {
            if (node instanceof CTEScanNode) {
                return ((CTEScanNode) node).getCommonCTERefNum();
            }
            else if (node instanceof ProjectNode) {
                return getChildCTERefNum(((ProjectNode) node).getSource());
            }
            else if (node instanceof FilterNode) {
                return getChildCTERefNum(((FilterNode) node).getSource());
            }
            else if (node.getSources().size() == 1 && node instanceof ExchangeNode) {
                return getChildCTERefNum(node.getSources().get(0));
            }
            return null;
        }

        private static boolean isSymbolBaseColumn(String name)
        {
            return !(name.startsWith("sum") || name.startsWith("avg") || name.startsWith("count")
                        || name.startsWith("max") || name.startsWith("min"));
        }

        private static boolean isExprBaseColumn(RowExpression rowExpression)
        {
            // This is temp way to continue optimization of queries where Filter node is there on top of CTE
            // but filter is on derived expression.
            // Later anyway this will be removed to support optimization for such cases also.
            if (OriginalExpressionUtils.isExpression(rowExpression)) {
                Expression expression = castToExpression(rowExpression);
                if (expression instanceof SymbolReference) {
                    SymbolReference symbol = (SymbolReference) expression;
                    return isSymbolBaseColumn(symbol.getName());
                }
            }

            return false;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode node, RewriteContext<ExpressionDetails> context)
        {
            Integer commonCTERefNum = node.getCommonCTERefNum();
            if (pruneCTEWithCrossJoin) {
                if (cTEWithCrossJoinList.contains(commonCTERefNum)) {
                    node = (CTEScanNode) visitPlan(node, context);
                    return node.getSource();
                }
            }
            if (!isNodeAlreadyVisited) {
                if (pruneCTEWithFilter && context.get() != null && context.get().getPredicate() != null) {
                    List<Symbol> deterministicSymbols;
                    if (node.getSource() instanceof ProjectNode) {
                        ProjectNode projectNode = (ProjectNode) node.getSource();
                        deterministicSymbols = projectNode.getAssignments().entrySet().stream()
                                .filter(entry -> isExprBaseColumn(entry.getValue()))
                                .map(Map.Entry::getKey)
                                .collect(Collectors.toList());
                    }
                    else {
                        deterministicSymbols = node.getOutputSymbols().stream()
                                                .filter(entry -> isSymbolBaseColumn(entry.getName()))
                                                .collect(Collectors.toList());
                    }

                    if (SymbolsExtractor.extractUnique(context.get().getPredicate()).stream().anyMatch(deterministicSymbols::contains)
                            && !(!node.getSource().getSources().isEmpty() && node.getSource().getSources().get(0) instanceof WindowNode)) {
                        // If there is any filter on top of CTE, then we dont apply optimization; so return child from here.
                        node = (CTEScanNode) visitPlan(node, context);
                        return node.getSource();
                    }
                }

                cteUsageMap.merge(commonCTERefNum, 1, Integer::sum);
            }
            else {
                if (cteUsageMap.get(commonCTERefNum) == 1 || cteToPrune.contains(commonCTERefNum)) {
                    node = (CTEScanNode) visitPlan(node, context);
                    return node.getSource();
                }
            }
            return visitPlan(node, context);
        }

        // If only there was any CTE with just one usage, we need to traverse again to remove CTE node otherwise no need.
        private boolean isSecondTraverseRequired()
        {
            isNodeAlreadyVisited = cteUsageMap.size() != 0 && cteUsageMap.values().stream().filter(x -> x <= 1).count() > 0 || cteToPrune.size() > 0;
            return isNodeAlreadyVisited;
        }
    }

    public static class ExpressionDetails
    {
        int dynFilCount;
        RowExpression predicate;

        public ExpressionDetails(int dynFilCount, RowExpression predicate)
        {
            this.dynFilCount = dynFilCount;
            this.predicate = predicate;
        }

        public int getDynFilCount()
        {
            return dynFilCount;
        }

        public RowExpression getPredicate()
        {
            return predicate;
        }
    }
}
