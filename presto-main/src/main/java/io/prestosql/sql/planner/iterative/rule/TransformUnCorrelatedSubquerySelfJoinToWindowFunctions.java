/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.QueryUtil;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.SystemSessionProperties.shouldTransformSelfJoinToWindowFunction;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

/**
 * Matches the Queries looks as below, Which involves self-join of Sub-queries
 * to access previous/next row in the same group.
 * <pre>
 * WITH cte1
 *    AS (SELECT c1,
 *               c2,
 *               Sum(c3)  c3
 *        FROM t1,
 *             t2
 *        GROUP BY
 *             c1,
 *             c2),
 *   cte2
 *    AS (SELECT cte1.c1,
 *               cte1.c2,
 *               cte1.c3 current,
 *               cte1_lag.c3 lag,
 *               cte1_lead.c3 lead
 *        FROM cte1,
 *             cte1 cte1_lag,
 *             cte1 cte1_lead
 *        WHERE
 *             cte1.c1=cte1_lag.c1
 *             AND cte1.c1=cte1_lead.c1
 *             AND cte1.c2=cte1_lag.c2
 *             AND cte1.c2=cte1_lead.c2
 *             AND cte1.c3=cte1_lag.c3+1
 *             AND cte1.c3=cte1_lead.c3-1)
 *
 *  SELECT *
 *  FROM cte2;
 *  </pre>
 */
public class TransformUnCorrelatedSubquerySelfJoinToWindowFunctions
        implements Rule<ProjectNode>
{
    private static final Logger LOG = Logger.get(TransformUnCorrelatedSubquerySelfJoinToWindowFunctions.class);

    private static final Capture<ProjectNode> PROJECT_NODE = newCapture();
    private static final Capture<FilterNode> FILTER_NODE = newCapture();
    private static final Capture<JoinNode> JOIN_NODE = newCapture();
    private static final Pattern<ProjectNode> PROJECT_NODE_PATTERN = project()
            .capturedAs(PROJECT_NODE)
            .with(source().matching(filter().capturedAs(FILTER_NODE)
                    .with(source().matching(join().capturedAs(JOIN_NODE)))));
    private final Metadata metadata;

    public TransformUnCorrelatedSubquerySelfJoinToWindowFunctions(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldTransformSelfJoinToWindowFunction(session);
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PROJECT_NODE_PATTERN;
    }

    @Override
    public Result apply(ProjectNode projectNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);
        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }
        Optional<PlanNode> leftOpt = context.getLookup().resolveGroup(joinNode.getLeft()).findFirst();
        if (!leftOpt.isPresent()) {
            return Result.empty();
        }
        Optional<PlanNode> rightOpt = context.getLookup().resolveGroup(joinNode.getRight()).findFirst();
        if (!rightOpt.isPresent()) {
            return Result.empty();
        }
        PlanNode left = leftOpt.get();
        PlanNode right = rightOpt.get();
        JoinNode joinDest;
        if (left instanceof ProjectNode && right instanceof JoinNode) {
            joinDest = (JoinNode) right;
        }
        else if (right instanceof ProjectNode && left instanceof JoinNode) {
            joinDest = (JoinNode) left;
        }
        else {
            return Result.empty();
        }
        FilterNode filterNode = captures.get(FILTER_NODE);
        Optional<TransformParams> innerSelfJoin = transformSelfJoin(joinDest, captures, context);
        if (innerSelfJoin.isPresent()) {
            Optional<TransformParams> outerSelfJoin = transformSelfJoin(joinNode, captures, context);
            if (outerSelfJoin.isPresent()) {
                //Find the previous symbol used to projected columns from lagging/leading table after join
                // and use same symbols to project lead/lag functions with CASE
                Optional<Symbol> mapSymbol = projectNode.getAssignments().getMap()
                        .keySet().stream().filter(e -> !joinDest.getOutputSymbols().contains(e)).findFirst();
                Optional<Symbol> innerWindowSymbol = mapSymbol.map(s -> context.getSymbolAllocator().newSymbol(s));
                WindowNode innerWindow = createWindowNode(context, innerSelfJoin, innerSelfJoin.get().newSource, innerWindowSymbol);
                Optional<Symbol> outerMapSymbol = projectNode.getAssignments().getMap()
                        .keySet().stream().filter(e -> !innerWindow.getOutputSymbols().contains(e))
                        .findFirst();
                Optional<Symbol> outerWindowSymbol = outerMapSymbol.map(s -> context.getSymbolAllocator().newSymbol(s));
                WindowNode windowNode = createWindowNode(context, outerSelfJoin, innerWindow, outerWindowSymbol);
                //Consider all columns used as Equi Join clause filters as Non-NULL predicates
                List<Symbol> predicateSymbols = new ArrayList<>();
                predicateSymbols.addAll(innerSelfJoin.get().partitionBy);
                predicateSymbols.addAll(outerSelfJoin.get().partitionBy);
                //consider windowFunctions predicate functions also for Non-NULL predicate
                Optional<Symbol> innerPredicate = innerWindow.getWindowFunctions().keySet().stream().findFirst();
                Optional<Symbol> outerPredicate = windowNode.getWindowFunctions().keySet().stream().findFirst();
                innerPredicate.map(predicateSymbols::add);
                outerPredicate.map(predicateSymbols::add);
                predicateSymbols = predicateSymbols.stream()
                        .filter(windowNode.getOutputSymbols()::contains)
                        .distinct()
                        .collect(Collectors.toList());
                Expression predicates = ExpressionUtils.combinePredicates(LogicalBinaryExpression.Operator.AND,
                        predicateSymbols.stream()
                                .map(s -> toSymbolReference(s))
                                .map(IsNotNullPredicate::new)
                                .collect(Collectors.toList()));

                //create the projection with all required lead/lag functions for filtering
                FilterNode newFilterNode = new FilterNode(filterNode.getId(), windowNode, castToRowExpression(predicates));
                Assignments.Builder intermediateBuilder = new Assignments.Builder();
                intermediateBuilder.putAll(projectNode.getAssignments());
                intermediateBuilder.put(innerSelfJoin.get().predicateSymbol, castToRowExpression(toSymbolReference(innerSelfJoin.get().predicateSymbol)));
                intermediateBuilder.put(outerSelfJoin.get().predicateSymbol, castToRowExpression(toSymbolReference(outerSelfJoin.get().predicateSymbol)));
                intermediateBuilder.put(innerPredicate.get(), castToRowExpression(toSymbolReference(innerPredicate.get())));
                intermediateBuilder.put(outerPredicate.get(), castToRowExpression(toSymbolReference(outerPredicate.get())));
                intermediateBuilder.put(innerWindowSymbol.get(), castToRowExpression(toSymbolReference(innerWindowSymbol.get())));
                intermediateBuilder.put(outerWindowSymbol.get(), castToRowExpression(toSymbolReference(outerWindowSymbol.get())));
                Assignments intermediateAssignments = intermediateBuilder.build().filter(windowNode.getOutputSymbols());
                ProjectNode filterProject = new ProjectNode(context.getIdAllocator().getNextId(),
                        newFilterNode, intermediateAssignments);
                //Add the Case projection from innerWindow and outerWindow
                Expression innerWindowCase = new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        toSymbolReference(innerPredicate.get()),
                        new ArithmeticBinaryExpression(innerSelfJoin.get().isLead ?
                                ArithmeticBinaryExpression.Operator.ADD : ArithmeticBinaryExpression.Operator.SUBTRACT,
                                toSymbolReference(innerSelfJoin.get().predicateSymbol),
                                new GenericLiteral("BIGINT", "1")));
                Expression innerCase = QueryUtil.caseWhen(innerWindowCase, toSymbolReference(innerWindowSymbol.get()));
                //New projections with case
                Assignments.Builder builder = new Assignments.Builder();
                builder.putAll(projectNode.getAssignments().filter(filterProject.getOutputSymbols()));
                builder.put(mapSymbol.get(), castToRowExpression(innerCase));
                Expression outerWindowCase = new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        toSymbolReference(outerPredicate.get()),
                        new ArithmeticBinaryExpression(outerSelfJoin.get().isLead ?
                                ArithmeticBinaryExpression.Operator.ADD : ArithmeticBinaryExpression.Operator.SUBTRACT,
                                toSymbolReference(outerSelfJoin.get().predicateSymbol),
                                new GenericLiteral("BIGINT", "1")));
                Expression outerCase = QueryUtil.caseWhen(outerWindowCase, toSymbolReference(outerWindowSymbol.get()));
                builder.put(outerMapSymbol.get(), castToRowExpression(outerCase));
                ProjectNode transformedProject = new ProjectNode(projectNode.getId(), filterProject, builder.build());
                LOG.info("[RULE]Applied Transform rule to remove self join using Window Operator");
                return Result.ofPlanNode(transformedProject);
            }
            return Result.empty();
        }
        return Result.empty();
    }

    private WindowNode createWindowNode(Context context, Optional<TransformParams> transformParams,
                                        PlanNode newSource,
                                        Optional<Symbol> mapSymbol)
    {
        List<Symbol> partitionBy = transformParams.get().partitionBy.stream()
                .filter(newSource.getOutputSymbols()::contains)
                .collect(Collectors.toList());
        Optional<OrderingScheme> orderingScheme = transformParams.get().orderScheme;
        WindowNode.Specification specification = new WindowNode.Specification(partitionBy, orderingScheme);
        Symbol windowSymbol = transformParams.get().windowSymbol;
        Type windowSymbolType = context.getSymbolAllocator().getTypes().get(windowSymbol);
        boolean isLead = transformParams.get().isLead;
        //create the window function for the value needs to be projected from the lagging/leading row
        WindowNode.Function valueFunction = createFunction(isLead, windowSymbol, windowSymbolType);
        Symbol predicateSymbol = transformParams.get().predicateSymbol;
        Type predicateSymbolType = context.getSymbolAllocator().getTypes().get(predicateSymbol);
        //create the window function for the column which needs to be considered in predicates to decide to project values
        WindowNode.Function predicateFunction = createFunction(isLead, predicateSymbol, predicateSymbolType);
        Symbol f1symbol = mapSymbol.orElseGet(
                () -> context.getSymbolAllocator().newSymbol(valueFunction.getFunctionCall().getDisplayName(), windowSymbolType));
        Symbol predicateWindowSymbol = context.getSymbolAllocator().newSymbol("windowPredicate", predicateSymbolType);
        WindowNode windowNode = new WindowNode(context.getIdAllocator().getNextId(), newSource,
                specification,
                ImmutableMap.of(f1symbol, valueFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);
        return new WindowNode(context.getIdAllocator().getNextId(), windowNode,
                specification,
                ImmutableMap.of(predicateWindowSymbol, predicateFunction),
                Optional.empty(),
                ImmutableSet.of(),
                0);
    }

    private WindowNode.Function createFunction(boolean isLead, Symbol wSymbol, Type wSymbolType)
    {
        String functionName = isLead ? "lead" : "lag";
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(functionName, TypeSignatureProvider.fromTypes(wSymbolType));
        return new WindowNode.Function(
                call(functionName,
                        functionHandle,
                        wSymbolType,
                        castToRowExpression(toSymbolReference(wSymbol))),
                ImmutableList.of(castToRowExpression(toSymbolReference(wSymbol))),
                new WindowNode.Frame(Types.WindowFrameType.RANGE,
                        Types.FrameBoundType.UNBOUNDED_PRECEDING,
                        Optional.empty(),
                        Types.FrameBoundType.CURRENT_ROW,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
    }

    Optional<TransformParams> transformSelfJoin(JoinNode joinNode, Captures captures, Context context)
    {
        Details leftDetails = new Details();
        Details rightDetails = new Details();
        SymbolsResolvingVisitor resolvingVisitor = new SymbolsResolvingVisitor(context.getLookup());
        joinNode.getLeft().accept(resolvingVisitor, leftDetails);
        joinNode.getRight().accept(resolvingVisitor, rightDetails);
        boolean equals = leftDetails.equals(rightDetails);
        if (!equals) {
            return Optional.empty();
        }
        FilterNode filterNode = captures.get(FILTER_NODE);
        Expression filterPredicate = castToExpression(filterNode.getPredicate());
        if (!(filterPredicate instanceof LogicalBinaryExpression)) {
            return Optional.empty();
        }
        List<Expression> expressions = ExpressionUtils.extractPredicates((LogicalBinaryExpression) filterPredicate);
        List<Symbol> equalClauseSymbols = expressions.stream().filter(n -> n instanceof ComparisonExpression)
                .map(ComparisonExpression.class::cast)
                .filter(n -> n.getOperator() == ComparisonExpression.Operator.EQUAL
                        && n.getLeft() instanceof SymbolReference
                        && n.getRight() instanceof SymbolReference)
                .map(ComparisonExpression::getChildren)
                .flatMap(l -> l.stream())
                .map(SymbolReference.class::cast)
                .distinct()
                .map(SymbolUtils::from)
                .collect(Collectors.toList());

        for (Expression e : expressions) {
            if (e instanceof ComparisonExpression) {
                ComparisonExpression comparisonExpression = (ComparisonExpression) e;
                Expression left = comparisonExpression.getLeft();
                Expression right = comparisonExpression.getRight();
                Symbol s;
                ArithmeticBinaryExpression a;
                if (left instanceof SymbolReference &&
                        right instanceof ArithmeticBinaryExpression) {
                    a = (ArithmeticBinaryExpression) right;
                    s = SymbolUtils.from(left);
                }
                else if (left instanceof ArithmeticBinaryExpression && right instanceof SymbolReference) {
                    a = (ArithmeticBinaryExpression) left;
                    s = SymbolUtils.from(right);
                }
                else {
                    continue;
                }
                if (a.getOperator() != ArithmeticBinaryExpression.Operator.ADD &&
                        a.getOperator() != ArithmeticBinaryExpression.Operator.SUBTRACT) {
                    continue;
                }
                Symbol second = a.getLeft() instanceof SymbolReference ? SymbolUtils.from(a.getLeft()) : SymbolUtils.from(a.getRight());
                PlanNode transformedSource = null;
                PlanNode toBeRemoved = null;
                Details tranformingDetails = null;
                Details toBeRemovedDetails = null;
                if (leftDetails.contains(s) && rightDetails.contains(second)) {
                    transformedSource = joinNode.getLeft();
                    toBeRemoved = joinNode.getRight();
                    tranformingDetails = leftDetails;
                    toBeRemovedDetails = rightDetails;
                }
                else if (leftDetails.contains(second) && rightDetails.contains(s)) {
                    transformedSource = joinNode.getRight();
                    toBeRemoved = joinNode.getLeft();
                    tranformingDetails = rightDetails;
                    toBeRemovedDetails = leftDetails;
                }
                else {
                    continue;
                }

                //Check whether both refers to same column(s) of tables
                if (!isEquals(resolve(toSymbolReference(s), tranformingDetails),
                        resolve(toSymbolReference(second), toBeRemovedDetails), tranformingDetails, toBeRemovedDetails)) {
                    continue;
                }

                ProjectNode projectNode = captures.get(PROJECT_NODE);
                List<Symbol> possibleWindowSymbols = toBeRemoved.getOutputSymbols().stream()
                        .filter(projectNode.getOutputSymbols()::contains)
                        .collect(Collectors.toList());
                List<Symbol> similar = findSimilar(toBeRemovedDetails, possibleWindowSymbols, transformedSource.getOutputSymbols(), tranformingDetails);
                if (similar.isEmpty()) {
                    continue;
                }

                List<Symbol> partitionBy = equalClauseSymbols.stream()
                        .filter(transformedSource.getOutputSymbols()::contains)
                        .collect(Collectors.toList());
                Optional<OrderingScheme> orderingScheme = Optional.of(
                        OrderingSchemeUtils.fromOrderBy(new OrderBy(ImmutableList.of(new SortItem(toSymbolReference(s),
                                SortItem.Ordering.ASCENDING, SortItem.NullOrdering.LAST)))));
                return Optional.of(new TransformParams(transformedSource, s, similar.get(0), partitionBy, orderingScheme,
                        a.getOperator() == ArithmeticBinaryExpression.Operator.ADD));
            }
        }
        return Optional.empty();
    }

    private List<Symbol> findSimilar(Details sourceDetails, List<Symbol> sourceSymbols, List<Symbol> destOutputSymbols, Details destDetails)
    {
        List<Symbol> similar = new ArrayList<>();
        for (Symbol s : sourceSymbols) {
            Optional<?> src = resolve(toSymbolReference(s), sourceDetails);
            for (Symbol d : destOutputSymbols) {
                Optional<?> dst = resolve(toSymbolReference(d), destDetails);
                if (isEquals(src, dst, sourceDetails, destDetails)) {
                    similar.add(d);
                }
            }
        }
        return similar;
    }

    /**
     * Checks whether resolved symbols are available and
     * internally pointing to same column(s) of the table
     *
     * @param firstOpt
     * @param secondOpt
     * @param firstDetails
     * @param secondDetails
     * @return
     */
    private boolean isEquals(Optional<?> firstOpt, Optional<?> secondOpt, Details firstDetails, Details secondDetails)
    {
        if (firstOpt.isPresent() != secondOpt.isPresent()) {
            return false;
        }
        Object first = firstOpt.get();
        Object second = secondOpt.get();
        if (!first.getClass().equals(second.getClass())) {
            return false;
        }
        if (first instanceof ColumnHandle) {
            if (((ColumnHandle) first).getColumnName().equals(((ColumnHandle) second).getColumnName())) {
                return true;
            }
        }
        else if (first instanceof AggregationNode.Aggregation) {
            AggregationNode.Aggregation a1 = (AggregationNode.Aggregation) first;
            AggregationNode.Aggregation a2 = (AggregationNode.Aggregation) second;
            if (!a1.getFunctionHandle().equals(a2.getFunctionHandle())) {
                return false;
            }
            List<RowExpression> a1args = a1.getArguments();
            List<RowExpression> a2args = a2.getArguments();
            return compareArgs(a1args, a2args, firstDetails, secondDetails);
        }
        else if (first instanceof WindowNode.Function) {
            WindowNode.Function f1 = (WindowNode.Function) first;
            WindowNode.Function f2 = (WindowNode.Function) second;
            if (!f1.getFunctionHandle().equals(f2.getFunctionHandle())) {
                return false;
            }
            List<RowExpression> args1 = f1.getArguments();
            List<RowExpression> args2 = f2.getArguments();
            return compareArgs(args1, args2, firstDetails, secondDetails);
        }
        return false;
    }

    private boolean compareArgs(List<RowExpression> args1, List<RowExpression> args2, Details firstDetails, Details secondDetails)
    {
        if (args1.size() != args2.size()) {
            return false;
        }
        for (int i = 0; i < args1.size(); i++) {
            Expression one = castToExpression(args1.get(i));
            Expression two = castToExpression(args2.get(i));
            if (one instanceof SymbolReference &&
                    two instanceof SymbolReference &&
                    !isEquals(resolve((SymbolReference) one, firstDetails),
                            resolve((SymbolReference) two, secondDetails),
                            firstDetails,
                            secondDetails)) {
                return false;
            }
        }
        return true;
    }

    private Optional<? extends Object> resolve(SymbolReference reference, Details details)
    {
        Optional<ColumnHandle> columnHandle = details.resolveColumn(reference);
        if (columnHandle.isPresent()) {
            return columnHandle;
        }
        Optional<AggregationNode.Aggregation> aggregation = details.resolveAggregation(reference);
        if (aggregation.isPresent()) {
            return aggregation;
        }
        Optional<WindowNode.Function> function = details.resolveWindowFunction(reference);
        if (function.isPresent()) {
            return function;
        }
        return Optional.empty();
    }

    private static class Details
    {
        Set<String> tables = new HashSet<>();
        Set<JoinNode.EquiJoinClause> joinClauses = new HashSet<>();
        Map<Symbol, ColumnHandle> columnMappings = new HashMap<>();
        Map<Symbol, Expression> symbolMappings = new HashMap<>();
        Map<Symbol, AggregationNode.Aggregation> aggregations = new HashMap<>();
        Map<Symbol, WindowNode.Function> windowFunctions = new HashMap<>();
        Set<ColumnHandle> referencedColumnHandles = new HashSet<>();

        public Expression resolve(SymbolReference symbol)
        {
            Expression expression = symbolMappings.get(SymbolUtils.from(symbol));
            if (expression != null && expression instanceof SymbolReference && !expression.equals(symbol)) {
                return resolve(symbol);
            }
            if (expression == null) {
                return symbol;
            }
            return expression;
        }

        public Optional<ColumnHandle> resolveColumn(SymbolReference symbolReference)
        {
            Expression resolve = resolve(symbolReference);
            if (resolve instanceof SymbolReference) {
                ColumnHandle columnHandle = columnMappings.get(SymbolUtils.from(resolve));
                if (columnHandle != null) {
                    return Optional.of(columnHandle);
                }
            }
            return Optional.empty();
        }

        public Optional<WindowNode.Function> resolveWindowFunction(SymbolReference symbolReference)
        {
            Expression resolved = resolve(symbolReference);
            if (resolved instanceof SymbolReference) {
                WindowNode.Function function = windowFunctions.get(SymbolUtils.from(resolved));
                if (function != null) {
                    return Optional.of(function);
                }
            }
            return Optional.empty();
        }

        public Optional<AggregationNode.Aggregation> resolveAggregation(SymbolReference symbolReference)
        {
            Expression resolved = resolve(symbolReference);
            if (resolved instanceof SymbolReference) {
                AggregationNode.Aggregation aggregation = aggregations.get(SymbolUtils.from(resolved));
                if (aggregation != null) {
                    return Optional.of(aggregation);
                }
            }
            return Optional.empty();
        }

        boolean contains(Symbol symbol)
        {
            return columnMappings.containsKey(symbol) ||
                    symbolMappings.containsKey(symbol) ||
                    aggregations.containsKey(symbol) ||
                    windowFunctions.containsKey(symbol);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Details)) {
                return false;
            }
            Details details = (Details) o;
            return Objects.equals(tables, details.tables) &&
                    Objects.equals(joinClauses, details.joinClauses) &&
                    Objects.equals(referencedColumnHandles, details.referencedColumnHandles);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tables, joinClauses, referencedColumnHandles);
        }
    }

    private static class SymbolsResolvingVisitor
            extends SimplePlanVisitor<Details>
    {
        private Lookup lookup;

        SymbolsResolvingVisitor(Lookup lookup)
        {
            this.lookup = lookup;
        }

        @Override
        public Void visitGroupReference(GroupReference node, Details context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Details context)
        {
            context.joinClauses.addAll(node.getCriteria());
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Details context)
        {
            context.symbolMappings.putAll(node.getAssignments().getMap()
                    .entrySet().stream()
                    .collect(Collectors.toMap(k -> k.getKey(),
                            v -> OriginalExpressionUtils.castToExpression(v.getValue()))));
            return super.visitProject(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Details context)
        {
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Details context)
        {
            context.windowFunctions.putAll(node.getWindowFunctions());
            return super.visitWindow(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Details context)
        {
            context.aggregations.putAll(node.getAggregations());
            return super.visitAggregation(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Details context)
        {
            context.tables.add(node.getTable().getFullyQualifiedName());
            context.columnMappings.putAll(node.getAssignments());
            context.referencedColumnHandles.addAll(node.getAssignments().values());
            return null;
        }
    }

    private class TransformParams
    {
        PlanNode newSource;
        Symbol windowSymbol;
        Symbol predicateSymbol;
        List<Symbol> partitionBy;
        Optional<OrderingScheme> orderScheme;
        boolean isLead;

        TransformParams(PlanNode planNode,
                        Symbol predicateSymbol,
                        Symbol windowSymbol,
                        List<Symbol> partitionBy,
                        Optional<OrderingScheme> orderingScheme,
                        boolean isLead)
        {
            this.newSource = planNode;
            this.predicateSymbol = predicateSymbol;
            this.windowSymbol = windowSymbol;
            this.partitionBy = partitionBy;
            this.orderScheme = orderingScheme;
            this.isLead = isLead;
        }
    }
}
