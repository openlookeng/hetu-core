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
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.SystemSessionProperties.shouldTransformSelfJoinAggregatesToWindowFunction;
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

public class TransformUncorrelatedSubquerySelfJoinAggregatesToWindowFunction
        implements Rule<ProjectNode>
{
    private static final Logger LOG = Logger.get(TransformUncorrelatedSubquerySelfJoinAggregatesToWindowFunction.class);

    private static final Capture<ProjectNode> PROJECT_NODE = newCapture();
    private static final Capture<FilterNode> FILTER_NODE = newCapture();
    private static final Capture<JoinNode> JOIN_NODE = newCapture();
    private static final Pattern<ProjectNode> PROJECT_NODE_PATTERN = project()
            .capturedAs(PROJECT_NODE)
            .with(source().matching(filter().capturedAs(FILTER_NODE)
                    .with(source().matching(join().capturedAs(JOIN_NODE)))));

    private final Metadata metadata;

    public TransformUncorrelatedSubquerySelfJoinAggregatesToWindowFunction(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.metadata = metadata;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldTransformSelfJoinAggregatesToWindowFunction(session);
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
        if (!(left instanceof AggregationNode && right instanceof AggregationNode)) {
            return Result.empty();
        }
        FilterNode filterNode = captures.get(FILTER_NODE);
        Optional<TransformParams> selfJoin = isSelfJoin(joinNode, captures, context);
        if (selfJoin.isPresent()) {
            WindowNode windowNode = createWindowNode(context, selfJoin.get(), selfJoin.get().newSource);
            Assignments.Builder assigns = Assignments.builder();
            assigns.putAll(windowNode.getOutputSymbols()
                    .stream()
                    .collect(Collectors
                            .toMap(s -> s, e -> castToRowExpression(toSymbolReference(e)))));
            ProjectNode reProject = new ProjectNode(context.getIdAllocator().getNextId(), windowNode,
                    assigns.build());

            /* add original filter predicates */
            List<Expression> filterPredicates = ExpressionUtils.extractPredicates((LogicalBinaryExpression) castToExpression(filterNode.getPredicate()));
            List<Expression> equalityPredicates = filterPredicates.stream()
                    .filter(expression -> expression instanceof ComparisonExpression)
                    .map(ComparisonExpression.class::cast)
                    .filter(n -> n.getOperator() == ComparisonExpression.Operator.EQUAL
                            && n.getLeft() instanceof SymbolReference
                            && n.getRight() instanceof SymbolReference)
                    .filter(n -> selfJoin.get().partitionBy.contains(SymbolUtils.from(n.getLeft()))
                            || selfJoin.get().partitionBy.contains(SymbolUtils.from(n.getRight())))
                    .collect(Collectors.toList());

            filterPredicates = filterPredicates.stream()
                    .filter(expression -> !equalityPredicates.contains(expression))
                    .collect(Collectors.toList());
            filterPredicates.addAll(selfJoin.get().partitionBy.stream()
                    .map(s -> toSymbolReference(s))
                    .map(IsNotNullPredicate::new)
                    .collect(Collectors.toList()));
            Expression predicates = ExpressionUtils.combinePredicates(LogicalBinaryExpression.Operator.AND, filterPredicates);

            FilterNode newFilterNode = new FilterNode(filterNode.getId(), reProject, castToRowExpression(predicates));
            Assignments.Builder builder = new Assignments.Builder();
            Assignments assignments = projectNode.getAssignments();
            builder.putAll(assignments);
            Assignments filter = builder.build();
            ProjectNode transformedProject = new ProjectNode(projectNode.getId(), newFilterNode, filter);

            LOG.info("[RULE]Applied Transform rule to remove CTE self join using Window Operator");
            return Result.ofPlanNode(transformedProject);
        }
        return Result.empty();
    }

    private WindowNode createWindowNode(Context context, TransformParams transformParams,
                                        PlanNode source)
    {
        PlanNode newSource = source;
        List<Symbol> partitionBy = transformParams.partitionBy.stream()
                .filter(newSource.getOutputSymbols()::contains)
                .collect(Collectors.toList());
        Optional<OrderingScheme> orderingScheme = transformParams.orderScheme;
        WindowNode.Specification specification = new WindowNode.Specification(partitionBy, orderingScheme);

        WindowNode current = null;
        for (Map.Entry<Symbol, Symbol> windowSymbol : transformParams.windowSymbol.entrySet()) {
            Type windowSymbolType = context.getSymbolAllocator().getTypes().get(windowSymbol.getKey());

            String functionName = transformParams.isLead ? "lead" : "lag";
            FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(functionName, TypeSignatureProvider.fromTypes(windowSymbolType));
            WindowNode.Function function = new WindowNode.Function(
                    call(functionName,
                            functionHandle,
                            windowSymbolType,
                            castToRowExpression(toSymbolReference(windowSymbol.getValue()))),
                    ImmutableList.of(castToRowExpression(toSymbolReference(windowSymbol.getValue()))),
                    new WindowNode.Frame(Types.WindowFrameType.ROWS,
                            Types.FrameBoundType.UNBOUNDED_PRECEDING,
                            Optional.empty(),
                            Types.FrameBoundType.CURRENT_ROW,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty()));

            current = new WindowNode(context.getIdAllocator().getNextId(),
                    newSource,
                    specification,
                    ImmutableMap.of(windowSymbol.getKey(), function),
                    Optional.empty(),
                    ImmutableSet.of(),
                    0);
            newSource = current;
        }

        return current;
    }

    Optional<TransformParams> isSelfJoin(JoinNode joinNode, Captures captures, Context context)
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

        Symbol leftS = null;
        LongLiteral leftL = null;
        for (Expression e : expressions) {
            if (e instanceof ComparisonExpression) {
                ComparisonExpression comparisonExpression = (ComparisonExpression) e;
                Expression left = comparisonExpression.getLeft();
                Expression right = comparisonExpression.getRight();
                Symbol s;
                LongLiteral a;
                if (left instanceof SymbolReference &&
                        right instanceof LongLiteral) {
                    a = (LongLiteral) right;
                    s = SymbolUtils.from(left);
                }
                else if (left instanceof LongLiteral && right instanceof SymbolReference) {
                    a = (LongLiteral) left;
                    s = SymbolUtils.from(right);
                }
                else {
                    continue;
                }

                if (leftL == null || leftS == null) {
                    leftL = a;
                    leftS = s;
                    continue;
                }
                else {
                    /* Name is subset here! */
                    if (leftS.getName().contains(s.getName()) || s.getName().contains(leftS.getName())) {
                        if (leftL.getValue() - a.getValue() > 1 || leftL.getValue() - a.getValue() < -1) {
                            continue;
                        }
                    }
                }

                Symbol second = s;
                PlanNode transformedSource = null;
                PlanNode toBeRemoved = null;
                Details tranformingDetails = null;
                Details toBeRemovedDetails = null;
                LongLiteral filterLiteral = null;
                Symbol filterSymbol = null;
                if (leftDetails.contains(leftS) && rightDetails.contains(second)) {
                    transformedSource = joinNode.getLeft();
                    toBeRemoved = joinNode.getRight();
                    tranformingDetails = leftDetails;
                    toBeRemovedDetails = rightDetails;
                    filterLiteral = a;
                    filterSymbol = second;
                }
                else if (leftDetails.contains(second) && rightDetails.contains(leftS)) {
                    transformedSource = joinNode.getRight();
                    toBeRemoved = joinNode.getLeft();
                    tranformingDetails = rightDetails;
                    toBeRemovedDetails = leftDetails;
                    filterLiteral = leftL;
                    filterSymbol = leftS;
                }
                else {
                    continue;
                }

                ProjectNode projectNode = captures.get(PROJECT_NODE);
                List<Symbol> possibleWindowSymbols = toBeRemoved.getOutputSymbols().stream()
                        .filter(projectNode.getSource().getAllSymbols()::contains)
                        .collect(Collectors.toList());
                Map<Symbol, Symbol> similar = findSimilar(toBeRemovedDetails, possibleWindowSymbols, transformedSource.getOutputSymbols(), tranformingDetails);
                if (similar.isEmpty()) {
                    continue;
                }

                List<Symbol> partitionBy = equalClauseSymbols.stream()
                        .filter(transformedSource.getOutputSymbols()::contains)
                        .collect(Collectors.toList());
                Optional<OrderingScheme> orderingScheme = Optional.of(
                        OrderingSchemeUtils.fromOrderBy(new OrderBy(ImmutableList.of(new SortItem(toSymbolReference(leftS),
                                SortItem.Ordering.ASCENDING, SortItem.NullOrdering.LAST)))));
                return Optional.of(new TransformParams(transformedSource,
                        similar.entrySet().stream()
                                .filter(es -> !partitionBy.contains(es.getValue()))
                                .collect(Collectors.toMap(es -> es.getKey(), es -> es.getValue())),
                        partitionBy, orderingScheme,
                        leftL.getValue() < a.getValue(),
                        ImmutableMap.of(filterSymbol, filterLiteral)));
            }
        }
        return Optional.empty();
    }

    private Map<Symbol, Symbol> findSimilar(Details sourceDetails, List<Symbol> sourceSymbols, List<Symbol> destOutputSymbols, Details destDetails)
    {
        Map<Symbol, Symbol> similar = new HashMap<>();
        for (Symbol s : sourceSymbols) {
            Optional<?> src = resolve(toSymbolReference(s), sourceDetails);
            for (Symbol d : destOutputSymbols) {
                Optional<?> dst = resolve(toSymbolReference(d), destDetails);
                if ((!src.isPresent() && !dst.isPresent())) {
                    if (s.getName().contains(d.getName())
                            || d.getName().contains(s.getName())) {
                        similar.put(s, d);
                        continue;
                    }

                    if (s.getName().substring(0, s.getName().lastIndexOf("_")).equals(d.getName().substring(0, d.getName().lastIndexOf("_")))) {
                        similar.put(s, d);
                        continue;
                    }
                }

                if (isEquals(src, dst)) {
                    similar.put(s, d);
                }
            }
        }
        return similar;
    }

    private boolean isEquals(Optional<?> first, Optional<?> second)
    {
        if (!first.isPresent() && !second.isPresent()) {
            return false;
        }

        if (first.isPresent() != second.isPresent()) {
            return false;
        }
        Object firstO = first.get();
        Object secondO = second.get();
        if (!firstO.getClass().equals(secondO.getClass())) {
            return false;
        }
        if (firstO instanceof ColumnHandle) {
            if (((ColumnHandle) firstO).getColumnName().equals(((ColumnHandle) secondO).getColumnName())) {
                return true;
            }
        }
        else if (firstO instanceof AggregationNode.Aggregation) {
            AggregationNode.Aggregation a1 = (AggregationNode.Aggregation) firstO;
            AggregationNode.Aggregation a2 = (AggregationNode.Aggregation) secondO;
            if (!a1.getFunctionHandle().equals(a2.getFunctionHandle())) {
                return false;
            }
            List<RowExpression> a1args = a1.getArguments();
            List<RowExpression> a2args = a2.getArguments();
            if (a1args.size() == a2args.size()) {
                return true;
            }
        }
        else if (firstO instanceof WindowNode.Function) {
            WindowNode.Function f1 = (WindowNode.Function) firstO;
            WindowNode.Function f2 = (WindowNode.Function) secondO;
            if (!f1.getFunctionHandle().equals(f2.getFunctionHandle())) {
                return false;
            }
            List<RowExpression> f1Args = f1.getArguments();
            List<RowExpression> f2Args = f2.getArguments();
            if (f1Args.size() == f2Args.size()) {
                return true;
            }
        }
        return false;
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

    private class TransformParams
    {
        PlanNode newSource;
        Map<Symbol, Symbol> windowSymbol;
        List<Symbol> partitionBy;
        Optional<OrderingScheme> orderScheme;
        boolean isLead;
        Map<Symbol, LongLiteral> windowFilterSymbols;

        TransformParams(PlanNode planNode,
                        Map<Symbol, Symbol> windowSymbol,
                        List<Symbol> partitionBy,
                        Optional<OrderingScheme> orderingScheme,
                        boolean isLead,
                        Map<Symbol, LongLiteral> windowFilterSymbol)
        {
            this.newSource = planNode;
            this.windowSymbol = windowSymbol;
            this.partitionBy = partitionBy;
            this.orderScheme = orderingScheme;
            this.isLead = isLead;
            this.windowFilterSymbols = windowFilterSymbol;
        }
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
            else if (resolve instanceof Cast && ((Cast) resolve).getExpression() instanceof SymbolReference) {
                ColumnHandle columnHandle = columnMappings.get(SymbolUtils.from(((Cast) resolve).getExpression()));
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
                    this.equals(details) &&
                    Objects.equals(referencedColumnHandles, details.referencedColumnHandles);
        }

        private boolean equals(Details rightDetails)
        {
            Set<JoinNode.EquiJoinClause> left = joinClauses;
            Set<JoinNode.EquiJoinClause> right = rightDetails.joinClauses;
            Set<ColumnHandle> leftCol = left.stream()
                    .flatMap(equiJoinClause -> Stream.of(equiJoinClause.getLeft(), equiJoinClause.getRight()))
                    .map(symbol -> resolveColumn(toSymbolReference(symbol)).orElseGet(null))
                    .collect(Collectors.toSet());
            Set<ColumnHandle> rightCol = right.stream()
                    .flatMap(equiJoinClause -> Stream.of(equiJoinClause.getLeft(), equiJoinClause.getRight()))
                    .map(symbol -> rightDetails.resolveColumn(toSymbolReference(symbol)).orElseGet(null))
                    .collect(Collectors.toSet());

            return Objects.equals(leftCol, rightCol);
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
}
