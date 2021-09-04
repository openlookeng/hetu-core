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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.LiteralInterpreter;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.RowExpressionInterpreter;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.SymbolReference;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.cost.ComparisonStatsCalculator.estimateExpressionToExpressionComparison;
import static io.prestosql.cost.ComparisonStatsCalculator.estimateExpressionToLiteralComparison;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.addStatsAndSumDistinctValues;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.capStats;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.subtractSubsetStats;
import static io.prestosql.cost.StatsUtil.toStatsRepresentation;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.spi.relation.SpecialForm.Form.IS_NULL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.DynamicFilters.isDynamicFilter;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static io.prestosql.sql.planner.SymbolUtils.from;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.IS_DISTINCT_FROM;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static java.lang.Double.NaN;
import static java.lang.Double.isInfinite;
import static java.lang.Double.isNaN;
import static java.lang.Double.min;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class FilterStatsCalculator
{
    static final double UNKNOWN_FILTER_COEFFICIENT = 0.9;

    private final Metadata metadata;
    private final ScalarStatsCalculator scalarStatsCalculator;
    private final StatsNormalizer normalizer;
    private final LiteralEncoder literalEncoder;
    private final FunctionResolution functionResolution;

    @Inject
    public FilterStatsCalculator(Metadata metadata, ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
        this.normalizer = requireNonNull(normalizer, "normalizer is null");
        this.literalEncoder = new LiteralEncoder(metadata);
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            Expression predicate,
            Session session,
            TypeProvider types)
    {
        Expression simplifiedExpression = simplifyExpression(session, predicate, types);
        return new FilterExpressionStatsCalculatingVisitor(statsEstimate, session, types)
                .process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            ConnectorSession session,
            TypeProvider types,
            Map<Integer, Symbol> layout)
    {
        RowExpression simplifiedExpression = simplifyExpression(session, predicate);
        return new FilterRowExpressionStatsCalculatingVisitor(statsEstimate, session, types, layout).process(simplifiedExpression);
    }

    public PlanNodeStatsEstimate filterStats(
            PlanNodeStatsEstimate statsEstimate,
            RowExpression predicate,
            Session session,
            TypeProvider types,
            Map<Integer, Symbol> layout)
    {
        return filterStats(statsEstimate, predicate, session.toConnectorSession(), types, layout);
    }

    private Expression simplifyExpression(Session session, Expression predicate, TypeProvider types)
    {
        // TODO reuse io.prestosql.sql.planner.iterative.rule.SimplifyExpressions.rewrite

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, predicate, types);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(predicate, metadata, session, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return literalEncoder.toExpression(value, BOOLEAN);
    }

    private RowExpression simplifyExpression(ConnectorSession session, RowExpression predicate)
    {
        RowExpressionInterpreter interpreter = new RowExpressionInterpreter(predicate, metadata, session, OPTIMIZED);
        Object value = interpreter.optimize();

        if (value == null) {
            // Expression evaluates to SQL null, which in Filter is equivalent to false. This assumes the expression is a top-level expression (eg. not in NOT).
            value = false;
        }
        return LiteralEncoder.toRowExpression(value, BOOLEAN);
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata,
                session,
                types,
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }

    private class FilterExpressionStatsCalculatingVisitor
            extends AstVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        FilterExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        public PlanNodeStatsEstimate process(Node node, @Nullable Void context)
        {
            return normalizer.normalize(super.process(node, context), types);
        }

        @Override
        protected PlanNodeStatsEstimate visitExpression(Expression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitNotExpression(NotExpression node, Void context)
        {
            if (node.getValue() instanceof IsNullPredicate) {
                return process(new IsNotNullPredicate(((IsNullPredicate) node.getValue()).getValue()));
            }
            return subtractSubsetStats(input, process(node.getValue()));
        }

        @Override
        protected PlanNodeStatsEstimate visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            switch (node.getOperator()) {
                case AND:
                    return estimateLogicalAnd(node.getLeft(), node.getRight());
                case OR:
                    return estimateLogicalOr(node.getLeft(), node.getRight());
                default:
                    throw new IllegalArgumentException("Unexpected binary operator: " + node.getOperator());
            }
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(Expression left, Expression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(Expression left, Expression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate andEstimate = new FilterExpressionStatsCalculatingVisitor(leftEstimate, session, types).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return capStats(
                    subtractSubsetStats(
                            addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        @Override
        protected PlanNodeStatsEstimate visitBooleanLiteral(BooleanLiteral node, Void context)
        {
            if (node.getValue()) {
                return input;
            }

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
            result.setOutputRowCount(0.0);
            input.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = from(node.getValue());
                SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * (1 - symbolStats.getNullsFraction()));
                result.addSymbolStatistics(symbol, symbolStats.mapNullsFraction(x -> 0.0));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            if (node.getValue() instanceof SymbolReference) {
                Symbol symbol = from(node.getValue());
                SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * symbolStats.getNullsFraction());
                result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        protected PlanNodeStatsEstimate visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            if (!(node.getValue() instanceof SymbolReference)) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMin()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getExpressionStats(node.getMax()).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }

            SymbolStatsEstimate valueStats = input.getSymbolStatistics(from(node.getValue()));
            Expression lowerBound = new ComparisonExpression(GREATER_THAN_OR_EQUAL, node.getValue(), node.getMin());
            Expression upperBound = new ComparisonExpression(LESS_THAN_OR_EQUAL, node.getValue(), node.getMax());

            Expression transformed;
            if (isInfinite(valueStats.getLowValue())) {
                // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                // We rely on 'and()' being processed left to right
                transformed = and(lowerBound, upperBound);
            }
            else {
                transformed = and(upperBound, lowerBound);
            }
            return process(transformed);
        }

        @Override
        protected PlanNodeStatsEstimate visitInPredicate(InPredicate node, Void context)
        {
            if (!(node.getValueList() instanceof InListExpression)) {
                return PlanNodeStatsEstimate.unknown();
            }

            InListExpression inList = (InListExpression) node.getValueList();
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = inList.getValues().stream()
                    .map(inValue -> process(new ComparisonExpression(EQUAL, node.getValue(), inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            SymbolStatsEstimate valueStats = getExpressionStats(node.getValue());
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (node.getValue() instanceof SymbolReference) {
                Symbol valueSymbol = from(node.getValue());
                SymbolStatsEstimate newSymbolStats = inEstimate.getSymbolStatistics(valueSymbol)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addSymbolStatistics(valueSymbol, newSymbolStats);
            }
            return result.build();
        }

        @Override
        protected PlanNodeStatsEstimate visitComparisonExpression(ComparisonExpression node, Void context)
        {
            ComparisonExpression.Operator operator = node.getOperator();
            Expression left = node.getLeft();
            Expression right = node.getRight();

            checkArgument(!(left instanceof Literal && right instanceof Literal), "Literal-to-literal not supported here, should be eliminated earlier");

            if (!(left instanceof SymbolReference) && right instanceof SymbolReference) {
                // normalize so that symbol is on the left
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof Literal && !(right instanceof Literal)) {
                // normalize so that literal is on the right
                return process(new ComparisonExpression(operator.flip(), right, left));
            }

            if (left instanceof SymbolReference && left.equals(right)) {
                return process(new IsNotNullPredicate(left));
            }

            SymbolStatsEstimate leftStats = getExpressionStats(left);
            Optional<Symbol> leftSymbol = left instanceof SymbolReference ? Optional.of(from(left)) : Optional.empty();
            if (right instanceof Literal) {
                OptionalDouble literal = doubleValueFromLiteral(getType(left), (Literal) right);
                return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, literal, operator);
            }

            SymbolStatsEstimate rightStats = getExpressionStats(right);
            if (rightStats.isSingleValue()) {
                OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, value, operator);
            }

            Optional<Symbol> rightSymbol = right instanceof SymbolReference ? Optional.of(from(right)) : Optional.empty();
            return estimateExpressionToExpressionComparison(input, leftStats, leftSymbol, rightStats, rightSymbol, operator);
        }

        @Override
        protected PlanNodeStatsEstimate visitFunctionCall(FunctionCall node, Void context)
        {
            if (node.getName().getPrefix().equals("$internal$dynamic_filter_function")) {
                return process(BooleanLiteral.TRUE_LITERAL, context);
            }
            return PlanNodeStatsEstimate.unknown();
        }

        private Type getType(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                Symbol symbol = from(expression);
                return requireNonNull(types.get(symbol), () -> format("No type for symbol %s", symbol));
            }

            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata,
                    session,
                    types,
                    ImmutableList.of(),
                    // At this stage, there should be no subqueries in the plan.
                    node -> new VerifyException("Unexpected subquery"),
                    WarningCollector.NOOP,
                    false);
            return expressionAnalyzer.analyze(expression, Scope.create());
        }

        private SymbolStatsEstimate getExpressionStats(Expression expression)
        {
            if (expression instanceof SymbolReference) {
                Symbol symbol = from(expression);
                return requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol));
            }
            return scalarStatsCalculator.calculate(expression, input, session, types);
        }

        private OptionalDouble doubleValueFromLiteral(Type type, Literal literal)
        {
            Object literalValue = LiteralInterpreter.evaluate(metadata, session.toConnectorSession(), literal);
            return toStatsRepresentation(metadata, session.toConnectorSession(), type, literalValue);
        }
    }

    private class FilterRowExpressionStatsCalculatingVisitor
            implements RowExpressionVisitor<PlanNodeStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final TypeProvider types;
        private final Map<Integer, Symbol> layout;

        FilterRowExpressionStatsCalculatingVisitor(PlanNodeStatsEstimate input, ConnectorSession session, TypeProvider types, Map<Integer, Symbol> layout)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
            this.types = requireNonNull(types, "types is null");
            this.layout = layout;
        }

        @Override
        public PlanNodeStatsEstimate visitSpecialForm(SpecialForm node, Void context)
        {
            switch (node.getForm()) {
                case AND: {
                    return estimateLogicalAnd(node.getArguments().get(0), node.getArguments().get(1));
                }
                case OR: {
                    return estimateLogicalOr(node.getArguments().get(0), node.getArguments().get(1));
                }
                case IN: {
                    return estimateIn(node.getArguments().get(0), node.getArguments().subList(1, node.getArguments().size()));
                }
                case IS_NULL: {
                    return estimateIsNull(node.getArguments().get(0));
                }
                case BETWEEN: {
                    return handleBetween(node.getArguments().get(0), node.getArguments().get(1), node.getArguments().get(2));
                }
                default:
                    return PlanNodeStatsEstimate.unknown();
            }
        }

        @Override
        public PlanNodeStatsEstimate visitConstant(ConstantExpression node, Void context)
        {
            if (node.getType().equals(BOOLEAN)) {
                if (node.getValue() != null && (boolean) node.getValue()) {
                    return input;
                }
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
                result.setOutputRowCount(0.0);
                input.getSymbolsWithKnownStatistics().forEach(symbol -> result.addSymbolStatistics(symbol, SymbolStatsEstimate.zero()));
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitLambda(LambdaDefinitionExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitVariableReference(VariableReferenceExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        @Override
        public PlanNodeStatsEstimate visitCall(CallExpression node, Void context)
        {
            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(node.getFunctionHandle());
            // comparison case
            if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                OperatorType operatorType = functionMetadata.getOperatorType().get();
                RowExpression left = node.getArguments().get(0);
                RowExpression right = node.getArguments().get(1);

                checkArgument(!(left instanceof ConstantExpression && right instanceof ConstantExpression), "Literal-to-literal not supported here, should be eliminated earlier");

                if (!(left instanceof VariableReferenceExpression) && right instanceof VariableReferenceExpression) {
                    // normalize so that variable is on the left
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(flippedOperator.name(),
                            metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(flippedOperator, fromTypes(right.getType(), left.getType())),
                            BOOLEAN, right, left));
                }

                if (left instanceof ConstantExpression) {
                    // normalize so that literal is on the right
                    OperatorType flippedOperator = flip(operatorType);
                    return process(call(flippedOperator.name(),
                            metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(flippedOperator, fromTypes(right.getType(), left.getType())),
                            BOOLEAN, right, left));
                }

                if (left instanceof VariableReferenceExpression && left.equals(right)) {
                    return process(not(isNull(left)));
                }

                SymbolStatsEstimate leftStats = getRowExpressionStats(left);
                Optional<Symbol> leftSymbol;
                if (left instanceof VariableReferenceExpression) {
                    leftSymbol = Optional.of(new Symbol(((VariableReferenceExpression) left).getName()));
                }
                else if (left instanceof InputReferenceExpression && ((InputReferenceExpression) left).getField() <= layout.size()) {
                    leftSymbol = Optional.of(layout.get(((InputReferenceExpression) left).getField()));
                }
                else {
                    leftSymbol = Optional.empty();
                }
                if (right instanceof ConstantExpression && !right.getType().equals(VarcharType.VARCHAR)) {
                    Object rightValue = ((ConstantExpression) right).getValue();
                    if (rightValue == null) {
                        return visitConstant(constantNull(BOOLEAN), null);
                    }
                    OptionalDouble literal = toStatsRepresentation(metadata, session, right.getType(), rightValue);
                    return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, literal, getComparisonOperator(operatorType));
                }

                SymbolStatsEstimate rightStats = getRowExpressionStats(right);
                if (rightStats.isSingleValue()) {
                    OptionalDouble value = isNaN(rightStats.getLowValue()) ? OptionalDouble.empty() : OptionalDouble.of(rightStats.getLowValue());
                    return estimateExpressionToLiteralComparison(input, leftStats, leftSymbol, value, getComparisonOperator(operatorType));
                }

                Optional<Symbol> rightSymbol;
                if (right instanceof VariableReferenceExpression) {
                    rightSymbol = Optional.of(new Symbol(((VariableReferenceExpression) right).getName()));
                }
                else if (right instanceof InputReferenceExpression && ((InputReferenceExpression) right).getField() <= layout.size()) {
                    rightSymbol = Optional.of(layout.get(((InputReferenceExpression) right).getField()));
                }
                else {
                    rightSymbol = Optional.empty();
                }
                return estimateExpressionToExpressionComparison(input, leftStats, leftSymbol, rightStats, rightSymbol, getComparisonOperator(operatorType));
            }

            //BETWEEN case
            if (functionResolution.isBetweenFunction(node.getFunctionHandle())) {
                return handleBetween(node.getArguments().get(0), node.getArguments().get(1), node.getArguments().get(2));
            }

            // NOT case
            if (functionResolution.isNotFunction(node.getFunctionHandle())) {
                RowExpression arguemnt = node.getArguments().get(0);
                if (arguemnt instanceof SpecialForm && ((SpecialForm) arguemnt).getForm().equals(IS_NULL)) {
                    // IS NOT NULL case
                    RowExpression innerArugment = ((SpecialForm) arguemnt).getArguments().get(0);
                    if (innerArugment instanceof VariableReferenceExpression) {
                        VariableReferenceExpression variable = (VariableReferenceExpression) innerArugment;
                        SymbolStatsEstimate variableStats = input.getSymbolStatistics(new Symbol(variable.getName()));
                        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                        result.setOutputRowCount(input.getOutputRowCount() * (1 - variableStats.getNullsFraction()));
                        result.addSymbolStatistics(new Symbol(variable.getName()), variableStats.mapNullsFraction(x -> 0.0));
                        return result.build();
                    }
                    if (innerArugment instanceof InputReferenceExpression && ((InputReferenceExpression) innerArugment).getField() <= layout.size()) {
                        Symbol symbol = layout.get(((InputReferenceExpression) innerArugment).getField());
                        SymbolStatsEstimate symbolStats = input.getSymbolStatistics(symbol);
                        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                        result.setOutputRowCount(input.getOutputRowCount() * (1 - symbolStats.getNullsFraction()));
                        result.addSymbolStatistics(symbol, symbolStats.mapNullsFraction(x -> 0.0));
                        return result.build();
                    }
                    return PlanNodeStatsEstimate.unknown();
                }
                return subtractSubsetStats(input, process(arguemnt));
            }

            if (isDynamicFilter(node)) {
                return process(TRUE_CONSTANT);
            }

            return PlanNodeStatsEstimate.unknown();
        }

        private PlanNodeStatsEstimate handleBetween(RowExpression value, RowExpression min, RowExpression max)
        {
            if (!(value instanceof VariableReferenceExpression ||
                    (value instanceof InputReferenceExpression && ((InputReferenceExpression) value).getField() <= layout.size()))) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getRowExpressionStats(min).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }
            if (!getRowExpressionStats(max).isSingleValue()) {
                return PlanNodeStatsEstimate.unknown();
            }

            SymbolStatsEstimate valueStats;
            if (value instanceof VariableReferenceExpression) {
                valueStats = input.getSymbolStatistics(new Symbol(((VariableReferenceExpression) value).getName()));
            }
            else {
                valueStats = input.getSymbolStatistics(layout.get(((InputReferenceExpression) value).getField()));
            }
            RowExpression lowerBound = call(
                    OperatorType.GREATER_THAN_OR_EQUAL.name(),
                    metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(OperatorType.GREATER_THAN_OR_EQUAL, fromTypes(value.getType(), min.getType())),
                    BOOLEAN,
                    value,
                    min);
            RowExpression upperBound = call(
                    OperatorType.LESS_THAN_OR_EQUAL.name(),
                    metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(OperatorType.LESS_THAN_OR_EQUAL, fromTypes(value.getType(), max.getType())),
                    BOOLEAN,
                    value,
                    max);

            RowExpression transformed;
            if (isInfinite(valueStats.getLowValue())) {
                // We want to do heuristic cut (infinite range to finite range) ASAP and then do filtering on finite range.
                // We rely on 'and()' being processed left to right
                transformed = LogicalRowExpressions.and(lowerBound, upperBound);
            }
            else {
                transformed = LogicalRowExpressions.and(upperBound, lowerBound);
            }
            return process(transformed);
        }

        @Override
        public PlanNodeStatsEstimate visitInputReference(InputReferenceExpression node, Void context)
        {
            return PlanNodeStatsEstimate.unknown();
        }

        private FilterRowExpressionStatsCalculatingVisitor newEstimate(PlanNodeStatsEstimate input)
        {
            return new FilterRowExpressionStatsCalculatingVisitor(input, session, types, layout);
        }

        private PlanNodeStatsEstimate process(RowExpression rowExpression)
        {
            return normalizer.normalize(rowExpression.accept(this, null), types);
        }

        private PlanNodeStatsEstimate estimateLogicalAnd(RowExpression left, RowExpression right)
        {
            // first try to estimate in the fair way
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (!leftEstimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate logicalAndEstimate = newEstimate(leftEstimate).process(right);
                if (!logicalAndEstimate.isOutputRowCountUnknown()) {
                    return logicalAndEstimate;
                }
            }

            // If some of the filters cannot be estimated, take the smallest estimate.
            // Apply 0.9 filter factor as "unknown filter" factor.
            PlanNodeStatsEstimate rightEstimate = process(right);
            PlanNodeStatsEstimate smallestKnownEstimate;
            if (leftEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = rightEstimate;
            }
            else if (rightEstimate.isOutputRowCountUnknown()) {
                smallestKnownEstimate = leftEstimate;
            }
            else {
                smallestKnownEstimate = leftEstimate.getOutputRowCount() <= rightEstimate.getOutputRowCount() ? leftEstimate : rightEstimate;
            }
            if (smallestKnownEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }
            return smallestKnownEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT);
        }

        private PlanNodeStatsEstimate estimateLogicalOr(RowExpression left, RowExpression right)
        {
            PlanNodeStatsEstimate leftEstimate = process(left);
            if (leftEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate rightEstimate = process(right);
            if (rightEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate andEstimate = newEstimate(leftEstimate).process(right);
            if (andEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            return capStats(
                    subtractSubsetStats(
                            addStatsAndSumDistinctValues(leftEstimate, rightEstimate),
                            andEstimate),
                    input);
        }

        private PlanNodeStatsEstimate estimateIn(RowExpression value, List<RowExpression> candidates)
        {
            ImmutableList<PlanNodeStatsEstimate> equalityEstimates = candidates.stream()
                    .map(inValue -> process(call(
                            OperatorType.EQUAL.name(),
                            metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(OperatorType.EQUAL, fromTypes(value.getType(), inValue.getType())),
                            BOOLEAN, value, inValue)))
                    .collect(toImmutableList());

            if (equalityEstimates.stream().anyMatch(PlanNodeStatsEstimate::isOutputRowCountUnknown)) {
                return PlanNodeStatsEstimate.unknown();
            }

            PlanNodeStatsEstimate inEstimate = equalityEstimates.stream()
                    .reduce(PlanNodeStatsEstimateMath::addStatsAndSumDistinctValues)
                    .orElse(PlanNodeStatsEstimate.unknown());

            if (inEstimate.isOutputRowCountUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            SymbolStatsEstimate valueStats = getRowExpressionStats(value);
            if (valueStats.isUnknown()) {
                return PlanNodeStatsEstimate.unknown();
            }

            double notNullValuesBeforeIn = input.getOutputRowCount() * (1 - valueStats.getNullsFraction());

            PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
            result.setOutputRowCount(min(inEstimate.getOutputRowCount(), notNullValuesBeforeIn));

            if (value instanceof VariableReferenceExpression) {
                Symbol symbol = new Symbol(((VariableReferenceExpression) value).getName());
                SymbolStatsEstimate newSymbolStats = inEstimate.getSymbolStatistics(symbol)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addSymbolStatistics(symbol, newSymbolStats);
            }
            else if (value instanceof InputReferenceExpression && ((InputReferenceExpression) value).getField() <= layout.size()) {
                Symbol symbol = layout.get(((InputReferenceExpression) value).getField());
                SymbolStatsEstimate newSymbolStats = inEstimate.getSymbolStatistics(symbol)
                        .mapDistinctValuesCount(newDistinctValuesCount -> min(newDistinctValuesCount, valueStats.getDistinctValuesCount()));
                result.addSymbolStatistics(symbol, newSymbolStats);
            }
            return result.build();
        }

        private PlanNodeStatsEstimate estimateIsNull(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                Symbol symbol = new Symbol(((VariableReferenceExpression) expression).getName());
                SymbolStatsEstimate variableStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }

            if (expression instanceof InputReferenceExpression && ((InputReferenceExpression) expression).getField() <= layout.size()) {
                Symbol symbol = layout.get(((InputReferenceExpression) expression).getField());
                SymbolStatsEstimate variableStats = input.getSymbolStatistics(symbol);
                PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.buildFrom(input);
                result.setOutputRowCount(input.getOutputRowCount() * variableStats.getNullsFraction());
                result.addSymbolStatistics(symbol, SymbolStatsEstimate.builder()
                        .setNullsFraction(1.0)
                        .setLowValue(NaN)
                        .setHighValue(NaN)
                        .setDistinctValuesCount(0.0)
                        .build());
                return result.build();
            }
            return PlanNodeStatsEstimate.unknown();
        }

        private RowExpression isNull(RowExpression expression)
        {
            return new SpecialForm(IS_NULL, BOOLEAN, expression);
        }

        private RowExpression not(RowExpression expression)
        {
            return call("not", functionResolution.notFunction(), expression.getType(), expression);
        }

        private ComparisonExpression.Operator getComparisonOperator(OperatorType operator)
        {
            switch (operator) {
                case EQUAL:
                    return EQUAL;
                case NOT_EQUAL:
                    return NOT_EQUAL;
                case LESS_THAN:
                    return LESS_THAN;
                case LESS_THAN_OR_EQUAL:
                    return LESS_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return GREATER_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return GREATER_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return IS_DISTINCT_FROM;
                default:
                    throw new IllegalStateException("Unsupported comparison operator type: " + operator);
            }
        }

        private OperatorType flip(OperatorType operatorType)
        {
            switch (operatorType) {
                case EQUAL:
                    return OperatorType.EQUAL;
                case NOT_EQUAL:
                    return OperatorType.NOT_EQUAL;
                case LESS_THAN:
                    return OperatorType.GREATER_THAN;
                case LESS_THAN_OR_EQUAL:
                    return OperatorType.GREATER_THAN_OR_EQUAL;
                case GREATER_THAN:
                    return OperatorType.LESS_THAN;
                case GREATER_THAN_OR_EQUAL:
                    return OperatorType.LESS_THAN_OR_EQUAL;
                case IS_DISTINCT_FROM:
                    return OperatorType.IS_DISTINCT_FROM;
                default:
                    throw new IllegalArgumentException("Unsupported comparison: " + operatorType);
            }
        }

        private SymbolStatsEstimate getRowExpressionStats(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                Symbol symbol = new Symbol(((VariableReferenceExpression) expression).getName());
                return requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol));
            }

            if (expression instanceof InputReferenceExpression && ((InputReferenceExpression) expression).getField() <= layout.size()) {
                Symbol symbol = layout.get(((InputReferenceExpression) expression).getField());
                return requireNonNull(input.getSymbolStatistics(symbol), () -> format("No statistics for symbol %s", symbol));
            }
            return scalarStatsCalculator.calculate(expression, input, session, layout);
        }
    }
}
