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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.WhenClause;

import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.plan.AggregationNode.globalAggregation;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.SimplePlanRewriter.rewriteWith;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;
import static io.prestosql.sql.tree.QuantifiedComparisonExpression.Quantifier.ALL;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class TransformQuantifiedComparisonApplyToLateralJoin
        implements PlanOptimizer
{
    private final StandardFunctionResolution functionResolution;

    public TransformQuantifiedComparisonApplyToLateralJoin(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        return rewriteWith(new Rewriter(idAllocator, types, planSymbolAllocator, functionResolution), plan, null);
    }

    private static class Rewriter
            extends SimplePlanRewriter<PlanNode>
    {
        private static final QualifiedName MIN = QualifiedName.of("min");
        private static final QualifiedName MAX = QualifiedName.of("max");
        private static final QualifiedName COUNT = QualifiedName.of("count");

        private final PlanNodeIdAllocator idAllocator;
        private final TypeProvider types;
        private final PlanSymbolAllocator planSymbolAllocator;
        private final StandardFunctionResolution functionResolution;

        public Rewriter(PlanNodeIdAllocator idAllocator, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, StandardFunctionResolution functionResolution)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.types = requireNonNull(types, "types is null");
            this.planSymbolAllocator = requireNonNull(planSymbolAllocator, "symbolAllocator is null");
            this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<PlanNode> context)
        {
            if (node.getSubqueryAssignments().size() != 1) {
                return context.defaultRewrite(node);
            }

            Expression expression = castToExpression(getOnlyElement(node.getSubqueryAssignments().getExpressions()));
            if (!(expression instanceof QuantifiedComparisonExpression)) {
                return context.defaultRewrite(node);
            }

            QuantifiedComparisonExpression quantifiedComparison = (QuantifiedComparisonExpression) expression;

            return rewriteQuantifiedApplyNode(node, quantifiedComparison, context);
        }

        private PlanNode rewriteQuantifiedApplyNode(ApplyNode node, QuantifiedComparisonExpression quantifiedComparison, RewriteContext<PlanNode> context)
        {
            PlanNode subqueryPlan = context.rewrite(node.getSubquery());

            Symbol outputColumn = getOnlyElement(subqueryPlan.getOutputSymbols());
            Type outputColumnType = types.get(outputColumn);
            checkState(outputColumnType.isOrderable(), "Subquery result type must be orderable");

            Symbol minValue = planSymbolAllocator.newSymbol(MIN.toString(), outputColumnType);
            Symbol maxValue = planSymbolAllocator.newSymbol(MAX.toString(), outputColumnType);
            Symbol countAllValue = planSymbolAllocator.newSymbol("count_all", BIGINT);
            Symbol countNonNullValue = planSymbolAllocator.newSymbol("count_non_null", BIGINT);

            List<Expression> outputColumnReferences = ImmutableList.of(toSymbolReference(outputColumn));
            List<TypeSignature> outputColumnTypeSignature = ImmutableList.of(outputColumnType.getTypeSignature());

            subqueryPlan = new AggregationNode(
                    idAllocator.getNextId(),
                    subqueryPlan,
                    ImmutableMap.of(
                            minValue, new Aggregation(
                                    new CallExpression(
                                            "min",
                                            functionResolution.minFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                                    outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            maxValue, new Aggregation(
                                    new CallExpression(
                                            "max",
                                            functionResolution.maxFunction(outputColumnType),
                                            outputColumnType,
                                            outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                                    outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            countAllValue, new Aggregation(
                                    new CallExpression(
                                            "count",
                                            functionResolution.countFunction(),
                                            BIGINT,
                                            emptyList()),
                                    ImmutableList.of(),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty()),
                            countNonNullValue, new Aggregation(
                                    new CallExpression(
                                            "count",
                                            functionResolution.countFunction(outputColumnType),
                                            BIGINT,
                                            outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList())),
                                    outputColumnReferences.stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()),
                                    false,
                                    Optional.empty(),
                                    Optional.empty(),
                                    Optional.empty())),
                    globalAggregation(),
                    ImmutableList.of(),
                    AggregationNode.Step.SINGLE,
                    Optional.empty(),
                    Optional.empty(),
                    AggregationNode.AggregationType.HASH,
                    Optional.empty());

            PlanNode lateralJoinNode = new LateralJoinNode(
                    node.getId(),
                    context.rewrite(node.getInput()),
                    subqueryPlan,
                    node.getCorrelation(),
                    LateralJoinNode.Type.INNER,
                    TRUE_LITERAL,
                    node.getOriginSubquery());

            Expression valueComparedToSubquery = rewriteUsingBounds(quantifiedComparison, minValue, maxValue, countAllValue, countNonNullValue);

            Symbol quantifiedComparisonSymbol = getOnlyElement(node.getSubqueryAssignments().getSymbols());

            return projectExpressions(lateralJoinNode, Assignments.of(quantifiedComparisonSymbol, castToRowExpression(valueComparedToSubquery)));
        }

        public Expression rewriteUsingBounds(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue, Symbol countAllValue, Symbol countNonNullValue)
        {
            BooleanLiteral emptySetResult = quantifiedComparison.getQuantifier().equals(ALL) ? TRUE_LITERAL : FALSE_LITERAL;
            Function<List<Expression>, Expression> quantifier = quantifiedComparison.getQuantifier().equals(ALL) ?
                    ExpressionUtils::combineConjuncts : ExpressionUtils::combineDisjuncts;
            Expression comparisonWithExtremeValue = getBoundComparisons(quantifiedComparison, minValue, maxValue);

            return new SimpleCaseExpression(
                    toSymbolReference(countAllValue),
                    ImmutableList.of(new WhenClause(
                            new GenericLiteral("bigint", "0"),
                            emptySetResult)),
                    Optional.of(quantifier.apply(ImmutableList.of(
                            comparisonWithExtremeValue,
                            new SearchedCaseExpression(
                                    ImmutableList.of(
                                            new WhenClause(
                                                    new ComparisonExpression(NOT_EQUAL, toSymbolReference(countAllValue), toSymbolReference(countNonNullValue)),
                                                    new Cast(new NullLiteral(), BooleanType.BOOLEAN.toString()))),
                                    Optional.of(emptySetResult))))));
        }

        private Expression getBoundComparisons(QuantifiedComparisonExpression quantifiedComparison, Symbol minValue, Symbol maxValue)
        {
            if (quantifiedComparison.getOperator() == EQUAL && quantifiedComparison.getQuantifier() == ALL) {
                // A = ALL B <=> min B = max B && A = min B
                return combineConjuncts(
                        new ComparisonExpression(EQUAL, toSymbolReference(minValue), toSymbolReference(maxValue)),
                        new ComparisonExpression(EQUAL, quantifiedComparison.getValue(), toSymbolReference(maxValue)));
            }

            if (EnumSet.of(LESS_THAN, LESS_THAN_OR_EQUAL, GREATER_THAN, GREATER_THAN_OR_EQUAL).contains(quantifiedComparison.getOperator())) {
                // A < ALL B <=> A < min B
                // A > ALL B <=> A > max B
                // A < ANY B <=> A < max B
                // A > ANY B <=> A > min B
                Symbol boundValue = shouldCompareValueWithLowerBound(quantifiedComparison) ? minValue : maxValue;
                return new ComparisonExpression(quantifiedComparison.getOperator(), quantifiedComparison.getValue(), toSymbolReference(boundValue));
            }
            throw new IllegalArgumentException("Unsupported quantified comparison: " + quantifiedComparison);
        }

        private static boolean shouldCompareValueWithLowerBound(QuantifiedComparisonExpression quantifiedComparison)
        {
            switch (quantifiedComparison.getQuantifier()) {
                case ALL:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return true;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return false;
                    }
                    break;
                case ANY:
                case SOME:
                    switch (quantifiedComparison.getOperator()) {
                        case LESS_THAN:
                        case LESS_THAN_OR_EQUAL:
                            return false;
                        case GREATER_THAN:
                        case GREATER_THAN_OR_EQUAL:
                            return true;
                    }
                    break;
            }
            throw new IllegalArgumentException("Unexpected quantifier: " + quantifiedComparison.getQuantifier());
        }

        private ProjectNode projectExpressions(PlanNode input, Assignments subqueryAssignments)
        {
            Assignments assignments = Assignments.builder()
                    .putAll(AssignmentUtils.identityAsSymbolReferences(input.getOutputSymbols()))
                    .putAll(subqueryAssignments)
                    .build();
            return new ProjectNode(
                    idAllocator.getNextId(),
                    input,
                    assignments);
        }
    }
}
