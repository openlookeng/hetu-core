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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.plan.AggregationNode.globalAggregation;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.LateralJoinNode.Type.INNER;
import static io.prestosql.sql.planner.plan.LateralJoinNode.Type.LEFT;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN;
import static java.util.Objects.requireNonNull;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - LateralJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - LateralJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToLateralNode
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private static final QualifiedName COUNT = QualifiedName.of("count");
    private final StandardFunctionResolution functionResolution;

    public TransformExistsApplyToLateralNode(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        Expression expression = castToExpression(getOnlyElement(parent.getSubqueryAssignments().getExpressions()));
        if (!(expression instanceof ExistsPredicate)) {
            return Result.empty();
        }

        Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        return nonDefaultAggregation
                .map(Result::ofPlanNode)
                .orElseGet(() -> Result.ofPlanNode(rewriteToDefaultAggregation(parent, context)));
    }

    private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputSymbols().isEmpty(), "Expected subquery output symbols to be pruned");

        Symbol exists = getOnlyElement(applyNode.getSubqueryAssignments().getSymbols());
        Symbol subqueryTrue = context.getSymbolAllocator().newSymbol("subqueryTrue", BOOLEAN);

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(AssignmentUtils.identityAsSymbolReferences(applyNode.getInput().getOutputSymbols()));
        assignments.put(exists, castToRowExpression(new CoalesceExpression(ImmutableList.of(toSymbolReference(subqueryTrue), BooleanLiteral.FALSE_LITERAL))));

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        false),
                Assignments.of(subqueryTrue, castToRowExpression(TRUE_LITERAL)));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getLookup());
        if (!decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new LateralJoinNode(
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        TRUE_LITERAL,
                        applyNode.getOriginSubquery()),
                assignments.build()));
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode parent, Context context)
    {
        Symbol count = context.getSymbolAllocator().newSymbol(COUNT.toString(), BIGINT);
        Symbol exists = getOnlyElement(parent.getSubqueryAssignments().getSymbols());

        return new LateralJoinNode(
                parent.getId(),
                parent.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                context.getIdAllocator().getNextId(),
                                parent.getSubquery(),
                                ImmutableMap.of(count, new Aggregation(
                                        new CallExpression(
                                                "count",
                                                functionResolution.countFunction(),
                                                BIGINT,
                                                ImmutableList.of(),
                                                Optional.empty()),
                                        ImmutableList.of(),
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
                                Optional.empty()),
                        Assignments.of(exists, castToRowExpression(new ComparisonExpression(GREATER_THAN, toSymbolReference(count), new Cast(new LongLiteral("0"), BIGINT.toString()))))),
                parent.getCorrelation(),
                INNER,
                TRUE_LITERAL,
                parent.getOriginSubquery());
    }
}
