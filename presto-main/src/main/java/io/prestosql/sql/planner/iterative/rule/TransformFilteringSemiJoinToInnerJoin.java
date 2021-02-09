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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.isRewriteFilteringSemiJoinToInnerJoin;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.sql.ExpressionUtils.and;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.semiJoin;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;

/**
 * Rewrite filtering semi-join to inner join.
 * <p/>
 * Transforms:
 * <pre>
 * - Filter (semiJoinSymbol AND predicate)
 *    - SemiJoin (semiJoinSymbol <- (a IN b))
 *        source: plan A producing symbol a
 *        filtering source: plan B producing symbol b
 * </pre>
 * <p/>
 * Into:
 * <pre>
 * - Project (semiJoinSymbol <- TRUE)
 *    - Join INNER on (a = b), joinFilter (predicate with semiJoinSymbol replaced with TRUE)
 *       - source
 *       - Aggregation distinct(b)
 *          - filtering source
 * </pre>
 */
public class TransformFilteringSemiJoinToInnerJoin
        implements Rule<FilterNode>
{
    private static final Capture<SemiJoinNode> SEMI_JOIN = newCapture();

    private static final Pattern<FilterNode> PATTERN = filter()
            .with(source().matching(semiJoin().capturedAs(SEMI_JOIN)));

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isRewriteFilteringSemiJoinToInnerJoin(session);
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        SemiJoinNode semiJoin = captures.get(SEMI_JOIN);

        // Do no transform semi-join in context of DELETE
        if (PlanNodeSearcher.searchFrom(semiJoin.getSource(), context.getLookup())
                .where(node -> node instanceof TableScanNode && ((TableScanNode) node).isForDelete())
                .matches()) {
            return Result.empty();
        }

        Symbol semiJoinSymbol = semiJoin.getSemiJoinOutput();
        Predicate<Expression> isSemiJoinSymbol = expression -> expression.equals(SymbolUtils.toSymbolReference(semiJoinSymbol));

        List<Expression> conjuncts = extractConjuncts(castToExpression(filterNode.getPredicate()));
        if (conjuncts.stream().noneMatch(isSemiJoinSymbol)) {
            return Result.empty();
        }
        Expression filteredPredicate = and(conjuncts.stream()
                .filter(expression -> !expression.equals(SymbolUtils.toSymbolReference(semiJoinSymbol)))
                .collect(toImmutableList()));

        Expression simplifiedPredicate = inlineSymbols(symbol -> {
            if (symbol.equals(semiJoinSymbol)) {
                return TRUE_LITERAL;
            }
            return SymbolUtils.toSymbolReference(symbol);
        }, filteredPredicate);

        Optional<Expression> joinFilter = simplifiedPredicate.equals(TRUE_LITERAL) ? Optional.empty() : Optional.of(simplifiedPredicate);

        PlanNode filteringSourceDistinct = new AggregationNode(
                context.getIdAllocator().getNextId(),
                semiJoin.getFilteringSource(),
                ImmutableMap.of(),
                singleGroupingSet(ImmutableList.of(semiJoin.getFilteringSourceJoinSymbol())),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty());

        JoinNode innerJoin = new JoinNode(
                semiJoin.getId(),
                INNER,
                semiJoin.getSource(),
                filteringSourceDistinct,
                ImmutableList.of(new EquiJoinClause(semiJoin.getSourceJoinSymbol(), semiJoin.getFilteringSourceJoinSymbol())),
                semiJoin.getSource().getOutputSymbols(),
                joinFilter.isPresent() ? Optional.of(castToRowExpression(joinFilter.get())) : Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of()); // TODO: dynamic filter from SemiJoinNode

        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                innerJoin,
                Assignments.builder()
                        .putAll(AssignmentUtils.identityAsSymbolReferences(innerJoin.getOutputSymbols()))
                        .put(semiJoinSymbol, castToRowExpression(TRUE_LITERAL))
                        .build());

        return Result.ofPlanNode(project);
    }
}
