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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

final class FilterMatcher
        implements Matcher
{
    private final RowExpression predicate;
    private final Optional<RowExpression> dynamicFilter;

    FilterMatcher(RowExpression predicate, Optional<RowExpression> dynamicFilter)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), functionResolution, metadata.getFunctionAndTypeManager());

        FilterNode filterNode = (FilterNode) node;
        if (isExpression(filterNode.getPredicate())) {
            ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);
            if (dynamicFilter.isPresent()) {
                return new MatchResult(verifier.process(castToExpression(filterNode.getPredicate()),
                        combineConjuncts(castToExpression(predicate), castToExpression(dynamicFilter.get()))));
            }

            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(filterNode.getPredicate());
            List<Expression> expressionList = extractResult.getStaticConjuncts().stream().map(OriginalExpressionUtils::castToExpression).collect(Collectors.toList());
            return new MatchResult(verifier.process(combineConjuncts(expressionList), castToExpression(predicate)));
        }

        RowExpressionVerifier verifier = new RowExpressionVerifier(symbolAliases, metadata, session, filterNode.getOutputSymbols());
        if (dynamicFilter.isPresent()) {
            return new MatchResult(verifier.process(combineConjuncts(castToExpression(predicate), castToExpression(dynamicFilter.get())), filterNode.getPredicate()));
        }
        DynamicFilters.ExtractResult extractResult = extractDynamicFilters(filterNode.getPredicate());
        return new MatchResult(verifier.process(castToExpression(predicate), logicalRowExpressions.combineConjuncts(extractResult.getStaticConjuncts())));
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("predicate", predicate)
                .toString();
    }
}
