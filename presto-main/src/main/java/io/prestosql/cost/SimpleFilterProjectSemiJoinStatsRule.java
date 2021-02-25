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

import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionUtils;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.cost.SemiJoinStatsCalculator.computeAntiJoin;
import static io.prestosql.cost.SemiJoinStatsCalculator.computeSemiJoin;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.extractConjuncts;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.sql.relational.ProjectNodeUtils.isIdentity;
import static java.util.Objects.requireNonNull;

/**
 * It is not yet proven whether this heuristic is any better or worse. Either this rule will be enhanced
 * in the future or it will be dropped altogether.
 */
public class SimpleFilterProjectSemiJoinStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public SimpleFilterProjectSemiJoinStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator can not be null");
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNode nodeSource = lookup.resolve(node.getSource());
        SemiJoinNode semiJoinNode;
        if (nodeSource instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) nodeSource;
            if (!isIdentity(projectNode)) {
                return Optional.empty();
            }
            PlanNode projectNodeSource = lookup.resolve(projectNode.getSource());
            if (!(projectNodeSource instanceof SemiJoinNode)) {
                return Optional.empty();
            }
            semiJoinNode = (SemiJoinNode) projectNodeSource;
        }
        else if (nodeSource instanceof SemiJoinNode) {
            semiJoinNode = (SemiJoinNode) nodeSource;
        }
        else {
            return Optional.empty();
        }

        return calculate(node, semiJoinNode, sourceStats, session, types);
    }

    private Optional<PlanNodeStatsEstimate> calculate(FilterNode filterNode, SemiJoinNode semiJoinNode, StatsProvider statsProvider, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(semiJoinNode.getSource());
        PlanNodeStatsEstimate filteringSourceStats = statsProvider.getStats(semiJoinNode.getFilteringSource());
        Symbol filteringSourceJoinSymbol = semiJoinNode.getFilteringSourceJoinSymbol();
        Symbol sourceJoinSymbol = semiJoinNode.getSourceJoinSymbol();

        Optional<SemiJoinOutputFilter> semiJoinOutputFilter;
        if (isExpression(filterNode.getPredicate())) {
            semiJoinOutputFilter = extractSemiJoinOutputFilter(castToExpression(filterNode.getPredicate()), semiJoinNode.getSemiJoinOutput());
        }
        else {
            VariableReferenceExpression semiJoinOutput = new VariableReferenceExpression(semiJoinNode.getSemiJoinOutput().getName(), types.get(semiJoinNode.getSemiJoinOutput()));
            semiJoinOutputFilter = extractSemiJoinOutputFilter(filterNode.getPredicate(), semiJoinOutput);
        }

        if (!semiJoinOutputFilter.isPresent()) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate semiJoinStats;
        if (semiJoinOutputFilter.get().isNegated()) {
            semiJoinStats = computeAntiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }
        else {
            semiJoinStats = computeSemiJoin(sourceStats, filteringSourceStats, sourceJoinSymbol, filteringSourceJoinSymbol);
        }

        if (semiJoinStats.isOutputRowCountUnknown()) {
            return Optional.of(PlanNodeStatsEstimate.unknown());
        }

        // apply remaining predicate
        PlanNodeStatsEstimate filteredStats;
        if (isExpression(filterNode.getPredicate())) {
            filteredStats = filterStatsCalculator.filterStats(semiJoinStats, castToExpression(semiJoinOutputFilter.get().getRemainingPredicate()), session, types);
        }
        else {
            Map<Integer, Symbol> layout = SymbolUtils.toLayOut(filterNode.getOutputSymbols());
            filteredStats = filterStatsCalculator.filterStats(semiJoinStats, semiJoinOutputFilter.get().getRemainingPredicate(), session, types, layout);
        }

        if (filteredStats.isOutputRowCountUnknown()) {
            return Optional.of(semiJoinStats.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT));
        }
        return Optional.of(filteredStats);
    }

    private static Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(Expression predicate, Symbol semiJoinOutput)
    {
        List<Expression> conjuncts = extractConjuncts(predicate);
        List<Expression> semiJoinOutputReferences = conjuncts.stream()
                .filter(conjunct -> isSemiJoinOutputReference(conjunct, semiJoinOutput))
                .collect(toImmutableList());

        if (semiJoinOutputReferences.size() != 1) {
            return Optional.empty();
        }

        Expression semiJoinOutputReference = Iterables.getOnlyElement(semiJoinOutputReferences);
        Expression remainingPredicate = combineConjuncts(conjuncts.stream()
                .filter(conjunct -> conjunct != semiJoinOutputReference)
                .collect(toImmutableList()));
        boolean negated = semiJoinOutputReference instanceof NotExpression;
        return Optional.of(new SemiJoinOutputFilter(negated, castToRowExpression(remainingPredicate)));
    }

    private Optional<SemiJoinOutputFilter> extractSemiJoinOutputFilter(RowExpression predicate, RowExpression input)
    {
        checkState(!isExpression(predicate));
        List<RowExpression> conjuncts = RowExpressionUtils.extractConjuncts(predicate);
        List<RowExpression> semiJoinOutputReferences = conjuncts.stream()
                .filter(conjunct -> isSemiJoinOutputReference(conjunct, input))
                .collect(toImmutableList());

        if (semiJoinOutputReferences.size() != 1) {
            return Optional.empty();
        }

        RowExpression semiJoinOutputReference = Iterables.getOnlyElement(semiJoinOutputReferences);
        RowExpression remainingPredicate = RowExpressionUtils.combineConjuncts(conjuncts.stream()
                .filter(conjunct -> conjunct != semiJoinOutputReference)
                .collect(toImmutableList()));
        boolean negated = isNotFunction(semiJoinOutputReference);
        return Optional.of(new SemiJoinOutputFilter(negated, remainingPredicate));
    }

    private boolean isSemiJoinOutputReference(RowExpression conjunct, RowExpression input)
    {
        return conjunct.equals(input) || (isNotFunction(conjunct) && ((CallExpression) conjunct).getArguments().get(0).equals(input));
    }

    private static boolean isSemiJoinOutputReference(Expression conjunct, Symbol semiJoinOutput)
    {
        SymbolReference semiJoinOuputSymbolReference = toSymbolReference(semiJoinOutput);
        return conjunct.equals(semiJoinOuputSymbolReference) ||
                (conjunct instanceof NotExpression && ((NotExpression) conjunct).getValue().equals(semiJoinOuputSymbolReference));
    }

    private boolean isNotFunction(RowExpression expression)
    {
        return expression instanceof CallExpression && (((CallExpression) expression).getSignature().getName().equalsIgnoreCase("not"));
    }

    private static class SemiJoinOutputFilter
    {
        private final boolean negated;
        private final RowExpression remainingPredicate;

        public SemiJoinOutputFilter(boolean negated, RowExpression remainingPredicate)
        {
            this.negated = negated;
            this.remainingPredicate = requireNonNull(remainingPredicate, "remainingPredicate can not be null");
        }

        public boolean isNegated()
        {
            return negated;
        }

        public RowExpression getRemainingPredicate()
        {
            return remainingPredicate;
        }
    }
}
