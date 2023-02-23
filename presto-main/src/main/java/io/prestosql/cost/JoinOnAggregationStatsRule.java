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

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.util.MoreMath;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.getJoinMultiClauseIndependenceFactor;
import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.cost.PlanNodeStatsEstimateMath.estimateCorrelatedConjunctionRowCount;
import static io.prestosql.cost.SymbolStatsEstimate.buildFrom;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.Patterns.joinOnAggregation;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class JoinOnAggregationStatsRule
        extends SimpleStatsRule<JoinOnAggregationNode>
{
    private static final Pattern<JoinOnAggregationNode> PATTERN = joinOnAggregation();
    private static final double DEFAULT_UNMATCHED_JOIN_COMPLEMENT_NDVS_COEFFICIENT = 0.5;

    private final FilterStatsCalculator filterStatsCalculator;
    private final StatsNormalizer normalizer;
    private final double unmatchedJoinComplementNdvsCoefficient;

    public JoinOnAggregationStatsRule(FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer)
    {
        this(filterStatsCalculator, normalizer, DEFAULT_UNMATCHED_JOIN_COMPLEMENT_NDVS_COEFFICIENT);
    }

    @VisibleForTesting
    JoinOnAggregationStatsRule(FilterStatsCalculator filterStatsCalculator, StatsNormalizer normalizer, double unmatchedJoinComplementNdvsCoefficient)
    {
        super(normalizer);
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.normalizer = normalizer;
        this.unmatchedJoinComplementNdvsCoefficient = unmatchedJoinComplementNdvsCoefficient;
    }

    @Override
    public Pattern<JoinOnAggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(JoinOnAggregationNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate leftStats = sourceStats.getStats(node.getLeft());
        PlanNodeStatsEstimate rightStats = sourceStats.getStats(node.getRight());
        PlanNodeStatsEstimate leftAggrStats = AggregationStatsRule.groupBy(leftStats, node.getLeftAggr().getGroupingKeys(), node.getLeftAggr().getAggregations());
        PlanNodeStatsEstimate rightAggrStats = AggregationStatsRule.groupBy(rightStats, node.getRightAggr().getGroupingKeys(), node.getRightAggr().getAggregations());
        // TODO Vineet Check if following consideration is correct??
        PlanNodeStatsEstimate leftAggrOnAggrStats = AggregationStatsRule.groupBy(leftAggrStats, node.getAggrOnLeft().getGroupingKeys(), node.getAggrOnLeft().getAggregations());
        PlanNodeStatsEstimate rightAggrOnAggrStats = AggregationStatsRule.groupBy(rightAggrStats, node.getAggrOnRight().getGroupingKeys(), node.getAggrOnRight().getAggregations());
        PlanNodeStatsEstimate crossJoinStats = crossJoinStats(node, leftAggrOnAggrStats, rightAggrOnAggrStats, types);

        switch (node.getType()) {
            case INNER:
                return Optional.of(computeInnerJoinStats(node, crossJoinStats, session, types));
            default:
                throw new IllegalStateException("Unknown group join type: " + node.getType());
        }
    }

    private PlanNodeStatsEstimate computeInnerJoinStats(JoinOnAggregationNode node, PlanNodeStatsEstimate crossJoinStats, Session session, TypeProvider types)
    {
        List<EquiJoinClause> equiJoinCriteria = node.getCriteria();

        Map<Integer, Symbol> layout = new HashMap<>();
        int channel = 0;
        for (Symbol symbol : node.getOutputSymbols()) {
            layout.put(channel++, symbol);
        }

        if (equiJoinCriteria.isEmpty()) {
            if (!node.getFilter().isPresent()) {
                return crossJoinStats;
            }
            // TODO: this might explode stats
            if (isExpression(node.getFilter().get())) {
                return filterStatsCalculator.filterStats(crossJoinStats, castToExpression(node.getFilter().get()), session, types);
            }
            else {
                return filterStatsCalculator.filterStats(crossJoinStats, node.getFilter().get(), session, types, layout);
            }
        }

        PlanNodeStatsEstimate equiJoinEstimate = filterByEquiJoinClauses(crossJoinStats, node.getCriteria(), session, types);

        if (equiJoinEstimate.isOutputRowCountUnknown()) {
            return PlanNodeStatsEstimate.unknown();
        }

        if (!node.getFilter().isPresent()) {
            return equiJoinEstimate;
        }

        PlanNodeStatsEstimate filteredEquiJoinEstimate;
        if (isExpression(node.getFilter().get())) {
            filteredEquiJoinEstimate = filterStatsCalculator.filterStats(equiJoinEstimate, castToExpression(node.getFilter().get()), session, types);
        }
        else {
            filteredEquiJoinEstimate = filterStatsCalculator.filterStats(equiJoinEstimate, node.getFilter().get(), session, types, layout);
        }

        if (filteredEquiJoinEstimate.isOutputRowCountUnknown()) {
            return normalizer.normalize(equiJoinEstimate.mapOutputRowCount(rowCount -> rowCount * UNKNOWN_FILTER_COEFFICIENT), types);
        }

        return filteredEquiJoinEstimate;
    }

    private PlanNodeStatsEstimate filterByEquiJoinClauses(
            PlanNodeStatsEstimate stats,
            Collection<EquiJoinClause> clauses,
            Session session,
            TypeProvider types)
    {
        checkArgument(!clauses.isEmpty(), "clauses is empty");
        // Join equality clauses are usually correlated. Therefore, we shouldn't treat each join equality
        // clause separately because stats estimates would be way off.
        List<PlanNodeStatsEstimateWithClause> knownEstimates = clauses.stream()
                .map(clause -> {
                    ComparisonExpression predicate = new ComparisonExpression(EQUAL, toSymbolReference(clause.getLeft()), toSymbolReference(clause.getRight()));
                    return new PlanNodeStatsEstimateWithClause(filterStatsCalculator.filterStats(stats, predicate, session, types), clause);
                })
                .collect(toImmutableList());

        double outputRowCount = estimateCorrelatedConjunctionRowCount(
                stats,
                knownEstimates.stream().map(PlanNodeStatsEstimateWithClause::getEstimate).collect(toImmutableList()),
                getJoinMultiClauseIndependenceFactor(session));
        if (isNaN(outputRowCount)) {
            return PlanNodeStatsEstimate.unknown();
        }
        return normalizer.normalize(new PlanNodeStatsEstimate(outputRowCount, intersectCorrelatedJoinClause(stats, knownEstimates)), types);
    }

    private static double firstNonNaN(double... values)
    {
        for (double value : values) {
            if (!isNaN(value)) {
                return value;
            }
        }
        throw new IllegalArgumentException("All values are NaN");
    }

    private PlanNodeStatsEstimate crossJoinStats(JoinOnAggregationNode node, PlanNodeStatsEstimate leftStats, PlanNodeStatsEstimate rightStats, TypeProvider types)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(leftStats.getOutputRowCount() * rightStats.getOutputRowCount());

        node.getLeft().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, leftStats.getSymbolStatistics(symbol)));
        node.getRight().getOutputSymbols().forEach(symbol -> builder.addSymbolStatistics(symbol, rightStats.getSymbolStatistics(symbol)));

        return normalizer.normalize(builder.build(), types);
    }

    private static Map<Symbol, SymbolStatsEstimate> intersectCorrelatedJoinClause(
            PlanNodeStatsEstimate stats,
            List<PlanNodeStatsEstimateWithClause> equiJoinClauseEstimates)
    {
        // Add initial statistics (including stats for columns which are not part of equi-join clauses)
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder()
                .addSymbolStatistics(stats.getSymbolStatistics());

        for (PlanNodeStatsEstimateWithClause estimateWithClause : equiJoinClauseEstimates) {
            EquiJoinClause clause = estimateWithClause.getClause();
            // we just clear null fraction and adjust ranges here, selectivity is handled outside this function
            SymbolStatsEstimate leftStats = stats.getSymbolStatistics(clause.getLeft());
            SymbolStatsEstimate rightStats = stats.getSymbolStatistics(clause.getRight());
            StatisticRange leftRange = StatisticRange.from(leftStats);
            StatisticRange rightRange = StatisticRange.from(rightStats);

            StatisticRange intersect = leftRange.intersect(rightRange);
            double leftFilterValue = firstNonNaN(leftRange.overlapPercentWith(intersect), 1);
            double rightFilterValue = firstNonNaN(rightRange.overlapPercentWith(intersect), 1);
            double leftNdvInRange = leftFilterValue * leftRange.getDistinctValuesCount();
            double rightNdvInRange = rightFilterValue * rightRange.getDistinctValuesCount();
            double retainedNdv = MoreMath.min(leftNdvInRange, rightNdvInRange);

            SymbolStatsEstimate newLeftStats = buildFrom(leftStats)
                    .setNullsFraction(0)
                    .setStatisticsRange(intersect)
                    .setDistinctValuesCount(retainedNdv)
                    .build();

            SymbolStatsEstimate newRightStats = buildFrom(rightStats)
                    .setNullsFraction(0)
                    .setStatisticsRange(intersect)
                    .setDistinctValuesCount(retainedNdv)
                    .build();

            result.addSymbolStatistics(clause.getLeft(), newLeftStats)
                    .addSymbolStatistics(clause.getRight(), newRightStats);
        }
        return result.build().getSymbolStatistics();
    }

    private static class PlanNodeStatsEstimateWithClause
    {
        private final PlanNodeStatsEstimate estimate;
        private final EquiJoinClause clause;

        private PlanNodeStatsEstimateWithClause(PlanNodeStatsEstimate estimate, EquiJoinClause clause)
        {
            this.estimate = requireNonNull(estimate, "estimate is null");
            this.clause = requireNonNull(clause, "clause is null");
        }

        private PlanNodeStatsEstimate getEstimate()
        {
            return estimate;
        }

        private EquiJoinClause getClause()
        {
            return clause;
        }
    }
}
