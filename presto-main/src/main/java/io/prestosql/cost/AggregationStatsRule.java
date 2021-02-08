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

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.spi.plan.AggregationNode.Step.FINAL;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregationStatsRule
        extends SimpleStatsRule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    public AggregationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(AggregationNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.getGroupingSetCount() != 1) {
            // In this case there will be GroupId node below which would have calculated actual stats, so we can
            // directly use source stats itself.
            return Optional.of(statsProvider.getStats(node.getSource()));
        }

        if (node.getStep() != FINAL && node.getStep() != PARTIAL && node.getStep() != SINGLE) {
            return Optional.empty();
        }

        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getGroupingKeys(),
                node.getAggregations()));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols, Map<Symbol, Aggregation> aggregations)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            result.addSymbolStatistics(groupBySymbol, symbolStatistics.mapNullsFraction(nullsFraction -> {
                if (nullsFraction == 0.0) {
                    return 0.0;
                }
                return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
            }));
        }

        double rowsCount = 1;
        boolean knowsStatsFound = false;
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            if (symbolStatistics.isUnknown()) {
                // Incase there is group on any function, then we will not have stats for the groupSymbol
                // so for now we dont consider the same for overall stats calculation.
                continue;
            }

            knowsStatsFound = true;
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }

        if (knowsStatsFound) {
            result.setOutputRowCount(min(rowsCount, sourceStats.getOutputRowCount()));
        }
        else {
            // If there was no proper stats in any of the grouping symbols, we just take fixed % of source.
            result.setOutputRowCount(sourceStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }

        for (Map.Entry<Symbol, Aggregation> aggregationEntry : aggregations.entrySet()) {
            result.addSymbolStatistics(aggregationEntry.getKey(), estimateAggregationStats(aggregationEntry.getValue(), sourceStats));
        }

        return result.build();
    }

    private static SymbolStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");

        // TODO implement simple aggregations like: min, max, count, sum
        return SymbolStatsEstimate.unknown();
    }
}
