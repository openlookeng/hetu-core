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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.Collection;
import java.util.Optional;

import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.sql.planner.plan.Patterns.markDistinct;
import static java.lang.Math.min;

public class MarkDistinctStatsRule
        extends SimpleStatsRule<MarkDistinctNode>
{
    private static final Pattern<MarkDistinctNode> PATTERN = markDistinct();

    public MarkDistinctStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<MarkDistinctNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(MarkDistinctNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getDistinctSymbols()));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<Symbol> groupBySymbols)
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
        boolean knowStatsFound = false;
        for (Symbol groupBySymbol : groupBySymbols) {
            SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupBySymbol);
            if (symbolStatistics.isUnknown()) {
                // Incase there is distinct on any function, then we will not have stats for the markerSymbol
                // so for now we dont consider the same for overall stats calculation.
                continue;
            }

            knowStatsFound = true;
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }

        if (knowStatsFound) {
            result.setOutputRowCount(min(rowsCount, sourceStats.getOutputRowCount()));
        }
        else {
            // If there was no proper stats in any of the grouping symbols, we just take fixed % of source.
            result.setOutputRowCount(sourceStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }

        return result.build();
    }
}
