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
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.sql.planner.plan.Patterns.groupId;
import static java.lang.Math.min;

public class GroupIdStatsRule
        extends SimpleStatsRule<GroupIdNode>
{
    private static final Pattern<GroupIdNode> PATTERN = groupId();

    public GroupIdStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<GroupIdNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(GroupIdNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        return Optional.of(groupBy(
                statsProvider.getStats(node.getSource()),
                node.getGroupingSets(),
                node.getGroupingColumns()));
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, List<List<Symbol>> groupingSets, Map<Symbol, Symbol> groupingColumns)
    {
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        double rowsCount;
        double finalRowCount = 0;
        boolean knowsStatsFound = false;
        for (List<Symbol> groupingSet : groupingSets) {
            if (groupingSet.size() == 0) {
                finalRowCount = 1;
                continue;
            }

            rowsCount = 1;
            for (Symbol groupBySymbol : groupingSet) {
                Symbol currentCol = groupingColumns.get(groupBySymbol);
                SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(currentCol);
                result.addSymbolStatistics(currentCol, symbolStatistics.mapNullsFraction(nullsFraction -> {
                    if (nullsFraction == 0.0) {
                        return 0.0;
                    }
                    return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
                }));
            }

            for (Symbol groupBySymbol : groupingSet) {
                SymbolStatsEstimate symbolStatistics = sourceStats.getSymbolStatistics(groupingColumns.get(groupBySymbol));
                if (symbolStatistics.isUnknown()) {
                    // Incase there is group on any function, then we will not have stats for the groupSymbol
                    // so for now we dont consider the same for overall stats calculation.
                    continue;
                }

                knowsStatsFound = true;
                int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
                rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
            }

            rowsCount = min(rowsCount, sourceStats.getOutputRowCount());
            // corresponding to each multi col group one NULL value (i.e. corresponding to all) is added, so adjust for the same.
            finalRowCount += rowsCount + (groupingSet.size() > 1 ? (sourceStats.getOutputRowCount() / rowsCount) : 0);
        }

        if (knowsStatsFound) {
            result.setOutputRowCount(finalRowCount + 1);
        }
        else {
            result.setOutputRowCount(sourceStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }

        return result.build();
    }
}
