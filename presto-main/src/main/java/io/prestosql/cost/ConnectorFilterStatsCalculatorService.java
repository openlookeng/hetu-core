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

import com.google.common.collect.ImmutableBiMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.plan.FilterStatsCalculatorService;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.TypeProvider;

import java.util.Map;

import static io.prestosql.cost.TableScanStatsRule.toSymbolStatistics;
import static java.util.Objects.requireNonNull;

public class ConnectorFilterStatsCalculatorService
        implements FilterStatsCalculatorService
{
    private final FilterStatsCalculator filterStatsCalculator;

    public ConnectorFilterStatsCalculatorService(FilterStatsCalculator filterStatsCalculator)
    {
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
    }

    @Override
    public TableStatistics filterStats(
            TableStatistics tableStatistics,
            RowExpression predicate,
            ConnectorSession session,
            Map<ColumnHandle, String> columnNames,
            Map<String, Type> columnTypes,
            Map<Symbol, Type> types,
            Map<Integer, Symbol> layout)
    {
        PlanNodeStatsEstimate tableStats = toPlanNodeStats(tableStatistics, columnNames, columnTypes);
        TypeProvider typeProvider = TypeProvider.viewOf(types);
        PlanNodeStatsEstimate filteredStats = filterStatsCalculator.filterStats(tableStats, predicate, session, typeProvider, layout);
        return toTableStatistics(filteredStats, ImmutableBiMap.copyOf(columnNames).inverse());
    }

    private static PlanNodeStatsEstimate toPlanNodeStats(
            TableStatistics tableStatistics,
            Map<ColumnHandle, String> columnNames,
            Map<String, Type> columnTypes)
    {
        PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue());
        for (Map.Entry<ColumnHandle, ColumnStatistics> entry : tableStatistics.getColumnStatistics().entrySet()) {
            String columnName = columnNames.getOrDefault(entry.getKey(), null);
            if (columnName == null) {
                continue;
            }
            Symbol symbol = new Symbol(columnName);
            builder.addSymbolStatistics(symbol, toSymbolStatistics(tableStatistics, entry.getValue(), columnTypes.get(columnName)));
        }
        return builder.build();
    }

    private static TableStatistics toTableStatistics(PlanNodeStatsEstimate planNodeStats, Map<String, ColumnHandle> columnByName)
    {
        TableStatistics.Builder builder = TableStatistics.builder();
        if (planNodeStats.isOutputRowCountUnknown()) {
            builder.setRowCount(Estimate.unknown());
            return builder.build();
        }

        double rowCount = planNodeStats.getOutputRowCount();
        builder.setRowCount(Estimate.of(rowCount));
        for (Map.Entry<Symbol, SymbolStatsEstimate> entry : planNodeStats.getSymbolStatistics().entrySet()) {
            builder.setColumnStatistics(columnByName.get(entry.getKey().getName()), toColumnStatistics(entry.getValue(), rowCount));
        }
        return builder.build();
    }

    private static ColumnStatistics toColumnStatistics(SymbolStatsEstimate variableStatsEstimate, double rowCount)
    {
        if (variableStatsEstimate.isUnknown()) {
            return ColumnStatistics.empty();
        }

        double nullsFractionDouble = variableStatsEstimate.getNullsFraction();
        double nonNullRowsCount = rowCount * (1.0 - nullsFractionDouble);

        ColumnStatistics.Builder builder = ColumnStatistics.builder();
        if (!Double.isNaN(nullsFractionDouble)) {
            builder.setNullsFraction(Estimate.of(nullsFractionDouble));
        }

        if (!Double.isNaN(variableStatsEstimate.getDistinctValuesCount())) {
            builder.setDistinctValuesCount(Estimate.of(variableStatsEstimate.getDistinctValuesCount()));
        }

        if (!Double.isNaN(variableStatsEstimate.getAverageRowSize())) {
            builder.setDataSize(Estimate.of(variableStatsEstimate.getAverageRowSize() * nonNullRowsCount));
        }

        if (!Double.isNaN(variableStatsEstimate.getLowValue()) && !Double.isNaN(variableStatsEstimate.getHighValue())) {
            builder.setRange(new DoubleRange(variableStatsEstimate.getLowValue(), variableStatsEstimate.getHighValue()));
        }
        return builder.build();
    }
}
