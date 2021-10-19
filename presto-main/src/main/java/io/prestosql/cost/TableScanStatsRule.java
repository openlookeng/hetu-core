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
import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.tree.Expression;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.SystemSessionProperties.isDefaultFilterFactorEnabled;
import static io.prestosql.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static java.lang.Double.NaN;
import static java.util.Objects.requireNonNull;

public class TableScanStatsRule
        extends SimpleStatsRule<TableScanNode>
{
    private static final Pattern<TableScanNode> PATTERN = tableScan();

    private final Metadata metadata;
    private final FilterStatsCalculator filterStatsCalculator;
    private final ExpressionDomainTranslator domainTranslator;

    public TableScanStatsRule(Metadata metadata, StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer); // Use stats normalization since connector can return inconsistent stats values
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.filterStatsCalculator = requireNonNull(filterStatsCalculator, "filterStatsCalculator is null");
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata));
    }

    @Override
    public Pattern<TableScanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TableScanNode node, StatsProvider sourceStats, Lookup lookup, Session session, TypeProvider types)
    {
        // TODO Construct predicate like AddExchanges's LayoutConstraintEvaluator
        TupleDomain<ColumnHandle> predicate = metadata.getTableProperties(session, node.getTable()).getPredicate();
        Constraint constraint = new Constraint(predicate);

        TableStatistics tableStatistics = metadata.getTableStatistics(session, node.getTable(), constraint, true);
        verify(tableStatistics != null, "tableStatistics is null for %s", node);

        Map<Symbol, SymbolStatsEstimate> outputSymbolStats = new HashMap<>();
        Map<ColumnHandle, Symbol> remainingSymbols = new HashMap<>();
        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

        boolean isPredicatesPushDown = false;
        if ((predicate.isAll() || predicate.getDomains().get().equals(node.getEnforcedConstraint().getDomains().get()))
                && !(node.getEnforcedConstraint().isAll() || node.getEnforcedConstraint().isNone())) {
            predicate = node.getEnforcedConstraint();
            isPredicatesPushDown = true;

            predicate.getDomains().get().entrySet().stream().forEach(e -> {
                remainingSymbols.put(e.getKey(), new Symbol(e.getKey().getColumnName()));
            });
        }

        for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
            Symbol symbol = entry.getKey();
            Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getValue()));
            SymbolStatsEstimate symbolStatistics = columnStatistics
                    .map(statistics -> toSymbolStatistics(tableStatistics, statistics, types.get(symbol)))
                    .orElse(SymbolStatsEstimate.unknown());
            outputSymbolStats.put(symbol, symbolStatistics);
            remainingSymbols.remove(entry.getValue());
        }

        PlanNodeStatsEstimate tableEstimates = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(tableStatistics.getRowCount().getValue())
                .addSymbolStatistics(outputSymbolStats)
                .build();

        if (isPredicatesPushDown) {
            if (remainingSymbols.size() > 0) {
                ImmutableBiMap.Builder<ColumnHandle, Symbol> assignmentBuilder = ImmutableBiMap.builder();
                assignments = assignmentBuilder.putAll(assignments).putAll(remainingSymbols).build();

                for (Map.Entry<ColumnHandle, Symbol> entry : remainingSymbols.entrySet()) {
                    Symbol symbol = entry.getValue();
                    Optional<ColumnStatistics> columnStatistics = Optional.ofNullable(tableStatistics.getColumnStatistics().get(entry.getKey()));
                    SymbolStatsEstimate symbolStatistics = columnStatistics
                            .map(statistics -> toSymbolStatistics(tableStatistics, statistics, types.get(symbol)))
                            .orElse(SymbolStatsEstimate.unknown());
                    outputSymbolStats.put(symbol, symbolStatistics);
                }

                /* Refresh TableEstimates for remaining columns */
                tableEstimates = PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(tableStatistics.getRowCount().getValue())
                        .addSymbolStatistics(outputSymbolStats)
                        .build();
            }

            Expression pushDownExpression = domainTranslator.toPredicate(predicate.transform(assignments :: get));
            PlanNodeStatsEstimate estimate = filterStatsCalculator.filterStats(tableEstimates, pushDownExpression, session, types);
            if ((isDefaultFilterFactorEnabled(session) || sourceStats.isEnforceDefaultFilterFactor()) && estimate.isOutputRowCountUnknown()) {
                PlanNodeStatsEstimate finalTableEstimates = tableEstimates;
                estimate = tableEstimates.mapOutputRowCount(sourceRowCount -> finalTableEstimates.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
            }

            return Optional.of(estimate);
        }

        return Optional.of(tableEstimates);
    }

    public static SymbolStatsEstimate toSymbolStatistics(TableStatistics tableStatistics, ColumnStatistics columnStatistics, Type type)
    {
        requireNonNull(tableStatistics, "tableStatistics is null");
        requireNonNull(columnStatistics, "columnStatistics is null");
        requireNonNull(type, "type is null");

        double nullsFraction = columnStatistics.getNullsFraction().getValue();
        double nonNullRowsCount = tableStatistics.getRowCount().getValue() * (1.0 - nullsFraction);
        double averageRowSize;
        if (nonNullRowsCount == 0) {
            averageRowSize = 0;
        }
        else if (type instanceof FixedWidthType) {
            // For a fixed-width type, engine knows the row size.
            averageRowSize = NaN;
        }
        else {
            averageRowSize = columnStatistics.getDataSize().getValue() / nonNullRowsCount;
        }
        SymbolStatsEstimate.Builder result = SymbolStatsEstimate.builder();
        result.setNullsFraction(nullsFraction);
        result.setDistinctValuesCount(columnStatistics.getDistinctValuesCount().getValue());
        result.setAverageRowSize(averageRowSize);
        columnStatistics.getRange().ifPresent(range -> {
            result.setLowValue(range.getMin());
            result.setHighValue(range.getMax());
        });
        return result.build();
    }
}
