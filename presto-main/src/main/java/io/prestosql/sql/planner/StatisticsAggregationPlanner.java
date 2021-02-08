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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.aggregation.MaxDataSizeForStats;
import io.prestosql.operator.aggregation.SumDataSizeForStats;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

public class StatisticsAggregationPlanner
{
    private final PlanSymbolAllocator planSymbolAllocator;
    private final Metadata metadata;

    public StatisticsAggregationPlanner(PlanSymbolAllocator planSymbolAllocator, Metadata metadata)
    {
        this.planSymbolAllocator = requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public TableStatisticAggregation createStatisticsAggregation(TableStatisticsMetadata statisticsMetadata, Map<String, Symbol> columnToSymbolMap)
    {
        StatisticAggregationsDescriptor.Builder<Symbol> descriptor = StatisticAggregationsDescriptor.builder();

        List<String> groupingColumns = statisticsMetadata.getGroupingColumns();
        List<Symbol> groupingSymbols = groupingColumns.stream()
                .map(columnToSymbolMap::get)
                .collect(toImmutableList());

        for (int i = 0; i < groupingSymbols.size(); i++) {
            descriptor.addGrouping(groupingColumns.get(i), groupingSymbols.get(i));
        }

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
        for (TableStatisticType type : statisticsMetadata.getTableStatistics()) {
            if (type != ROW_COUNT) {
                throw new PrestoException(NOT_SUPPORTED, "Table-wide statistic type not supported: " + type);
            }
            AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                    metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of()),
                    ImmutableList.of(),
                    false,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());
            Symbol symbol = planSymbolAllocator.newSymbol("rowCount", BIGINT);
            aggregations.put(symbol, aggregation);
            descriptor.addTableStatistic(ROW_COUNT, symbol);
        }

        for (ColumnStatisticMetadata columnStatisticMetadata : statisticsMetadata.getColumnStatistics()) {
            String columnName = columnStatisticMetadata.getColumnName();
            ColumnStatisticType statisticType = columnStatisticMetadata.getStatisticType();
            Symbol inputSymbol = columnToSymbolMap.get(columnName);
            verify(inputSymbol != null, "inputSymbol is null");
            Type inputType = planSymbolAllocator.getTypes().get(inputSymbol);
            verify(inputType != null, "inputType is null for symbol: %s", inputSymbol);
            ColumnStatisticsAggregation aggregation = createColumnAggregation(statisticType, inputSymbol, inputType);
            Symbol symbol = planSymbolAllocator.newSymbol(statisticType + ":" + columnName, aggregation.getOutputType());
            aggregations.put(symbol, aggregation.getAggregation());
            descriptor.addColumnStatistic(columnStatisticMetadata, symbol);
        }

        StatisticAggregations aggregation = new StatisticAggregations(aggregations.build(), groupingSymbols);
        return new TableStatisticAggregation(aggregation, descriptor.build());
    }

    private ColumnStatisticsAggregation createColumnAggregation(ColumnStatisticType statisticType, Symbol input, Type inputType)
    {
        switch (statisticType) {
            case MIN_VALUE:
                return createAggregation(QualifiedName.of("min"), toSymbolReference(input), inputType, inputType);
            case MAX_VALUE:
                return createAggregation(QualifiedName.of("max"), toSymbolReference(input), inputType, inputType);
            case NUMBER_OF_DISTINCT_VALUES:
                return createAggregation(QualifiedName.of("approx_distinct"), toSymbolReference(input), inputType, BIGINT);
            case NUMBER_OF_NON_NULL_VALUES:
                return createAggregation(QualifiedName.of("count"), toSymbolReference(input), inputType, BIGINT);
            case NUMBER_OF_TRUE_VALUES:
                return createAggregation(QualifiedName.of("count_if"), toSymbolReference(input), BOOLEAN, BIGINT);
            case TOTAL_SIZE_IN_BYTES:
                return createAggregation(QualifiedName.of(SumDataSizeForStats.NAME), toSymbolReference(input), inputType, BIGINT);
            case MAX_VALUE_SIZE_IN_BYTES:
                return createAggregation(QualifiedName.of(MaxDataSizeForStats.NAME), toSymbolReference(input), inputType, BIGINT);
            default:
                throw new IllegalArgumentException("Unsupported statistic type: " + statisticType);
        }
    }

    private ColumnStatisticsAggregation createAggregation(QualifiedName functionName, SymbolReference input, Type inputType, Type outputType)
    {
        Signature signature = metadata.resolveFunction(functionName, fromTypes(inputType));
        Type resolvedType = metadata.getType(getOnlyElement(signature.getArgumentTypes()));
        verify(resolvedType.equals(inputType), "resolved function input type does not match the input type: %s != %s", resolvedType, inputType);
        return new ColumnStatisticsAggregation(
                new AggregationNode.Aggregation(
                        signature,
                        ImmutableList.of(castToRowExpression(input)),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()),
                outputType);
    }

    public static class TableStatisticAggregation
    {
        private final StatisticAggregations aggregations;
        private final StatisticAggregationsDescriptor<Symbol> descriptor;

        private TableStatisticAggregation(
                StatisticAggregations aggregations,
                StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            this.aggregations = requireNonNull(aggregations, "statisticAggregations is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
        }

        public StatisticAggregations getAggregations()
        {
            return aggregations;
        }

        public StatisticAggregationsDescriptor<Symbol> getDescriptor()
        {
            return descriptor;
        }
    }

    public static class ColumnStatisticsAggregation
    {
        private final AggregationNode.Aggregation aggregation;
        private final Type outputType;

        private ColumnStatisticsAggregation(AggregationNode.Aggregation aggregation, Type outputType)
        {
            this.aggregation = requireNonNull(aggregation, "aggregation is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
        }

        public AggregationNode.Aggregation getAggregation()
        {
            return aggregation;
        }

        public Type getOutputType()
        {
            return outputType;
        }
    }
}
