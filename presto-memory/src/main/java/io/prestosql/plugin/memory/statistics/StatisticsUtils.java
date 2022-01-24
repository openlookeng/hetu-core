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

package io.prestosql.plugin.memory.statistics;

import com.google.common.collect.ImmutableSet;
import io.prestosql.plugin.memory.MemoryColumnHandle;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.SqlDate;
import io.prestosql.spi.type.SqlDecimal;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.prestosql.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static io.prestosql.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.prestosql.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.prestosql.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.prestosql.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static io.prestosql.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class StatisticsUtils
{
    private StatisticsUtils()
    {
    }

    public static Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE) || type.equals(TIMESTAMP)) {
            // TODO https://github.com/prestodb/presto/issues/7122 support non-legacy TIMESTAMP
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (isVarcharType(type) || isCharType(type)) {
            // TODO Collect MIN,MAX once it is used by the optimizer
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of();
        }
        // Throwing here to make sure this method is updated when a new type is added in Hive connector
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static OptionalLong getIntegerValue(ConnectorSession session, Type type, Block block)
    {
        // works for BIGINT as well as for other integer types TINYINT/SMALLINT/INTEGER that store values as byte/short/int
        return block.isNull(0) ? OptionalLong.empty() : OptionalLong.of(((Number) type.getObjectValue(session, block, 0)).longValue());
    }

    private static OptionalDouble getDoubleValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? OptionalDouble.empty() : OptionalDouble.of(((Number) type.getObjectValue(session, block, 0)).doubleValue());
    }

    private static Optional<LocalDate> getDateValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? Optional.empty() : Optional.of(LocalDate.ofEpochDay(((SqlDate) type.getObjectValue(session, block, 0)).getDays()));
    }

    private static OptionalLong getTimestampValue(Block block)
    {
        // TODO https://github.com/prestodb/presto/issues/7122
        return block.isNull(0) ? OptionalLong.empty() : OptionalLong.of(MILLISECONDS.toSeconds(block.getLong(0, 0)));
    }

    private static Optional<BigDecimal> getDecimalValue(ConnectorSession session, Type type, Block block)
    {
        return block.isNull(0) ? Optional.empty() : Optional.of(((SqlDecimal) type.getObjectValue(session, block, 0)).toBigDecimal());
    }

    private static void setMinMax(ConnectorSession session, Type type, Block min, Block max, ColumnStatisticsData.Builder builder)
    {
        if (type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT)) {
            OptionalLong minVal = getIntegerValue(session, type, min);
            OptionalLong maxVal = getIntegerValue(session, type, max);
            if (minVal.isPresent() && maxVal.isPresent()) {
                builder.setMin(minVal.getAsLong());
                builder.setMax(maxVal.getAsLong());
            }
        }
        else if (type.equals(DOUBLE) || type.equals(REAL)) {
            OptionalDouble minVal = getDoubleValue(session, type, min);
            OptionalDouble maxVal = getDoubleValue(session, type, max);
            if (minVal.isPresent() && maxVal.isPresent()) {
                builder.setMin(minVal.getAsDouble());
                builder.setMax(maxVal.getAsDouble());
            }
        }
        else if (type.equals(DATE)) {
            Optional<LocalDate> minVal = getDateValue(session, type, min);
            Optional<LocalDate> maxVal = getDateValue(session, type, max);
            if (minVal.isPresent() && maxVal.isPresent()) {
                builder.setMin(minVal.get().toEpochDay());
                builder.setMax(maxVal.get().toEpochDay());
            }
        }
        else if (type.equals(TIMESTAMP)) {
            OptionalLong minVal = getTimestampValue(min);
            OptionalLong maxVal = getTimestampValue(max);
            if (minVal.isPresent() && maxVal.isPresent()) {
                builder.setMin(minVal.getAsLong());
                builder.setMax(maxVal.getAsLong());
            }
        }
        else if (type instanceof DecimalType) {
            Optional<BigDecimal> minVal = getDecimalValue(session, type, min);
            Optional<BigDecimal> maxVal = getDecimalValue(session, type, max);
            if (minVal.isPresent() && maxVal.isPresent()) {
                builder.setMin(minVal.get().doubleValue());
                builder.setMax(maxVal.get().doubleValue());
            }
        }
        else {
            throw new IllegalArgumentException("Unexpected type: " + type);
        }
    }

    private static ColumnStatisticsData fromComputedStatistics(ConnectorSession session, Map<ColumnStatisticType, Block> stats, long rowCount, Type columnType)
    {
        ColumnStatisticsData.Builder columnStatBuilder = ColumnStatisticsData.builder();

        // MIN VALUE, MAX VALUE
        // We ask the engine to compute either both or neither
        verify(stats.containsKey(MIN_VALUE) == stats.containsKey(MAX_VALUE));
        if (stats.containsKey(MIN_VALUE)) {
            setMinMax(session, columnType, stats.get(MIN_VALUE), stats.get(MAX_VALUE), columnStatBuilder);
        }

        // NDV
        if (stats.containsKey(NUMBER_OF_DISTINCT_VALUES)) {
            long numberOfDistinctValues = BIGINT.getLong(stats.get(NUMBER_OF_DISTINCT_VALUES), 0);
            columnStatBuilder.setDistinctValuesCount(numberOfDistinctValues);
        }

        // NUMBER OF NULLS
        if (stats.containsKey(NUMBER_OF_NON_NULL_VALUES)) {
            long nullCount = rowCount - BIGINT.getLong(stats.get(NUMBER_OF_NON_NULL_VALUES), 0);
            columnStatBuilder.setNullsCount(nullCount);
        }

        // TOTAL_VALUES_SIZE_IN_BYTES
        if (stats.containsKey(TOTAL_SIZE_IN_BYTES)) {
            long dataSize = BIGINT.getLong(stats.get(TOTAL_SIZE_IN_BYTES), 0);
            columnStatBuilder.setDataSize(dataSize);
        }

        return columnStatBuilder.build();
    }

    public static TableStatisticsData fromComputedStatistics(ConnectorSession session, Collection<ComputedStatistics> computedStatistics, long rowCount, Map<String, ColumnHandle> columnHandles, TypeManager typeManager)
    {
        TableStatisticsData.Builder tableStatBuilder = TableStatisticsData.builder();
        tableStatBuilder.setRowCount(rowCount);

        // organize the column stats into a per-column view
        Map<String, Map<ColumnStatisticType, Block>> perColumnStats = new HashMap<>();
        for (ComputedStatistics stat : computedStatistics) {
            for (Map.Entry<ColumnStatisticMetadata, Block> entry : stat.getColumnStatistics().entrySet()) {
                perColumnStats.putIfAbsent(entry.getKey().getColumnName(), new HashMap<>());
                perColumnStats.get(entry.getKey().getColumnName()).put(entry.getKey().getStatisticType(), entry.getValue());
            }
        }

        // build the per-column statistics
        for (Map.Entry<String, ColumnHandle> entry : columnHandles.entrySet()) {
            Map<ColumnStatisticType, Block> columnStat = perColumnStats.get(entry.getKey());
            if (columnStat == null) {
                continue;
            }

            MemoryColumnHandle handle = (MemoryColumnHandle) entry.getValue();
            Type columnType = handle.getType(typeManager);
            tableStatBuilder.setColumnStatistics(handle.getColumnName(), fromComputedStatistics(session, columnStat, rowCount, columnType));
        }

        return tableStatBuilder.build();
    }

    private static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) ||
                type.equals(DOUBLE) || type.equals(REAL) ||
                type instanceof DecimalType;
    }
}
