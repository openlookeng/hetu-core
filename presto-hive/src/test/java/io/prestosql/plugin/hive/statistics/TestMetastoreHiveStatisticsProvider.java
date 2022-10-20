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
package io.prestosql.plugin.hive.statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.plugin.hive.HiveBasicStatistics;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.OrcFileWriterConfig;
import io.prestosql.plugin.hive.ParquetFileWriterConfig;
import io.prestosql.plugin.hive.PartitionStatistics;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.DateStatistics;
import io.prestosql.plugin.hive.metastore.DecimalStatistics;
import io.prestosql.plugin.hive.metastore.DoubleStatistics;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.IntegerStatistics;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.DoubleRange;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HivePartition.UNPARTITIONED_ID;
import static io.prestosql.plugin.hive.HivePartitionManager.parsePartition;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.parsePartitionValue;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.PartitionsRowCount;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateDataSize;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateDataSizeForPartitioningKey;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateDistinctPartitionKeys;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateDistinctValuesCount;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateNullsFraction;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateNullsFractionForPartitioningKey;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculatePartitionsRowCount;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateRange;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.calculateRangeForPartitioningKey;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.convertPartitionValueToDouble;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.createDataColumnStatistics;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.getPartitionsSample;
import static io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider.validatePartitionStatistics;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.Double.NaN;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestMetastoreHiveStatisticsProvider
{
    private static final Storage STORAGE_1 = new Storage(StorageFormat.fromHiveStorageFormat(ORC), "", Optional.empty(), false, ImmutableMap.of());
    private static final SchemaTableName TABLE = new SchemaTableName("schema", "table");
    private static final String PARTITION = "partition";
    private static final String COLUMN = "column";
    private static final DecimalType DECIMAL = createDecimalType(5, 3);

    private static final HiveColumnHandle PARTITION_COLUMN_1 = new HiveColumnHandle("p1", HIVE_STRING, VARCHAR.getTypeSignature(), 0, PARTITION_KEY, Optional.empty());
    private static final HiveColumnHandle PARTITION_COLUMN_2 = new HiveColumnHandle("p2", HIVE_LONG, BIGINT.getTypeSignature(), 1, PARTITION_KEY, Optional.empty());
    private static final Table table = new Table(TABLE.getSchemaName(), TABLE.getTableName(), "user", "MANAGED_TABLE", STORAGE_1, ImmutableList.of(), ImmutableList.of(new Column("p1", HIVE_STRING, Optional.empty()), new Column("p2", HIVE_LONG, Optional.empty())), ImmutableMap.of(), Optional.of("original"), Optional.of("expanded"));

    @Test
    public void testGetPartitionsSample()
    {
        HivePartition p1 = partition("p1=string1/p2=1234");
        HivePartition p2 = partition("p1=string2/p2=2345");
        HivePartition p3 = partition("p1=string3/p2=3456");
        HivePartition p4 = partition("p1=string4/p2=4567");
        HivePartition p5 = partition("p1=string5/p2=5678");

        assertEquals(getPartitionsSample(ImmutableList.of(p1), 1), ImmutableList.of(p1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1), 2), ImmutableList.of(p1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2), 2), ImmutableList.of(p1, p2));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3), 2), ImmutableList.of(p1, p3));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1), getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 1));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3), getPartitionsSample(ImmutableList.of(p1, p2, p3, p4), 3));
        assertEquals(getPartitionsSample(ImmutableList.of(p1, p2, p3, p4, p5), 3), ImmutableList.of(p1, p5, p4));
    }

    @Test
    public void testValidatePartitionStatistics()
    {
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(-1, 0, 0, 0))
                        .build(),
                invalidPartitionStatistics("fileCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, -1, 0, 0))
                        .build(),
                invalidPartitionStatistics("rowCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, -1, 0))
                        .build(),
                invalidPartitionStatistics("inMemoryDataSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, -1))
                        .build(),
                invalidPartitionStatistics("onDiskDataSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setMaxValueSizeInBytes(-1).build()))
                        .build(),
                invalidColumnStatistics("maxValueSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setTotalSizeInBytes(-1).build()))
                        .build(),
                invalidColumnStatistics("totalSizeInBytes must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(-1).build()))
                        .build(),
                invalidColumnStatistics("nullsCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(1).build()))
                        .build(),
                invalidColumnStatistics("nullsCount must be less than or equal to rowCount. nullsCount: 1. rowCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setDistinctValuesCount(-1).build()))
                        .build(),
                invalidColumnStatistics("distinctValuesCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setDistinctValuesCount(1).build()))
                        .build(),
                invalidColumnStatistics("distinctValuesCount must be less than or equal to rowCount. distinctValuesCount: 1. rowCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 1, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setDistinctValuesCount(1).setNullsCount(1).build()))
                        .build(),
                invalidColumnStatistics("distinctValuesCount must be less than or equal to nonNullsCount. distinctValuesCount: 1. nonNullsCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(1), OptionalLong.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("integerStatistics.min must be less than or equal to integerStatistics.max. integerStatistics.min: 1. integerStatistics.max: -1."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(1), OptionalDouble.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("doubleStatistics.min must be less than or equal to doubleStatistics.max. doubleStatistics.min: 1.0. doubleStatistics.max: -1.0."));
        validatePartitionStatistics(
                TABLE,
                ImmutableMap.of(
                        PARTITION,
                        PartitionStatistics.builder()
                                .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                                .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createDoubleColumnStatistics(OptionalDouble.of(NaN), OptionalDouble.of(NaN), OptionalLong.empty(), OptionalLong.empty())))
                                .build()));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createDecimalColumnStatistics(Optional.of(BigDecimal.valueOf(1)), Optional.of(BigDecimal.valueOf(-1)), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("decimalStatistics.min must be less than or equal to decimalStatistics.max. decimalStatistics.min: 1. decimalStatistics.max: -1."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createDateColumnStatistics(Optional.of(LocalDate.ofEpochDay(1)), Optional.of(LocalDate.ofEpochDay(-1)), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("dateStatistics.min must be less than or equal to dateStatistics.max. dateStatistics.min: 1970-01-02. dateStatistics.max: 1969-12-31."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(-1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("trueCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.of(-1), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("falseCount must be greater than or equal to zero: -1"));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.empty(), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("booleanStatistics.trueCount must be less than or equal to rowCount. booleanStatistics.trueCount: 1. rowCount: 0."));
        assertInvalidStatistics(
                PartitionStatistics.builder()
                        .setBasicStatistics(new HiveBasicStatistics(0, 0, 0, 0))
                        .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.of(1), OptionalLong.empty())))
                        .build(),
                invalidColumnStatistics("booleanStatistics.falseCount must be less than or equal to rowCount. booleanStatistics.falseCount: 1. rowCount: 0."));
    }

    @Test
    public void testCalculatePartitionsRowCount()
    {
        assertThat(calculatePartitionsRowCount(ImmutableList.of(), 0)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(PartitionStatistics.empty()), 1)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(PartitionStatistics.empty(), PartitionStatistics.empty()), 2)).isEmpty();
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10)), 1))
                .isEqualTo(Optional.of(new MetastoreHiveStatisticsProvider.PartitionsRowCount(10, 10)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10)), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(10, 20)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), PartitionStatistics.empty()), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(10, 20)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20)), 2))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 30)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20)), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 45)));
        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(20), PartitionStatistics.empty()), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount(15, 45)));

        assertThat(calculatePartitionsRowCount(ImmutableList.of(rowsCount(10), rowsCount(100), rowsCount(1000)), 3))
                .isEqualTo(Optional.of(new PartitionsRowCount((10 + 100 + 1000) / 3.0, 10 + 100 + 1000)));
        // Exclude outliers from average row count
        assertThat(calculatePartitionsRowCount(ImmutableList.<PartitionStatistics>builder()
                        .addAll(nCopies(10, rowsCount(100)))
                        .add(rowsCount(1))
                        .add(rowsCount(1000))
                        .build(),
                50))
                .isEqualTo(Optional.of(new PartitionsRowCount(100, (100 * 48) + 1 + 1000)));
    }

    @Test
    public void testCalculateDistinctPartitionKeys()
    {
        assertEquals(calculateDistinctPartitionKeys(PARTITION_COLUMN_1, ImmutableList.of()), 0);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=string1/p2=1234"))),
                1);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string2/p2=1234"))),
                2);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_2,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string2/p2=1234"))),
                1);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_2,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string1/p2=1235"))),
                2);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=string1/p2=1235"))),
                1);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_2,
                        ImmutableList.of(partition("p1=123/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=1235"))),
                1);
        assertEquals(
                calculateDistinctPartitionKeys(
                        PARTITION_COLUMN_2,
                        ImmutableList.of(partition("p1=123/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"))),
                0);
    }

    @Test
    public void testCalculateNullsFractionForPartitioningKey()
    {
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=string1/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                        2000,
                        0),
                0.0);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=string1/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                        2000,
                        4000),
                0.0);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                        ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000)),
                        2000,
                        4000),
                0.25);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                        ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", PartitionStatistics.empty()),
                        2000,
                        4000),
                0.5);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234")),
                        ImmutableMap.of(),
                        2000,
                        4000),
                0.5);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                        ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", rowsCount(2000)),
                        3000,
                        4000),
                0.75);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                        ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", PartitionStatistics.empty()),
                        3000,
                        4000),
                1.0);
        assertEquals(
                calculateNullsFractionForPartitioningKey(
                        PARTITION_COLUMN_1,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=__HIVE_DEFAULT_PARTITION__/p2=4321")),
                        ImmutableMap.of("p1=__HIVE_DEFAULT_PARTITION__/p2=1234", rowsCount(1000), "p1=__HIVE_DEFAULT_PARTITION__/p2=4321", PartitionStatistics.empty()),
                        4000,
                        4000),
                1.0);
    }

    @Test
    public void testCalculateDataSizeForPartitioningKey()
    {
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_2,
                        BIGINT,
                        ImmutableList.of(partition("p1=string1/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                        2000),
                Estimate.unknown());
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000)),
                        2000),
                Estimate.of(7000));
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", PartitionStatistics.empty()),
                        2000),
                Estimate.of(14000));
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000), "p1=str2/p2=1234", rowsCount(2000)),
                        3000),
                Estimate.of(15000));
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                        ImmutableMap.of("p1=string1/p2=1234", rowsCount(1000), "p1=str2/p2=1234", PartitionStatistics.empty()),
                        3000),
                Estimate.of(19000));
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=str2/p2=1234")),
                        ImmutableMap.of(),
                        3000),
                Estimate.of(33000));
        assertEquals(
                calculateDataSizeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=__HIVE_DEFAULT_PARTITION__/p2=1234"), partition("p1=str2/p2=1234")),
                        ImmutableMap.of(),
                        3000),
                Estimate.of(12000));
    }

    @Test
    public void testCalculateRangeForPartitioningKey()
    {
        assertEquals(
                calculateRangeForPartitioningKey(
                        PARTITION_COLUMN_1,
                        VARCHAR,
                        ImmutableList.of(partition("p1=string1/p2=1234"))),
                Optional.empty());
        assertEquals(
                calculateRangeForPartitioningKey(
                        PARTITION_COLUMN_2,
                        BIGINT,
                        ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"))),
                Optional.empty());
        assertEquals(
                calculateRangeForPartitioningKey(
                        PARTITION_COLUMN_2,
                        BIGINT,
                        ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"))),
                Optional.empty());
        assertEquals(
                calculateRangeForPartitioningKey(
                        PARTITION_COLUMN_2,
                        BIGINT,
                        ImmutableList.of(partition("p1=string1/p2=__HIVE_DEFAULT_PARTITION__"), partition("p1=string1/p2=1"))),
                Optional.of(new DoubleRange(1, 1)));
        assertEquals(
                calculateRangeForPartitioningKey(
                        PARTITION_COLUMN_2,
                        BIGINT,
                        ImmutableList.of(partition("p1=string1/p2=2"), partition("p1=string1/p2=1"))),
                Optional.of(new DoubleRange(1, 2)));
    }

    @Test
    public void testConvertPartitionValueToDouble()
    {
        assertConvertPartitionValueToDouble(BIGINT, "123456", 123456);
        assertConvertPartitionValueToDouble(INTEGER, "12345", 12345);
        assertConvertPartitionValueToDouble(SMALLINT, "1234", 1234);
        assertConvertPartitionValueToDouble(TINYINT, "123", 123);
        assertConvertPartitionValueToDouble(DOUBLE, "0.1", 0.1);
        assertConvertPartitionValueToDouble(REAL, "0.2", (double) (float) 0.2);
        assertConvertPartitionValueToDouble(createDecimalType(5, 2), "123.45", 123.45);
        assertConvertPartitionValueToDouble(createDecimalType(25, 5), "12345678901234567890.12345", 12345678901234567890.12345);
        assertConvertPartitionValueToDouble(DATE, "1970-01-02", 1);
    }

    private static void assertConvertPartitionValueToDouble(Type type, String value, double expected)
    {
        Object prestoValue = parsePartitionValue(format("p=%s", value), value, type).getValue();
        assertEquals(convertPartitionValueToDouble(type, prestoValue), expected);
    }

    @Test
    public void testCreateDataColumnStatistics()
    {
        assertEquals(createDataColumnStatistics(COLUMN, BIGINT, 1000, ImmutableList.of()), ColumnStatistics.empty());
        assertEquals(
                createDataColumnStatistics(COLUMN, BIGINT, 1000, ImmutableList.of(PartitionStatistics.empty(), PartitionStatistics.empty())),
                ColumnStatistics.empty());
        assertEquals(
                createDataColumnStatistics(
                        COLUMN,
                        BIGINT,
                        1000,
                        ImmutableList.of(new PartitionStatistics(HiveBasicStatistics.createZeroStatistics(), ImmutableMap.of("column2", HiveColumnStatistics.empty())))),
                ColumnStatistics.empty());
    }

    @Test
    public void testCalculateDistinctValuesCount()
    {
        assertEquals(calculateDistinctValuesCount(ImmutableList.of()), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.empty())), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.empty(), HiveColumnStatistics.empty())), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(distinctValuesCount(1))), Estimate.of(1));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(distinctValuesCount(1), distinctValuesCount(2))), Estimate.of(2));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(distinctValuesCount(1), HiveColumnStatistics.empty())), Estimate.of(1));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty()))), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(0), OptionalLong.empty()))), Estimate.of(1));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(10), OptionalLong.empty(), OptionalLong.empty()))), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(10), OptionalLong.of(10), OptionalLong.empty()))), Estimate.of(2));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.empty(), OptionalLong.of(10), OptionalLong.empty()))), Estimate.unknown());
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(0), OptionalLong.of(10), OptionalLong.empty()))), Estimate.of(1));
        assertEquals(calculateDistinctValuesCount(ImmutableList.of(HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(0), OptionalLong.of(0), OptionalLong.empty()))), Estimate.of(0));
        assertEquals(
                calculateDistinctValuesCount(ImmutableList.of(
                        HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(0), OptionalLong.of(10), OptionalLong.empty()),
                        HiveColumnStatistics.createBooleanColumnStatistics(OptionalLong.of(1), OptionalLong.of(10), OptionalLong.empty()))),
                Estimate.of(2));
    }

    @Test
    public void testCalculateNullsFraction()
    {
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of()), Estimate.unknown());
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(PartitionStatistics.empty())), Estimate.unknown());
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000))), Estimate.unknown());
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000), nullsCount(500))), Estimate.unknown());
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCount(1000), nullsCount(500), rowsCountAndNullsCount(1000, 500))), Estimate.of(0.5));
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCountAndNullsCount(2000, 200), rowsCountAndNullsCount(1000, 100))), Estimate.of(0.1));
        assertEquals(calculateNullsFraction(COLUMN, ImmutableList.of(rowsCountAndNullsCount(0, 0), rowsCountAndNullsCount(0, 0))), Estimate.of(0));
    }

    @Test
    public void testCalculateDataSize()
    {
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(), 0), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(), 1000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(PartitionStatistics.empty()), 1000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(rowsCount(1000)), 1000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(dataSize(1000)), 1000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(dataSize(1000), rowsCount(1000)), 1000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(500, 1000)), 2000), Estimate.of(4000));
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(0, 0)), 2000), Estimate.unknown());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(0, 0)), 0), Estimate.zero());
        assertEquals(calculateDataSize(COLUMN, ImmutableList.of(rowsCountAndDataSize(1000, 0)), 2000), Estimate.of(0));
        assertEquals(
                calculateDataSize(
                        COLUMN,
                        ImmutableList.of(
                                rowsCountAndDataSize(500, 1000),
                                rowsCountAndDataSize(1000, 5000)),
                        5000),
                Estimate.of(20000));
        assertEquals(
                calculateDataSize(
                        COLUMN,
                        ImmutableList.of(
                                dataSize(1000),
                                rowsCountAndDataSize(500, 1000),
                                rowsCount(3000),
                                rowsCountAndDataSize(1000, 5000)),
                        5000),
                Estimate.of(20000));
    }

    @Test
    public void testCalculateRange()
    {
        assertEquals(calculateRange(VARCHAR, ImmutableList.of()), Optional.empty());
        assertEquals(calculateRange(VARCHAR, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.empty()))), Optional.empty());
        assertEquals(calculateRange(VARCHAR, ImmutableList.of(integerRange(1, 2))), Optional.empty());
        assertEquals(calculateRange(BIGINT, ImmutableList.of(integerRange(1, 2))), Optional.of(new DoubleRange(1, 2)));
        assertEquals(calculateRange(BIGINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE))), Optional.of(new DoubleRange(Long.MIN_VALUE, Long.MAX_VALUE)));
        assertEquals(calculateRange(INTEGER, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE))), Optional.of(new DoubleRange(Integer.MIN_VALUE, Integer.MAX_VALUE)));
        assertEquals(calculateRange(SMALLINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE))), Optional.of(new DoubleRange(Short.MIN_VALUE, Short.MAX_VALUE)));
        assertEquals(calculateRange(TINYINT, ImmutableList.of(integerRange(Long.MIN_VALUE, Long.MAX_VALUE))), Optional.of(new DoubleRange(Byte.MIN_VALUE, Byte.MAX_VALUE)));
        assertEquals(calculateRange(BIGINT, ImmutableList.of(integerRange(1, 5), integerRange(3, 7))), Optional.of(new DoubleRange(1, 7)));
        assertEquals(calculateRange(BIGINT, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.empty()), integerRange(3, 7))), Optional.of(new DoubleRange(3, 7)));
        assertEquals(calculateRange(BIGINT, ImmutableList.of(integerRange(OptionalLong.empty(), OptionalLong.of(8)), integerRange(3, 7))), Optional.of(new DoubleRange(3, 7)));
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(integerRange(1, 2))), Optional.empty());
        assertEquals(calculateRange(REAL, ImmutableList.of(integerRange(1, 2))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(OptionalDouble.empty(), OptionalDouble.empty()))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, 0.2))), Optional.of(new DoubleRange(0.1, 0.2)));
        assertEquals(calculateRange(BIGINT, ImmutableList.of(doubleRange(0.1, 0.2))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, 0.2), doubleRange(0.15, 0.25))), Optional.of(new DoubleRange(0.1, 0.25)));
        assertEquals(calculateRange(REAL, ImmutableList.of(doubleRange(0.1, 0.2), doubleRange(0.15, 0.25))), Optional.of(new DoubleRange(0.1, 0.25)));
        assertEquals(calculateRange(REAL, ImmutableList.of(doubleRange(OptionalDouble.empty(), OptionalDouble.of(0.2)), doubleRange(0.15, 0.25))), Optional.of(new DoubleRange(0.15, 0.25)));
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(NaN, 0.2))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(0.1, NaN))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(NaN, NaN))), Optional.empty());
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))), Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertEquals(calculateRange(REAL, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))), Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Float.NEGATIVE_INFINITY, Float.POSITIVE_INFINITY))), Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertEquals(calculateRange(DOUBLE, ImmutableList.of(doubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY), doubleRange(0.1, 0.2))), Optional.of(new DoubleRange(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)));
        assertEquals(calculateRange(DATE, ImmutableList.of(doubleRange(0.1, 0.2))), Optional.empty());
        assertEquals(calculateRange(DATE, ImmutableList.of(dateRange("1970-01-01", "1970-01-02"))), Optional.of(new DoubleRange(0, 1)));
        assertEquals(calculateRange(DATE, ImmutableList.of(dateRange(Optional.empty(), Optional.empty()))), Optional.empty());
        assertEquals(calculateRange(DATE, ImmutableList.of(dateRange(Optional.of("1970-01-01"), Optional.empty()))), Optional.empty());
        assertEquals(calculateRange(DATE, ImmutableList.of(dateRange("1970-01-01", "1970-01-05"), dateRange("1970-01-03", "1970-01-07"))), Optional.of(new DoubleRange(0, 6)));
        assertEquals(calculateRange(DECIMAL, ImmutableList.of(doubleRange(0.1, 0.2))), Optional.empty());
        assertEquals(calculateRange(DECIMAL, ImmutableList.of(decimalRange(BigDecimal.valueOf(1), BigDecimal.valueOf(5)))), Optional.of(new DoubleRange(1, 5)));
        assertEquals(calculateRange(DECIMAL, ImmutableList.of(decimalRange(Optional.empty(), Optional.empty()))), Optional.empty());
        assertEquals(calculateRange(DECIMAL, ImmutableList.of(decimalRange(Optional.of(BigDecimal.valueOf(1)), Optional.empty()))), Optional.empty());
        assertEquals(calculateRange(DECIMAL, ImmutableList.of(decimalRange(BigDecimal.valueOf(1), BigDecimal.valueOf(5)), decimalRange(BigDecimal.valueOf(3), BigDecimal.valueOf(7)))), Optional.of(new DoubleRange(1, 7)));
    }

    @Test
    public void testGetTableStatistics()
    {
        String partitionName = "p1=string1/p2=1234";
        PartitionStatistics statistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(1000), OptionalLong.empty(), OptionalLong.empty()))
                .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(-100), OptionalLong.of(100), OptionalLong.of(500), OptionalLong.of(300))))
                .build();
        MetastoreHiveStatisticsProvider statisticsProvider = new MetastoreHiveStatisticsProvider((session, schemaTableName, hivePartitions, table) -> ImmutableMap.of(partitionName, statistics));
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        HiveColumnHandle columnHandle = new HiveColumnHandle(COLUMN, HIVE_LONG, BIGINT.getTypeSignature(), 2, REGULAR, Optional.empty());
        TableStatistics expected = TableStatistics.builder()
                .setRowCount(Estimate.of(1000))
                .setColumnStatistics(
                        PARTITION_COLUMN_1,
                        ColumnStatistics.builder()
                                .setDataSize(Estimate.of(7000))
                                .setNullsFraction(Estimate.of(0))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .setColumnStatistics(
                        PARTITION_COLUMN_2,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(1234, 1234))
                                .setNullsFraction(Estimate.of(0))
                                .setDistinctValuesCount(Estimate.of(1))
                                .build())
                .setColumnStatistics(
                        columnHandle,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(-100, 100))
                                .setNullsFraction(Estimate.of(0.5))
                                .setDistinctValuesCount(Estimate.of(300))
                                .build())
                .build();
        assertEquals(
                statisticsProvider.getTableStatistics(
                        session,
                        TABLE,
                        ImmutableMap.of(
                                "p1", PARTITION_COLUMN_1,
                                "p2", PARTITION_COLUMN_2,
                                COLUMN, columnHandle),
                        ImmutableMap.of(
                                "p1", VARCHAR,
                                "p2", BIGINT,
                                COLUMN, BIGINT),
                        ImmutableList.of(partition(partitionName)), true, table),
                expected);
    }

    @Test
    public void testGetTableStatisticsUnpartitioned()
    {
        PartitionStatistics statistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(1000), OptionalLong.empty(), OptionalLong.empty()))
                .setColumnStatistics(ImmutableMap.of(COLUMN, HiveColumnStatistics.createIntegerColumnStatistics(OptionalLong.of(-100), OptionalLong.of(100), OptionalLong.of(500), OptionalLong.of(300))))
                .build();
        MetastoreHiveStatisticsProvider statisticsProvider = new MetastoreHiveStatisticsProvider((session, schemaTableName, hivePartitions, table) -> ImmutableMap.of(UNPARTITIONED_ID, statistics));
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        HiveColumnHandle columnHandle = new HiveColumnHandle(COLUMN, HIVE_LONG, BIGINT.getTypeSignature(), 2, REGULAR, Optional.empty());
        TableStatistics expected = TableStatistics.builder()
                .setRowCount(Estimate.of(1000))
                .setColumnStatistics(
                        columnHandle,
                        ColumnStatistics.builder()
                                .setRange(new DoubleRange(-100, 100))
                                .setNullsFraction(Estimate.of(0.5))
                                .setDistinctValuesCount(Estimate.of(300))
                                .build())
                .build();
        assertEquals(
                statisticsProvider.getTableStatistics(
                        session,
                        TABLE,
                        ImmutableMap.of(COLUMN, columnHandle),
                        ImmutableMap.of(COLUMN, BIGINT),
                        ImmutableList.of(new HivePartition(TABLE)), true, table),
                expected);
    }

    @Test
    public void testGetTableStatisticsEmpty()
    {
        String partitionName = "p1=string1/p2=1234";
        MetastoreHiveStatisticsProvider statisticsProvider = new MetastoreHiveStatisticsProvider((session, schemaTableName, hivePartitions, table) -> ImmutableMap.of(partitionName, PartitionStatistics.empty()));
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig(), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        assertEquals(
                statisticsProvider.getTableStatistics(
                        session,
                        TABLE,
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableList.of(partition(partitionName)), true, table),
                TableStatistics.empty());
    }

    @Test
    public void testGetTableStatisticsSampling()
    {
        MetastoreHiveStatisticsProvider statisticsProvider = new MetastoreHiveStatisticsProvider((session, schemaTableName, hivePartitions, table) -> {
            assertEquals(schemaTableName, TABLE);
            assertEquals(hivePartitions.size(), 1);
            return ImmutableMap.of();
        });
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(
                new HiveConfig().setPartitionStatisticsSampleSize(1),
                new OrcFileWriterConfig(),
                new ParquetFileWriterConfig())
                .getSessionProperties());
        statisticsProvider.getTableStatistics(
                session,
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition("p1=string1/p2=1234"), partition("p1=string1/p2=1235")), true, table);
    }

    @Test
    public void testGetTableStatisticsValidationFailure()
    {
        PartitionStatistics corruptedStatistics = PartitionStatistics.builder()
                .setBasicStatistics(new HiveBasicStatistics(-1, 0, 0, 0))
                .build();
        String partitionName = "p1=string1/p2=1234";
        MetastoreHiveStatisticsProvider statisticsProvider = new MetastoreHiveStatisticsProvider((session, schemaTableName, hivePartitions, table) -> ImmutableMap.of(partitionName, corruptedStatistics));
        TestingConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(
                new HiveConfig().setIgnoreCorruptedStatistics(false),
                new OrcFileWriterConfig(),
                new ParquetFileWriterConfig())
                .getSessionProperties());
        assertThatThrownBy(() -> statisticsProvider.getTableStatistics(
                session,
                TABLE,
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableList.of(partition(partitionName)), true, table))
                .isInstanceOf(PrestoException.class)
                .hasFieldOrPropertyWithValue("errorCode", HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode());
        TestingConnectorSession ignoreSession = new TestingConnectorSession(new HiveSessionProperties(
                new HiveConfig().setIgnoreCorruptedStatistics(true),
                new OrcFileWriterConfig(),
                new ParquetFileWriterConfig())
                .getSessionProperties());
        assertEquals(
                statisticsProvider.getTableStatistics(
                        ignoreSession,
                        TABLE,
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        ImmutableList.of(partition(partitionName)), true, table),
                TableStatistics.empty());
    }

    private static void assertInvalidStatistics(PartitionStatistics partitionStatistics, String expectedMessage)
    {
        assertThatThrownBy(() -> validatePartitionStatistics(TABLE, ImmutableMap.of(PARTITION, partitionStatistics)))
                .isInstanceOf(PrestoException.class)
                .hasFieldOrPropertyWithValue("errorCode", HiveErrorCode.HIVE_CORRUPTED_COLUMN_STATISTICS.toErrorCode())
                .hasMessage(expectedMessage);
    }

    private static String invalidPartitionStatistics(String message)
    {
        return format("Corrupted partition statistics (Table: %s Partition: [%s]): %s", TABLE, PARTITION, message);
    }

    private static String invalidColumnStatistics(String message)
    {
        return format("Corrupted partition statistics (Table: %s Partition: [%s] Column: %s): %s", TABLE, PARTITION, COLUMN, message);
    }

    private static HivePartition partition(String name)
    {
        return parsePartition(TABLE, name, ImmutableList.of(PARTITION_COLUMN_1, PARTITION_COLUMN_2), ImmutableList.of(VARCHAR, BIGINT));
    }

    private static PartitionStatistics rowsCount(long rowsCount)
    {
        return new PartitionStatistics(new HiveBasicStatistics(0, rowsCount, 0, 0), ImmutableMap.of());
    }

    private static PartitionStatistics nullsCount(long nullsCount)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(nullsCount).build()));
    }

    private static PartitionStatistics dataSize(long dataSize)
    {
        return new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setTotalSizeInBytes(dataSize).build()));
    }

    private static PartitionStatistics rowsCountAndNullsCount(long rowsCount, long nullsCount)
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(0, rowsCount, 0, 0),
                ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setNullsCount(nullsCount).build()));
    }

    private static PartitionStatistics rowsCountAndDataSize(long rowsCount, long dataSize)
    {
        return new PartitionStatistics(
                new HiveBasicStatistics(0, rowsCount, 0, 0),
                ImmutableMap.of(COLUMN, HiveColumnStatistics.builder().setTotalSizeInBytes(dataSize).build()));
    }

    private static HiveColumnStatistics distinctValuesCount(long count)
    {
        return HiveColumnStatistics.builder()
                .setDistinctValuesCount(count)
                .build();
    }

    private static HiveColumnStatistics integerRange(long min, long max)
    {
        return integerRange(OptionalLong.of(min), OptionalLong.of(max));
    }

    private static HiveColumnStatistics integerRange(OptionalLong min, OptionalLong max)
    {
        return HiveColumnStatistics.builder()
                .setIntegerStatistics(new IntegerStatistics(min, max))
                .build();
    }

    private static HiveColumnStatistics doubleRange(double min, double max)
    {
        return doubleRange(OptionalDouble.of(min), OptionalDouble.of(max));
    }

    private static HiveColumnStatistics doubleRange(OptionalDouble min, OptionalDouble max)
    {
        return HiveColumnStatistics.builder()
                .setDoubleStatistics(new DoubleStatistics(min, max))
                .build();
    }

    private static HiveColumnStatistics dateRange(String min, String max)
    {
        return dateRange(Optional.of(min), Optional.of(max));
    }

    private static HiveColumnStatistics dateRange(Optional<String> min, Optional<String> max)
    {
        return HiveColumnStatistics.builder()
                .setDateStatistics(new DateStatistics(min.map(TestMetastoreHiveStatisticsProvider::parseDate), max.map(TestMetastoreHiveStatisticsProvider::parseDate)))
                .build();
    }

    private static LocalDate parseDate(String date)
    {
        return LocalDate.parse(date);
    }

    private static HiveColumnStatistics decimalRange(BigDecimal min, BigDecimal max)
    {
        return decimalRange(Optional.of(min), Optional.of(max));
    }

    private static HiveColumnStatistics decimalRange(Optional<BigDecimal> min, Optional<BigDecimal> max)
    {
        return HiveColumnStatistics.builder()
                .setDecimalStatistics(new DecimalStatistics(min, max))
                .build();
    }
}
