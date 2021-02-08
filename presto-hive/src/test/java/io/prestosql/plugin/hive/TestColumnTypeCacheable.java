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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slices;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.CharType.createCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestColumnTypeCacheable
{
    @Test
    public void testDecimalCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", createDecimalType(4, 2));
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(createDecimalType(4, 2), BigDecimal.valueOf(10.88d).unscaledValue().longValue()))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(createDecimalType(4, 2), BigDecimal.valueOf(20.56d).unscaledValue().longValue()))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "10.88", 2, "22.22", 3, "20.56", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "22.22"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "10.88"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20.56"))).count(), 4);
    }

    @Test
    public void testBooleanCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", BOOLEAN);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BOOLEAN, true))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BOOLEAN, false))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "false", 2, "true", 3)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 6);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "true"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "false"))).count(), 2);
    }

    @Test
    public void testTinyintCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", TINYINT);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(TINYINT, 100L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(TINYINT, 101L))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "102", 2, "100", 3, "101", 2)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 8);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "100"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "101"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "102"))).count(), 0);
    }

    @Test
    public void testSmallintCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", SMALLINT);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(SMALLINT, 20001L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(SMALLINT, 20002L))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "20000", 2, "20001", 3, "20002", 2)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 8);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20001"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20002"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20000"))).count(), 0);
    }

    @Test
    public void testIntegerCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", INTEGER);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(INTEGER, 8001L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(INTEGER, 8002L))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "8001", 2, "8002", 3, "8000", 2)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 8);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "8002"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "8001"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "8000"))).count(), 0);
    }

    @Test
    public void testBigintCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", BIGINT);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BIGINT, 20200522L))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(BIGINT, 20200521L))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "20200520", 2, "20200521", 3, "20200522", 2)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 8);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 5);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200521"))).count(), 3);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200522"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "20200520"))).count(), 0);
    }

    @Test
    public void testRealTypeCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", REAL);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(REAL, (long) Float.floatToRawIntBits(1.0f)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(REAL, (long) Float.floatToRawIntBits(1000.10f)))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "1.0", 2, "2", 3, "1000.10", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1.0"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "2"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1000.10"))).count(), 4);
    }

    @Test
    public void testDoubleTypeCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", DOUBLE);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(DOUBLE, 1.0d))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(DOUBLE, 1000.10d))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "1.0", 2, "2", 3, "1000.10", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1.0"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "2"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1000.10"))).count(), 4);
    }

    @Test
    public void testDateCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", DATE);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(DATE, HiveUtil.parseHiveDate("1995-10-09")))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(DATE, HiveUtil.parseHiveDate("1995-11-14")))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "1995-10-09", 2, "2020-07-22", 3, "1995-11-14", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1995-10-09"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "2020-07-22"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1995-11-14"))).count(), 4);
    }

    @Test
    public void testTimestampCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", TIMESTAMP);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(TIMESTAMP, HiveUtil.parseHiveTimestamp("1995-10-09 00:00:00", new HiveConfig().getDateTimeZone())))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(TIMESTAMP, HiveUtil.parseHiveTimestamp("1995-11-14 00:00:00", new HiveConfig().getDateTimeZone())))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "1995-10-09 00:00:00", 2, "2020-07-22 00:00:00", 3, "1995-11-14 00:00:00", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1995-10-09 00:00:00"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "2020-07-22 00:00:00"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "1995-11-14 00:00:00"))).count(), 4);
    }

    @Test
    public void testVarcharCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", VARCHAR);
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(VARCHAR, Slices.utf8Slice("abc")))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(VARCHAR, Slices.utf8Slice("xyz")))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "abc", 2, "def", 3, "xyz", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "abc"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "def"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "xyz"))).count(), 4);
    }

    @Test
    public void testCharCacheable()
    {
        ConnectorSession session = new TestingConnectorSession(new HiveSessionProperties(new HiveConfig().setDynamicFilterPartitionFilteringEnabled(false), new OrcFileWriterConfig(), new ParquetFileWriterConfig()).getSessionProperties());
        ColumnMetadata ptdMetadata = new ColumnMetadata("pt_d", createCharType(3));
        Set<TupleDomain<ColumnMetadata>> cachePredicates = ImmutableSet.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(createCharType(3), Slices.utf8Slice("abc")))),
                TupleDomain.withColumnDomains(ImmutableMap.of(ptdMetadata, Domain.singleValue(createCharType(3), Slices.utf8Slice("xyz")))));
        HiveSplitSource hiveSplitSource = HiveSplitSource.allAtOnce(
                session,
                "database",
                "table",
                10,
                10000,
                new DataSize(10, MEGABYTE),
                Integer.MAX_VALUE,
                new TestingHiveSplitLoader(),
                Executors.newFixedThreadPool(5),
                new CounterStat(),
                null,
                cachePredicates, null,
                new HiveConfig(),
                HiveStorageFormat.ORC);
        int[] idPrefix = new int[] {1};
        ImmutableMap
                .of("__HIVE_DEFAULT_PARTITION__", 1, "abc", 2, "def", 3, "xyz", 4)
                .forEach((ptdValue, splitCount) -> {
                    for (int i = 1; i <= splitCount; i++) {
                        hiveSplitSource.addToQueue(new TestPartitionSplit(idPrefix[0] * 10 + i, ImmutableList.of(new HivePartitionKey("pt_d", ptdValue)), "pt_d=" + ptdValue));
                    }
                    idPrefix[0] = idPrefix[0] + 1;
                });
        List<ConnectorSplit> splits = getSplits(hiveSplitSource, 10);
        assertEquals(splits.size(), 10);
        assertEquals(splits.stream().filter(ConnectorSplit::isCacheable).count(), 6);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "abc"))).count(), 2);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "def"))).count(), 0);
        assertEquals(splits.stream()
                .filter(ConnectorSplit::isCacheable)
                .map(HiveSplitWrapper::getOnlyHiveSplit)
                .filter(hiveSplit -> hiveSplit
                        .getPartitionKeys()
                        .contains(new HivePartitionKey("pt_d", "xyz"))).count(), 4);
    }

    private static class TestingHiveSplitLoader
            implements HiveSplitLoader
    {
        @Override
        public void start(HiveSplitSource splitSource)
        {
        }

        @Override
        public void stop()
        {
        }
    }

    private static class TestPartitionSplit
            extends InternalHiveSplit
    {
        private TestPartitionSplit(int id, List<HivePartitionKey> partitionKeys, String partitionName)
        {
            this(id, partitionKeys, partitionName, OptionalInt.empty());
        }

        private TestPartitionSplit(int id, List<HivePartitionKey> partitionKeys, String partitionName, OptionalInt bucketNumber)
        {
            super(
                    partitionName,
                    "path",
                    0,
                    100,
                    100,
                    0,
                    properties("id", String.valueOf(id)),
                    partitionKeys,
                    ImmutableList.of(new InternalHiveBlock(0, 100, ImmutableList.of())),
                    bucketNumber,
                    true,
                    false,
                    ImmutableMap.of(),
                    Optional.empty(),
                    false,
                    Optional.empty(),
                    Optional.empty());
        }

        private static Properties properties(String key, String value)
        {
            Properties properties = new Properties();
            properties.put(key, value);
            return properties;
        }
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, int maxSize)
    {
        return getSplits(source, OptionalInt.empty(), maxSize);
    }

    private static List<ConnectorSplit> getSplits(ConnectorSplitSource source, OptionalInt bucketNumber, int maxSize)
    {
        if (bucketNumber.isPresent()) {
            return getFutureValue(source.getNextBatch(new HivePartitionHandle(bucketNumber.getAsInt()), maxSize)).getSplits();
        }
        else {
            return getFutureValue(source.getNextBatch(NOT_PARTITIONED, maxSize)).getSplits();
        }
    }
}
