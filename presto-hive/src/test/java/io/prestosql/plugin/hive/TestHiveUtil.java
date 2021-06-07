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
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.util.BloomFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getDeserializer;
import static io.prestosql.plugin.hive.HiveUtil.getPartitionKeyColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.getRegularColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.HiveUtil.parseHiveTimestamp;
import static io.prestosql.plugin.hive.HiveUtil.shouldUseRecordReaderFromInputFormat;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_OUTPUT_FORMAT;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_SERDE;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveUtil
{
    private static final Storage STORAGE_1 = new Storage(StorageFormat.fromHiveStorageFormat(ORC), "", Optional.empty(), false, ImmutableMap.of());
    private static final Table TABLE_1 = new Table("schema",
            "table",
            "user",
            "MANAGED_TABLE",
            STORAGE_1,
            ImmutableList.of(new Column("col_1", HiveType.HIVE_INT, Optional.empty()), new Column("col_2", HiveType.HIVE_INT, Optional.empty()), new Column("col_3", HiveType.HIVE_INT, Optional.empty())),
            ImmutableList.of(new Column("part_col_1", HIVE_STRING, Optional.empty())),
            ImmutableMap.of(),
            Optional.of("original"),
            Optional.of("expanded"));
    private static final HiveBucketProperty HIVE_BUCKET_PROPERTY = new HiveBucketProperty(ImmutableList.of("col_3"), HiveBucketing.BucketingVersion.BUCKETING_V2, 2, ImmutableList.of());
    private static final Storage STORAGE_2 = new Storage(StorageFormat.fromHiveStorageFormat(ORC), "", Optional.of(HIVE_BUCKET_PROPERTY), false, ImmutableMap.of());
    private static final Table TABLE_2 = new Table("schema",
            "table",
            "user",
            "MANAGED_TABLE",
            STORAGE_2,
            ImmutableList.of(new Column("col_1", HiveType.HIVE_INT, Optional.empty()), new Column("col_2", HiveType.HIVE_INT, Optional.empty()), new Column("col_3", HiveType.HIVE_INT, Optional.empty())),
            ImmutableList.of(new Column("part_col_1", HIVE_STRING, Optional.empty())),
            ImmutableMap.of(),
            Optional.of("original"),
            Optional.of("expanded"));

    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, DateTimeZone.UTC);
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss"), unixTime(time, 0));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.S"), unixTime(time, 1));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSS"), unixTime(time, 3));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS"), unixTime(time, 6));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), unixTime(time, 7));
    }

    @Test
    public void testGetThriftDeserializer()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ThriftDeserializer.class.getName());
        schema.setProperty(SERIALIZATION_CLASS, IntString.class.getName());
        schema.setProperty(SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());

        assertInstanceOf(getDeserializer(new Configuration(false), schema), ThriftDeserializer.class);
    }

    @Test
    public void testToPartitionValues()
            throws MetaException
    {
        assertToPartitionValues("ds=2015-12-30/event_type=QueryCompletion");
        assertToPartitionValues("ds=2015-12-30");
        assertToPartitionValues("a=1/b=2/c=3");
        assertToPartitionValues("a=1");
        assertToPartitionValues("pk=!@%23$%25%5E&%2A()%2F%3D");
        assertToPartitionValues("pk=__HIVE_DEFAULT_PARTITION__");
    }

    @Test
    public void testShouldUseRecordReaderFromInputFormat()
    {
        Properties schema = new Properties();
        schema.setProperty(FILE_INPUT_FORMAT, "org.apache.hudi.hadoop.HoodieParquetInputFormat");
        schema.setProperty(META_TABLE_SERDE, "parquet.hive.serde.ParquetHiveSerDe");
        schema.setProperty(FILE_OUTPUT_FORMAT, "");
        assertTrue(shouldUseRecordReaderFromInputFormat(new Configuration(), schema));

        schema.setProperty(FILE_INPUT_FORMAT, "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat");
        schema.setProperty(META_TABLE_SERDE, "parquet.hive.serde.ParquetHiveSerDe");
        schema.setProperty(FILE_OUTPUT_FORMAT, "");
        assertTrue(shouldUseRecordReaderFromInputFormat(new Configuration(), schema));
    }

    @Test
    public void testIsPartitionFiltered()
    {
        TypeManager typeManager = new TestingTypeManager();
        assertFalse(isPartitionFiltered(null, null, typeManager), "Should not filter partition if either partitions or dynamicFilters is null");

        Set<DynamicFilter> dynamicFilters = new HashSet<>();
        List<HivePartitionKey> partitions = new ArrayList<>();

        assertFalse(isPartitionFiltered(partitions, null, typeManager), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(null, ImmutableList.of(dynamicFilters), typeManager), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should not filter partition if partitions and dynamicFilters are empty");

        partitions.add(new HivePartitionKey("pt_d", "0"));
        partitions.add(new HivePartitionKey("app_id", "10000"));
        assertFalse(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should not filter partition if dynamicFilters is empty");

        ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_LONG, parseTypeSignature(BIGINT), 0, PARTITION_KEY, Optional.empty());
        BloomFilter dayFilter = new BloomFilter(1024 * 1024, 0.01);
        dynamicFilters.add(new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));
        assertTrue(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should filter partition if any dynamicFilter has 0 element count");

        dayFilter.add(1L);
        assertTrue(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should filter partition if partition value not in dynamicFilter");

        dayFilter.add(0L);
        assertFalse(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should not filter partition if partition value is in dynamicFilter");

        Set<DynamicFilter> dynamicFilters1 = new HashSet<>();
        BloomFilter dayFilter1 = new BloomFilter(1024 * 1024, 0.01);
        dynamicFilters1.add(new BloomFilterDynamicFilter("1", dayColumn, dayFilter1, DynamicFilter.Type.GLOBAL));
        dayFilter1.add(0L);
        assertFalse(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters1), typeManager), "Should not filter partition if partition value is in dynamicFilter");
    }

    @Test
    public void testIsPartitionFilteredWithNonPartitionFilter()
    {
        TypeManager typeManager = new TestingTypeManager();
        Set<DynamicFilter> dynamicFilters = new HashSet<>();
        List<HivePartitionKey> partitions = new ArrayList<>();

        partitions.add(new HivePartitionKey("pt_d", "0"));
        partitions.add(new HivePartitionKey("app_id", "10000"));

        ColumnHandle nameColumn = new HiveColumnHandle("name", HIVE_STRING, parseTypeSignature(VARCHAR), 0, REGULAR, Optional.empty());
        Set nameFilter = new HashSet();
        nameFilter.add("Alice");
        dynamicFilters.add(new HashSetDynamicFilter("1", nameColumn, nameFilter, DynamicFilter.Type.GLOBAL));
        assertFalse(isPartitionFiltered(partitions, ImmutableList.of(dynamicFilters), typeManager), "Should not filter partition if dynamicFilter is on non-partition column");
    }

    @Test
    public void testGetRegularColumnHandles()
    {
        List<HiveColumnHandle> regularColumns = getRegularColumnHandles(TABLE_1);
        assertEquals(regularColumns.get(0).isRequired(), false);
        assertEquals(regularColumns.get(1).isRequired(), false);
        assertEquals(regularColumns.get(2).isRequired(), false);
        List<HiveColumnHandle> bucketedRegularColumns = getRegularColumnHandles(TABLE_2);
        assertEquals(bucketedRegularColumns.get(0).isRequired(), false);
        assertEquals(bucketedRegularColumns.get(1).isRequired(), false);
        assertEquals(bucketedRegularColumns.get(2).isRequired(), true);
    }

    @Test
    public void testGetPartitionKeyColumnHandles()
    {
        List<HiveColumnHandle> partitionColumns = getPartitionKeyColumnHandles(TABLE_1);
        assertEquals(partitionColumns.get(0).isRequired(), true);
    }

    private static void assertToPartitionValues(String partitionName)
            throws MetaException
    {
        List<String> actual = toPartitionValues(partitionName);
        AbstractList<String> expected = new ArrayList<>();
        for (String s : actual) {
            expected.add(null);
        }
        Warehouse.makeValsFromName(partitionName, expected);
        assertEquals(actual, expected);
    }

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time));
    }

    private static long unixTime(DateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.getMillis() / factor) * factor;
    }

    static DateTimeZone nonDefaultTimeZone()
    {
        String defaultId = DateTimeZone.getDefault().getID();
        for (String id : DateTimeZone.getAvailableIDs()) {
            if (!id.equals(defaultId)) {
                DateTimeZone zone = DateTimeZone.forID(id);
                if (zone.getStandardOffset(0) != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }
}
