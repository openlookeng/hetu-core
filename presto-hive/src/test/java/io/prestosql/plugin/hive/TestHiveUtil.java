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

import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.TestingTypeManager;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.util.BloomFilter;
import it.unimi.dsi.fastutil.ints.IntArrayList;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.PARTITION_KEY;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.filterRows;
import static io.prestosql.plugin.hive.HiveUtil.getDeserializer;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.HiveUtil.parseHiveTimestamp;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, nonDefaultTimeZone());
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
    public void testIsPartitionFiltered()
    {
        TypeManager typeManager = new TestingTypeManager();
        assertFalse(isPartitionFiltered(null, null, typeManager), "Should not filter partition if either partitions or dynamicFilters is null");

        Set<DynamicFilter> dynamicFilters = new HashSet<>();
        List<HivePartitionKey> partitions = new ArrayList<>();

        assertFalse(isPartitionFiltered(partitions, null, typeManager), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(null, dynamicFilters, typeManager), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should not filter partition if partitions and dynamicFilters are empty");

        partitions.add(new HivePartitionKey("pt_d", "0"));
        partitions.add(new HivePartitionKey("app_id", "10000"));
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should not filter partition if dynamicFilters is empty");

        ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_LONG, parseTypeSignature(BIGINT), 0, PARTITION_KEY, Optional.empty());
        BloomFilter dayFilter = new BloomFilter(1024 * 1024, 0.01);
        dynamicFilters.add(new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));
        assertTrue(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should filter partition if any dynamicFilter has 0 element count");

        dayFilter.add(1L);
        assertTrue(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should filter partition if partition value not in dynamicFilter");

        dayFilter.add(0L);
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should not filter partition if partition value is in dynamicFilter");
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
        //BloomFilter nameFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1024 * 1024, 0.01);
        Set nameFilter = new HashSet();
        nameFilter.add("Alice");
        dynamicFilters.add(new HashSetDynamicFilter("1", nameColumn, nameFilter, DynamicFilter.Type.GLOBAL));
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, typeManager), "Should not filter partition if dynamicFilter is on non-partition column");
    }

    @Test
    public void testFilterRows()
    {
        final int numValues = 1024;
        BlockBuilder builder = new LongArrayBlockBuilder(null, numValues);
        for (int i = 0; i < numValues; i++) {
            builder.writeLong(i);
        }
        Page page = new Page(builder.build());

        Map<Integer, DynamicFilter> dynamicFilters = new HashMap<>();
        ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_INT, parseTypeSignature(INTEGER), 0, REGULAR, Optional.empty());
        BloomFilter dayFilter = new BloomFilter(1024 * 1024, 0.01);
        dayFilter.add("1024".getBytes());
        dynamicFilters.put(0, new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));

        IntArrayList rowsToKeep = filterRows(page, dynamicFilters, new Type[] {BigintType.BIGINT});

        assertEquals(rowsToKeep.size(), 0);
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
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time), nonDefaultTimeZone());
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
