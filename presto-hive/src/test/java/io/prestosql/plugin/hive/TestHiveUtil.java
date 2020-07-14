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
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.HashSetDynamicFilter;
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

import java.nio.charset.Charset;
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
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_STRING;
import static io.prestosql.plugin.hive.HiveUtil.getDeserializer;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.HiveUtil.parseHiveTimestamp;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
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
        assertFalse(isPartitionFiltered(null, null, null), "Should not filter partition if either partitions or dynamicFilters is null");

        Set<DynamicFilter> dynamicFilters = new HashSet<>();
        List<HivePartitionKey> partitions = new ArrayList<>();

        assertFalse(isPartitionFiltered(partitions, null, null), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(null, dynamicFilters, null), "Should not filter partition if either partitions or dynamicFilters is null");
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, null), "Should not filter partition if partitions and dynamicFilters are empty");

        partitions.add(new HivePartitionKey("pt_d", "0"));
        partitions.add(new HivePartitionKey("app_id", "10000"));
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, null), "Should not filter partition if dynamicFilters is empty");

        ColumnHandle dayColumn = new HiveColumnHandle("pt_d", HIVE_INT, parseTypeSignature(INTEGER), 0, PARTITION_KEY, Optional.empty());
        BloomFilter dayFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1024 * 1024, 0.01);
        dynamicFilters.add(new BloomFilterDynamicFilter("1", dayColumn, dayFilter, DynamicFilter.Type.GLOBAL));
        assertTrue(isPartitionFiltered(partitions, dynamicFilters, null), "Should filter partition if any dynamicFilter has 0 element count");
        assertTrue(isPartitionFiltered(ImmutableList.of(), dynamicFilters, null), "Should filter partition if any dynamicFilter has 0 element count");

        dayFilter.put("1");
        assertTrue(isPartitionFiltered(partitions, dynamicFilters, null), "Should filter partition if partition value not in dynamicFilter");

        dayFilter.put("0");
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, null), "Should not filter partition if partition value is in dynamicFilter");
    }

    @Test
    public void testIsPartitionFilteredWithNonPartitionFilter()
    {
        Set<DynamicFilter> dynamicFilters = new HashSet<>();
        List<HivePartitionKey> partitions = new ArrayList<>();

        partitions.add(new HivePartitionKey("pt_d", "0"));
        partitions.add(new HivePartitionKey("app_id", "10000"));

        ColumnHandle nameColumn = new HiveColumnHandle("name", HIVE_STRING, parseTypeSignature(VARCHAR), 0, REGULAR, Optional.empty());
        //BloomFilter nameFilter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 1024 * 1024, 0.01);
        Set nameFilter = new HashSet();
        nameFilter.add("Alice");
        dynamicFilters.add(new HashSetDynamicFilter("1", nameColumn, nameFilter, DynamicFilter.Type.GLOBAL));
        assertFalse(isPartitionFiltered(partitions, dynamicFilters, null), "Should not filter partition if dynamicFilter is on non-partition column");
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
