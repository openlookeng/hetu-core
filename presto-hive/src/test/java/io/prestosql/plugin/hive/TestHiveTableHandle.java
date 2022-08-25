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
import io.airlift.json.JsonCodec;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestHiveTableHandle
{
    private final JsonCodec<HiveTableHandle> codec = JsonCodec.jsonCodec(HiveTableHandle.class);
    private HiveTableHandle hiveTableHandleTest;

    @BeforeMethod
    public void setup()
    {
        hiveTableHandleTest = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), Optional.empty());
    }

    @Test
    public void testRoundTrip()
    {
        HiveTableHandle hiveTableHandle = new HiveTableHandle("schema", "table", ImmutableMap.of(), ImmutableList.of(), Optional.empty());

        String json = codec.toJson(hiveTableHandle);
        HiveTableHandle actual = codec.fromJson(json);

        assertEquals(actual.getSchemaTableName(), hiveTableHandle.getSchemaTableName());
    }

    @Test
    public void testGetUpdateProcessor()
    {
        Optional<HiveUpdateProcessor> updateProcessor = hiveTableHandleTest.getUpdateProcessor();
    }

    @Test
    public void testGetSchemaPrefixedTableName()
    {
        String schemaPrefixedTableName = hiveTableHandleTest.getSchemaPrefixedTableName();
    }

    @Test
    public void testIsFilterSupported()
    {
        boolean filterSupported = hiveTableHandleTest.isFilterSupported();
    }

    @Test
    public void testCreateFrom()
    {
        hiveTableHandleTest.createFrom(hiveTableHandleTest);
    }

    @Test
    public void testHasDisjunctFiltersPushdown()
    {
        hiveTableHandleTest.hasDisjunctFiltersPushdown();
    }

    @Test
    public void testEquals()
    {
        hiveTableHandleTest.equals(null);
    }

    @Test
    public void testBasicEquals()
    {
        hiveTableHandleTest.basicEquals(hiveTableHandleTest);
    }

    @Test
    public void testHashCode()
    {
        hiveTableHandleTest.hashCode();
    }

    @Test
    public void testToString()
    {
        hiveTableHandleTest.toString();
        hiveTableHandleTest.isDeleteAsInsertSupported();
    }

    @Test
    public void test()
    {
        hiveTableHandleTest.isDeleteAsInsertSupported();
        hiveTableHandleTest.isUpdateAsInsertSupported();
        hiveTableHandleTest.isSuitableForPushdown();
        hiveTableHandleTest.isTableCacheable();
        hiveTableHandleTest.isHeuristicIndexSupported();
        hiveTableHandleTest.isPartitionColumn("col");
        hiveTableHandleTest.isReuseTableScanSupported();
        hiveTableHandleTest.isTablePropertiesCacheSupported();
        hiveTableHandleTest.isSortBasedAggregationSupported();
    }
}
