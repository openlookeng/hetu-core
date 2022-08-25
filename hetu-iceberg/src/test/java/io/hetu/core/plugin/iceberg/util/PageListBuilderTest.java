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
package io.hetu.core.plugin.iceberg.util;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.TimeZoneKey;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static org.testng.Assert.assertEquals;

public class PageListBuilderTest
{
    private PageListBuilder pageListBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        pageListBuilderUnderTest = new PageListBuilder(Arrays.asList(BooleanType.BOOLEAN));
    }

    @Test
    public void testReset()
    {
        // Setup
        // Run the test
        pageListBuilderUnderTest.reset();

        // Verify the results
    }

    @Test
    public void testBuild()
    {
        // Setup
        // Run the test
        pageListBuilderUnderTest.build();
        // Verify the results
    }

    @Test
    public void testEndRow()
    {
        // Setup
        // Run the test
        PageListBuilder pageListBuilderUnder = new PageListBuilder(new ArrayList<>());
        pageListBuilderUnder.beginRow();
        pageListBuilderUnder.endRow();

        // Verify the results
    }

    @Test
    public void testAppendNull()
    {
        // Setup
        // Run the test
        PageListBuilder pageListBuilderUnder = new PageListBuilder(Arrays.asList(BooleanType.BOOLEAN, BooleanType.BOOLEAN));
        pageListBuilderUnder.beginRow();
        pageListBuilderUnder.appendNull();

        // Verify the results
    }

    @Test
    public void testAppendInteger()
    {
        // Setup
        // Run the test
        PageListBuilder pageListBuilderUnder = new PageListBuilder(Arrays.asList(BooleanType.BOOLEAN, BooleanType.BOOLEAN));
        pageListBuilderUnder.beginRow();
        pageListBuilderUnder.appendInteger(1);
        // Verify the results
    }

    @Test
    public void testAppendBigint()
    {
        // Setup
        // Run the test
        PageListBuilder pageListBuilderUnder = new PageListBuilder(Arrays.asList(BooleanType.BOOLEAN, BooleanType.BOOLEAN));
        pageListBuilderUnder.beginRow();
        pageListBuilderUnder.appendBigint(0L);

        // Verify the results
    }

    @Test
    public void testAppendTimestampTzMillis()
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        pageListBuilderUnderTest.appendTimestampTzMillis(0L, timeZoneKey);

        // Verify the results
    }

    @Test
    public void testAppendVarchar()
    {
        // Setup
        // Run the test
        pageListBuilderUnderTest.appendVarchar("value");

        // Verify the results
    }

    @Test
    public void testAppendVarbinary()
    {
        // Setup
        final Slice value = null;

        // Run the test
        pageListBuilderUnderTest.appendVarbinary(value);

        // Verify the results
    }

    @Test
    public void testAppendIntegerArray()
    {
        // Setup
        final Iterable<Integer> values = Arrays.asList(0);

        // Run the test
        pageListBuilderUnderTest.appendIntegerArray(values);

        // Verify the results
    }

    @Test
    public void testAppendBigintArray()
    {
        // Setup
        final Iterable<Long> values = Arrays.asList(0L);

        // Run the test
        pageListBuilderUnderTest.appendBigintArray(values);

        // Verify the results
    }

    @Test
    public void testAppendVarcharArray()
    {
        // Setup
        final Iterable<String> values = Arrays.asList("value");

        // Run the test
        pageListBuilderUnderTest.appendVarcharArray(values);

        // Verify the results
    }

    @Test
    public void testAppendVarcharVarcharMap()
    {
        // Setup
        final Map<String, String> values = new HashMap<>();

        // Run the test
        pageListBuilderUnderTest.appendVarcharVarcharMap(values);

        // Verify the results
    }

    @Test
    public void testAppendIntegerBigintMap()
    {
        // Setup
        final Map<Integer, Long> values = new HashMap<>();

        // Run the test
        pageListBuilderUnderTest.appendIntegerBigintMap(values);

        // Verify the results
    }

    @Test
    public void testAppendIntegerVarcharMap()
    {
        // Setup
        final Map<Integer, String> values = new HashMap<>();

        // Run the test
        PageListBuilder pageListBuilderUnder = new PageListBuilder(Arrays.asList(BooleanType.BOOLEAN, BooleanType.BOOLEAN));
        pageListBuilderUnder.beginRow();
        pageListBuilderUnder.appendIntegerVarcharMap(values);

        // Verify the results
    }

    @Test
    public void testNextColumn()
    {
        // Setup
        // Run the test
        final BlockBuilder result = pageListBuilderUnderTest.nextColumn();

        // Verify the results
    }

    @Test
    public void testForTable()
    {
        // Setup
        final ConnectorTableMetadata table = new ConnectorTableMetadata(new SchemaTableName("schemaName", "tableName"),
                Arrays.asList(
                        new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(), false)),
                new HashMap<>(), Optional.of("value"), Optional.of(
                Arrays.asList(new ColumnMetadata("name", null, false, "comment", "extraInfo", false, new HashMap<>(),
                        false))), Optional.of(new HashSet<>(
                Arrays.asList("value"))));

        // Run the test
        final PageListBuilder result = PageListBuilder.forTable(table);
        assertEquals(Arrays.asList(new Page(0, new Properties(), null)), result.build());
        assertEquals(null, result.nextColumn());
    }
}
