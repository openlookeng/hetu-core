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
package io.prestosql.spi.statistics;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TableStatisticsMetadataTest
{
    private TableStatisticsMetadata tableStatisticsMetadataUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tableStatisticsMetadataUnderTest = new TableStatisticsMetadata(new HashSet<>(
                Arrays.asList(new ColumnStatisticMetadata("columnName", ColumnStatisticType.MIN_VALUE))), new HashSet<>(
                Arrays.asList(TableStatisticType.ROW_COUNT)), Arrays.asList("value"));
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = tableStatisticsMetadataUnderTest.isEmpty();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testEmpty() throws Exception
    {
        // Run the test
        final TableStatisticsMetadata result = TableStatisticsMetadata.empty();
        assertEquals(new HashSet<>(
                        Arrays.asList(new ColumnStatisticMetadata("columnName", ColumnStatisticType.MIN_VALUE))),
                result.getColumnStatistics());
        assertEquals(new HashSet<>(Arrays.asList(TableStatisticType.ROW_COUNT)), result.getTableStatistics());
        assertEquals(Arrays.asList("value"), result.getGroupingColumns());
        assertTrue(result.isEmpty());
    }
}
