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

import java.util.HashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TableStatisticsTest
{
    private TableStatistics tableStatisticsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tableStatisticsUnderTest = new TableStatistics(Estimate.of(0.0), 0L, 0L, new HashMap<>());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(tableStatisticsUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, tableStatisticsUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", tableStatisticsUnderTest.toString());
    }

    @Test
    public void testEmpty() throws Exception
    {
        // Run the test
        final TableStatistics result = TableStatistics.empty();
        assertEquals(Estimate.of(0.0), result.getRowCount());
        assertEquals(0L, result.getFileCount());
        assertEquals(0L, result.getOnDiskDataSizeInBytes());
        assertEquals(new HashMap<>(), result.getColumnStatistics());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testBuilder() throws Exception
    {
        // Setup
        // Run the test
        final TableStatistics.Builder result = TableStatistics.builder();

        // Verify the results
    }
}
