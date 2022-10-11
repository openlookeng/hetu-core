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

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ColumnStatisticsTest
{
    private ColumnStatistics columnStatisticsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        columnStatisticsUnderTest = new ColumnStatistics(Estimate.of(0.0), Estimate.of(0.0), Estimate.of(0.0),
                Optional.of(new DoubleRange(0.0, 0.0)));
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(columnStatisticsUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, columnStatisticsUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", columnStatisticsUnderTest.toString());
    }

    @Test
    public void testEmpty() throws Exception
    {
        // Run the test
        final ColumnStatistics result = ColumnStatistics.empty();
        assertEquals(Estimate.of(0.0), result.getNullsFraction());
        assertEquals(Estimate.of(0.0), result.getDistinctValuesCount());
        assertEquals(Estimate.of(0.0), result.getDataSize());
        assertEquals(Optional.of(new DoubleRange(0.0, 0.0)), result.getRange());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testBuilder() throws Exception
    {
        // Setup
        // Run the test
        final ColumnStatistics.Builder result = ColumnStatistics.builder();

        // Verify the results
    }
}
