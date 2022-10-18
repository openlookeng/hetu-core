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
package io.prestosql.orc.metadata.statistics;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TimestampStatisticsTest
{
    private TimestampStatistics timestampStatisticsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        timestampStatisticsUnderTest = new TimestampStatistics(0L, 0L);
    }

    @Test
    public void testGetRetainedSizeInBytes()
    {
        timestampStatisticsUnderTest.getRetainedSizeInBytes();
    }

    @Test
    public void testEquals() throws Exception
    {
        timestampStatisticsUnderTest.equals("o");
    }

    @Test
    public void testHashCode()
    {
        timestampStatisticsUnderTest.hashCode();
    }

    @Test
    public void testToString()
    {
        timestampStatisticsUnderTest.toString();
    }

    @Test
    public void testAddHash()
    {
        // Setup
        final StatisticsHasher hasher = new StatisticsHasher();

        // Run the test
        timestampStatisticsUnderTest.addHash(hasher);

        // Verify the results
    }
}
