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

public class NoOpBloomFilterBuilderTest
{
    private NoOpBloomFilterBuilder noOpBloomFilterBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        noOpBloomFilterBuilderUnderTest = new NoOpBloomFilterBuilder();
    }

    @Test
    public void testAddString()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = noOpBloomFilterBuilderUnderTest.addString(null);

        // Verify the results
    }

    @Test
    public void testAddLong()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = noOpBloomFilterBuilderUnderTest.addLong(0L);

        // Verify the results
    }

    @Test
    public void testAddDouble()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = noOpBloomFilterBuilderUnderTest.addDouble(0.0);

        // Verify the results
    }

    @Test
    public void testAddFloat()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = noOpBloomFilterBuilderUnderTest.addFloat(0.0f);

        // Verify the results
    }

    @Test
    public void testBuildBloomFilter()
    {
        // Run the test
        noOpBloomFilterBuilderUnderTest.buildBloomFilter();
    }
}
