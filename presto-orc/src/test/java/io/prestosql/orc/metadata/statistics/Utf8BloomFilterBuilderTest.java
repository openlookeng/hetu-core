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

import io.airlift.slice.Slice;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class Utf8BloomFilterBuilderTest
{
    private Utf8BloomFilterBuilder utf8BloomFilterBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        utf8BloomFilterBuilderUnderTest = new Utf8BloomFilterBuilder(1, 0.5);
    }

    @Test
    public void testAddString()
    {
        // Setup
        final Slice val = null;

        // Run the test
        final BloomFilterBuilder result = utf8BloomFilterBuilderUnderTest.addString(val);

        // Verify the results
    }

    @Test
    public void testAddLong()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = utf8BloomFilterBuilderUnderTest.addLong(0L);

        // Verify the results
    }

    @Test
    public void testAddDouble()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = utf8BloomFilterBuilderUnderTest.addDouble(0.0);

        // Verify the results
    }

    @Test
    public void testAddFloat()
    {
        // Setup
        // Run the test
        final BloomFilterBuilder result = utf8BloomFilterBuilderUnderTest.addFloat(0.0f);

        // Verify the results
    }

    @Test
    public void testBuildBloomFilter()
    {
        utf8BloomFilterBuilderUnderTest.buildBloomFilter();
    }
}
