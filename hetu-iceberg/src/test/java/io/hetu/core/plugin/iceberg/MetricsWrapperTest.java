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
package io.hetu.core.plugin.iceberg;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class MetricsWrapperTest
{
    private MetricsWrapper metricsWrapperUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        metricsWrapperUnderTest = new MetricsWrapper(0L, new HashMap<>(), new HashMap<>(), new HashMap<>(),
                new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    @Test
    public void testRecordCount()
    {
        // Setup
        // Run the test
        final Long result = metricsWrapperUnderTest.recordCount();

        // Verify the results
        assertEquals(Optional.of(0L), result);
    }

    @Test
    public void testColumnSizes()
    {
        // Setup
        final Map<Integer, Long> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Long> result = metricsWrapperUnderTest.columnSizes();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testValueCounts()
    {
        // Setup
        final Map<Integer, Long> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Long> result = metricsWrapperUnderTest.valueCounts();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testNullValueCounts()
    {
        // Setup
        final Map<Integer, Long> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Long> result = metricsWrapperUnderTest.nullValueCounts();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testNanValueCounts()
    {
        // Setup
        final Map<Integer, Long> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, Long> result = metricsWrapperUnderTest.nanValueCounts();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testLowerBounds()
    {
        // Setup
        final Map<Integer, ByteBuffer> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, ByteBuffer> result = metricsWrapperUnderTest.lowerBounds();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpperBounds()
    {
        // Setup
        final Map<Integer, ByteBuffer> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, ByteBuffer> result = metricsWrapperUnderTest.upperBounds();

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
