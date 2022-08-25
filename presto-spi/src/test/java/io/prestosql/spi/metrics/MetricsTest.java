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
package io.prestosql.spi.metrics;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;

import static org.testng.Assert.assertThrows;

public class MetricsTest
{
    private Metrics metricsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        metricsUnderTest = new Metrics(new HashMap<>());
    }

    @Test
    public void testMergeWith() throws Exception
    {
        // Setup
        final Metrics other = new Metrics(new HashMap<>());

        // Run the test
        final Metrics result = metricsUnderTest.mergeWith(other);

        // Verify the results
    }

    @Test
    public void testMergeWith_ThrowsNullPointerException() throws Exception
    {
        // Setup
        final Metrics other = new Metrics(new HashMap<>());

        // Run the test
        assertThrows(NullPointerException.class, () -> metricsUnderTest.mergeWith(other));
    }

    @Test
    public void testAccumulator() throws Exception
    {
        // Setup
        // Run the test
        final Metrics.Accumulator result = Metrics.accumulator();

        // Verify the results
    }
}
