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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class EstimateTest
{
    @Test
    public void testUnknown() throws Exception
    {
        // Run the test
        final Estimate result = Estimate.unknown();
        assertTrue(result.isUnknown());
        assertEquals(0.0, result.getValue(), 0.0001);
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testZero() throws Exception
    {
        // Run the test
        final Estimate result = Estimate.zero();
        assertTrue(result.isUnknown());
        assertEquals(0.0, result.getValue(), 0.0001);
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }

    @Test
    public void testOf() throws Exception
    {
        // Run the test
        final Estimate result = Estimate.of(0.0);
        assertTrue(result.isUnknown());
        assertEquals(0.0, result.getValue(), 0.0001);
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
    }
}
