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
package io.prestosql.spi.plan;

import io.prestosql.spi.block.SortOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class OrderingSchemeTest
{
    private OrderingScheme orderingSchemeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        orderingSchemeUnderTest = new OrderingScheme(Arrays.asList(new Symbol("name")), new HashMap<>());
    }

    @Test
    public void testGetOrderingList() throws Exception
    {
        // Setup
        // Run the test
        final List<SortOrder> result = orderingSchemeUnderTest.getOrderingList();

        // Verify the results
        assertEquals(Arrays.asList(SortOrder.ASC_NULLS_FIRST), result);
    }

    @Test
    public void testGetOrdering() throws Exception
    {
        // Setup
        final Symbol symbol = new Symbol("name");

        // Run the test
        final SortOrder result = orderingSchemeUnderTest.getOrdering(symbol);

        // Verify the results
        assertEquals(SortOrder.ASC_NULLS_FIRST, result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(orderingSchemeUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, orderingSchemeUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", orderingSchemeUnderTest.toString());
    }
}
