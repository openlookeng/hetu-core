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
package io.prestosql.spi.connector;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class GroupingPropertyTest
{
    private GroupingProperty<Object> groupingPropertyUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        groupingPropertyUnderTest = new GroupingProperty<>(Arrays.asList());
    }

    @Test
    public void testIsOrderSensitive() throws Exception
    {
        assertTrue(groupingPropertyUnderTest.isOrderSensitive());
    }

    @Test
    public void testConstrain() throws Exception
    {
        // Setup
        final Set<Object> columns = new HashSet<>();

        // Run the test
        final LocalProperty<Object> result = groupingPropertyUnderTest.constrain(columns);

        // Verify the results
    }

    @Test
    public void testIsSimplifiedBy() throws Exception
    {
        // Setup
        final LocalProperty<Object> known = null;

        // Run the test
        final boolean result = groupingPropertyUnderTest.isSimplifiedBy(known);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testTranslate() throws Exception
    {
        // Setup
        final Function<Object, Optional<Object>> translator = val -> {
            return null;
        };

        // Run the test
        final Optional<LocalProperty<Object>> result = groupingPropertyUnderTest.translate(translator);

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = groupingPropertyUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(groupingPropertyUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, groupingPropertyUnderTest.hashCode());
    }
}
