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

import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ConstantPropertyTest
{
    private ConstantProperty<Object> constantPropertyUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        constantPropertyUnderTest = new ConstantProperty<>(null);
    }

    @Test
    public void testIsOrderSensitive() throws Exception
    {
        assertTrue(constantPropertyUnderTest.isOrderSensitive());
    }

    @Test
    public void testGetColumns() throws Exception
    {
        // Setup
        // Run the test
        final Set<Object> result = constantPropertyUnderTest.getColumns();

        // Verify the results
    }

    @Test
    public void testTranslate() throws Exception
    {
        // Setup
        final Function<Object, Optional<Object>> translator = val -> {
            return null;
        };

        // Run the test
        final Optional<LocalProperty<Object>> result = constantPropertyUnderTest.translate(translator);

        // Verify the results
    }

    @Test
    public void testIsSimplifiedBy() throws Exception
    {
        // Setup
        final LocalProperty<Object> known = null;

        // Run the test
        final boolean result = constantPropertyUnderTest.isSimplifiedBy(known);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", constantPropertyUnderTest.toString());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(constantPropertyUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, constantPropertyUnderTest.hashCode());
    }
}
