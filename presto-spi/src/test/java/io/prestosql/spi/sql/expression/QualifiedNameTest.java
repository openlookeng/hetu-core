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
package io.prestosql.spi.sql.expression;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class QualifiedNameTest
{
    private QualifiedName qualifiedNameUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        qualifiedNameUnderTest = new QualifiedName(Arrays.asList("value"));
    }

    @Test
    public void testGetSuffix() throws Exception
    {
        // Setup
        // Run the test
        final String result = qualifiedNameUnderTest.getSuffix();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = qualifiedNameUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = qualifiedNameUnderTest.equals("o");

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Setup
        // Run the test
        final int result = qualifiedNameUnderTest.hashCode();

        // Verify the results
        assertEquals(0, result);
    }
}
