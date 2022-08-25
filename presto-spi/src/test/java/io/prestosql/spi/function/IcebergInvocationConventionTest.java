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
package io.prestosql.spi.function;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IcebergInvocationConventionTest
{
    private IcebergInvocationConvention icebergInvocationConvention;

    @BeforeMethod
    public void setUp() throws Exception
    {
        icebergInvocationConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);
    }

    @Test
    public void testGetArgumentConvention() throws Exception
    {
        // Setup
        // Run the test
        final IcebergInvocationConvention.InvocationArgumentConvention result = icebergInvocationConvention.getArgumentConvention(
                0);

        // Verify the results
        assertEquals(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL, result);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = icebergInvocationConvention.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetArgumentConventions() throws Exception
    {
        assertEquals(Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                icebergInvocationConvention.getArgumentConventions());
    }

    @Test
    public void testSupportsInstanceFactor() throws Exception
    {
        assertTrue(icebergInvocationConvention.supportsInstanceFactor());
    }

    @Test
    public void testSimpleConvention() throws Exception
    {
        // Run the test
        final IcebergInvocationConvention result = IcebergInvocationConvention.simpleConvention(
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL,
                IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL);
        assertEquals(IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, result.getReturnConvention());
        assertEquals(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL, result.getArgumentConvention(0));
//        assertTrue(result.hasSession());
        assertEquals("result", result.toString());
        assertEquals(Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                result.getArgumentConventions());
        assertTrue(result.supportsInstanceFactor());
    }
}
