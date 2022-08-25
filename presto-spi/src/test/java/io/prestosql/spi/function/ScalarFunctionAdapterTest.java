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

import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertTrue;

public class ScalarFunctionAdapterTest
{
    private ScalarFunctionAdapter scalarFunctionAdapterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        scalarFunctionAdapterUnderTest = new ScalarFunctionAdapter(
                ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED);
    }

    @Test
    public void testCanAdapt() throws Exception
    {
        // Setup
        final IcebergInvocationConvention actualConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);
        final IcebergInvocationConvention expectedConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final boolean result = scalarFunctionAdapterUnderTest.canAdapt(actualConvention, expectedConvention);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testAdapt() throws Exception
    {
        // Setup
        final MethodHandle methodHandle = null;
        final List<Type> actualArgumentTypes = Arrays.asList();
        final IcebergInvocationConvention actualConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);
        final IcebergInvocationConvention expectedConvention = new IcebergInvocationConvention(
                Arrays.asList(IcebergInvocationConvention.InvocationArgumentConvention.NEVER_NULL),
                IcebergInvocationConvention.InvocationReturnConvention.FAIL_ON_NULL, false, false);

        // Run the test
        final MethodHandle result = scalarFunctionAdapterUnderTest.adapt(methodHandle, actualArgumentTypes,
                actualConvention, expectedConvention);

        // Verify the results
    }
}
