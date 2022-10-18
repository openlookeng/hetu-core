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

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class BuiltInScalarFunctionImplementationTest
{
    @Mock
    private MethodHandle mockMethodHandle;

    private BuiltInScalarFunctionImplementation builtInScalarFunctionImplementationUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        builtInScalarFunctionImplementationUnderTest = new BuiltInScalarFunctionImplementation(false,
                Arrays.asList(new BuiltInScalarFunctionImplementation.ArgumentProperty(
                        BuiltInScalarFunctionImplementation.ArgumentType.VALUE_TYPE,
                        Optional.of(BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL),
                        Optional.of(Object.class))), mockMethodHandle,
                Optional.empty());
    }

    @Test
    public void testIsNullable() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = builtInScalarFunctionImplementationUnderTest.isNullable();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetArgumentProperty() throws Exception
    {
        // Setup
        final BuiltInScalarFunctionImplementation.ArgumentProperty expectedResult = new BuiltInScalarFunctionImplementation.ArgumentProperty(
                BuiltInScalarFunctionImplementation.ArgumentType.VALUE_TYPE,
                Optional.of(BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL),
                Optional.of(Object.class));

        // Run the test
        final BuiltInScalarFunctionImplementation.ArgumentProperty result = builtInScalarFunctionImplementationUnderTest.getArgumentProperty(
                0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetMethodHandle() throws Exception
    {
        // Setup
        // Run the test
        final MethodHandle result = builtInScalarFunctionImplementationUnderTest.getMethodHandle();

        // Verify the results
    }

    @Test
    public void testGetInstanceFactory() throws Exception
    {
        // Setup
        // Run the test
        final Optional<MethodHandle> result = builtInScalarFunctionImplementationUnderTest.getInstanceFactory();

        // Verify the results
    }

    @Test
    public void testGetAllChoices() throws Exception
    {
        // Setup
        // Run the test
        final List<BuiltInScalarFunctionImplementation.ScalarImplementationChoice> result = builtInScalarFunctionImplementationUnderTest.getAllChoices();

        // Verify the results
    }
}
