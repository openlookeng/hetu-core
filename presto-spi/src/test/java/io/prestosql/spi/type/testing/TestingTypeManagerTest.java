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
package io.prestosql.spi.type.testing;

import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestingTypeManagerTest
{
    private TestingTypeManager testingTypeManagerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        testingTypeManagerUnderTest = new TestingTypeManager();
    }

    @Test
    public void testGetType1()
    {
        // Setup
        final TypeSignature signature = new TypeSignature("base", TypeSignatureParameter.of(0L));

        // Run the test
        final Type result = testingTypeManagerUnderTest.getType(signature);

        // Verify the results
    }

    @Test
    public void testGetType1_ThrowsTypeNotFoundException() throws Exception
    {
        // Setup
        final TypeSignature signature = new TypeSignature("base", TypeSignatureParameter.of(0L));

        // Run the test
        assertThrows(TypeNotFoundException.class, () -> testingTypeManagerUnderTest.getType(signature));
    }

    @Test
    public void testGetParameterizedType() throws Exception
    {
        // Setup
        final List<TypeSignatureParameter> typeParameters = Arrays.asList(TypeSignatureParameter.of(0L));

        // Run the test
        final Type result = testingTypeManagerUnderTest.getParameterizedType("baseTypeName", typeParameters);

        // Verify the results
    }

    @Test
    public void testGetParameterizedType_ThrowsTypeNotFoundException() throws Exception
    {
        // Setup
        final List<TypeSignatureParameter> typeParameters = Arrays.asList(TypeSignatureParameter.of(0L));

        // Run the test
        assertThrows(TypeNotFoundException.class,
                () -> testingTypeManagerUnderTest.getParameterizedType("baseTypeName", typeParameters));
    }

    @Test
    public void testGetTypes() throws Exception
    {
        // Setup
        // Run the test
        final List<Type> result = testingTypeManagerUnderTest.getTypes();

        // Verify the results
    }

    @Test
    public void testGetParametricTypes() throws Exception
    {
        // Setup
        // Run the test
        final Collection<ParametricType> result = testingTypeManagerUnderTest.getParametricTypes();

        // Verify the results
    }

    @Test
    public void testGetCommonSuperType() throws Exception
    {
        // Setup
        // Run the test
        final Optional<Type> result = testingTypeManagerUnderTest.getCommonSuperType(null, null);

        // Verify the results
    }

    @Test
    public void testCanCoerce() throws Exception
    {
        assertTrue(testingTypeManagerUnderTest.canCoerce(null, null));
    }

    @Test
    public void testIsTypeOnlyCoercion() throws Exception
    {
        assertTrue(testingTypeManagerUnderTest.isTypeOnlyCoercion(null, null));
    }

    @Test
    public void testCoerceTypeBase() throws Exception
    {
        // Setup
        // Run the test
        final Optional<Type> result = testingTypeManagerUnderTest.coerceTypeBase(null, "resultTypeBase");

        // Verify the results
    }

    @Test
    public void testResolveOperator() throws Exception
    {
        // Setup
        final List<? extends Type> argumentTypes = Arrays.asList();

        // Run the test
        final MethodHandle result = testingTypeManagerUnderTest.resolveOperator(OperatorType.COMPARISON_UNORDERED_FIRST,
                argumentTypes);

        // Verify the results
    }

    @Test
    public void testGetTypeOperators() throws Exception
    {
        // Setup
        // Run the test
        final TypeOperators result = testingTypeManagerUnderTest.getTypeOperators();

        // Verify the results
    }

    @Test
    public void testGetType2() throws Exception
    {
        // Setup
        final TypeId id = TypeId.of("id");

        // Run the test
        final Type result = testingTypeManagerUnderTest.getType(id);

        // Verify the results
    }
}
