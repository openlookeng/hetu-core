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

import io.prestosql.spi.type.ParametricType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeOperators;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.VarcharType.VARCHAR;

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
        final TypeSignature signature = VARCHAR.getTypeSignature();

        // Run the test
        final Type result = testingTypeManagerUnderTest.getType(signature);

        // Verify the results
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
    public void testGetParameterizedType_ThrowsTypeNotFoundException()
    {
        // Setup
        final List<TypeSignatureParameter> typeParameters = Arrays.asList(TypeSignatureParameter.of(0L));

        // Run the test
        testingTypeManagerUnderTest.getParameterizedType("baseTypeName", typeParameters);
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
    public void testGetCommonSuperType()
    {
        // Setup
        // Run the test
        final Optional<Type> result = testingTypeManagerUnderTest.getCommonSuperType(null, null);

        // Verify the results
    }

    @Test
    public void testCanCoerce()
    {
        testingTypeManagerUnderTest.canCoerce(null, null);
    }

    @Test
    public void testIsTypeOnlyCoercion()
    {
        testingTypeManagerUnderTest.isTypeOnlyCoercion(null, null);
    }

    @Test
    public void testCoerceTypeBase()
    {
        // Setup
        // Run the test
        final Optional<Type> result = testingTypeManagerUnderTest.coerceTypeBase(null, "resultTypeBase");

        // Verify the results
    }

    @Test
    public void testGetTypeOperators()
    {
        // Setup
        // Run the test
        final TypeOperators result = testingTypeManagerUnderTest.getTypeOperators();

        // Verify the results
    }
}
