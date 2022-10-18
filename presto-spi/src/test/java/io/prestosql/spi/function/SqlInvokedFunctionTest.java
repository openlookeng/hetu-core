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

import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SqlInvokedFunctionTest
{
    @Mock
    private QualifiedObjectName mockFunctionName;
    @Mock
    private TypeSignature mockReturnType;
    @Mock
    private RoutineCharacteristics mockRoutineCharacteristics;

    private SqlInvokedFunction sqlInvokedFunctionUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        sqlInvokedFunctionUnderTest = new SqlInvokedFunction(mockFunctionName,
                Arrays.asList(new Parameter("name", new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                mockReturnType, "description", mockRoutineCharacteristics, "body", new HashMap<>(),
                Optional.of("value"));
    }

    @Test
    public void testWithVersion() throws Exception
    {
        // Setup
        final SqlInvokedFunction expectedResult = new SqlInvokedFunction(
                new QualifiedObjectName("catalogName", "schemaName", "objectName"),
                Arrays.asList(new Parameter("name", new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                new TypeSignature("base", TypeSignatureParameter.of(0L)), "description", new RoutineCharacteristics(
                Optional.of(new RoutineCharacteristics.Language("language")),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT)), "body", new HashMap<>(),
                Optional.of("value"));

        // Run the test
        final SqlInvokedFunction result = sqlInvokedFunctionUnderTest.withVersion("version");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIsHidden() throws Exception
    {
        assertTrue(sqlInvokedFunctionUnderTest.isHidden());
    }

    @Test
    public void testIsDeterministic() throws Exception
    {
        // Setup
        when(mockRoutineCharacteristics.isDeterministic()).thenReturn(false);

        // Run the test
        final boolean result = sqlInvokedFunctionUnderTest.isDeterministic();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsCalledOnNullInput() throws Exception
    {
        // Setup
        when(mockRoutineCharacteristics.isCalledOnNullInput()).thenReturn(false);

        // Run the test
        final boolean result = sqlInvokedFunctionUnderTest.isCalledOnNullInput();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetVersion() throws Exception
    {
        // Setup
        // Run the test
        final Optional<String> result = sqlInvokedFunctionUnderTest.getVersion();

        // Verify the results
        assertEquals(Optional.of("value"), result);
    }

    @Test
    public void testGetRequiredFunctionHandle()
    {
        // Setup
        final SqlFunctionHandle expectedResult = new SqlFunctionHandle(
                new SqlFunctionId(new QualifiedObjectName("catalogName", "schemaName", "objectName"),
                        Arrays.asList(new TypeSignature("base", TypeSignatureParameter.of(0L)))), "version");

        // Run the test
        final SqlFunctionHandle result = sqlInvokedFunctionUnderTest.getRequiredFunctionHandle();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetRequiredVersion() throws Exception
    {
        // Setup
        // Run the test
        final String result = sqlInvokedFunctionUnderTest.getRequiredVersion();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testHasSameDefinitionAs() throws Exception
    {
        // Setup
        final SqlInvokedFunction function = new SqlInvokedFunction(
                new QualifiedObjectName("catalogName", "schemaName", "objectName"),
                Arrays.asList(new Parameter("name", new TypeSignature("base", TypeSignatureParameter.of(0L)))),
                new TypeSignature("base", TypeSignatureParameter.of(0L)), "description", new RoutineCharacteristics(
                Optional.of(new RoutineCharacteristics.Language("language")),
                Optional.of(RoutineCharacteristics.Determinism.DETERMINISTIC),
                Optional.of(RoutineCharacteristics.NullCallClause.RETURNS_NULL_ON_NULL_INPUT)), "body", new HashMap<>(),
                Optional.of("value"));

        // Run the test
        final boolean result = sqlInvokedFunctionUnderTest.hasSameDefinitionAs(function);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(sqlInvokedFunctionUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, sqlInvokedFunctionUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = sqlInvokedFunctionUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }
}
