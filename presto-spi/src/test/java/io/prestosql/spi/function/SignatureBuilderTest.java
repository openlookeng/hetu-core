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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class SignatureBuilderTest
{
    private SignatureBuilder signatureBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        signatureBuilderUnderTest = new SignatureBuilder();
    }

    @Test
    public void testOperatorType() throws Exception
    {
        // Setup
        // Run the test
        final SignatureBuilder result = signatureBuilderUnderTest.operatorType(OperatorType.COMPARISON_UNORDERED_FIRST);

        // Verify the results
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        final Signature expectedResult = new Signature(
                new QualifiedObjectName("catalogName", "schemaName", "objectName"), FunctionKind.SCALAR,
                Arrays.asList(new TypeVariableConstraint("name", false, false, "variadicBound")),
                Arrays.asList(new LongVariableConstraint("name", "expression")),
                new TypeSignature("base", TypeSignatureParameter.of(0L)),
                Arrays.asList(new TypeSignature("base", TypeSignatureParameter.of(0L))), false);

        // Run the test
        final Signature result = signatureBuilderUnderTest.build();

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
