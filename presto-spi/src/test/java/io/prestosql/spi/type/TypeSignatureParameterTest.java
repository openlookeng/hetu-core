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
package io.prestosql.spi.type;

//import org.testng.annotations.Test;
//
//import java.util.Optional;
//
//import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;

public class TypeSignatureParameterTest
{
//    @Test
//    public void testTypeParameter() throws Exception
//    {
//        // Setup
//        final TypeSignature typeSignature = new TypeSignature("base", TypeSignatureParameter.of(0L));
//
//        // Run the test
//        final TypeSignatureParameter result = TypeSignatureParameter.typeParameter(typeSignature);
//        assertEquals("value", result.toString());
//        assertEquals(ParameterKind.TYPE, result.getKind());
//        assertTrue(result.isTypeSignature());
//        assertTrue(result.isLongLiteral());
//        assertTrue(result.isNamedTypeSignature());
//        assertTrue(result.isVariable());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(0L, result.getLongLiteral());
//        assertEquals(new NamedTypeSignature(Optional.of(new RowFieldName("name", false)),
//                new TypeSignature("base", TypeSignatureParameter.of(0L))), result.getNamedTypeSignature());
//        assertEquals("value", result.getVariable());
//        assertEquals(Optional.of(new TypeSignature("base", TypeSignatureParameter.of(0L))),
//                result.getTypeSignatureOrNamedTypeSignature());
//        assertTrue(result.isCalculated());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//    }
//
//    @Test
//    public void testOf1()
//    {
//        // Setup
//        final TypeSignature typeSignature = new TypeSignature("base", TypeSignatureParameter.of(0L));
//
//        // Run the test
//        final TypeSignatureParameter result = TypeSignatureParameter.of(typeSignature);
//        assertEquals("value", result.toString());
//        assertEquals(ParameterKind.TYPE, result.getKind());
//        assertTrue(result.isTypeSignature());
//        assertTrue(result.isLongLiteral());
//        assertTrue(result.isNamedTypeSignature());
//        assertTrue(result.isVariable());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(0L, result.getLongLiteral());
//        assertEquals(new NamedTypeSignature(Optional.of(new RowFieldName("name", false)),
//                new TypeSignature("base", TypeSignatureParameter.of(0L))), result.getNamedTypeSignature());
//        assertEquals("value", result.getVariable());
//        assertEquals(Optional.of(new TypeSignature("base", TypeSignatureParameter.of(0L))),
//                result.getTypeSignatureOrNamedTypeSignature());
//        assertTrue(result.isCalculated());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//    }
//
//    @Test
//    public void testOf2() throws Exception
//    {
//        // Run the test
//        final TypeSignatureParameter result = TypeSignatureParameter.of(0L);
//        assertEquals("value", result.toString());
//        assertEquals(ParameterKind.TYPE, result.getKind());
//        assertTrue(result.isTypeSignature());
//        assertTrue(result.isLongLiteral());
//        assertTrue(result.isNamedTypeSignature());
//        assertTrue(result.isVariable());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(0L, result.getLongLiteral());
//        assertEquals(new NamedTypeSignature(Optional.of(new RowFieldName("name", false)),
//                new TypeSignature("base", TypeSignatureParameter.of(0L))), result.getNamedTypeSignature());
//        assertEquals("value", result.getVariable());
//        assertEquals(Optional.of(new TypeSignature("base", TypeSignatureParameter.of(0L))),
//                result.getTypeSignatureOrNamedTypeSignature());
//        assertTrue(result.isCalculated());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//    }
//
//    @Test
//    public void testOf3()
//    {
//        // Setup
//        final NamedTypeSignature namedTypeSignature = new NamedTypeSignature(
//                Optional.of(new RowFieldName("name", false)), new TypeSignature("base", TypeSignatureParameter.of(0L)));
//
//        // Run the test
//        final TypeSignatureParameter result = TypeSignatureParameter.of(namedTypeSignature);
//        assertEquals("value", result.toString());
//        assertEquals(ParameterKind.TYPE, result.getKind());
//        assertTrue(result.isTypeSignature());
//        assertTrue(result.isLongLiteral());
//        assertTrue(result.isNamedTypeSignature());
//        assertTrue(result.isVariable());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(0L, result.getLongLiteral());
//        assertEquals(new NamedTypeSignature(Optional.of(new RowFieldName("name", false)),
//                new TypeSignature("base", TypeSignatureParameter.of(0L))), result.getNamedTypeSignature());
//        assertEquals("value", result.getVariable());
//        assertEquals(Optional.of(new TypeSignature("base", TypeSignatureParameter.of(0L))),
//                result.getTypeSignatureOrNamedTypeSignature());
//        assertTrue(result.isCalculated());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//    }
//
//    @Test
//    public void testOf4()
//    {
//        // Run the test
//        final TypeSignatureParameter result = TypeSignatureParameter.of("variable");
//        assertEquals("value", result.toString());
//        assertEquals(ParameterKind.TYPE, result.getKind());
//        assertTrue(result.isTypeSignature());
//        assertTrue(result.isLongLiteral());
//        assertTrue(result.isNamedTypeSignature());
//        assertTrue(result.isVariable());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(0L, result.getLongLiteral());
//        assertEquals(new NamedTypeSignature(Optional.of(new RowFieldName("name", false)),
//                new TypeSignature("base", TypeSignatureParameter.of(0L))), result.getNamedTypeSignature());
//        assertEquals("value", result.getVariable());
//        assertEquals(Optional.of(new TypeSignature("base", TypeSignatureParameter.of(0L))),
//                result.getTypeSignatureOrNamedTypeSignature());
//        assertTrue(result.isCalculated());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//    }
}
