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

//import io.prestosql.spi.block.Block;
//import io.prestosql.spi.block.BlockBuilderStatus;
//import io.prestosql.spi.connector.ConnectorSession;
//import org.testng.annotations.Test;
//
//import java.util.Arrays;
//import java.util.Optional;
//
//import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;

public class DecimalTypeTest
{
//    @Test
//    public void testCreateDecimalType1() throws Exception
//    {
//        // Run the test
//        final DecimalType result = DecimalType.createDecimalType(0, 0);
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        assertEquals(0, result.getPrecision());
//        assertEquals(0, result.getScale());
//        assertTrue(result.isShort());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals("signature", result.getDisplayName());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals("result", result.getObject(null, 0));
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        assertTrue(result.equalTo(null, 0, null, 0));
//        assertEquals(0L, result.hash(null, 0));
//        assertEquals(0, result.compareTo(null, 0, null, 0));
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        final Block<T> block = null;
//        assertEquals(null, result.get(block, 0));
//        final ConnectorSession session = null;
//        final Block<T> block1 = null;
//        assertEquals("result", result.getObjectValue(session, block1, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//        assertEquals(0, result.getFixedSize());
//        assertEquals(null, result.createFixedSizeBlockBuilder(0));
//    }

//    @Test
//    public void testCreateDecimalType2() throws Exception
//    {
//        // Run the test
//        final DecimalType result = DecimalType.createDecimalType(0);
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        assertEquals(0, result.getPrecision());
//        assertEquals(0, result.getScale());
//        assertTrue(result.isShort());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals("signature", result.getDisplayName());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals("result", result.getObject(null, 0));
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        assertTrue(result.equalTo(null, 0, null, 0));
//        assertEquals(0L, result.hash(null, 0));
//        assertEquals(0, result.compareTo(null, 0, null, 0));
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        final Block<T> block = null;
//        assertEquals(null, result.get(block, 0));
//        final ConnectorSession session = null;
//        final Block<T> block1 = null;
//        assertEquals("result", result.getObjectValue(session, block1, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//        assertEquals(0, result.getFixedSize());
//        assertEquals(null, result.createFixedSizeBlockBuilder(0));
//    }

//    @Test
//    public void testCreateDecimalType3() throws Exception
//    {
//        // Run the test
//        final DecimalType result = DecimalType.createDecimalType();
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        assertEquals(0, result.getPrecision());
//        assertEquals(0, result.getScale());
//        assertTrue(result.isShort());
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals("signature", result.getDisplayName());
//        assertEquals(Object.class, result.getJavaType());
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals("result", result.getObject(null, 0));
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        assertTrue(result.equalTo(null, 0, null, 0));
//        assertEquals(0L, result.hash(null, 0));
//        assertEquals(0, result.compareTo(null, 0, null, 0));
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        final Block<T> block = null;
//        assertEquals(null, result.get(block, 0));
//        final ConnectorSession session = null;
//        final Block<T> block1 = null;
//        assertEquals("result", result.getObjectValue(session, block1, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//        assertEquals(0, result.getFixedSize());
//        assertEquals(null, result.createFixedSizeBlockBuilder(0));
//    }
}
