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
import org.testng.annotations.Test;

//import java.util.Arrays;
//import java.util.List;
//import java.util.Optional;
//
//import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertTrue;

public class RowTypeTest
{
//    @Test
//    public void testFrom() throws Exception
//    {
//        // Setup
//        final List<RowType.Field> fields = Arrays.asList(new RowType.Field(Optional.of("value"), null));
//
//        // Run the test
//        final RowType result = RowType.from(fields);
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        assertEquals("result", result.getDisplayName());
//        final ConnectorSession session = null;
//        final Block block = null;
//        assertEquals("result", result.getObjectValue(session, block, 0));
//        final Block block1 = null;
//        assertEquals(null, result.getObject(block1, 0));
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals(Arrays.asList(new RowType.Field(Optional.of("value"), null)), result.getFields());
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        final Block<T> leftBlock = null;
//        final Block<T> rightBlock = null;
//        assertTrue(result.equalTo(leftBlock, 0, rightBlock, 0));
//        final Block<T> leftBlock1 = null;
//        final Block<T> rightBlock1 = null;
//        assertEquals(0, result.compareTo(leftBlock1, 0, rightBlock1, 0));
//        final Block<T> block2 = null;
//        assertEquals(0L, result.hash(block2, 0));
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(Object.class, result.getJavaType());
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        final Block<T> block3 = null;
//        assertEquals(null, result.get(block3, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//    }

//    @Test
//    public void testAnonymous() throws Exception
//    {
//        // Setup
//        final List<Type> types = Arrays.asList();
//
//        // Run the test
//        final RowType result = RowType.anonymous(types);
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        assertEquals("result", result.getDisplayName());
//        final ConnectorSession session = null;
//        final Block block = null;
//        assertEquals("result", result.getObjectValue(session, block, 0));
//        final Block block1 = null;
//        assertEquals(null, result.getObject(block1, 0));
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals(Arrays.asList(new RowType.Field(Optional.of("value"), null)), result.getFields());
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        final Block<T> leftBlock = null;
//        final Block<T> rightBlock = null;
//        assertTrue(result.equalTo(leftBlock, 0, rightBlock, 0));
//        final Block<T> leftBlock1 = null;
//        final Block<T> rightBlock1 = null;
//        assertEquals(0, result.compareTo(leftBlock1, 0, rightBlock1, 0));
//        final Block<T> block2 = null;
//        assertEquals(0L, result.hash(block2, 0));
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(Object.class, result.getJavaType());
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        final Block<T> block3 = null;
//        assertEquals(null, result.get(block3, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//    }

//    @Test
//    public void testCreateWithTypeSignature() throws Exception
//    {
//        // Setup
//        final TypeSignature typeSignature = new TypeSignature("base", TypeSignatureParameter.of(0L));
//        final List<RowType.Field> fields = Arrays.asList(new RowType.Field(Optional.of("value"), null));
//
//        // Run the test
//        final RowType result = RowType.createWithTypeSignature(typeSignature, fields);
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        assertEquals("result", result.getDisplayName());
//        final ConnectorSession session = null;
//        final Block block = null;
//        assertEquals("result", result.getObjectValue(session, block, 0));
//        final Block block1 = null;
//        assertEquals(null, result.getObject(block1, 0));
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals(Arrays.asList(new RowType.Field(Optional.of("value"), null)), result.getFields());
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        final Block<T> leftBlock = null;
//        final Block<T> rightBlock = null;
//        assertTrue(result.equalTo(leftBlock, 0, rightBlock, 0));
//        final Block<T> leftBlock1 = null;
//        final Block<T> rightBlock1 = null;
//        assertEquals(0, result.compareTo(leftBlock1, 0, rightBlock1, 0));
//        final Block<T> block2 = null;
//        assertEquals(0L, result.hash(block2, 0));
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(Object.class, result.getJavaType());
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        final Block<T> block3 = null;
//        assertEquals(null, result.get(block3, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//    }

    @Test
    public void testField1()
    {
        // Setup
        final Type type = null;

        // Run the test
        final RowType.Field result = RowType.field("name", type);

        // Verify the results
    }

    @Test
    public void testField2() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final RowType.Field result = RowType.field(type);

        // Verify the results
    }

//    @Test
//    public void testRowType() throws Exception
//    {
//        // Setup
//        final RowType.Field field = new RowType.Field(Optional.of("value"), null);
//
//        // Run the test
//        final RowType result = RowType.rowType(field);
//        final BlockBuilderStatus blockBuilderStatus = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
//        final BlockBuilderStatus blockBuilderStatus1 = null;
//        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
//        assertEquals("result", result.getDisplayName());
//        final ConnectorSession session = null;
//        final Block block = null;
//        assertEquals("result", result.getObjectValue(session, block, 0));
//        final Block block1 = null;
//        assertEquals(null, result.getObject(block1, 0));
//        assertEquals(Arrays.asList(), result.getTypeParameters());
//        assertEquals(Arrays.asList(new RowType.Field(Optional.of("value"), null)), result.getFields());
//        assertTrue(result.isComparable());
//        assertTrue(result.isOrderable());
//        final Block<T> leftBlock = null;
//        final Block<T> rightBlock = null;
//        assertTrue(result.equalTo(leftBlock, 0, rightBlock, 0));
//        final Block<T> leftBlock1 = null;
//        final Block<T> rightBlock1 = null;
//        assertEquals(0, result.compareTo(leftBlock1, 0, rightBlock1, 0));
//        final Block<T> block2 = null;
//        assertEquals(0L, result.hash(block2, 0));
//        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
//        assertEquals(Object.class, result.getJavaType());
//        assertTrue(result.getBoolean(null, 0));
//        assertEquals(0L, result.getLong(null, 0));
//        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
//        assertEquals(null, result.getSlice(null, 0));
//        assertEquals("signature", result.toString());
//        assertTrue(result.equals("o"));
//        assertEquals(0, result.hashCode());
//        final Block<T> block3 = null;
//        assertEquals(null, result.get(block3, 0));
//        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
//        assertEquals(null, result.read(null));
//        final TypeOperators typeOperators = new TypeOperators((val1, val2) -> {
//            return "value";
//        });
//        assertEquals(TypeOperatorDeclaration.extractOperatorDeclaration(Object.class, null, Object.class),
//                result.getTypeOperatorDeclaration(typeOperators));
//    }
}
