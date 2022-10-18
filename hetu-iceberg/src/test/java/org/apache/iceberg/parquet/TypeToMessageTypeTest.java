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
package org.apache.iceberg.parquet;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class TypeToMessageTypeTest
{
    private TypeToMessageType typeToMessageTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        typeToMessageTypeUnderTest = new TypeToMessageType();
    }

    @Test
    public void testConvert()
    {
        // Setup
        final MessageType expectedResult = new MessageType("name", Arrays.asList());

        // Run the test
        final MessageType result = typeToMessageTypeUnderTest.convert(new Schema(), "name");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testStruct()
    {
        // Setup
        final Types.StructType struct = Types.StructType.of(
                Arrays.asList(Types.NestedField.optional(0, "name", new Types.BinaryType())));
        final GroupType expectedResult = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());

        // Run the test
        final GroupType result = typeToMessageTypeUnderTest.struct(struct, Type.Repetition.REQUIRED, 0, "name");
    }

    @Test
    public void testField()
    {
        // Setup
        final Types.NestedField field = Types.NestedField.optional(0, "name", new Types.BinaryType());

        // Run the test
        final Type result = typeToMessageTypeUnderTest.field(field);
    }

    @Test
    public void testList()
    {
        // Setup
        final Types.ListType list = Types.ListType.ofOptional(0, new Types.BinaryType());
        final GroupType expectedResult = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());

        // Run the test
        final GroupType result = typeToMessageTypeUnderTest.list(list, Type.Repetition.REQUIRED, 0, "name");
    }

    @Test
    public void testMap()
    {
        // Setup
        final Types.MapType map = Types.MapType.ofOptional(0, 0, new Types.BinaryType(), new Types.BinaryType());
        final GroupType expectedResult = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());

        // Run the test
        final GroupType result = typeToMessageTypeUnderTest.map(map, Type.Repetition.REQUIRED, 0, "name");
    }

    @Test
    public void testPrimitive()
    {
        // Setup
        // Run the test
        typeToMessageTypeUnderTest.primitive(new Types.BooleanType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.IntegerType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.LongType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.FloatType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.DoubleType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.DateType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.TimeType.get(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.TimestampType.withoutZone(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.FixedType.ofLength(20), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.DecimalType.of(1, 1), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.DecimalType.of(18, 18), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(Types.DecimalType.of(20, 20), Type.Repetition.REQUIRED, 0, "original");
        typeToMessageTypeUnderTest.primitive(new Types.StringType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.BinaryType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.UUIDType(), Type.Repetition.REQUIRED, 0, "originalName");
        typeToMessageTypeUnderTest.primitive(new Types.BinaryType(), Type.Repetition.REQUIRED, 0, "originalName");
    }
}
