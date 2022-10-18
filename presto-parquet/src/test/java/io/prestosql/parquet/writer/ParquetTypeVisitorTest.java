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
package io.prestosql.parquet.writer;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

public class ParquetTypeVisitorTest
{
    private ParquetTypeVisitor<Object> parquetTypeVisitorUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        parquetTypeVisitorUnderTest = new ParquetTypeVisitor<>();
    }

    @Test
    public void testMessage() throws Exception
    {
        // Setup
        final MessageType message = new MessageType("name", Arrays.asList());
        final List<Object> fields = Arrays.asList();

        // Run the test
        final Object result = parquetTypeVisitorUnderTest.message(message, fields);

        // Verify the results
    }

    @Test
    public void testStruct() throws Exception
    {
        // Setup
        final GroupType struct = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());
        final List<Object> fields = Arrays.asList();

        // Run the test
        final Object result = parquetTypeVisitorUnderTest.struct(struct, fields);

        // Verify the results
    }

    @Test
    public void testList()
    {
        // Setup
        final GroupType array = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());

        // Run the test
        final Object result = parquetTypeVisitorUnderTest.list(array, null);

        // Verify the results
    }

    @Test
    public void testMap() throws Exception
    {
        // Setup
        final GroupType map = new GroupType(Type.Repetition.REQUIRED, "name", Arrays.asList());

        // Run the test
        final Object result = parquetTypeVisitorUnderTest.map(map, null, null);

        // Verify the results
    }

    @Test
    public void testPrimitive() throws Exception
    {
        // Setup
        final PrimitiveType primitive = new PrimitiveType(Type.Repetition.REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, 0, "name");

        // Run the test
        final Object result = parquetTypeVisitorUnderTest.primitive(primitive);

        // Verify the results
    }

    @Test
    public void testVisit()
    {
        // Setup
        final Type type = null;
        final ParquetTypeVisitor<Object> visitor = new ParquetTypeVisitor<>();

        // Run the test
        final Object result = ParquetTypeVisitor.visit(type, visitor);

        // Verify the results
    }
}
