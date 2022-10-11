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
package io.prestosql.parquet;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.GroupColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.PrimitiveColumnIO;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ParquetTypeUtilsTest
{
    @Test
    public void testGetColumns() throws Exception
    {
        // Setup
        final MessageType fileSchema = new MessageType("name", Arrays.asList());
        final MessageType requestedSchema = new MessageType("name", Arrays.asList());

        // Run the test
        final List<PrimitiveColumnIO> result = ParquetTypeUtils.getColumns(fileSchema, requestedSchema);

        // Verify the results
    }

    @Test
    public void testGetColumnIO()
    {
        // Setup
        final MessageType fileSchema = new MessageType("name", Arrays.asList());
        final MessageType requestedSchema = new MessageType("name", Arrays.asList());

        // Run the test
        final MessageColumnIO result = ParquetTypeUtils.getColumnIO(fileSchema, requestedSchema);

        // Verify the results
    }

    @Test
    public void testGetMapKeyValueColumn()
    {
        // Setup
        final GroupColumnIO inputGroupColumnIO = null;

        // Run the test
        final GroupColumnIO result = ParquetTypeUtils.getMapKeyValueColumn(inputGroupColumnIO);

        // Verify the results
    }

    @Test
    public void testGetArrayElementColumn()
    {
        // Setup
        final ColumnIO inputColumnIO = null;

        // Run the test
        final ColumnIO result = ParquetTypeUtils.getArrayElementColumn(inputColumnIO);

        // Verify the results
    }

    @Test
    public void testGetDescriptors()
    {
        // Setup
        final MessageType fileSchema = new MessageType("name", Arrays.asList());
        final MessageType requestedSchema = new MessageType("name", Arrays.asList());
        final Map<List<String>, RichColumnDescriptor> expectedResult = new HashMap<>();

        // Run the test
        final Map<List<String>, RichColumnDescriptor> result = ParquetTypeUtils.getDescriptors(fileSchema,
                requestedSchema);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDescriptor()
    {
        // Setup
        final List<PrimitiveColumnIO> columns = Arrays.asList();
        final Optional<RichColumnDescriptor> expectedResult = Optional.of(
                new RichColumnDescriptor(new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                        new PrimitiveType(
                                Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name")));

        // Run the test
        final Optional<RichColumnDescriptor> result = ParquetTypeUtils.getDescriptor(columns, Arrays.asList("value"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetFieldIndex()
    {
        // Setup
        final MessageType fileSchema = new MessageType("name", Arrays.asList());

        // Run the test
        final int result = ParquetTypeUtils.getFieldIndex(fileSchema, "name");

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetParquetEncoding()
    {
        assertEquals(ParquetEncoding.PLAIN, ParquetTypeUtils.getParquetEncoding(Encoding.PLAIN));
    }

    @Test
    public void testGetParquetTypeByName()
    {
        // Setup
        final MessageType messageType = new MessageType("name", Arrays.asList());
        final Type expectedResult = null;

        // Run the test
        final Type result = ParquetTypeUtils.getParquetTypeByName("columnName", messageType);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testLookupColumnByName()
    {
        // Setup
        final GroupColumnIO groupColumnIO = null;

        // Run the test
        final ColumnIO result = ParquetTypeUtils.lookupColumnByName(groupColumnIO, "columnName");

        // Verify the results
    }

    @Test
    public void testCreateDecimalType1()
    {
        // Setup
        final RichColumnDescriptor descriptor = new RichColumnDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));

        // Run the test
        final Optional<io.prestosql.spi.type.Type> result = ParquetTypeUtils.createDecimalType(descriptor);

        // Verify the results
    }

    @Test
    public void testIsValueNull()
    {
        assertTrue(ParquetTypeUtils.isValueNull(false, 0, 0));
    }

    @Test
    public void testGetShortDecimalValue()
    {
        assertEquals(0L, ParquetTypeUtils.getShortDecimalValue("content".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testLookupColumnById()
    {
        // Setup
        final GroupColumnIO groupColumnIO = null;

        // Run the test
        final ColumnIO result = ParquetTypeUtils.lookupColumnById(groupColumnIO, 0);

        // Verify the results
    }
}
