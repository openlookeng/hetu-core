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
package io.prestosql.parquet.reader;

import io.prestosql.parquet.DictionaryPage;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetEncoding;
import io.prestosql.parquet.RichColumnDescriptor;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.testng.Assert.assertEquals;

public class PrimitiveColumnReaderTest
{
    @Test
    public void testCreateReader()
    {
        // Setup
        final RichColumnDescriptor descriptor = new RichColumnDescriptor(
                new ColumnDescriptor(new String[]{"path"}, new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                new PrimitiveType(
                        Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"));
        final DateTimeZone timeZone = DateTimeZone.forID("id");

        // Run the test
        final PrimitiveColumnReader result = PrimitiveColumnReader.createReader(descriptor, timeZone);
        assertEquals(new PageReader(
                CompressionCodecName.UNCOMPRESSED, Arrays.asList(),
                new DictionaryPage(null, 0, 0, ParquetEncoding.PLAIN)), result.getPageReader());
        assertEquals(new ColumnDescriptor(new String[]{"path"},
                        new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"), 0, 0),
                result.getDescriptor());
        final Field field = null;
        assertEquals(new ColumnChunk(null, new int[]{0}, new int[]{0}), result.readPrimitive(field));
    }
}
