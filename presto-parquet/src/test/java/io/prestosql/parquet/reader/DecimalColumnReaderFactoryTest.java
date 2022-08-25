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

import io.prestosql.parquet.RichColumnDescriptor;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.testng.annotations.Test;

public class DecimalColumnReaderFactoryTest
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

        // Run the test
        final PrimitiveColumnReader result = DecimalColumnReaderFactory.createReader(descriptor, 0, 0);

        // Verify the results
    }
}
