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

import io.prestosql.spi.type.Type;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ParquetWritersTest
{
    @Test
    public void testGetColumnWriters()
    {
        // Setup
        final MessageType messageType = new MessageType("name", Arrays.asList());
        final Map<List<String>, Type> trinoTypes = new HashMap<>();
        final ParquetProperties parquetProperties = ParquetProperties.builder().build();

        // Run the test
        final List<ColumnWriter> result = ParquetWriters.getColumnWriters(messageType, trinoTypes, parquetProperties,
                CompressionCodecName.UNCOMPRESSED);

        // Verify the results
    }
}
