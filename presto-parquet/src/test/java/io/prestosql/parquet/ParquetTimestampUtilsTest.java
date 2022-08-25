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

import io.prestosql.plugin.base.type.DecodedTimestamp;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class ParquetTimestampUtilsTest
{
    @Test
    public void testGetTimestampMillis()
    {
        // Setup
        final Binary timestampBinary = Binary.fromReusedByteArray("content".getBytes());

        // Run the test
        final long result = ParquetTimestampUtils.getTimestampMillis(timestampBinary);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testDecodeInt96Timestamp()
    {
        // Setup
        final Binary timestampBinary = Binary.fromReusedByteArray("content".getBytes());

        // Run the test
        final DecodedTimestamp result = ParquetTimestampUtils.decodeInt96Timestamp(timestampBinary);

        // Verify the results
    }

    @Test
    public void testDecodeInt64Timestamp()
    {
        // Setup
        // Run the test
        final DecodedTimestamp result = ParquetTimestampUtils.decodeInt64Timestamp(0L,
                LogicalTypeAnnotation.TimeUnit.MILLIS);

        // Verify the results
    }
}
