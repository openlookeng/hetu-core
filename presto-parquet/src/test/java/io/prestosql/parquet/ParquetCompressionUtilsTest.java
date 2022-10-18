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

import io.airlift.slice.Slice;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ParquetCompressionUtilsTest
{
    @Test
    public void testDecompress1() throws Exception
    {
        // Setup
        final Slice input = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = ParquetCompressionUtils.decompress(CompressionCodecName.UNCOMPRESSED, input, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDecompress1_ThrowsIOException()
    {
        // Setup
        final Slice input = null;

        // Run the test
        assertThrows(
                IOException.class,
                () -> ParquetCompressionUtils.decompress(CompressionCodecName.UNCOMPRESSED, input, 0));
    }
}
