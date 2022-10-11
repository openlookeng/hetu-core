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
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.testng.annotations.Test;

public class ParquetReaderUtilsTest
{
    @Test
    public void testToInputStream1()
    {
        // Setup
        final Slice slice = null;

        // Run the test
        final ByteBufferInputStream result = ParquetReaderUtils.toInputStream(slice);

        // Verify the results
    }

    @Test
    public void testToInputStream2()
    {
        // Setup
        final DictionaryPage page = new DictionaryPage(null, 0, 0, ParquetEncoding.PLAIN);

        // Run the test
        final ByteBufferInputStream result = ParquetReaderUtils.toInputStream(page);

        // Verify the results
    }
}
