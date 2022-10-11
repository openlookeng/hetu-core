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
package io.prestosql.plugin.hive.parquet;

import io.airlift.units.DataSize;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParquetWriterConfigTest
{
    private ParquetWriterConfig parquetWriterConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        parquetWriterConfigUnderTest = new ParquetWriterConfig();
    }

    @Test
    public void testGetPageSize()
    {
        DataSize pageSize = parquetWriterConfigUnderTest.getPageSize();
        parquetWriterConfigUnderTest.setPageSize(pageSize);
    }

    @Test
    public void testIsParquetOptimizedWriterEnabled()
    {
        boolean parquetOptimizedWriterEnabled = parquetWriterConfigUnderTest.isParquetOptimizedWriterEnabled();
        parquetWriterConfigUnderTest.setParquetOptimizedWriterEnabled(parquetOptimizedWriterEnabled);
    }

    @Test
    public void testGetBlockSize()
    {
        DataSize blockSize = parquetWriterConfigUnderTest.getBlockSize();
        parquetWriterConfigUnderTest.setBlockSize(blockSize);
    }

    @Test
    public void testToParquetWriterOptions()
    {
        // Setup
        // Run the test
        final ParquetWriterOptions result = parquetWriterConfigUnderTest.toParquetWriterOptions();

        // Verify the results
    }
}
