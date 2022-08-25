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

import io.airlift.units.DataSize;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParquetReaderOptionsTest
{
    private ParquetReaderOptions parquetReaderOptionsUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        parquetReaderOptionsUnderTest = new ParquetReaderOptions();
    }

    @Test
    public void testWithIgnoreStatistics()
    {
        // Setup
        // Run the test
        final ParquetReaderOptions result = parquetReaderOptionsUnderTest.withIgnoreStatistics(false);

        // Verify the results
    }

    @Test
    public void testWithMaxReadBlockSize()
    {
        // Setup
        final DataSize maxReadBlockSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderOptions result = parquetReaderOptionsUnderTest.withMaxReadBlockSize(maxReadBlockSize);

        // Verify the results
    }

    @Test
    public void testWithMaxMergeDistance()
    {
        // Setup
        final DataSize maxMergeDistance = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderOptions result = parquetReaderOptionsUnderTest.withMaxMergeDistance(maxMergeDistance);

        // Verify the results
    }

    @Test
    public void testWithMaxBufferSize()
    {
        // Setup
        final DataSize maxBufferSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderOptions result = parquetReaderOptionsUnderTest.withMaxBufferSize(maxBufferSize);

        // Verify the results
    }

    @Test
    public void testWithUseColumnIndex()
    {
        // Setup
        // Run the test
        final ParquetReaderOptions result = parquetReaderOptionsUnderTest.withUseColumnIndex(false);

        // Verify the results
    }
}
