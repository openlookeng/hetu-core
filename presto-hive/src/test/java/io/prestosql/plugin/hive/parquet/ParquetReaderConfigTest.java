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
import io.prestosql.parquet.ParquetReaderOptions;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ParquetReaderConfigTest
{
    private ParquetReaderConfig parquetReaderConfigUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        parquetReaderConfigUnderTest = new ParquetReaderConfig();
    }

    @Test
    public void testIsIgnoreStatistics()
    {
        // Setup
        // Run the test
        final boolean result = parquetReaderConfigUnderTest.isIgnoreStatistics();
    }

    @Test
    public void testSetIgnoreStatistics()
    {
        // Setup
        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setIgnoreStatistics(false);

        // Verify the results
    }

    @Test
    public void testSetFailOnCorruptedStatistics()
    {
        // Setup
        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setFailOnCorruptedStatistics(false);

        // Verify the results
    }

    @Test
    public void testGetMaxReadBlockSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = parquetReaderConfigUnderTest.getMaxReadBlockSize();
    }

    @Test
    public void testSetMaxReadBlockSize()
    {
        // Setup
        final DataSize maxReadBlockSize = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setMaxReadBlockSize(maxReadBlockSize);

        // Verify the results
    }

    @Test
    public void testGetMaxMergeDistance()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = parquetReaderConfigUnderTest.getMaxMergeDistance();
    }

    @Test
    public void testSetMaxMergeDistance()
    {
        // Setup
        final DataSize distance = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setMaxMergeDistance(distance);

        // Verify the results
    }

    @Test
    public void testGetMaxBufferSize()
    {
        // Setup
        final DataSize expectedResult = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final DataSize result = parquetReaderConfigUnderTest.getMaxBufferSize();
    }

    @Test
    public void testSetMaxBufferSize()
    {
        // Setup
        final DataSize size = new DataSize(0.0, DataSize.Unit.BYTE);

        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setMaxBufferSize(size);

        // Verify the results
    }

    @Test
    public void testSetUseColumnIndex()
    {
        // Setup
        // Run the test
        final ParquetReaderConfig result = parquetReaderConfigUnderTest.setUseColumnIndex(false);

        // Verify the results
    }

    @Test
    public void testIsUseColumnIndex()
    {
        // Setup
        // Run the test
        final boolean result = parquetReaderConfigUnderTest.isUseColumnIndex();
    }

    @Test
    public void testToParquetReaderOptions()
    {
        // Setup
        // Run the test
        final ParquetReaderOptions result = parquetReaderConfigUnderTest.toParquetReaderOptions();

        // Verify the results
    }
}
