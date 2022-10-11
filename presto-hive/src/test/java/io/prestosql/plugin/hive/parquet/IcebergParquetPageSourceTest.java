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

import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.Page;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.OptionalLong;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class IcebergParquetPageSourceTest
{
    @Mock
    private ParquetReader mockParquetReader;

    private IcebergParquetPageSource icebergParquetPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        icebergParquetPageSourceUnderTest = new IcebergParquetPageSource(mockParquetReader, Arrays.asList(new FieldSetterFactoryTest.Type()), Arrays.asList(false), Arrays.asList(Optional.empty()));
    }

    @Test
    public void testGetCompletedBytes() throws Exception
    {
        final long result = icebergParquetPageSourceUnderTest.getCompletedBytes();
    }

    @Test
    public void testGetCompletedPositions()
    {
        // Setup
        final OptionalLong result = icebergParquetPageSourceUnderTest.getCompletedPositions();
    }

    @Test
    public void testGetReadTimeNanos() throws Exception
    {
        // Setup
        // Configure ParquetReader.getDataSource(...).
        final long result = icebergParquetPageSourceUnderTest.getReadTimeNanos();
    }

    @Test
    public void testIsFinished()
    {
        icebergParquetPageSourceUnderTest.isFinished();
    }

    @Test
    public void testGetMemoryUsage()
    {
        // Setup
        // Configure ParquetReader.getSystemMemoryContext(...).
        final long result = icebergParquetPageSourceUnderTest.getMemoryUsage();
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        final Page result = icebergParquetPageSourceUnderTest.getNextPage();
    }

    @Test
    public void testGetNextPage_ParquetReaderCloseThrowsIOException() throws Exception
    {
        // Setup
        when(mockParquetReader.nextBatch()).thenReturn(0);
        doThrow(IOException.class).when(mockParquetReader).close();

        // Run the test
        final Page result = icebergParquetPageSourceUnderTest.getNextPage();

        // Verify the results
    }

    @Test
    public void testGetSystemMemoryUsage()
    {
        // Setup
        // Configure ParquetReader.getSystemMemoryContext(...).
        final long result = icebergParquetPageSourceUnderTest.getSystemMemoryUsage();
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        icebergParquetPageSourceUnderTest.close();
    }

    @Test
    public void testClose_ParquetReaderThrowsIOException() throws Exception
    {
        // Setup
        doThrow(IOException.class).when(mockParquetReader).close();

        // Run the test
        icebergParquetPageSourceUnderTest.close();

        // Verify the results
    }
}
