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

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.RowGroup;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class ParquetWriterTest
{
    @Mock
    private ParquetWriterOptions mockWriterOption;

    private ParquetWriter parquetWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        parquetWriterUnderTest = new ParquetWriter(new ByteArrayOutputStream(),
                new MessageType("name", Arrays.asList()), new HashMap<>(), mockWriterOption,
                CompressionCodecName.UNCOMPRESSED, "trinoVersion");
    }

    @Test
    public void testGetWrittenBytes()
    {
        // Setup
        // Run the test
        final long result = parquetWriterUnderTest.getWrittenBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedBytes()
    {
        // Setup
        // Run the test
        final long result = parquetWriterUnderTest.getRetainedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testWrite() throws Exception
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);
        when(mockWriterOption.getBatchSize()).thenReturn(0);
        when(mockWriterOption.getMaxRowGroupSize()).thenReturn(0L);
        when(mockWriterOption.getMaxPageSize()).thenReturn(0);

        // Run the test
        parquetWriterUnderTest.write(page);

        // Verify the results
    }

    @Test
    public void testWrite_ThrowsIOException()
    {
        // Setup
        final Page page = new Page(0, new Properties(), null);
        when(mockWriterOption.getBatchSize()).thenReturn(0);
        when(mockWriterOption.getMaxRowGroupSize()).thenReturn(0L);
        when(mockWriterOption.getMaxPageSize()).thenReturn(0);

        // Run the test
        assertThrows(IOException.class, () -> parquetWriterUnderTest.write(page));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        parquetWriterUnderTest.close();

        // Verify the results
    }

    @Test
    public void testClose_ThrowsIOException()
    {
        // Setup
        // Run the test
        assertThrows(IOException.class, () -> parquetWriterUnderTest.close());
    }

    @Test
    public void testGetFooter() throws Exception
    {
        // Setup
        final List<RowGroup> rowGroups = Arrays.asList(new RowGroup(Arrays.asList(new ColumnChunk(0L)), 0L, 0L));
        final MessageType messageType = new MessageType("name", Arrays.asList());
        final Slice expectedResult = null;

        // Run the test
        final Slice result = parquetWriterUnderTest.getFooter(rowGroups, messageType);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetFooter_ThrowsIOException()
    {
        // Setup
        final List<RowGroup> rowGroups = Arrays.asList(new RowGroup(Arrays.asList(new ColumnChunk(0L)), 0L, 0L));
        final MessageType messageType = new MessageType("name", Arrays.asList());

        // Run the test
        assertThrows(IOException.class, () -> parquetWriterUnderTest.getFooter(rowGroups, messageType));
    }

    @Test
    public void testFormatCreatedBy()
    {
        assertEquals("result", ParquetWriter.formatCreatedBy("trinoVersion"));
    }
}
