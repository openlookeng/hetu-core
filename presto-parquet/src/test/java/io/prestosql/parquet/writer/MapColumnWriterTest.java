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
import org.apache.parquet.format.ColumnMetaData;
import org.apache.parquet.format.CompressionCodec;
import org.apache.parquet.format.Encoding;
import org.apache.parquet.format.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class MapColumnWriterTest
{
    @Mock
    private ColumnWriter mockKeyWriter;
    @Mock
    private ColumnWriter mockValueWriter;

    private MapColumnWriter mapColumnWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        mapColumnWriterUnderTest = new MapColumnWriter(mockKeyWriter, mockValueWriter, 0, 0);
    }

    @Test
    public void testWriteBlock() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;

        // Run the test
        mapColumnWriterUnderTest.writeBlock(columnChunk);

        // Verify the results
        verify(mockKeyWriter).writeBlock(any(ColumnChunk.class));
        verify(mockValueWriter).writeBlock(any(ColumnChunk.class));
    }

    @Test
    public void testWriteBlock_KeyWriterThrowsIOException() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;
        doThrow(IOException.class).when(mockKeyWriter).writeBlock(any(ColumnChunk.class));

        // Run the test
        assertThrows(IOException.class, () -> mapColumnWriterUnderTest.writeBlock(columnChunk));
        verify(mockValueWriter).writeBlock(any(ColumnChunk.class));
    }

    @Test
    public void testWriteBlock_ValueWriterThrowsIOException() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;
        doThrow(IOException.class).when(mockValueWriter).writeBlock(any(ColumnChunk.class));

        // Run the test
        assertThrows(IOException.class, () -> mapColumnWriterUnderTest.writeBlock(columnChunk));
        verify(mockKeyWriter).writeBlock(any(ColumnChunk.class));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        mapColumnWriterUnderTest.close();

        // Verify the results
        verify(mockKeyWriter).close();
        verify(mockValueWriter).close();
    }

    @Test
    public void testGetBuffer() throws Exception
    {
        // Setup
        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockKeyWriter.getBuffer()).thenReturn(bufferData);

        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData1 = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockValueWriter.getBuffer()).thenReturn(bufferData1);

        // Run the test
        final List<ColumnWriter.BufferData> result = mapColumnWriterUnderTest.getBuffer();

        // Verify the results
    }

    @Test
    public void testGetBuffer_KeyWriterReturnsNoItems() throws Exception
    {
        // Setup
        when(mockKeyWriter.getBuffer()).thenReturn(Collections.emptyList());

        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockValueWriter.getBuffer()).thenReturn(bufferData);

        // Run the test
        final List<ColumnWriter.BufferData> result = mapColumnWriterUnderTest.getBuffer();

        // Verify the results
    }

    @Test
    public void testGetBuffer_KeyWriterThrowsIOException() throws Exception
    {
        // Setup
        when(mockKeyWriter.getBuffer()).thenThrow(IOException.class);

        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockValueWriter.getBuffer()).thenReturn(bufferData);

        // Run the test
        assertThrows(IOException.class, () -> mapColumnWriterUnderTest.getBuffer());
    }

    @Test
    public void testGetBuffer_ValueWriterReturnsNoItems() throws Exception
    {
        // Setup
        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockKeyWriter.getBuffer()).thenReturn(bufferData);

        when(mockValueWriter.getBuffer()).thenReturn(Collections.emptyList());

        // Run the test
        final List<ColumnWriter.BufferData> result = mapColumnWriterUnderTest.getBuffer();

        // Verify the results
    }

    @Test
    public void testGetBuffer_ValueWriterThrowsIOException() throws Exception
    {
        // Setup
        // Configure ColumnWriter.getBuffer(...).
        final List<ColumnWriter.BufferData> bufferData = Arrays.asList(
                new ColumnWriter.BufferData(Arrays.asList(ParquetDataOutput.createDataOutput((Slice) null)), new ColumnMetaData(
                        Type.BOOLEAN, Arrays.asList(Encoding.PLAIN), Arrays.asList("value"),
                        CompressionCodec.UNCOMPRESSED, 0L, 0L, 0L, 0L)));
        when(mockKeyWriter.getBuffer()).thenReturn(bufferData);

        when(mockValueWriter.getBuffer()).thenThrow(IOException.class);

        // Run the test
        assertThrows(IOException.class, () -> mapColumnWriterUnderTest.getBuffer());
    }

    @Test
    public void testGetBufferedBytes()
    {
        // Setup
        when(mockKeyWriter.getBufferedBytes()).thenReturn(0L);
        when(mockValueWriter.getBufferedBytes()).thenReturn(0L);

        // Run the test
        final long result = mapColumnWriterUnderTest.getBufferedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedBytes()
    {
        // Setup
        when(mockKeyWriter.getRetainedBytes()).thenReturn(0L);
        when(mockValueWriter.getRetainedBytes()).thenReturn(0L);

        // Run the test
        final long result = mapColumnWriterUnderTest.getRetainedBytes();

        // Verify the results
        assertEquals(0L, result);
    }
}
