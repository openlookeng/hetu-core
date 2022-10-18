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

public class ArrayColumnWriterTest
{
    @Mock
    private ColumnWriter mockElementWriter;

    private ArrayColumnWriter arrayColumnWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        arrayColumnWriterUnderTest = new ArrayColumnWriter(mockElementWriter, 0, 0);
    }

    @Test
    public void testWriteBlock() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;

        // Run the test
        arrayColumnWriterUnderTest.writeBlock(columnChunk);

        // Verify the results
        verify(mockElementWriter).writeBlock(any(ColumnChunk.class));
    }

    @Test
    public void testWriteBlock_ColumnWriterThrowsIOException() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;
        doThrow(IOException.class).when(mockElementWriter).writeBlock(any(ColumnChunk.class));

        // Run the test
        assertThrows(IOException.class, () -> arrayColumnWriterUnderTest.writeBlock(columnChunk));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        arrayColumnWriterUnderTest.close();

        // Verify the results
        verify(mockElementWriter).close();
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
        when(mockElementWriter.getBuffer()).thenReturn(bufferData);

        // Run the test
        final List<ColumnWriter.BufferData> result = arrayColumnWriterUnderTest.getBuffer();

        // Verify the results
    }

    @Test
    public void testGetBuffer_ColumnWriterReturnsNoItems() throws Exception
    {
        // Setup
        when(mockElementWriter.getBuffer()).thenReturn(Collections.emptyList());

        // Run the test
        final List<ColumnWriter.BufferData> result = arrayColumnWriterUnderTest.getBuffer();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testGetBuffer_ColumnWriterThrowsIOException() throws Exception
    {
        // Setup
        when(mockElementWriter.getBuffer()).thenThrow(IOException.class);

        // Run the test
        assertThrows(IOException.class, () -> arrayColumnWriterUnderTest.getBuffer());
    }

    @Test
    public void testGetBufferedBytes()
    {
        // Setup
        when(mockElementWriter.getBufferedBytes()).thenReturn(0L);

        // Run the test
        final long result = arrayColumnWriterUnderTest.getBufferedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedBytes()
    {
        // Setup
        when(mockElementWriter.getRetainedBytes()).thenReturn(0L);

        // Run the test
        final long result = arrayColumnWriterUnderTest.getRetainedBytes();

        // Verify the results
        assertEquals(0L, result);
    }
}
