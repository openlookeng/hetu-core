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

import io.prestosql.parquet.writer.valuewriter.PrimitiveValueWriter;
import io.prestosql.spi.block.Block;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class PrimitiveColumnWriterTest
{
    @Mock
    private ColumnDescriptor mockColumnDescriptor;
    @Mock
    private PrimitiveValueWriter mockPrimitiveValueWriter;
    @Mock
    private ValuesWriter mockDefinitionLevelWriter;
    @Mock
    private ValuesWriter mockRepetitionLevelWriter;

    private PrimitiveColumnWriter primitiveColumnWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        primitiveColumnWriterUnderTest = new PrimitiveColumnWriter(mockColumnDescriptor, mockPrimitiveValueWriter,
                mockDefinitionLevelWriter, mockRepetitionLevelWriter,
                CompressionCodecName.UNCOMPRESSED, 0);
    }

    @Test
    public void testWriteBlock() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;
        when(mockDefinitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockPrimitiveValueWriter.getBufferedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Run the test
        primitiveColumnWriterUnderTest.writeBlock(columnChunk);

        // Verify the results
        verify(mockPrimitiveValueWriter).write(any(Block.class));
        verify(mockDefinitionLevelWriter).writeInteger(0);
        verify(mockRepetitionLevelWriter).writeInteger(0);
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
    }

    @Test
    public void testWriteBlock_ThrowsIOException()
    {
        // Setup
        final ColumnChunk columnChunk = null;
        when(mockDefinitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockPrimitiveValueWriter.getBufferedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Run the test
        assertThrows(IOException.class, () -> primitiveColumnWriterUnderTest.writeBlock(columnChunk));
        verify(mockPrimitiveValueWriter).write(any(Block.class));
        verify(mockDefinitionLevelWriter).writeInteger(0);
        verify(mockRepetitionLevelWriter).writeInteger(0);
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        primitiveColumnWriterUnderTest.close();

        // Verify the results
    }

    @Test
    public void testGetBuffer() throws Exception
    {
        // Setup
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Configure PrimitiveValueWriter.toDictPageAndClose(...).
        final DictionaryPage dictionaryPage = new DictionaryPage(BytesInput.concat(Arrays.asList()), 0, Encoding.PLAIN);
        when(mockPrimitiveValueWriter.toDictPageAndClose()).thenReturn(dictionaryPage);

        // Configure ColumnDescriptor.getPrimitiveType(...).
        final PrimitiveType primitiveType = new PrimitiveType(Type.Repetition.REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, 0, "name");
        when(mockColumnDescriptor.getPrimitiveType()).thenReturn(primitiveType);

        when(mockColumnDescriptor.getPath()).thenReturn(new String[]{"result"});

        // Run the test
        final List<ColumnWriter.BufferData> result = primitiveColumnWriterUnderTest.getBuffer();

        // Verify the results
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
        verify(mockPrimitiveValueWriter).resetDictionary();
    }

    @Test
    public void testGetBuffer_PrimitiveValueWriterToDictPageAndCloseReturnsNull() throws Exception
    {
        // Setup
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.toDictPageAndClose()).thenReturn(null);

        // Configure ColumnDescriptor.getPrimitiveType(...).
        final PrimitiveType primitiveType = new PrimitiveType(Type.Repetition.REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, 0, "name");
        when(mockColumnDescriptor.getPrimitiveType()).thenReturn(primitiveType);

        when(mockColumnDescriptor.getPath()).thenReturn(new String[]{"result"});

        // Run the test
        final List<ColumnWriter.BufferData> result = primitiveColumnWriterUnderTest.getBuffer();

        // Verify the results
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
        verify(mockPrimitiveValueWriter).resetDictionary();
    }

    @Test
    public void testGetBuffer_ColumnDescriptorGetPathReturnsNoItems() throws Exception
    {
        // Setup
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Configure PrimitiveValueWriter.toDictPageAndClose(...).
        final DictionaryPage dictionaryPage = new DictionaryPage(BytesInput.concat(Arrays.asList()), 0, Encoding.PLAIN);
        when(mockPrimitiveValueWriter.toDictPageAndClose()).thenReturn(dictionaryPage);

        // Configure ColumnDescriptor.getPrimitiveType(...).
        final PrimitiveType primitiveType = new PrimitiveType(Type.Repetition.REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, 0, "name");
        when(mockColumnDescriptor.getPrimitiveType()).thenReturn(primitiveType);

        when(mockColumnDescriptor.getPath()).thenReturn(new String[]{});

        // Run the test
        final List<ColumnWriter.BufferData> result = primitiveColumnWriterUnderTest.getBuffer();

        // Verify the results
        assertEquals(Collections.emptyList(), result);
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
        verify(mockPrimitiveValueWriter).resetDictionary();
    }

    @Test
    public void testGetBuffer_ThrowsIOException()
    {
        // Setup
        when(mockRepetitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockDefinitionLevelWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        when(mockPrimitiveValueWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));
        doReturn(Statistics.createStats(null)).when(mockPrimitiveValueWriter).getStatistics();
        when(mockRepetitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockDefinitionLevelWriter.getEncoding()).thenReturn(Encoding.PLAIN);
        when(mockPrimitiveValueWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Configure PrimitiveValueWriter.toDictPageAndClose(...).
        final DictionaryPage dictionaryPage = new DictionaryPage(BytesInput.concat(Arrays.asList()), 0, Encoding.PLAIN);
        when(mockPrimitiveValueWriter.toDictPageAndClose()).thenReturn(dictionaryPage);

        // Configure ColumnDescriptor.getPrimitiveType(...).
        final PrimitiveType primitiveType = new PrimitiveType(Type.Repetition.REQUIRED,
                PrimitiveType.PrimitiveTypeName.INT64, 0, "name");
        when(mockColumnDescriptor.getPrimitiveType()).thenReturn(primitiveType);

        when(mockColumnDescriptor.getPath()).thenReturn(new String[]{"result"});

        // Run the test
        assertThrows(IOException.class, () -> primitiveColumnWriterUnderTest.getBuffer());
        verify(mockRepetitionLevelWriter).reset();
        verify(mockDefinitionLevelWriter).reset();
        verify(mockPrimitiveValueWriter).reset();
        verify(mockPrimitiveValueWriter).resetDictionary();
    }

    @Test
    public void testGetBufferedBytes()
    {
        // Setup
        when(mockDefinitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getBufferedSize()).thenReturn(0L);
        when(mockPrimitiveValueWriter.getBufferedSize()).thenReturn(0L);

        // Run the test
        final long result = primitiveColumnWriterUnderTest.getBufferedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedBytes()
    {
        // Setup
        when(mockPrimitiveValueWriter.getAllocatedSize()).thenReturn(0L);
        when(mockDefinitionLevelWriter.getAllocatedSize()).thenReturn(0L);
        when(mockRepetitionLevelWriter.getAllocatedSize()).thenReturn(0L);

        // Run the test
        final long result = primitiveColumnWriterUnderTest.getRetainedBytes();

        // Verify the results
        assertEquals(0L, result);
    }
}
