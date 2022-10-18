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
package io.prestosql.parquet.writer.valuewriter;

import io.prestosql.spi.block.Block;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class PrimitiveValueWriterTest
{
    @Mock
    private ValuesWriter mockValuesWriter;

    private PrimitiveValueWriter primitiveValueWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        primitiveValueWriterUnderTest = new PrimitiveValueWriter(
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT64, 0, "name"),
                mockValuesWriter) {
            @Override
            public void write(Block block)
            {
            }
        };
    }

    @Test
    public void testGetValueWriter()
    {
        // Setup
        // Run the test
        final ValuesWriter result = primitiveValueWriterUnderTest.getValueWriter();

        // Verify the results
    }

    @Test
    public void testGetTypeLength()
    {
        // Setup
        // Run the test
        final int result = primitiveValueWriterUnderTest.getTypeLength();

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetBufferedSize()
    {
        // Setup
        when(mockValuesWriter.getBufferedSize()).thenReturn(0L);

        // Run the test
        final long result = primitiveValueWriterUnderTest.getBufferedSize();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetBytes()
    {
        // Setup
        when(mockValuesWriter.getBytes()).thenReturn(BytesInput.concat(Arrays.asList()));

        // Run the test
        final BytesInput result = primitiveValueWriterUnderTest.getBytes();

        // Verify the results
    }

    @Test
    public void testGetEncoding()
    {
        // Setup
        when(mockValuesWriter.getEncoding()).thenReturn(Encoding.PLAIN);

        // Run the test
        final Encoding result = primitiveValueWriterUnderTest.getEncoding();

        // Verify the results
        assertEquals(Encoding.PLAIN, result);
    }

    @Test
    public void testReset() throws Exception
    {
        // Setup
        // Run the test
        primitiveValueWriterUnderTest.reset();

        // Verify the results
        verify(mockValuesWriter).reset();
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        primitiveValueWriterUnderTest.close();

        // Verify the results
        verify(mockValuesWriter).close();
    }

    @Test
    public void testToDictPageAndClose()
    {
        // Setup
        // Configure ValuesWriter.toDictPageAndClose(...).
        final DictionaryPage dictionaryPage = new DictionaryPage(BytesInput.concat(Arrays.asList()), 0, Encoding.PLAIN);
        when(mockValuesWriter.toDictPageAndClose()).thenReturn(dictionaryPage);

        // Run the test
        final DictionaryPage result = primitiveValueWriterUnderTest.toDictPageAndClose();

        // Verify the results
    }

    @Test
    public void testToDictPageAndClose_ValuesWriterReturnsNull()
    {
        // Setup
        when(mockValuesWriter.toDictPageAndClose()).thenReturn(null);

        // Run the test
        final DictionaryPage result = primitiveValueWriterUnderTest.toDictPageAndClose();

        // Verify the results
        assertNull(result);
    }

    @Test
    public void testResetDictionary()
    {
        // Setup
        // Run the test
        primitiveValueWriterUnderTest.resetDictionary();

        // Verify the results
        verify(mockValuesWriter).resetDictionary();
    }

    @Test
    public void testGetAllocatedSize()
    {
        // Setup
        when(mockValuesWriter.getAllocatedSize()).thenReturn(0L);

        // Run the test
        final long result = primitiveValueWriterUnderTest.getAllocatedSize();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testMemUsageString()
    {
        // Setup
        when(mockValuesWriter.memUsageString("prefix")).thenReturn("result");

        // Run the test
        final String result = primitiveValueWriterUnderTest.memUsageString("prefix");

        // Verify the results
        assertEquals("result", result);
    }
}
