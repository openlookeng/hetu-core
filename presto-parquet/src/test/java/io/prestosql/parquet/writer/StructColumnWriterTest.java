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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class StructColumnWriterTest
{
    private StructColumnWriter structColumnWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        structColumnWriterUnderTest = new StructColumnWriter(Arrays.asList(), 0);
    }

    @Test
    public void testWriteBlock() throws Exception
    {
        // Setup
        final ColumnChunk columnChunk = null;

        // Run the test
        structColumnWriterUnderTest.writeBlock(columnChunk);

        // Verify the results
    }

    @Test
    public void testWriteBlock_ThrowsIOException()
    {
        // Setup
        final ColumnChunk columnChunk = null;

        // Run the test
        assertThrows(IOException.class, () -> structColumnWriterUnderTest.writeBlock(columnChunk));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        structColumnWriterUnderTest.close();

        // Verify the results
    }

    @Test
    public void testGetBuffer() throws Exception
    {
        // Setup
        // Run the test
        final List<ColumnWriter.BufferData> result = structColumnWriterUnderTest.getBuffer();

        // Verify the results
    }

    @Test
    public void testGetBuffer_ThrowsIOException()
    {
        // Setup
        // Run the test
        assertThrows(IOException.class, () -> structColumnWriterUnderTest.getBuffer());
    }

    @Test
    public void testGetBufferedBytes()
    {
        // Setup
        // Run the test
        final long result = structColumnWriterUnderTest.getBufferedBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedBytes()
    {
        // Setup
        // Run the test
        final long result = structColumnWriterUnderTest.getRetainedBytes();

        // Verify the results
        assertEquals(0L, result);
    }
}
