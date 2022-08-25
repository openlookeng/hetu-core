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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.type.TypeManager;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class MapBlockEncodingTest
{
    @Mock
    private TypeManager mockTypeManager;

    private MapBlockEncoding mapBlockEncodingUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        mapBlockEncodingUnderTest = new MapBlockEncoding(mockTypeManager);
    }

    @Test
    public void testGetName() throws Exception
    {
        assertEquals("MAP", mapBlockEncodingUnderTest.getName());
    }

    @Test
    public void testWriteBlock1()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final SliceOutput sliceOutput = null;
        final Block block = null;

        // Run the test
        mapBlockEncodingUnderTest.writeBlock(blockEncodingSerde, sliceOutput, block);

        // Verify the results
    }

    @Test
    public void testReadBlock1()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final SliceInput sliceInput = null;

        // Run the test
        final Block result = mapBlockEncodingUnderTest.readBlock(blockEncodingSerde, sliceInput);

        // Verify the results
    }

    @Test
    public void testReadBlock2()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream inputStream = new ByteArrayInputStream("content".getBytes());

        // Run the test
        final Block result = mapBlockEncodingUnderTest.readBlock(blockEncodingSerde, inputStream);

        // Verify the results
    }

    @Test
    public void testReadBlock2_EmptyInputStream()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream inputStream = new ByteArrayInputStream(new byte[]{});

        // Run the test
        final Block result = mapBlockEncodingUnderTest.readBlock(blockEncodingSerde, inputStream);

        // Verify the results
    }

    @Test
    public void testReadBlock2_BrokenInputStream()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream inputStream = new InputStream() {
            private final IOException exception = new IOException("Error");

            @Override
            public int read() throws IOException
            {
                throw exception;
            }

            @Override
            public int available() throws IOException
            {
                throw exception;
            }

            @Override
            public long skip(final long n) throws IOException
            {
                throw exception;
            }

            @Override
            public synchronized void reset() throws IOException
            {
                throw exception;
            }

            @Override
            public void close() throws IOException
            {
                throw exception;
            }
        };

        // Run the test
        final Block result = mapBlockEncodingUnderTest.readBlock(blockEncodingSerde, inputStream);

        // Verify the results
    }

    @Test
    public void testWriteBlock2() throws Exception
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final OutputStream outputStream = new ByteArrayOutputStream();
        final Block block = null;

        // Run the test
        mapBlockEncodingUnderTest.writeBlock(blockEncodingSerde, outputStream, block);

        // Verify the results
    }

    @Test
    public void testWriteBlock2_BrokenOutputStream() throws Exception
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final OutputStream outputStream = new OutputStream()
        {
            private final IOException exception = new IOException("Error");

            @Override
            public void write(final int b) throws IOException
            {
                throw exception;
            }

            @Override
            public void flush() throws IOException
            {
                throw exception;
            }

            @Override
            public void close() throws IOException
            {
                throw exception;
            }
        };
        final Block block = null;

        // Run the test
        mapBlockEncodingUnderTest.writeBlock(blockEncodingSerde, outputStream, block);

        // Verify the results
    }
}
