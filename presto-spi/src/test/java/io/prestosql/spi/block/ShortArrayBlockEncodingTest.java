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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class ShortArrayBlockEncodingTest
{
    private ShortArrayBlockEncoding shortArrayBlockEncodingUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        shortArrayBlockEncodingUnderTest = new ShortArrayBlockEncoding();
    }

    @Test
    public void testGetName() throws Exception
    {
        assertEquals("SHORT_ARRAY", shortArrayBlockEncodingUnderTest.getName());
    }

    @Test
    public void testWriteBlock1()
    {
        // Setup
        final SliceOutput sliceOutput = null;
        final Block block = null;

        // Run the test
        shortArrayBlockEncodingUnderTest.writeBlock(null, sliceOutput, block);

        // Verify the results
    }

    @Test
    public void testReadBlock1()
    {
        // Setup
        final SliceInput sliceInput = null;

        // Run the test
        final Block result = shortArrayBlockEncodingUnderTest.readBlock(null, sliceInput);

        // Verify the results
    }

    @Test
    public void testWrite() throws Exception
    {
        // Setup
        final Kryo kryo = new Kryo();
        final Output output = new Output(0, 0);
        final ShortArrayBlock block = new ShortArrayBlock(0, Optional.of(new boolean[]{false}), new short[]{(short) 0});

        // Run the test
        shortArrayBlockEncodingUnderTest.write(kryo, output, block);

        // Verify the results
    }

    @Test
    public void testReadBlock2()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream input = new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Block result = shortArrayBlockEncodingUnderTest.readBlock(blockEncodingSerde, input);

        // Verify the results
    }

    @Test
    public void testReadBlock2_EmptyInput()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream input = new ByteArrayInputStream(new byte[]{});

        // Run the test
        final Block result = shortArrayBlockEncodingUnderTest.readBlock(blockEncodingSerde, input);

        // Verify the results
    }

    @Test
    public void testReadBlock2_BrokenInput()
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final InputStream input = new InputStream() {
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
        final Block result = shortArrayBlockEncodingUnderTest.readBlock(blockEncodingSerde, input);

        // Verify the results
    }

    @Test
    public void testWriteBlock2() throws Exception
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final OutputStream output = new ByteArrayOutputStream();
        final Block block = null;

        // Run the test
        shortArrayBlockEncodingUnderTest.writeBlock(blockEncodingSerde, output, block);

        // Verify the results
    }

    @Test
    public void testWriteBlock2_BrokenOutput() throws Exception
    {
        // Setup
        final BlockEncodingSerde blockEncodingSerde = null;
        final OutputStream output = new OutputStream()
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
        shortArrayBlockEncodingUnderTest.writeBlock(blockEncodingSerde, output, block);

        // Verify the results
    }
}
