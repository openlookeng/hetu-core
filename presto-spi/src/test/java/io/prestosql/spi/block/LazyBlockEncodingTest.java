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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class LazyBlockEncodingTest
{
    private LazyBlockEncoding lazyBlockEncodingUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        lazyBlockEncodingUnderTest = new LazyBlockEncoding();
    }

    @Test
    public void testGetName() throws Exception
    {
        assertEquals("LAZY", lazyBlockEncodingUnderTest.getName());
    }

    @Test
    public void testReadBlock1()
    {
        // Setup
        // Run the test
        final Block result = lazyBlockEncodingUnderTest.readBlock(null, null);

        // Verify the results
    }

    @Test
    public void testWriteBlock1()
    {
        // Setup
        // Run the test
        lazyBlockEncodingUnderTest.writeBlock(null, null, null);

        // Verify the results
    }

    @Test
    public void testReadBlock2()
    {
        // Setup
        final InputStream input = new ByteArrayInputStream("content".getBytes());

        // Run the test
        final Block result = lazyBlockEncodingUnderTest.readBlock(null, input);

        // Verify the results
    }

    @Test
    public void testReadBlock2_EmptyInput()
    {
        // Setup
        final InputStream input = new ByteArrayInputStream(new byte[]{});

        // Run the test
        final Block result = lazyBlockEncodingUnderTest.readBlock(null, input);

        // Verify the results
    }

    @Test
    public void testReadBlock2_BrokenInput()
    {
        // Setup
        final InputStream input = new InputStream()
        {
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
        final Block result = lazyBlockEncodingUnderTest.readBlock(null, input);

        // Verify the results
    }

    @Test
    public void testWriteBlock2() throws Exception
    {
        // Setup
        final OutputStream output = new ByteArrayOutputStream();

        // Run the test
        lazyBlockEncodingUnderTest.writeBlock(null, output, null);

        // Verify the results
    }

    @Test
    public void testWriteBlock2_BrokenOutput() throws Exception
    {
        // Setup
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

        // Run the test
        lazyBlockEncodingUnderTest.writeBlock(null, output, null);

        // Verify the results
    }

    @Test
    public void testReplacementBlockForWrite()
    {
        // Setup
        final Block block = null;

        // Run the test
        final Optional<Block> result = lazyBlockEncodingUnderTest.replacementBlockForWrite(block);

        // Verify the results
    }
}
