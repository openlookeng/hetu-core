/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.spi.filesystem;

import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.OpenOption;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class FileBasedLockTest
{
    @Mock
    private HetuFileSystemClient mockFs;

    private FileBasedLock fileBasedLockUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        fileBasedLockUnderTest = new FileBasedLock(mockFs, Paths.get("filename.txt"), 0L, 0L, 0L);
    }

    @Test
    public void testLock() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientNewOutputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new OutputStream()
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
        });
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientNewOutputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenThrow(IOException.class);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
    }

    @Test
    public void testLock_HetuFileSystemClientGetAttributeThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenThrow(IOException.class);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientDeleteThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        doThrow(IOException.class).when(mockFs).delete(Paths.get("filename.txt"));

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientNewInputStreamReturnsNoContent() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream(new byte[]{}));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientNewInputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new InputStream() {
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
        });
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testLock_HetuFileSystemClientNewInputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenThrow(IOException.class);

        // Run the test
        fileBasedLockUnderTest.lock();

        // Verify the results
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testLockInterruptibly() throws Exception
    {
        // Setup
        // Run the test
        fileBasedLockUnderTest.lockInterruptibly();

        // Verify the results
    }

    @Test
    public void testLockInterruptibly_ThrowsInterruptedException()
    {
        // Setup
        // Run the test
        assertThrows(InterruptedException.class, () -> fileBasedLockUnderTest.lockInterruptibly());
    }

    @Test
    public void testTryLock1() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientNewOutputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new OutputStream()
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
        });
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientNewOutputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testTryLock1_HetuFileSystemClientGetAttributeThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientDeleteThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        doThrow(IOException.class).when(mockFs).delete(Paths.get("filename.txt"));

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientNewInputStreamReturnsNoContent() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream(new byte[]{}));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientNewInputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new InputStream() {
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
        });
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock1_HetuFileSystemClientNewInputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testTryLock2() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientNewOutputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new OutputStream()
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
        });
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        assertThrows(InterruptedException.class, () -> fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS));
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientNewOutputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testTryLock2_HetuFileSystemClientGetAttributeThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientDeleteThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        doThrow(IOException.class).when(mockFs).delete(Paths.get("filename.txt"));

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientNewInputStreamReturnsNoContent() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream(new byte[]{}));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientNewInputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new InputStream() {
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
        });
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        assertThrows(InterruptedException.class, () -> fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS));
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testTryLock2_HetuFileSystemClientNewInputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.tryLock(0L, TimeUnit.MILLISECONDS);

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testUnlock() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.deleteIfExists(Paths.get("filename.txt"))).thenReturn(false);

        // Run the test
        fileBasedLockUnderTest.unlock();

        // Verify the results
    }

    @Test
    public void testUnlock_HetuFileSystemClientDeleteIfExistsThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.deleteIfExists(Paths.get("filename.txt"))).thenThrow(IOException.class);

        // Run the test
        fileBasedLockUnderTest.unlock();

        // Verify the results
    }

    @Test
    public void testNewCondition() throws Exception
    {
        // Setup
        // Run the test
        final Condition result = fileBasedLockUnderTest.newCondition();

        // Verify the results
    }

    @Test
    public void testNewCondition_ThrowsUnsupportedOperationException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(UnsupportedOperationException.class, () -> fileBasedLockUnderTest.newCondition());
    }

    @Test
    public void testIsLocked() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Run the test
        final boolean result = fileBasedLockUnderTest.isLocked();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testIsLocked_HetuFileSystemClientNewOutputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new OutputStream()
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
        });
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Run the test
        final boolean result = fileBasedLockUnderTest.isLocked();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testIsLocked_HetuFileSystemClientNewOutputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.isLocked();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsLocked_HetuFileSystemClientGetAttributeThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.isLocked();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testIsLocked_HetuFileSystemClientDeleteThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        doThrow(IOException.class).when(mockFs).delete(Paths.get("filename.txt"));

        // Run the test
        final boolean result = fileBasedLockUnderTest.isLocked();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testAcquiredLock() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientNewOutputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new OutputStream()
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
        });
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream("content".getBytes()));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientNewOutputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientGetAttributeThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientDeleteThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        doThrow(IOException.class).when(mockFs).delete(Paths.get("filename.txt"));

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientNewInputStreamReturnsNoContent() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new ByteArrayInputStream(new byte[]{}));
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientNewInputStreamReturnsBrokenIo() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");

        // Configure HetuFileSystemClient.newInputStream(...).
        final InputStream spyInputStream = spy(new InputStream() {
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
        });
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenReturn(spyInputStream);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
        verify(spyInputStream).close();
    }

    @Test
    public void testAcquiredLock_HetuFileSystemClientNewInputStreamThrowsIOException() throws Exception
    {
        // Setup
        when(mockFs.exists(Paths.get("filename.txt"))).thenReturn(false);

        // Configure HetuFileSystemClient.newOutputStream(...).
        final OutputStream spyOutputStream = spy(new ByteArrayOutputStream());
        when(mockFs.newOutputStream(eq(Paths.get("filename.txt")), any(OpenOption.class))).thenReturn(spyOutputStream);

        when(mockFs.getAttribute(Paths.get("filename.txt"), "lastModifiedTime")).thenReturn("result");
        when(mockFs.newInputStream(Paths.get("filename.txt"))).thenThrow(IOException.class);

        // Run the test
        final boolean result = fileBasedLockUnderTest.acquiredLock();

        // Verify the results
        assertTrue(result);
        verify(spyOutputStream).close();
        verify(mockFs).delete(Paths.get("filename.txt"));
    }

    @Test
    public void testGetLock() throws Exception
    {
        // Setup
        final HetuFileSystemClient fs = null;
        final Properties lockProperties = new Properties();

        // Run the test
        final FileBasedLock result = FileBasedLock.getLock(fs, lockProperties);
        assertTrue(result.tryLock());
        assertTrue(result.tryLock(0L, TimeUnit.MILLISECONDS));
        assertEquals(null, result.newCondition());
        assertTrue(result.isLocked());
        assertTrue(result.acquiredLock());
    }

    @Test
    public void testGetLock_ThrowsIOException()
    {
        // Setup
        final HetuFileSystemClient fs = null;
        final Properties lockProperties = new Properties();

        // Run the test
        assertThrows(IOException.class, () -> FileBasedLock.getLock(fs, lockProperties));
    }

    @Test
    public void testIsLockUtilFile() throws Exception
    {
        assertTrue(FileBasedLock.isLockUtilFile(Paths.get("filename.txt")));
    }
}
