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
package io.prestosql.plugin.hive.util;

import org.apache.commons.io.input.BrokenInputStream;
import org.apache.commons.io.input.NullInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class FSDataInputStreamTailTest
{
    @Test
    public void testReadTail() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(null);

        // Run the test
        final FSDataInputStreamTail result = FSDataInputStreamTail.readTail("path", 0L, inputStream, 0);
    }

    @Test
    public void testReadTail_EmptyInputStream() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new NullInputStream());

        // Run the test
        final FSDataInputStreamTail result = FSDataInputStreamTail.readTail("path", 0L, inputStream, 0);

        // Verify the results
    }

    @Test
    public void testReadTail_BrokenInputStream()
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new BrokenInputStream());

        // Run the test
        assertThrows(IOException.class, () -> FSDataInputStreamTail.readTail("path", 0L, inputStream, 0));
    }

    @Test
    public void testReadTailForFileSize() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new ByteArrayInputStream("content".getBytes(StandardCharsets.UTF_8)));

        // Run the test
        final long result = FSDataInputStreamTail.readTailForFileSize("path", 0L, inputStream);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testReadTailForFileSize_EmptyInputStream() throws Exception
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new NullInputStream());

        // Run the test
        final long result = FSDataInputStreamTail.readTailForFileSize("path", 0L, inputStream);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testReadTailForFileSize_BrokenInputStream()
    {
        // Setup
        final FSDataInputStream inputStream = new FSDataInputStream(new BrokenInputStream());

        // Run the test
        assertThrows(IOException.class, () -> FSDataInputStreamTail.readTailForFileSize("path", 0L, inputStream));
    }
}
