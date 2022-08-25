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
package io.hetu.core.plugin.iceberg.delete;

import org.apache.iceberg.FileContent;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.StructLike;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;

public class TrinoDeleteFileTest
{
    private TrinoDeleteFile trinoDeleteFileUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        trinoDeleteFileUnderTest = new TrinoDeleteFile(0L, 0, FileContent.DATA, "path", FileFormat.ORC, 0L, 0L,
                new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(),
                "content".getBytes(),
                Arrays.asList(0), 0, Arrays.asList(0L));
    }

    @Test
    public void testContent()
    {
        assertEquals(FileContent.DATA, trinoDeleteFileUnderTest.content());
    }

    @Test
    public void testPartition()
    {
        // Setup
        // Run the test
        final StructLike result = trinoDeleteFileUnderTest.partition();

        // Verify the results
    }

    @Test
    public void testLowerBoundsAsByteArray()
    {
        // Setup
        // Run the test
        final Map<Integer, byte[]> result = trinoDeleteFileUnderTest.lowerBoundsAsByteArray();

        // Verify the results
    }

    @Test
    public void testLowerBounds()
    {
        // Setup
        final Map<Integer, ByteBuffer> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, ByteBuffer> result = trinoDeleteFileUnderTest.lowerBounds();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpperBoundsAsByteArray()
    {
        // Setup
        // Run the test
        final Map<Integer, byte[]> result = trinoDeleteFileUnderTest.upperBoundsAsByteArray();

        // Verify the results
    }

    @Test
    public void testUpperBounds()
    {
        // Setup
        final Map<Integer, ByteBuffer> expectedResult = new HashMap<>();

        // Run the test
        final Map<Integer, ByteBuffer> result = trinoDeleteFileUnderTest.upperBounds();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testKeyMetadataAsByteArray()
    {
        assertEquals("content".getBytes(), trinoDeleteFileUnderTest.keyMetadataAsByteArray());
    }

    @Test
    public void testKeyMetadata()
    {
        // Setup
        final ByteBuffer expectedResult = ByteBuffer.wrap("content".getBytes());

        // Run the test
        final ByteBuffer result = trinoDeleteFileUnderTest.keyMetadata();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testCopy()
    {
        // Setup
        // Run the test
        trinoDeleteFileUnderTest.copy();

        // Verify the results
    }

    @Test
    public void testCopyWithoutStats()
    {
        // Setup
        // Run the test
        trinoDeleteFileUnderTest.copyWithoutStats();

        // Verify the results
    }

    @Test
    public void testGetRetainedSizeInBytes()
    {
        assertEquals(0L, trinoDeleteFileUnderTest.getRetainedSizeInBytes());
    }

    @Test
    public void testCopyOf()
    {
        // Run the test
        final TrinoDeleteFile result = TrinoDeleteFile.copyOf(new DeleteFile());
    }

    public static class DeleteFile
            implements org.apache.iceberg.DeleteFile
    {
        @Override
        public Long pos()
        {
            return Long.valueOf(1);
        }

        @Override
        public int specId()
        {
            return 0;
        }

        @Override
        public FileContent content()
        {
            return null;
        }

        @Override
        public CharSequence path()
        {
            return new CharSequence() {
                @Override
                public int length()
                {
                    return 0;
                }

                @Override
                public char charAt(int index)
                {
                    return 0;
                }

                @NotNull
                @Override
                public CharSequence subSequence(int start, int end)
                {
                    return null;
                }
            };
        }

        @Override
        public FileFormat format()
        {
            return null;
        }

        @Override
        public StructLike partition()
        {
            return null;
        }

        @Override
        public long recordCount()
        {
            return 0;
        }

        @Override
        public long fileSizeInBytes()
        {
            return 0;
        }

        @Override
        public Map<Integer, Long> columnSizes()
        {
            return null;
        }

        @Override
        public Map<Integer, Long> valueCounts()
        {
            return null;
        }

        @Override
        public Map<Integer, Long> nullValueCounts()
        {
            return null;
        }

        @Override
        public Map<Integer, Long> nanValueCounts()
        {
            return null;
        }

        @Override
        public Map<Integer, ByteBuffer> lowerBounds()
        {
            return null;
        }

        @Override
        public Map<Integer, ByteBuffer> upperBounds()
        {
            return null;
        }

        @Override
        public ByteBuffer keyMetadata()
        {
            return null;
        }

        @Override
        public List<Integer> equalityFieldIds()
        {
            return null;
        }

        @Override
        public DeleteFile copy()
        {
            return null;
        }

        @Override
        public DeleteFile copyWithoutStats()
        {
            return null;
        }
    }

    @Test
    public void test()
    {
        trinoDeleteFileUnderTest.pos();
        trinoDeleteFileUnderTest.specId();
        trinoDeleteFileUnderTest.content();
        trinoDeleteFileUnderTest.path();
        trinoDeleteFileUnderTest.format();
        trinoDeleteFileUnderTest.partition();
        trinoDeleteFileUnderTest.recordCount();
        trinoDeleteFileUnderTest.fileSizeInBytes();
        trinoDeleteFileUnderTest.columnSizes();
        trinoDeleteFileUnderTest.valueCounts();
        trinoDeleteFileUnderTest.nullValueCounts();
        trinoDeleteFileUnderTest.nanValueCounts();
        trinoDeleteFileUnderTest.lowerBoundsAsByteArray();
        trinoDeleteFileUnderTest.lowerBounds();
        trinoDeleteFileUnderTest.upperBoundsAsByteArray();
        trinoDeleteFileUnderTest.upperBounds();
        trinoDeleteFileUnderTest.keyMetadataAsByteArray();
        trinoDeleteFileUnderTest.keyMetadata();
        trinoDeleteFileUnderTest.equalityFieldIds();
        trinoDeleteFileUnderTest.sortOrderId();
        trinoDeleteFileUnderTest.splitOffsets();
    }
}
