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

import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class AbstractVariableWidthBlockTest
{
    private AbstractVariableWidthBlock<Object> abstractVariableWidthBlockUnderTest;

//    @BeforeMethod
//    public void setUp() throws Exception
//    {
//        abstractVariableWidthBlockUnderTest = new AbstractVariableWidthBlock<>() {
//            @Override
//            protected Slice getRawSlice(int position)
//            {
//                return null;
//            }
//
//            @Override
//            protected int getPositionOffset(int position)
//            {
//                return 0;
//            }
//
//            @Override
//            protected boolean isEntryNull(int position)
//            {
//                return false;
//            }
//
//            @Override
//            public int getSliceLength(int position)
//            {
//                return 0;
//            }
//
//            @Override
//            public int getPositionCount()
//            {
//                return 0;
//            }
//
//            @Override
//            public long getSizeInBytes()
//            {
//                return 0;
//            }
//
//            @Override
//            public long getLogicalSizeInBytes()
//            {
//                return 0;
//            }
//
//            @Override
//            public long getRegionSizeInBytes(int position, int length)
//            {
//                return 0;
//            }
//
//            @Override
//            public long getPositionsSizeInBytes(boolean[] positions)
//            {
//                return 0;
//            }
//
//            @Override
//            public long getRetainedSizeInBytes()
//            {
//                return 0;
//            }
//
//            @Override
//            public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
//            {
//            }
//
//            @Override
//            public Block<Object> getPositions(int[] positions, int offset, int length)
//            {
//                return null;
//            }
//
//            @Override
//            public Block<Object> copyPositions(int[] positions, int offset, int length)
//            {
//                return null;
//            }
//
//            @Override
//            public Block<Object> getRegion(int positionOffset, int length)
//            {
//                return null;
//            }
//
//            @Override
//            public Block<Object> copyRegion(int position, int length)
//            {
//                return null;
//            }
//
//            @Override
//            public boolean mayHaveNull()
//            {
//                return false;
//            }
//
//            @Override
//            public Block<Object> getLoadedBlock()
//            {
//                return null;
//            }
//
//            @Override
//            public Object get(int position)
//            {
//                return null;
//            }
//
//            @Override
//            public boolean[] filter(BloomFilter filter, boolean[] validPositions)
//            {
//                return new boolean[0];
//            }
//
//            @Override
//            public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test)
//            {
//                return 0;
//            }
//        };
//    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("VARIABLE_WIDTH", abstractVariableWidthBlockUnderTest.getEncodingName());
    }

    @Test
    public void testGetByte() throws Exception
    {
        assertEquals((byte) 0b0, abstractVariableWidthBlockUnderTest.getByte(0, 0));
    }

    @Test
    public void testGetShort() throws Exception
    {
        assertEquals((short) 0, abstractVariableWidthBlockUnderTest.getShort(0, 0));
    }

    @Test
    public void testGetInt() throws Exception
    {
        assertEquals(0, abstractVariableWidthBlockUnderTest.getInt(0, 0));
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, abstractVariableWidthBlockUnderTest.getLong(0, 0));
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = abstractVariableWidthBlockUnderTest.getSlice(0, 0, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetString() throws Exception
    {
        assertEquals("result", abstractVariableWidthBlockUnderTest.getString(0, 0, 0));
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        final Block otherBlock = null;

        // Run the test
        final boolean result = abstractVariableWidthBlockUnderTest.equals(0, 0, otherBlock, 0, 0, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testBytesEqual() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final boolean result = abstractVariableWidthBlockUnderTest.bytesEqual(0, 0, otherSlice, 0, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash() throws Exception
    {
        assertEquals(0L, abstractVariableWidthBlockUnderTest.hash(0, 0, 0));
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block otherBlock = null;

        // Run the test
        final int result = abstractVariableWidthBlockUnderTest.compareTo(0, 0, 0, otherBlock, 0, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testBytesCompare() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final int result = abstractVariableWidthBlockUnderTest.bytesCompare(0, 0, 0, otherSlice, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testWriteBytesTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractVariableWidthBlockUnderTest.writeBytesTo(0, 0, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractVariableWidthBlockUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractVariableWidthBlockUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> abstractVariableWidthBlockUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, abstractVariableWidthBlockUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(abstractVariableWidthBlockUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> abstractVariableWidthBlockUnderTest.isNull(0));
    }

    @Test
    public void testCheckReadablePosition() throws Exception
    {
        // Setup
        // Run the test
        abstractVariableWidthBlockUnderTest.checkReadablePosition(0);

        // Verify the results
    }
}
