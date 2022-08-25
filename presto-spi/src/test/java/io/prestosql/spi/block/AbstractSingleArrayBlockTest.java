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

public class AbstractSingleArrayBlockTest
{
    private AbstractSingleArrayBlock<Object> abstractSingleArrayBlockUnderTest;

//    @BeforeMethod
//    public void setUp() throws Exception
//    {
//        abstractSingleArrayBlockUnderTest = new AbstractSingleArrayBlock<>(0)
//        {
//            @Override
//            protected Block getBlock()
//            {
//                return null;
//            }
//
//            @Override
//            public String getString(int position, int offset, int length)
//            {
//                return null;
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
    public void testGetSliceLength() throws Exception
    {
        assertEquals(0, abstractSingleArrayBlockUnderTest.getSliceLength(0));
    }

    @Test
    public void testGetByte() throws Exception
    {
        assertEquals((byte) 0b0, abstractSingleArrayBlockUnderTest.getByte(0, 0));
    }

    @Test
    public void testGetShort() throws Exception
    {
        assertEquals((short) 0, abstractSingleArrayBlockUnderTest.getShort(0, 0));
    }

    @Test
    public void testGetInt() throws Exception
    {
        assertEquals(0, abstractSingleArrayBlockUnderTest.getInt(0, 0));
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, abstractSingleArrayBlockUnderTest.getLong(0, 0));
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = abstractSingleArrayBlockUnderTest.getSlice(0, 0, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetObject() throws Exception
    {
        // Setup
        // Run the test
        abstractSingleArrayBlockUnderTest.getObject(0, Object.class);

        // Verify the results
    }

    @Test
    public void testBytesEqual() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final boolean result = abstractSingleArrayBlockUnderTest.bytesEqual(0, 0, otherSlice, 0, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testBytesCompare() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final int result = abstractSingleArrayBlockUnderTest.bytesCompare(0, 0, 0, otherSlice, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testWriteBytesTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractSingleArrayBlockUnderTest.writeBytesTo(0, 0, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractSingleArrayBlockUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        final Block otherBlock = null;

        // Run the test
        final boolean result = abstractSingleArrayBlockUnderTest.equals(0, 0, otherBlock, 0, 0, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash() throws Exception
    {
        assertEquals(0L, abstractSingleArrayBlockUnderTest.hash(0, 0, 0));
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block rightBlock = null;

        // Run the test
        final int result = abstractSingleArrayBlockUnderTest.compareTo(0, 0, 0, rightBlock, 0, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractSingleArrayBlockUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> abstractSingleArrayBlockUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, abstractSingleArrayBlockUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(abstractSingleArrayBlockUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> abstractSingleArrayBlockUnderTest.isNull(0));
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("result", abstractSingleArrayBlockUnderTest.getEncodingName());
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractSingleArrayBlockUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractSingleArrayBlockUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        assertEquals(0L, abstractSingleArrayBlockUnderTest.getRegionSizeInBytes(0, 0));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        assertEquals(0L, abstractSingleArrayBlockUnderTest.getPositionsSizeInBytes(new boolean[]{false}));
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractSingleArrayBlockUnderTest.copyRegion(0, 0);

        // Verify the results
    }
}
