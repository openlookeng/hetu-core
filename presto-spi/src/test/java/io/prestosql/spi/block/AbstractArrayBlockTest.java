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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class AbstractArrayBlockTest
{
    private AbstractArrayBlock<Object> abstractArrayBlockUnderTest;

//    @BeforeMethod
//    public void setUp() throws Exception
//    {
//        abstractArrayBlockUnderTest = new AbstractArrayBlock<>()
//        {
//            @Override
//            protected Block getRawElementBlock()
//            {
//                return null;
//            }
//
//            @Override
//            protected int[] getOffsets()
//            {
//                return new int[0];
//            }
//
//            @Override
//            protected int getOffsetBase()
//            {
//                return 0;
//            }
//
//            @Override
//            protected boolean[] getValueIsNull()
//            {
//                return new boolean[0];
//            }
//
//            @Override
//            public int getSliceLength(int position)
//            {
//                return 0;
//            }
//
//            @Override
//            public byte getByte(int position, int offset)
//            {
//                return 0;
//            }
//
//            @Override
//            public short getShort(int position, int offset)
//            {
//                return 0;
//            }
//
//            @Override
//            public int getInt(int position, int offset)
//            {
//                return 0;
//            }
//
//            @Override
//            public long getLong(int position, int offset)
//            {
//                return 0;
//            }
//
//            @Override
//            public Slice getSlice(int position, int offset, int length)
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
//            public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
//            {
//                return false;
//            }
//
//            @Override
//            public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
//            {
//                return 0;
//            }
//
//            @Override
//            public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
//            {
//            }
//
//            @Override
//            public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
//            {
//                return false;
//            }
//
//            @Override
//            public long hash(int position, int offset, int length)
//            {
//                return 0;
//            }
//
//            @Override
//            public int compareTo(int leftPosition, int leftOffset, int leftLength, Block rightBlock, int rightPosition, int rightOffset, int rightLength)
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
    public void testGetOffset() throws Exception
    {
        assertEquals(0, abstractArrayBlockUnderTest.getOffset(0));
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("ARRAY", abstractArrayBlockUnderTest.getEncodingName());
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractArrayBlockUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractArrayBlockUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        assertEquals(0L, abstractArrayBlockUnderTest.getRegionSizeInBytes(0, 0));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        assertEquals(0L, abstractArrayBlockUnderTest.getPositionsSizeInBytes(new boolean[]{false}));
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractArrayBlockUnderTest.copyRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testGetObject() throws Exception
    {
        // Setup
        // Run the test
        abstractArrayBlockUnderTest.getObject(0, Object.class);

        // Verify the results
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractArrayBlockUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = abstractArrayBlockUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> abstractArrayBlockUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, abstractArrayBlockUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(abstractArrayBlockUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> abstractArrayBlockUnderTest.isNull(0));
    }

    @Test
    public void testApply() throws Exception
    {
        // Setup
        final AbstractArrayBlock.ArrayBlockFunction<Object> function = null;

        // Run the test
        abstractArrayBlockUnderTest.apply(function, 0);

        // Verify the results
    }
}
