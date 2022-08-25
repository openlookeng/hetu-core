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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class BlockListBlockTest
{
    private BlockListBlock<Object> blockListBlockUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        blockListBlockUnderTest = new BlockListBlock<>(new Block[]{}, 0, 0);
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        blockListBlockUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = blockListBlockUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> blockListBlockUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        assertEquals(0L, blockListBlockUnderTest.getRegionSizeInBytes(0, 0));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        assertEquals(0L, blockListBlockUnderTest.getPositionsSizeInBytes(new boolean[]{false}));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, blockListBlockUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        blockListBlockUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("result", blockListBlockUnderTest.getEncodingName());
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = blockListBlockUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = blockListBlockUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = blockListBlockUnderTest.copyRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(blockListBlockUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> blockListBlockUnderTest.isNull(0));
    }

    @Test
    public void testMayHaveNull() throws Exception
    {
        assertTrue(blockListBlockUnderTest.mayHaveNull());
    }

    @Test
    public void testGetByte() throws Exception
    {
        assertEquals((byte) 0b0, blockListBlockUnderTest.getByte(0, 0));
    }

    @Test
    public void testGetSliceLength() throws Exception
    {
        assertEquals(0, blockListBlockUnderTest.getSliceLength(0));
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = blockListBlockUnderTest.getSlice(0, 0, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetShort() throws Exception
    {
        assertEquals((short) 0, blockListBlockUnderTest.getShort(0, 0));
    }

    @Test
    public void testGetInt() throws Exception
    {
        assertEquals(0, blockListBlockUnderTest.getInt(0, 0));
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, blockListBlockUnderTest.getLong(0, 0));
    }

    @Test
    public void testGetString() throws Exception
    {
        assertEquals("result", blockListBlockUnderTest.getString(0, 0, 0));
    }

    @Test
    public void testGetObject() throws Exception
    {
        // Setup
        // Run the test
        blockListBlockUnderTest.getObject(0, Object.class);

        // Verify the results
    }

    @Test
    public void testBytesEqual() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final boolean result = blockListBlockUnderTest.bytesEqual(0, 0, otherSlice, 0, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testBytesCompare() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final int result = blockListBlockUnderTest.bytesCompare(0, 0, 0, otherSlice, 0, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGet() throws Exception
    {
        // Setup
        // Run the test
        blockListBlockUnderTest.get(0);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(blockListBlockUnderTest.equals("obj"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, blockListBlockUnderTest.hashCode());
    }
}
