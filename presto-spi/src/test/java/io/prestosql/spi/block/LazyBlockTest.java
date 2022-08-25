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
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class LazyBlockTest
{
    @Mock
    private LazyBlockLoader<Object> mockLoader;

    private LazyBlock<Object> lazyBlockUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        lazyBlockUnderTest = new LazyBlock<>(0, mockLoader);
    }

    @Test
    public void testGetSliceLength() throws Exception
    {
        // Setup
        // Run the test
        final int result = lazyBlockUnderTest.getSliceLength(0);

        // Verify the results
        assertEquals(0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetByte() throws Exception
    {
        // Setup
        // Run the test
        final byte result = lazyBlockUnderTest.getByte(0, 0);

        // Verify the results
        assertEquals((byte) 0b0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetShort() throws Exception
    {
        // Setup
        // Run the test
        final short result = lazyBlockUnderTest.getShort(0, 0);

        // Verify the results
        assertEquals((short) 0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetInt() throws Exception
    {
        // Setup
        // Run the test
        final int result = lazyBlockUnderTest.getInt(0, 0);

        // Verify the results
        assertEquals(0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetLong() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getLong(0, 0);

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = lazyBlockUnderTest.getSlice(0, 0, 0);

        // Verify the results
        assertEquals(expectedResult, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetString() throws Exception
    {
        // Setup
        // Run the test
        final String result = lazyBlockUnderTest.getString(0, 0, 0);

        // Verify the results
        assertEquals("result", result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetObject() throws Exception
    {
        // Setup
        // Run the test
        lazyBlockUnderTest.getObject(0, Object.class);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testBytesEqual() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final boolean result = lazyBlockUnderTest.bytesEqual(0, 0, otherSlice, 0, 0);

        // Verify the results
        assertTrue(result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testBytesCompare() throws Exception
    {
        // Setup
        final Slice otherSlice = null;

        // Run the test
        final int result = lazyBlockUnderTest.bytesCompare(0, 0, 0, otherSlice, 0, 0);

        // Verify the results
        assertEquals(0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testWriteBytesTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        lazyBlockUnderTest.writeBytesTo(0, 0, 0, blockBuilder);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        lazyBlockUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testEquals() throws Exception
    {
        // Setup
        final Block otherBlock = null;

        // Run the test
        final boolean result = lazyBlockUnderTest.equals(0, 0, otherBlock, 0, 0, 0);

        // Verify the results
        assertTrue(result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testHash() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.hash(0, 0, 0);

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block rightBlock = null;

        // Run the test
        final int result = lazyBlockUnderTest.compareTo(0, 0, 0, rightBlock, 0, 0, 0);

        // Verify the results
        assertEquals(0, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = lazyBlockUnderTest.getSingleValueBlock(0);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> lazyBlockUnderTest.getSingleValueBlock(0));
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getRegionSizeInBytes(0, 0);

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getPositionsSizeInBytes(new boolean[]{false});

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        // Setup
        // Run the test
        final long result = lazyBlockUnderTest.getEstimatedDataSizeForStats(0);

        // Verify the results
        assertEquals(0L, result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        lazyBlockUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        // Setup
        // Run the test
        final String result = lazyBlockUnderTest.getEncodingName();

        // Verify the results
        assertEquals("LAZY", result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = lazyBlockUnderTest.getPositions(new int[]{0}, 0, 0);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = lazyBlockUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = lazyBlockUnderTest.getRegion(0, 0);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = lazyBlockUnderTest.copyRegion(0, 0);

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testIsNull() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = lazyBlockUnderTest.isNull(0);

        // Verify the results
        assertTrue(result);
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testIsNull_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> lazyBlockUnderTest.isNull(0));
        verify(mockLoader).load(any(LazyBlock.class));
    }

    @Test
    public void testIsLoaded() throws Exception
    {
        assertTrue(lazyBlockUnderTest.isLoaded());
    }

    @Test
    public void testGetLoadedBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block<Object> result = lazyBlockUnderTest.getLoadedBlock();

        // Verify the results
        verify(mockLoader).load(any(LazyBlock.class));
    }
}
