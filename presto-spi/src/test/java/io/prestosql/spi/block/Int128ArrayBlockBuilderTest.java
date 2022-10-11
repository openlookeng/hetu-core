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

import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class Int128ArrayBlockBuilderTest
{
    @Mock
    private BlockBuilderStatus mockBlockBuilderStatus;

    private Int128ArrayBlockBuilder int128ArrayBlockBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        int128ArrayBlockBuilderUnderTest = new Int128ArrayBlockBuilder(mockBlockBuilderStatus, 0);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = int128ArrayBlockBuilderUnderTest.writeLong(0L);

        // Verify the results
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = int128ArrayBlockBuilderUnderTest.closeEntry();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = int128ArrayBlockBuilderUnderTest.appendNull();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = int128ArrayBlockBuilderUnderTest.build();

        // Verify the results
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = int128ArrayBlockBuilderUnderTest.newBlockBuilderLike(blockBuilderStatus);

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, int128ArrayBlockBuilderUnderTest.getSizeInBytes());
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        assertEquals(0L, int128ArrayBlockBuilderUnderTest.getRegionSizeInBytes(0, 0));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        assertEquals(0L, int128ArrayBlockBuilderUnderTest.getPositionsSizeInBytes(new boolean[]{false}));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, int128ArrayBlockBuilderUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        int128ArrayBlockBuilderUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, int128ArrayBlockBuilderUnderTest.getLong(0, 0));
    }

    @Test
    public void testMayHaveNull() throws Exception
    {
        assertTrue(int128ArrayBlockBuilderUnderTest.mayHaveNull());
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(int128ArrayBlockBuilderUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> int128ArrayBlockBuilderUnderTest.isNull(0));
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        int128ArrayBlockBuilderUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = int128ArrayBlockBuilderUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> int128ArrayBlockBuilderUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = int128ArrayBlockBuilderUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = int128ArrayBlockBuilderUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = int128ArrayBlockBuilderUnderTest.copyRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("INT128_ARRAY", int128ArrayBlockBuilderUnderTest.getEncodingName());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", int128ArrayBlockBuilderUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockBlockBuilderStatus.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = int128ArrayBlockBuilderUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        int128ArrayBlockBuilderUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockBlockBuilderStatus).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
