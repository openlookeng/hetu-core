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

public class LongArrayBlockBuilderTest
{
    @Mock
    private BlockBuilderStatus mockBlockBuilderStatus;

    private LongArrayBlockBuilder longArrayBlockBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        longArrayBlockBuilderUnderTest = new LongArrayBlockBuilder(mockBlockBuilderStatus, 0);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = longArrayBlockBuilderUnderTest.writeLong(0L);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = longArrayBlockBuilderUnderTest.closeEntry();

        // Verify the results
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = longArrayBlockBuilderUnderTest.appendNull();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = longArrayBlockBuilderUnderTest.build();

        // Verify the results
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = longArrayBlockBuilderUnderTest.newBlockBuilderLike(blockBuilderStatus);

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, longArrayBlockBuilderUnderTest.getSizeInBytes());
    }

    @Test
    public void testGetRegionSizeInBytes() throws Exception
    {
        assertEquals(0L, longArrayBlockBuilderUnderTest.getRegionSizeInBytes(0, 0));
    }

    @Test
    public void testGetPositionsSizeInBytes() throws Exception
    {
        assertEquals(0L, longArrayBlockBuilderUnderTest.getPositionsSizeInBytes(new boolean[]{false}));
    }

    @Test
    public void testGetEstimatedDataSizeForStats() throws Exception
    {
        assertEquals(0L, longArrayBlockBuilderUnderTest.getEstimatedDataSizeForStats(0));
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        longArrayBlockBuilderUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testGetLong() throws Exception
    {
        assertEquals(0L, longArrayBlockBuilderUnderTest.getLong(0, 0));
    }

    @Test
    public void testGetInt() throws Exception
    {
        assertEquals(0, longArrayBlockBuilderUnderTest.getInt(0, 0));
    }

    @Test
    public void testGetShort() throws Exception
    {
        assertEquals((short) 0, longArrayBlockBuilderUnderTest.getShort(0, 0));
    }

    @Test
    public void testGetByte() throws Exception
    {
        assertEquals((byte) 0b0, longArrayBlockBuilderUnderTest.getByte(0, 0));
    }

    @Test
    public void testMayHaveNull() throws Exception
    {
        assertTrue(longArrayBlockBuilderUnderTest.mayHaveNull());
    }

    @Test
    public void testIsNull() throws Exception
    {
        assertTrue(longArrayBlockBuilderUnderTest.isNull(0));
        assertThrows(IllegalArgumentException.class, () -> longArrayBlockBuilderUnderTest.isNull(0));
    }

    @Test
    public void testWritePositionTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        longArrayBlockBuilderUnderTest.writePositionTo(0, blockBuilder);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testGetSingleValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = longArrayBlockBuilderUnderTest.getSingleValueBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValueBlock_ThrowsIllegalArgumentException() throws Exception
    {
        // Setup
        // Run the test
        assertThrows(IllegalArgumentException.class, () -> longArrayBlockBuilderUnderTest.getSingleValueBlock(0));
    }

    @Test
    public void testCopyPositions() throws Exception
    {
        // Setup
        // Run the test
        final Block result = longArrayBlockBuilderUnderTest.copyPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = longArrayBlockBuilderUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testCopyRegion() throws Exception
    {
        // Setup
        // Run the test
        final Block result = longArrayBlockBuilderUnderTest.copyRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("LONG_ARRAY", longArrayBlockBuilderUnderTest.getEncodingName());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", longArrayBlockBuilderUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockBlockBuilderStatus.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = longArrayBlockBuilderUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        longArrayBlockBuilderUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockBlockBuilderStatus).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
