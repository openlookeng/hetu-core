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
import io.prestosql.spi.type.MapType;
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

public class MapBlockBuilderTest
{
    @Mock
    private MapType mockMapType;
    @Mock
    private BlockBuilderStatus mockBlockBuilderStatus;

    private MapBlockBuilder<Object> mapBlockBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        mapBlockBuilderUnderTest = new MapBlockBuilder<>(mockMapType, mockBlockBuilderStatus, 0);
    }

    @Test
    public void testGetRawKeyBlock()
    {
        // Setup
        // Run the test
        final Block result = mapBlockBuilderUnderTest.getRawKeyBlock();

        // Verify the results
    }

    @Test
    public void testGetRawValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = mapBlockBuilderUnderTest.getRawValueBlock();

        // Verify the results
    }

    @Test
    public void testGetHashTables() throws Exception
    {
        assertEquals(new int[]{0}, mapBlockBuilderUnderTest.getHashTables());
    }

    @Test
    public void testGetOffsets() throws Exception
    {
        assertEquals(new int[]{0}, mapBlockBuilderUnderTest.getOffsets());
    }

    @Test
    public void testGetOffsetBase() throws Exception
    {
        assertEquals(0, mapBlockBuilderUnderTest.getOffsetBase());
    }

    @Test
    public void testGetMapIsNull() throws Exception
    {
        assertEquals(new boolean[]{false}, mapBlockBuilderUnderTest.getMapIsNull());
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = mapBlockBuilderUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = mapBlockBuilderUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        mapBlockBuilderUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testBeginBlockEntry() throws Exception
    {
        // Setup
        // Run the test
        final SingleMapBlockWriter result = mapBlockBuilderUnderTest.beginBlockEntry();

        // Verify the results
    }

    @Test
    public void testCloseEntry1()
    {
        // Setup
        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.closeEntry();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testCloseEntryStrict() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.closeEntryStrict();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testCloseEntryStrict_ThrowsDuplicateMapKeyException()
    {
        // Setup
        // Run the test
        assertThrows(DuplicateMapKeyException.class, () -> mapBlockBuilderUnderTest.closeEntryStrict());
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.appendNull();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = mapBlockBuilderUnderTest.build();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", mapBlockBuilderUnderTest.toString());
    }

    @Test
    public void testAppendStructure() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.appendStructure(block);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testAppendStructureInternal() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.appendStructureInternal(block, 0);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = mapBlockBuilderUnderTest.newBlockBuilderLike(blockBuilderStatus);

        // Verify the results
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockBlockBuilderStatus.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = mapBlockBuilderUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        mapBlockBuilderUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockBlockBuilderStatus).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }

    @Test
    public void testBuildHashTable() throws Exception
    {
        // Setup
        final Block keyBlock = null;
        final MapType mapType = new MapType<>(null, null, null, null, null, null);

        // Run the test
        MapBlockBuilder.buildHashTable(keyBlock, 0, 0, mapType, new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testComputePosition() throws Exception
    {
        assertEquals(0, MapBlockBuilder.computePosition(0L, 0));
    }
}
