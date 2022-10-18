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
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class SingleMapBlockWriterTest
{
    @Mock
    private BlockBuilder mockKeyBlockBuilder;
    @Mock
    private BlockBuilder mockValueBlockBuilder;

    private SingleMapBlockWriter<Object> singleMapBlockWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        singleMapBlockWriterUnderTest = new SingleMapBlockWriter<>(0, mockKeyBlockBuilder, mockValueBlockBuilder);
    }

    @Test
    public void testGetOffset() throws Exception
    {
        assertEquals(0, singleMapBlockWriterUnderTest.getOffset());
    }

    @Test
    public void testGetRawKeyBlock()
    {
        // Setup
        // Run the test
        final Block result = singleMapBlockWriterUnderTest.getRawKeyBlock();

        // Verify the results
    }

    @Test
    public void testGetRawValueBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleMapBlockWriterUnderTest.getRawValueBlock();

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        when(mockKeyBlockBuilder.getSizeInBytes()).thenReturn(0L);
        when(mockValueBlockBuilder.getSizeInBytes()).thenReturn(0L);

        // Run the test
        final long result = singleMapBlockWriterUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        when(mockKeyBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);
        when(mockValueBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        final long result = singleMapBlockWriterUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);
        when(mockKeyBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);
        when(mockValueBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        singleMapBlockWriterUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testWriteByte() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.writeByte(0)).thenReturn(null);
        when(mockKeyBlockBuilder.writeByte(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.writeByte(0);

        // Verify the results
        verify(mockValueBlockBuilder).writeByte(0);
        verify(mockKeyBlockBuilder).writeByte(0);
    }

    @Test
    public void testWriteShort() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.writeShort(0)).thenReturn(null);
        when(mockKeyBlockBuilder.writeShort(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.writeShort(0);

        // Verify the results
        verify(mockValueBlockBuilder).writeShort(0);
        verify(mockKeyBlockBuilder).writeShort(0);
    }

    @Test
    public void testWriteInt() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.writeInt(0)).thenReturn(null);
        when(mockKeyBlockBuilder.writeInt(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.writeInt(0);

        // Verify the results
        verify(mockValueBlockBuilder).writeInt(0);
        verify(mockKeyBlockBuilder).writeInt(0);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.writeLong(0L)).thenReturn(null);
        when(mockKeyBlockBuilder.writeLong(0L)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.writeLong(0L);

        // Verify the results
        verify(mockValueBlockBuilder).writeLong(0L);
        verify(mockKeyBlockBuilder).writeLong(0L);
    }

    @Test
    public void testWriteBytes() throws Exception
    {
        // Setup
        final Slice source = null;
        when(mockValueBlockBuilder.writeBytes(null, 0, 0)).thenReturn(null);
        when(mockKeyBlockBuilder.writeBytes(null, 0, 0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.writeBytes(source, 0, 0);

        // Verify the results
        verify(mockValueBlockBuilder).writeBytes(null, 0, 0);
        verify(mockKeyBlockBuilder).writeBytes(null, 0, 0);
    }

    @Test
    public void testAppendStructure() throws Exception
    {
        // Setup
        final Block block = null;
        when(mockValueBlockBuilder.appendStructure(any(Block.class))).thenReturn(null);
        when(mockKeyBlockBuilder.appendStructure(any(Block.class))).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.appendStructure(block);

        // Verify the results
        verify(mockValueBlockBuilder).appendStructure(any(Block.class));
        verify(mockKeyBlockBuilder).appendStructure(any(Block.class));
    }

    @Test
    public void testAppendStructureInternal() throws Exception
    {
        // Setup
        final Block block = null;
        when(mockValueBlockBuilder.appendStructureInternal(any(Block.class), eq(0))).thenReturn(null);
        when(mockKeyBlockBuilder.appendStructureInternal(any(Block.class), eq(0))).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.appendStructureInternal(block, 0);

        // Verify the results
        verify(mockValueBlockBuilder).appendStructureInternal(any(Block.class), eq(0));
        verify(mockKeyBlockBuilder).appendStructureInternal(any(Block.class), eq(0));
    }

    @Test
    public void testBeginBlockEntry() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.beginBlockEntry()).thenReturn(null);
        when(mockKeyBlockBuilder.beginBlockEntry()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.beginBlockEntry();

        // Verify the results
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.appendNull()).thenReturn(null);
        when(mockKeyBlockBuilder.appendNull()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.appendNull();

        // Verify the results
        verify(mockValueBlockBuilder).appendNull();
        verify(mockKeyBlockBuilder).appendNull();
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        when(mockValueBlockBuilder.closeEntry()).thenReturn(null);
        when(mockKeyBlockBuilder.closeEntry()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.closeEntry();

        // Verify the results
        verify(mockValueBlockBuilder).closeEntry();
        verify(mockKeyBlockBuilder).closeEntry();
    }

    @Test
    public void testGetPositionCount() throws Exception
    {
        assertEquals(0, singleMapBlockWriterUnderTest.getPositionCount());
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("result", singleMapBlockWriterUnderTest.getEncodingName());
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleMapBlockWriterUnderTest.build();

        // Verify the results
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleMapBlockWriterUnderTest.newBlockBuilderLike(null);

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", singleMapBlockWriterUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockKeyBlockBuilder.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");
        when(mockValueBlockBuilder.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = singleMapBlockWriterUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        singleMapBlockWriterUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockKeyBlockBuilder).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
        verify(mockValueBlockBuilder).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
