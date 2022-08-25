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

public class SingleArrayBlockWriterTest
{
    @Mock
    private BlockBuilder mockBlockBuilder;

    private SingleArrayBlockWriter<Object> singleArrayBlockWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        singleArrayBlockWriterUnderTest = new SingleArrayBlockWriter<>(mockBlockBuilder, 0);
    }

    @Test
    public void testGetBlock()
    {
        // Setup
        // Run the test
        final Block result = singleArrayBlockWriterUnderTest.getBlock();

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        when(mockBlockBuilder.getSizeInBytes()).thenReturn(0L);

        // Run the test
        final long result = singleArrayBlockWriterUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        when(mockBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        final long result = singleArrayBlockWriterUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);
        when(mockBlockBuilder.getRetainedSizeInBytes()).thenReturn(0L);

        // Run the test
        singleArrayBlockWriterUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testWriteByte() throws Exception
    {
        // Setup
        when(mockBlockBuilder.writeByte(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.writeByte(0);

        // Verify the results
        verify(mockBlockBuilder).writeByte(0);
    }

    @Test
    public void testWriteShort() throws Exception
    {
        // Setup
        when(mockBlockBuilder.writeShort(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.writeShort(0);

        // Verify the results
        verify(mockBlockBuilder).writeShort(0);
    }

    @Test
    public void testWriteInt() throws Exception
    {
        // Setup
        when(mockBlockBuilder.writeInt(0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.writeInt(0);

        // Verify the results
        verify(mockBlockBuilder).writeInt(0);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        when(mockBlockBuilder.writeLong(0L)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.writeLong(0L);

        // Verify the results
        verify(mockBlockBuilder).writeLong(0L);
    }

    @Test
    public void testWriteBytes() throws Exception
    {
        // Setup
        final Slice source = null;
        when(mockBlockBuilder.writeBytes(null, 0, 0)).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.writeBytes(source, 0, 0);

        // Verify the results
        verify(mockBlockBuilder).writeBytes(null, 0, 0);
    }

    @Test
    public void testAppendStructure() throws Exception
    {
        // Setup
        final Block block = null;
        when(mockBlockBuilder.appendStructure(any(Block.class))).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.appendStructure(block);

        // Verify the results
        verify(mockBlockBuilder).appendStructure(any(Block.class));
    }

    @Test
    public void testAppendStructureInternal() throws Exception
    {
        // Setup
        final Block block = null;
        when(mockBlockBuilder.appendStructureInternal(any(Block.class), eq(0))).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.appendStructureInternal(block, 0);

        // Verify the results
        verify(mockBlockBuilder).appendStructureInternal(any(Block.class), eq(0));
    }

    @Test
    public void testBeginBlockEntry() throws Exception
    {
        // Setup
        when(mockBlockBuilder.beginBlockEntry()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.beginBlockEntry();

        // Verify the results
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        when(mockBlockBuilder.appendNull()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.appendNull();

        // Verify the results
        verify(mockBlockBuilder).appendNull();
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        when(mockBlockBuilder.closeEntry()).thenReturn(null);

        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.closeEntry();

        // Verify the results
        verify(mockBlockBuilder).closeEntry();
    }

    @Test
    public void testGetPositionCount() throws Exception
    {
        assertEquals(0, singleArrayBlockWriterUnderTest.getPositionCount());
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleArrayBlockWriterUnderTest.build();

        // Verify the results
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleArrayBlockWriterUnderTest.newBlockBuilderLike(null);

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", singleArrayBlockWriterUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockBlockBuilder.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = singleArrayBlockWriterUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        singleArrayBlockWriterUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockBlockBuilder).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
