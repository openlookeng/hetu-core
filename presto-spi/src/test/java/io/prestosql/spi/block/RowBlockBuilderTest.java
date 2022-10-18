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

import java.util.Arrays;
import java.util.function.BiConsumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class RowBlockBuilderTest
{
    @Mock
    private BlockBuilderStatus mockBlockBuilderStatus;

    private RowBlockBuilder<Object> rowBlockBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        rowBlockBuilderUnderTest = new RowBlockBuilder<>(Arrays.asList(), mockBlockBuilderStatus, 0);
    }

    @Test
    public void testGetRawFieldBlocks() throws Exception
    {
        // Setup
        // Run the test
        final Block[] result = rowBlockBuilderUnderTest.getRawFieldBlocks();

        // Verify the results
    }

    @Test
    public void testGetFieldBlockOffsets() throws Exception
    {
        assertEquals(new int[]{0}, rowBlockBuilderUnderTest.getFieldBlockOffsets());
    }

    @Test
    public void testGetOffsetBase() throws Exception
    {
        assertEquals(0, rowBlockBuilderUnderTest.getOffsetBase());
    }

    @Test
    public void testGetRowIsNull() throws Exception
    {
        assertEquals(new boolean[]{false}, rowBlockBuilderUnderTest.getRowIsNull());
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, rowBlockBuilderUnderTest.getSizeInBytes());
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        assertEquals(0L, rowBlockBuilderUnderTest.getRetainedSizeInBytes());
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        rowBlockBuilderUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testBeginBlockEntry() throws Exception
    {
        // Setup
        // Run the test
        final SingleRowBlockWriter result = rowBlockBuilderUnderTest.beginBlockEntry();

        // Verify the results
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = rowBlockBuilderUnderTest.closeEntry();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = rowBlockBuilderUnderTest.appendNull();

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = rowBlockBuilderUnderTest.build();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", rowBlockBuilderUnderTest.toString());
    }

    @Test
    public void testAppendStructure() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = rowBlockBuilderUnderTest.appendStructure(block);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testAppendStructureInternal() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = rowBlockBuilderUnderTest.appendStructureInternal(block, 0);

        // Verify the results
        verify(mockBlockBuilderStatus).addBytes(0);
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = rowBlockBuilderUnderTest.newBlockBuilderLike(blockBuilderStatus);

        // Verify the results
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;
        when(mockBlockBuilderStatus.capture(any(BlockEncodingSerdeProvider.class))).thenReturn("result");

        // Run the test
        final Object result = rowBlockBuilderUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        rowBlockBuilderUnderTest.restore("state", serdeProvider);

        // Verify the results
        verify(mockBlockBuilderStatus).restore(any(Object.class), any(BlockEncodingSerdeProvider.class));
    }
}
