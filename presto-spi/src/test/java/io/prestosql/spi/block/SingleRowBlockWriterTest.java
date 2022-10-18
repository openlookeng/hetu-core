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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.function.BiConsumer;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

public class SingleRowBlockWriterTest
{
    private SingleRowBlockWriter<Object> singleRowBlockWriterUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        singleRowBlockWriterUnderTest = new SingleRowBlockWriter<>(0, new BlockBuilder[]{});
    }

    @Test
    public void testGetFieldBlockBuilder() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.getFieldBlockBuilder(0);

        // Verify the results
    }

    @Test
    public void testGetRawFieldBlock() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleRowBlockWriterUnderTest.getRawFieldBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, singleRowBlockWriterUnderTest.getSizeInBytes());
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        assertEquals(0L, singleRowBlockWriterUnderTest.getRetainedSizeInBytes());
    }

    @Test
    public void testRetainedBytesForEachPart() throws Exception
    {
        // Setup
        final BiConsumer<Object, Long> mockConsumer = mock(BiConsumer.class);

        // Run the test
        singleRowBlockWriterUnderTest.retainedBytesForEachPart(mockConsumer);

        // Verify the results
    }

    @Test
    public void testWriteByte() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.writeByte(0);

        // Verify the results
    }

    @Test
    public void testWriteShort() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.writeShort(0);

        // Verify the results
    }

    @Test
    public void testWriteInt() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.writeInt(0);

        // Verify the results
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.writeLong(0L);

        // Verify the results
    }

    @Test
    public void testWriteBytes() throws Exception
    {
        // Setup
        final Slice source = null;

        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.writeBytes(source, 0, 0);

        // Verify the results
    }

    @Test
    public void testAppendStructure() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.appendStructure(block);

        // Verify the results
    }

    @Test
    public void testAppendStructureInternal() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.appendStructureInternal(block, 0);

        // Verify the results
    }

    @Test
    public void testBeginBlockEntry() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.beginBlockEntry();

        // Verify the results
    }

    @Test
    public void testAppendNull() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.appendNull();

        // Verify the results
    }

    @Test
    public void testCloseEntry() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.closeEntry();

        // Verify the results
    }

    @Test
    public void testGetPositionCount() throws Exception
    {
        assertEquals(0, singleRowBlockWriterUnderTest.getPositionCount());
    }

    @Test
    public void testGetEncodingName() throws Exception
    {
        assertEquals("result", singleRowBlockWriterUnderTest.getEncodingName());
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Block result = singleRowBlockWriterUnderTest.build();

        // Verify the results
    }

    @Test
    public void testNewBlockBuilderLike() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = singleRowBlockWriterUnderTest.newBlockBuilderLike(null);

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", singleRowBlockWriterUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        final Object result = singleRowBlockWriterUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        singleRowBlockWriterUnderTest.restore("state", serdeProvider);

        // Verify the results
    }
}
