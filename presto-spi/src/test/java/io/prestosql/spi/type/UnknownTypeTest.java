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
package io.prestosql.spi.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class UnknownTypeTest
{
    private UnknownType unknownTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        unknownTypeUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testGetFixedSize() throws Exception
    {
        assertEquals(0, unknownTypeUnderTest.getFixedSize());
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = unknownTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = unknownTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0);

        // Verify the results
    }

    @Test
    public void testCreateFixedSizeBlockBuilder() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = unknownTypeUnderTest.createFixedSizeBlockBuilder(0);

        // Verify the results
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(unknownTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(unknownTypeUnderTest.isOrderable());
    }

    @Test
    public void testHash() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = unknownTypeUnderTest.hash(block, 0);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testEqualTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final boolean result = unknownTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final int result = unknownTypeUnderTest.compareTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Object result = unknownTypeUnderTest.getObjectValue(null, block, 0);

        // Verify the results
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        unknownTypeUnderTest.appendTo(null, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetBoolean() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final boolean result = unknownTypeUnderTest.getBoolean(block, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testWriteBoolean() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        unknownTypeUnderTest.writeBoolean(blockBuilder, false);

        // Verify the results
    }
}
