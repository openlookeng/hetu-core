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

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TinyintTypeTest
{
    private TinyintType tinyintTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        tinyintTypeUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testGetFixedSize() throws Exception
    {
        assertEquals(0, tinyintTypeUnderTest.getFixedSize());
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = tinyintTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = tinyintTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0);

        // Verify the results
    }

    @Test
    public void testCreateFixedSizeBlockBuilder() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = tinyintTypeUnderTest.createFixedSizeBlockBuilder(0);

        // Verify the results
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(tinyintTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(tinyintTypeUnderTest.isOrderable());
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Object result = tinyintTypeUnderTest.getObjectValue(null, block, 0);

        // Verify the results
    }

    @Test
    public void testEqualTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final boolean result = tinyintTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash1()
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = tinyintTypeUnderTest.hash(block, 0);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final int result = tinyintTypeUnderTest.compareTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testGetRange() throws Exception
    {
        // Setup
        // Run the test
        final Optional<Type.Range> result = tinyintTypeUnderTest.getRange();

        // Verify the results
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        final Block block = null;
        final BlockBuilder blockBuilder = null;

        // Run the test
        tinyintTypeUnderTest.appendTo(block, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetLong() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = tinyintTypeUnderTest.getLong(block, 0);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        tinyintTypeUnderTest.writeLong(blockBuilder, 0L);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(tinyintTypeUnderTest.equals("other"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, tinyintTypeUnderTest.hashCode());
    }

    @Test
    public void testHash2() throws Exception
    {
        assertEquals(0L, TinyintType.hash((byte) 0b0));
    }
}
