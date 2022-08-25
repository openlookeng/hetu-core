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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class VarbinaryTypeTest
{
    private VarbinaryType varbinaryTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        varbinaryTypeUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(varbinaryTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(varbinaryTypeUnderTest.isOrderable());
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Object result = varbinaryTypeUnderTest.getObjectValue(null, block, 0);

        // Verify the results
    }

    @Test
    public void testEqualTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final boolean result = varbinaryTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = varbinaryTypeUnderTest.hash(block, 0);

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
        final int result = varbinaryTypeUnderTest.compareTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        final Block block = null;
        final BlockBuilder blockBuilder = null;

        // Run the test
        varbinaryTypeUnderTest.appendTo(block, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Block block = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = varbinaryTypeUnderTest.getSlice(block, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteSlice1()
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        final Slice value = null;

        // Run the test
        varbinaryTypeUnderTest.writeSlice(blockBuilder, value);

        // Verify the results
    }

    @Test
    public void testWriteSlice2() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        final Slice value = null;

        // Run the test
        varbinaryTypeUnderTest.writeSlice(blockBuilder, value, 0, 0);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(varbinaryTypeUnderTest.equals("other"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, varbinaryTypeUnderTest.hashCode());
    }

    @Test
    public void testIsVarbinaryType() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final boolean result = VarbinaryType.isVarbinaryType(type);

        // Verify the results
        assertTrue(result);
    }
}
