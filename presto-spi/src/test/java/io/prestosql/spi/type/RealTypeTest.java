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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class RealTypeTest
{
    private RealType realTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        realTypeUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Object result = realTypeUnderTest.getObjectValue(null, block, 0);

        // Verify the results
    }

    @Test
    public void testEqualTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final boolean result = realTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = realTypeUnderTest.hash(block, 0);

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
        final int result = realTypeUnderTest.compareTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        realTypeUnderTest.writeLong(blockBuilder, 0L);

        // Verify the results
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(realTypeUnderTest.equals("other"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, realTypeUnderTest.hashCode());
    }

    @Test
    public void testGetRange() throws Exception
    {
        // Setup
        // Run the test
        final Optional<Type.Range> result = realTypeUnderTest.getRange();

        // Verify the results
    }
}
