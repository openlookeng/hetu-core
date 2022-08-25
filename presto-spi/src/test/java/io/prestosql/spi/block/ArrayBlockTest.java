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

import io.prestosql.spi.util.BloomFilter;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ArrayBlockTest
{
    @Test
    public void testFromElementBlock() throws Exception
    {
        // Setup
        final Block values = null;

        // Run the test
        final Block result = ArrayBlock.fromElementBlock(0, Optional.of(new boolean[]{false}), new int[]{0}, values);

        // Verify the results
    }

    @Test
    public void testCreateArrayBlockInternal() throws Exception
    {
        // Setup
        final Block values = null;

        // Run the test
        final ArrayBlock result = ArrayBlock.createArrayBlockInternal(0, 0, new boolean[]{false}, new int[]{0}, values);
        assertEquals(0, result.getPositionCount());
        assertEquals(0L, result.getSizeInBytes());
        assertEquals(0L, result.getRetainedSizeInBytes());
        assertEquals("result", result.toString());
        assertEquals(null, result.getLoadedBlock());
        assertTrue(result.equals("obj"));
        assertEquals(0, result.hashCode());
        assertEquals(0, result.getOffset(0));
        assertEquals("ARRAY", result.getEncodingName());
        assertEquals(null, result.copyPositions(new int[]{0}, 0, 0));
        assertEquals(null, result.getRegion(0, 0));
        assertEquals(0L, result.getRegionSizeInBytes(0, 0));
        assertEquals(0L, result.getPositionsSizeInBytes(new boolean[]{false}));
        assertEquals(null, result.copyRegion(0, 0));
        assertEquals(null, result.getObject(0, Object.class));
        assertEquals(null, result.getSingleValueBlock(0));
        assertEquals(0L, result.getEstimatedDataSizeForStats(0));
        assertTrue(result.isNull(0));
        final AbstractArrayBlock.ArrayBlockFunction<Object> function = null;
        assertEquals(null, result.apply(function, 0));
        assertEquals(0, result.getSliceLength(0));
        assertEquals((byte) 0b0, result.getByte(0, 0));
        assertEquals((short) 0, result.getShort(0, 0));
        assertEquals(0, result.getInt(0, 0));
        assertEquals(0L, result.getLong(0, 0));
        assertEquals(null, result.getSlice(0, 0, 0));
        assertEquals("result", result.getString(0, 0, 0));
        assertTrue(result.bytesEqual(0, 0, null, 0, 0));
        assertEquals(0, result.bytesCompare(0, 0, 0, null, 0, 0));
        assertTrue(result.equals(0, 0, null, 0, 0, 0));
        assertEquals(0L, result.hash(0, 0, 0));
        assertEquals(0, result.compareTo(0, 0, 0, null, 0, 0, 0));
        assertEquals(0L, result.getLogicalSizeInBytes());
        assertEquals(null, result.getPositions(new int[]{0}, 0, 0));
        assertTrue(result.mayHaveNull());
        assertEquals(null, result.get(0));
        assertEquals(new boolean[]{false}, result.filter(new BloomFilter(0L, 0.0), new boolean[]{false}));
        assertEquals(0, result.filter(new int[]{0}, 0, new int[]{0}, val -> {
            return false;
        }));
    }
}
