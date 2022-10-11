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
import io.prestosql.spi.block.BlockBuilderStatus;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TimestampWithTimeZoneTypeTest
{
    @Test
    public void testCreateTimestampWithTimeZoneType()
    {
        // Run the test
        final TimestampWithTimeZoneType result = TimestampWithTimeZoneType.createTimestampWithTimeZoneType(0);
        final Block block = null;
        assertEquals("result", result.getObjectValue(null, block, 0));
        final Block leftBlock = null;
        final Block rightBlock = null;
        assertTrue(result.equalTo(leftBlock, 0, rightBlock, 0));
        final Block block1 = null;
        assertEquals(0L, result.hash(block1, 0));
        final Block leftBlock1 = null;
        final Block rightBlock1 = null;
        assertEquals(0, result.compareTo(leftBlock1, 0, rightBlock1, 0));
        assertTrue(result.equals("other"));
        assertEquals(0, result.hashCode());
        assertTrue(result.isShort());
        assertEquals(0, result.getPrecision());
        assertEquals(0, result.getFixedSize());
        assertTrue(result.isComparable());
        assertTrue(result.isOrderable());
        final Block block2 = null;
        assertEquals(0L, result.getLong(block2, 0));
        final Block block3 = null;
        assertEquals(null, result.getSlice(block3, 0));
        final BlockBuilderStatus blockBuilderStatus = null;
        assertEquals(null, result.createBlockBuilder(blockBuilderStatus, 0, 0));
        final BlockBuilderStatus blockBuilderStatus1 = null;
        assertEquals(null, result.createBlockBuilder(blockBuilderStatus1, 0));
        assertEquals(null, result.createFixedSizeBlockBuilder(0));
        assertEquals(new TypeSignature("base", TypeSignatureParameter.of(0L)), result.getTypeSignature());
        assertEquals("signature", result.getDisplayName());
        assertEquals(Object.class, result.getJavaType());
        assertEquals(Arrays.asList(), result.getTypeParameters());
        assertEquals("result", result.getObject(null, 0));
        assertTrue(result.getBoolean(null, 0));
        assertEquals(0.0, result.getDouble(null, 0), 0.0001);
        assertEquals("signature", result.toString());
        final Block<Object> block4 = null;
        assertEquals(null, result.get(block4, 0));
        assertEquals(Optional.of(new Type.Range("min", "max")), result.getRange());
    }
}
