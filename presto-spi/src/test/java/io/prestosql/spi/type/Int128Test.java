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

import org.testng.annotations.Test;

import java.math.BigInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class Int128Test
{
    @Test
    public void testFromBigEndian() throws Exception
    {
        // Run the test
        final Int128 result = Int128.fromBigEndian("content".getBytes());
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testFromBigEndian_ThrowsArithmeticException()
    {
        // Setup
        // Run the test
        assertThrows(ArithmeticException.class, () -> Int128.fromBigEndian("content".getBytes()));
    }

    @Test
    public void testValueOf1()
    {
        // Run the test
        final Int128 result = Int128.valueOf(new long[]{0L});
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testValueOf2() throws Exception
    {
        // Run the test
        final Int128 result = Int128.valueOf(0L, 0L);
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testValueOf3()
    {
        // Run the test
        final Int128 result = Int128.valueOf("value");
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testValueOf4()
    {
        // Run the test
        final Int128 result = Int128.valueOf(new BigInteger("100"));
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testValueOf5()
    {
        // Run the test
        final Int128 result = Int128.valueOf(0L);
        assertEquals(0L, result.getHigh());
        assertEquals(0L, result.getLow());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final Int128 other = Int128.fromBigEndian("content".getBytes());
        assertEquals(0, result.compareTo(other));
        assertEquals("result", result.toString());
        assertEquals(new BigInteger("100"), result.toBigInteger());
        assertEquals("content".getBytes(), result.toBigEndianBytes());
        assertEquals(0L, result.toLong());
        assertEquals(0L, result.toLongExact());
        assertEquals(new long[]{0L}, result.toLongArray());
        assertTrue(result.isZero());
        assertTrue(result.isNegative());
    }

    @Test
    public void testCompare() throws Exception
    {
        assertEquals(0, Int128.compare(0L, 0L, 0L, 0L));
    }
}
