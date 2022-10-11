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

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

public class Int128MathTest
{
    @Test
    public void testPowerOfTen() throws Exception
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.powerOfTen(0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRescale1()
    {
        // Setup
        // Run the test
        Int128Math.rescale(0L, 0L, 0, new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testRescale2() throws Exception
    {
        // Setup
        final Int128 decimal = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.rescale(decimal, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRescaleTruncate1() throws Exception
    {
        // Setup
        // Run the test
        Int128Math.rescaleTruncate(0L, 0L, 0, new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testRescaleTruncate2() throws Exception
    {
        // Setup
        final Int128 decimal = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.rescaleTruncate(decimal, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testAdd() throws Exception
    {
        // Setup
        // Run the test
        Int128Math.add(0L, 0L, 0L, 0L, new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testAddWithOverflow() throws Exception
    {
        assertEquals(0L, Int128Math.addWithOverflow(0L, 0L, 0L, 0L, new long[]{0L}, 0));
    }

    @Test
    public void testSubtract1()
    {
        // Setup
        // Run the test
        Int128Math.subtract(0L, 0L, 0L, 0L, new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testSubtract2() throws Exception
    {
        // Setup
        final Int128 left = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 right = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.subtract(left, right);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMultiply1()
    {
        // Setup
        final Int128 left = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 right = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.multiply(left, right);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMultiply3()
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.multiply(0L, 0L, 0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMultiply4()
    {
        // Setup
        final Int128 left = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.multiply(left, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMultiply5()
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.multiply(0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testMultiply256Destructive1()
    {
        // Setup
        final Int128 right = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        Int128Math.multiply256Destructive(new int[]{0}, right);

        // Verify the results
    }

    @Test
    public void testMultiply256Destructive2()
    {
        // Setup
        // Run the test
        Int128Math.multiply256Destructive(new int[]{0}, 0L);

        // Verify the results
    }

    @Test
    public void testMultiply256Destructive3()
    {
        // Setup
        // Run the test
        Int128Math.multiply256Destructive(new int[]{0}, 0);

        // Verify the results
    }

    @Test
    public void testCompareAbsolute1() throws Exception
    {
        // Setup
        final Int128 left = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 right = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final int result = Int128Math.compareAbsolute(left, right);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testCompareAbsolute2() throws Exception
    {
        assertEquals(0, Int128Math.compareAbsolute(0L, 0L, 0L, 0L));
    }

    @Test
    public void testAbsExact() throws Exception
    {
        // Setup
        final Int128 value = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.absExact(value);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testNegate1()
    {
        // Setup
        final Int128 value = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.negate(value);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testNegateExact() throws Exception
    {
        // Setup
        final Int128 value = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.negateExact(value);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShiftRight() throws Exception
    {
        // Setup
        // Run the test
        Int128Math.shiftRight(0L, 0L, 0, false, new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testFloorDiv1()
    {
        // Setup
        final Int128 dividend = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 divisor = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.floorDiv(dividend, divisor);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDivideRoundUp() throws Exception
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.divideRoundUp(0L, 0L, 0, 0L, 0L, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShiftLeft() throws Exception
    {
        // Setup
        // Run the test
        Int128Math.shiftLeft(new long[]{0L}, 0);

        // Verify the results
    }

    @Test
    public void testRemainder() throws Exception
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Int128Math.remainder(0L, 0L, 0, 0L, 0L, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShiftLeftMultiPrecision() throws Exception
    {
        assertEquals(new int[]{0}, Int128Math.shiftLeftMultiPrecision(new int[]{0}, 0, 0));
        assertEquals(new int[]{}, Int128Math.shiftLeftMultiPrecision(new int[]{0}, 0, 0));
    }

    @Test
    public void testShiftRightMultiPrecision() throws Exception
    {
        assertEquals(new int[]{0}, Int128Math.shiftRightMultiPrecision(new int[]{0}, 0, 0));
        assertEquals(new int[]{}, Int128Math.shiftRightMultiPrecision(new int[]{0}, 0, 0));
    }
}
