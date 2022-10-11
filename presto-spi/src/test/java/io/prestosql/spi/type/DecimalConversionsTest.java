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
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;

public class DecimalConversionsTest
{
    @Test
    public void testShortDecimalToDouble() throws Exception
    {
        assertEquals(0.0, DecimalConversions.shortDecimalToDouble(0L, 0L), 0.0001);
    }

    @Test
    public void testLongDecimalToDouble1() throws Exception
    {
        // Setup
        final Slice decimal = null;

        // Run the test
        final double result = DecimalConversions.longDecimalToDouble(decimal, 0L);

        // Verify the results
        assertEquals(0.0, result, 0.0001);
    }

    @Test
    public void testLongDecimalToDouble2() throws Exception
    {
        // Setup
        final Int128 decimal = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final double result = DecimalConversions.longDecimalToDouble(decimal, 0L);

        // Verify the results
        assertEquals(0.0, result, 0.0001);
    }

    @Test
    public void testShortDecimalToReal() throws Exception
    {
        assertEquals(0L, DecimalConversions.shortDecimalToReal(0L, 0L));
    }

    @Test
    public void testLongDecimalToReal() throws Exception
    {
        // Setup
        final Slice decimal = null;

        // Run the test
        final long result = DecimalConversions.longDecimalToReal(decimal, 0L);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testDoubleToShortDecimal() throws Exception
    {
        assertEquals(0L, DecimalConversions.doubleToShortDecimal(0.0, 0L, 0L));
    }

    @Test
    public void testDoubleToLongDecimal() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = DecimalConversions.doubleToLongDecimal(0.0, 0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testRealToShortDecimal() throws Exception
    {
        assertEquals(0L, DecimalConversions.realToShortDecimal(0L, 0L, 0L));
    }

    @Test
    public void testRealToLongDecimal() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = DecimalConversions.realToLongDecimal(0L, 0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testShortToShortCast() throws Exception
    {
        assertEquals(0L, DecimalConversions.shortToShortCast(0L, 0L, 0L, 0L, 0L, 0L, 0L));
    }

    @Test
    public void testShortToLongCast() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = DecimalConversions.shortToLongCast(0L, 0L, 0L, 0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testLongToShortCast() throws Exception
    {
        // Setup
        final Slice value = null;

        // Run the test
        final long result = DecimalConversions.longToShortCast(value, 0L, 0L, 0L, 0L);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testLongToLongCast() throws Exception
    {
        // Setup
        final Slice value = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = DecimalConversions.longToLongCast(value, 0L, 0L, 0L, 0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIntScale() throws Exception
    {
        assertEquals(0, DecimalConversions.intScale(0L));
    }
}
