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
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DecimalsTest
{
    @Test
    public void testLongTenToNth() throws Exception
    {
        assertEquals(0L, Decimals.longTenToNth(0));
    }

    @Test
    public void testBigIntegerTenToNth() throws Exception
    {
        assertEquals(new BigInteger("100"), Decimals.bigIntegerTenToNth(0));
    }

    @Test
    public void testParse1() throws Exception
    {
        // Setup
        final DecimalParseResult expectedResult = new DecimalParseResult("object", DecimalType.createDecimalType(0));

        // Run the test
        final DecimalParseResult result = Decimals.parse("stringValue");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testParseIncludeLeadingZerosInPrecision()
    {
        // Setup
        final DecimalParseResult expectedResult = new DecimalParseResult("object", DecimalType.createDecimalType(0));

        // Run the test
        final DecimalParseResult result = Decimals.parseIncludeLeadingZerosInPrecision("stringValue");

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEncodeUnscaledValue1() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Decimals.encodeUnscaledValue(new BigInteger("100"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEncodeUnscaledValue2() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Decimals.encodeUnscaledValue(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEncodeShortScaledValue() throws Exception
    {
        assertEquals(0L, Decimals.encodeShortScaledValue(new BigDecimal("0.00"), 0));
    }

    @Test
    public void testEncodeScaledValue1() throws Exception
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Decimals.encodeScaledValue(new BigDecimal("0.00"), 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEncodeInt128Value()
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Decimals.encodeInt128Value(new BigDecimal("0.00"), 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testEncodeScaledValue2() throws Exception
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Decimals.encodeScaledValue(new BigDecimal("0.00"), 0, RoundingMode.UP);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testValueOf1()
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Decimals.valueOf(new BigDecimal("0.00"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testValueOf2() throws Exception
    {
        // Setup
        final Int128 expectedResult = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final Int128 result = Decimals.valueOf(new BigInteger("100"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testThrowIfOverflows()
    {
        // Setup
        // Run the test
        Decimals.throwIfOverflows(0L, 0L);

        // Verify the results
    }

    @Test
    public void testOverflows1()
    {
        assertTrue(Decimals.overflows(0L, 0L));
    }

    @Test
    public void testEncodeScaledValue3()
    {
        // Setup
        final Slice expectedResult = null;

        // Run the test
        final Slice result = Decimals.encodeScaledValue(new BigDecimal("0.00"));

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testDecodeUnscaledValue() throws Exception
    {
        // Setup
        final Slice valueSlice = null;

        // Run the test
        final BigInteger result = Decimals.decodeUnscaledValue(valueSlice);

        // Verify the results
        assertEquals(new BigInteger("100"), result);
    }

    @Test
    public void testToString1()
    {
        assertEquals("result", Decimals.toString(0L, 0));
    }

    @Test
    public void testToString2() throws Exception
    {
        // Setup
        final Slice unscaledValue = null;

        // Run the test
        final String result = Decimals.toString(unscaledValue, 0);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testToString3() throws Exception
    {
        assertEquals("result", Decimals.toString(new BigInteger("100"), 0));
    }

    @Test
    public void testOverflows2()
    {
        assertTrue(Decimals.overflows(0L, 0));
    }

    @Test
    public void testOverflows3()
    {
        assertTrue(Decimals.overflows(new BigInteger("100"), 0));
    }

    @Test
    public void testOverflows4()
    {
        assertTrue(Decimals.overflows(new BigInteger("100")));
    }

    @Test
    public void testOverflows5()
    {
        assertTrue(Decimals.overflows(new BigDecimal("0.00"), 0L));
    }

    @Test
    public void testCheckOverflow() throws Exception
    {
        // Setup
        // Run the test
        Decimals.checkOverflow(new BigInteger("100"));

        // Verify the results
    }

    @Test
    public void testReadBigDecimal()
    {
        // Setup
        final DecimalType type = DecimalType.createDecimalType(0);
        final Block block = null;

        // Run the test
        final BigDecimal result = Decimals.readBigDecimal(type, block, 0);

        // Verify the results
        assertEquals(new BigDecimal("0.00"), result);
    }

    @Test
    public void testWriteBigDecimal()
    {
        // Setup
        final DecimalType decimalType = DecimalType.createDecimalType(0);
        final BlockBuilder blockBuilder = null;

        // Run the test
        Decimals.writeBigDecimal(decimalType, blockBuilder, new BigDecimal("0.00"));

        // Verify the results
    }

    @Test
    public void testRescale1()
    {
        // Setup
        final DecimalType type = DecimalType.createDecimalType(0);

        // Run the test
        final BigDecimal result = Decimals.rescale(new BigDecimal("0.00"), type);

        // Verify the results
        assertEquals(new BigDecimal("0.00"), result);
    }

    @Test
    public void testWriteShortDecimal() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        Decimals.writeShortDecimal(blockBuilder, 0L);

        // Verify the results
    }

    @Test
    public void testRescale2() throws Exception
    {
        assertEquals(0L, Decimals.rescale(0L, 0, 0));
    }

    @Test
    public void testRescale3() throws Exception
    {
        assertEquals(new BigInteger("100"), Decimals.rescale(new BigInteger("100"), 0, 0));
    }

    @Test
    public void testIsShortDecimal() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final boolean result = Decimals.isShortDecimal(type);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsLongDecimal() throws Exception
    {
        // Setup
        final Type type = null;

        // Run the test
        final boolean result = Decimals.isLongDecimal(type);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString5()
    {
        // Setup
        final Int128 unscaledValue = Int128.fromBigEndian("content".getBytes(StandardCharsets.UTF_8));

        // Run the test
        final String result = Decimals.toString(unscaledValue, 0);

        // Verify the results
        assertEquals("result", result);
    }
}
