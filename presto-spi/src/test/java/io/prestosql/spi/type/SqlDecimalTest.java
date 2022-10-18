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

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SqlDecimalTest
{
    private SqlDecimal sqlDecimalUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        sqlDecimalUnderTest = new SqlDecimal(new BigInteger("100"), 0, 0);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(sqlDecimalUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, sqlDecimalUnderTest.hashCode());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", sqlDecimalUnderTest.toString());
    }

    @Test
    public void testToBigDecimal()
    {
        assertEquals(new BigDecimal("0.00"), sqlDecimalUnderTest.toBigDecimal());
    }

    @Test
    public void testOf1()
    {
        // Run the test
        final SqlDecimal result = SqlDecimal.of("decimalValue");
        assertTrue(result.equals("o"));
        assertEquals(0, result.getPrecision());
        assertEquals(0, result.getScale());
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
        assertEquals(new BigDecimal("0.00"), result.toBigDecimal());
        assertEquals(new BigInteger("100"), result.getUnscaledValue());
    }

    @Test
    public void testOf2() throws Exception
    {
        // Run the test
        final SqlDecimal result = SqlDecimal.of("unscaledValue", 0, 0);
        assertTrue(result.equals("o"));
        assertEquals(0, result.getPrecision());
        assertEquals(0, result.getScale());
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
        assertEquals(new BigDecimal("0.00"), result.toBigDecimal());
        assertEquals(new BigInteger("100"), result.getUnscaledValue());
    }

    @Test
    public void testOf3()
    {
        // Run the test
        final SqlDecimal result = SqlDecimal.of(0L, 0, 0);
        assertTrue(result.equals("o"));
        assertEquals(0, result.getPrecision());
        assertEquals(0, result.getScale());
        assertEquals(0, result.hashCode());
        assertEquals("result", result.toString());
        assertEquals(new BigDecimal("0.00"), result.toBigDecimal());
        assertEquals(new BigInteger("100"), result.getUnscaledValue());
    }
}
