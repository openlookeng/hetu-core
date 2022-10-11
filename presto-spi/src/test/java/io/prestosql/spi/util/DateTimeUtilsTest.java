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
package io.prestosql.spi.util;

import io.prestosql.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class DateTimeUtilsTest
{
    @Test
    public void testParseDate() throws Exception
    {
        assertEquals(0, DateTimeUtils.parseDate("value"));
    }

    @Test
    public void testPrintDate() throws Exception
    {
        assertEquals("result", DateTimeUtils.printDate(0));
    }

    @Test
    public void testParseTimestampLiteral1() throws Exception
    {
        assertEquals(0L, DateTimeUtils.parseTimestampLiteral("value"));
    }

    @Test
    public void testParseTimestampLiteral2() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeUtils.parseTimestampLiteral(timeZoneKey, "value");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testParseTimestampWithTimeZone()
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeUtils.parseTimestampWithTimeZone(timeZoneKey, "timestampWithTimeZone");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testParseTimestampWithoutTimeZone1()
    {
        assertEquals(0L, DateTimeUtils.parseTimestampWithoutTimeZone("value"));
    }

    @Test
    public void testParseTimestampWithoutTimeZone2()
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeUtils.parseTimestampWithoutTimeZone(timeZoneKey, "value");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testPrintTimestampWithTimeZone()
    {
        assertEquals("result", DateTimeUtils.printTimestampWithTimeZone(0L));
    }

    @Test
    public void testPrintTimestampWithoutTimeZone()
    {
        assertEquals("result", DateTimeUtils.printTimestampWithoutTimeZone(0L));
    }

    @Test
    public void testTimestampHasTimeZone() throws Exception
    {
        assertTrue(DateTimeUtils.timestampHasTimeZone("value"));
    }

    @Test
    public void testParseTimeLiteral1() throws Exception
    {
        assertEquals(0L, DateTimeUtils.parseTimeLiteral("value"));
    }

    @Test
    public void testParseTimeLiteral2() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeUtils.parseTimeLiteral(timeZoneKey, "value");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testParseTimeWithTimeZone() throws Exception
    {
        assertEquals(0L, DateTimeUtils.parseTimeWithTimeZone("timeWithTimeZone"));
    }

    @Test
    public void testParseTimeWithoutTimeZone1() throws Exception
    {
        assertEquals(0L, DateTimeUtils.parseTimeWithoutTimeZone("value"));
    }

    @Test
    public void testParseTimeWithoutTimeZone2() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeUtils.parseTimeWithoutTimeZone(timeZoneKey, "value");

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testPrintTimeWithTimeZone() throws Exception
    {
        assertEquals("result", DateTimeUtils.printTimeWithTimeZone(0L));
    }

    @Test
    public void testPrintTimeWithoutTimeZone1() throws Exception
    {
        assertEquals("result", DateTimeUtils.printTimeWithoutTimeZone(0L));
    }

    @Test
    public void testPrintTimeWithoutTimeZone2() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final String result = DateTimeUtils.printTimeWithoutTimeZone(timeZoneKey, 0L);

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testTimeHasTimeZone() throws Exception
    {
        assertTrue(DateTimeUtils.timeHasTimeZone("value"));
    }
}
