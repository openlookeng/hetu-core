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
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class DateTimeZoneIndexTest
{
    @Test
    public void testGetChronology() throws Exception
    {
        // Setup
        final TimeZoneKey zoneKey = TimeZoneKey.getTimeZoneKey((short) 0);
        final ISOChronology expectedResult = ISOChronology.getInstanceUTC();

        // Run the test
        final ISOChronology result = DateTimeZoneIndex.getChronology(zoneKey);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUnpackChronology() throws Exception
    {
        // Setup
        final ISOChronology expectedResult = ISOChronology.getInstanceUTC();

        // Run the test
        final ISOChronology result = DateTimeZoneIndex.unpackChronology(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetDateTimeZone() throws Exception
    {
        // Setup
        final TimeZoneKey zoneKey = TimeZoneKey.getTimeZoneKey((short) 0);
        final DateTimeZone expectedResult = DateTimeZone.forID("id");

        // Run the test
        final DateTimeZone result = DateTimeZoneIndex.getDateTimeZone(zoneKey);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUnpackDateTimeZone() throws Exception
    {
        // Setup
        final DateTimeZone expectedResult = DateTimeZone.forID("id");

        // Run the test
        final DateTimeZone result = DateTimeZoneIndex.unpackDateTimeZone(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testPackDateTimeWithZone() throws Exception
    {
        assertEquals(0L, DateTimeZoneIndex.packDateTimeWithZone(new DateTime(2020, 1, 1, 1, 0, 0, 0)));
    }

    @Test
    public void testExtractZoneOffsetMinutes() throws Exception
    {
        assertEquals(0, DateTimeZoneIndex.extractZoneOffsetMinutes(0L));
    }
}
