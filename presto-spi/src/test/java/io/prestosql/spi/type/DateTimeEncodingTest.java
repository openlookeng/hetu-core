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

import static org.testng.Assert.assertEquals;

public class DateTimeEncodingTest
{
    @Test
    public void testPackDateTimeWithZone1()
    {
        assertEquals(0L, DateTimeEncoding.packDateTimeWithZone(0L, "zoneId"));
    }

    @Test
    public void testPackDateTimeWithZone2()
    {
        assertEquals(0L, DateTimeEncoding.packDateTimeWithZone(0L, 0));
    }

    @Test
    public void testPackDateTimeWithZone3()
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final long result = DateTimeEncoding.packDateTimeWithZone(0L, timeZoneKey);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testUnpackMillisUtc() throws Exception
    {
        assertEquals(0L, DateTimeEncoding.unpackMillisUtc(0L));
    }

    @Test
    public void testUnpackZoneKey()
    {
        // Setup
        final TimeZoneKey expectedResult = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final TimeZoneKey result = DateTimeEncoding.unpackZoneKey(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testUpdateMillisUtc() throws Exception
    {
        assertEquals(0L, DateTimeEncoding.updateMillisUtc(0L, 0L));
    }
}
