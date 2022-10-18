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
import static org.testng.Assert.assertTrue;

public class LongTimestampWithTimeZoneTest
{
    @Test
    public void testFromEpochSecondsAndFraction() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final LongTimestampWithTimeZone result = LongTimestampWithTimeZone.fromEpochSecondsAndFraction(0L, 0L,
                timeZoneKey);
        assertEquals(0L, result.getEpochMillis());
        assertEquals(0, result.getPicosOfMilli());
        assertEquals((short) 0, result.getTimeZoneKey());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final LongTimestampWithTimeZone other = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0, (short) 0);
        assertEquals(0, result.compareTo(other));
    }

    @Test
    public void testFromEpochMillisAndFraction1() throws Exception
    {
        // Setup
        final TimeZoneKey timeZoneKey = TimeZoneKey.getTimeZoneKey((short) 0);

        // Run the test
        final LongTimestampWithTimeZone result = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0,
                timeZoneKey);
        assertEquals(0L, result.getEpochMillis());
        assertEquals(0, result.getPicosOfMilli());
        assertEquals((short) 0, result.getTimeZoneKey());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final LongTimestampWithTimeZone other = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0, (short) 0);
        assertEquals(0, result.compareTo(other));
    }

    @Test
    public void testFromEpochMillisAndFraction2() throws Exception
    {
        // Run the test
        final LongTimestampWithTimeZone result = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0, (short) 0);
        assertEquals(0L, result.getEpochMillis());
        assertEquals(0, result.getPicosOfMilli());
        assertEquals((short) 0, result.getTimeZoneKey());
        assertTrue(result.equals("o"));
        assertEquals(0, result.hashCode());
        final LongTimestampWithTimeZone other = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0, (short) 0);
        assertEquals(0, result.compareTo(other));
    }
}
