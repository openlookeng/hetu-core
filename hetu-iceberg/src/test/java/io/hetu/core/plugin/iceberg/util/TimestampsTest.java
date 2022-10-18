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
package io.hetu.core.plugin.iceberg.util;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TimestampsTest
{
    @Test
    public void testTimestampTzToMicros()
    {
        // Setup
        final LongTimestampWithTimeZone timestamp = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0,
                (short) 0);

        // Run the test
        final long result = Timestamps.timestampTzToMicros(timestamp);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testTimestampTzFromMicros()
    {
        // Setup
        final LongTimestampWithTimeZone expectedResult = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0,
                (short) 0);

        // Run the test
        final LongTimestampWithTimeZone result = Timestamps.timestampTzFromMicros(0L);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetTimestampTz()
    {
        // Setup
        final Block block = null;
        final LongTimestampWithTimeZone expectedResult = LongTimestampWithTimeZone.fromEpochMillisAndFraction(0L, 0,
                (short) 0);

        // Run the test
        final LongTimestampWithTimeZone result = Timestamps.getTimestampTz(block, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }
}
