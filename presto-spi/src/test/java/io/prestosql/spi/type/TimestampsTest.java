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

import java.time.ZoneOffset;

import static org.testng.Assert.assertEquals;

public class TimestampsTest
{
    @Test
    public void testRound() throws Exception
    {
        assertEquals(0L, Timestamps.round(0L, 0));
    }

    @Test
    public void testRescale() throws Exception
    {
        assertEquals(0L, Timestamps.rescale(0L, 0, 0));
    }

    @Test
    public void testRoundDiv1()
    {
        assertEquals(0, Timestamps.roundDiv(0, 0L));
    }

    @Test
    public void testRoundDiv2()
    {
        assertEquals(0L, Timestamps.roundDiv(0L, 0L));
    }

    @Test
    public void testTruncateEpochMicrosToMillis() throws Exception
    {
        assertEquals(0L, Timestamps.truncateEpochMicrosToMillis(0L));
    }

    @Test
    public void testEpochMicrosToMillisWithRounding() throws Exception
    {
        assertEquals(0L, Timestamps.epochMicrosToMillisWithRounding(0L));
    }

    @Test
    public void testFormatTimestamp1()
    {
        assertEquals("result", Timestamps.formatTimestamp(0, 0L, 0));
    }

    @Test
    public void testFormatTimestampWithTimeZone()
    {
        assertEquals("result", Timestamps.formatTimestampWithTimeZone(0, 0L, 0, ZoneOffset.UTC));
    }
}
