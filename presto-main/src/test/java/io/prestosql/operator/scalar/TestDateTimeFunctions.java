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

package io.prestosql.operator.scalar;

import io.prestosql.Session;
import io.prestosql.spi.type.TimeType;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.testing.TestingSession;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;

import static io.prestosql.SystemSessionProperties.TIME_ZONE_ID;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimeZoneKey.UTC_KEY;
import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.spi.util.DateTimeZoneIndex.getDateTimeZone;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestDateTimeFunctions
        extends AbstractTestFunctions
{
    protected static final TimeZoneKey TIME_ZONE_KEY = TestingSession.DEFAULT_TIME_ZONE_KEY;
    protected static final DateTimeZone DATE_TIME_ZONE = getDateTimeZone(TIME_ZONE_KEY);
    protected static final DateTimeZone UTC_TIME_ZONE = getDateTimeZone(UTC_KEY);
    protected static final DateTimeZone DATE_TIME_ZONE_NUMERICAL = getDateTimeZone(getTimeZoneKey("-11:00"));
    protected static final TimeZoneKey KATHMANDU_ZONE_KEY = getTimeZoneKey("Asia/Kathmandu");
    protected static final DateTimeZone KATHMANDU_ZONE = getDateTimeZone(KATHMANDU_ZONE_KEY);
    protected static final ZoneOffset WEIRD_ZONE = ZoneOffset.ofHoursMinutes(7, 9);
    protected static final DateTimeZone WEIRD_DATE_TIME_ZONE = DateTimeZone.forID(WEIRD_ZONE.getId());

    protected static final DateTime DATE = new DateTime(2001, 8, 22, 0, 0, 0, 0, DateTimeZone.UTC);
    protected static final String DATE_LITERAL = "DATE '2001-08-22'";
    protected static final String DATE_ISO8601_STRING = "2001-08-22";

    protected static final LocalTime TIME = LocalTime.of(3, 4, 5, 321_000_000);
    protected static final String TIME_LITERAL = "TIME '03:04:05.321'";
    protected static final OffsetTime WEIRD_TIME = OffsetTime.of(3, 4, 5, 321_000_000, WEIRD_ZONE);
    protected static final String WEIRD_TIME_LITERAL = "TIME '03:04:05.321 +07:09'";

    protected static final DateTime TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC_TIME_ZONE); // This is TIMESTAMP w/o TZ
    protected static final DateTime TIMESTAMP_WITH_NUMERICAL_ZONE = new DateTime(2001, 8, 22, 3, 4, 5, 321, DATE_TIME_ZONE_NUMERICAL);
    protected static final String TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321'";
    protected static final String TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321-11:00";
    protected static final String TIMESTAMP_ISO8601_STRING_NO_TIME_ZONE = "2001-08-22T03:04:05.321";
    protected static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_DATE_TIME_ZONE);
    protected static final String WEIRD_TIMESTAMP_LITERAL = "TIMESTAMP '2001-08-22 03:04:05.321 +07:09'";
    protected static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+07:09";

    protected static final String INTERVAL_LITERAL = "INTERVAL '90061.234' SECOND";
    protected static final Duration DAY_TO_SECOND_INTERVAL = Duration.ofMillis(90061234);

    public TestDateTimeFunctions()
    {
        super(testSessionBuilder()
                .setTimeZoneKey(TIME_ZONE_KEY)
                .setStartTime(Instant.ofEpochMilli(new DateTime(2017, 4, 1, 12, 34, 56, 789, UTC_TIME_ZONE).getMillis()).getEpochSecond())
                .build());
    }

    @Test
    public void testToIso8601ForTimestampWithoutTimeZone()
    {
        assertFunction("to_iso8601(" + TIMESTAMP_LITERAL + ")", createVarcharType(35), TIMESTAMP_ISO8601_STRING_NO_TIME_ZONE);
    }

    @Test
    public void testFormatDateCannotImplicitlyAddTimeZoneToTimestampLiteral()
    {
        assertInvalidFunction(
                "format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')",
                "format_datetime for TIMESTAMP type, cannot use 'Z' nor 'z' in format, as this type does not contain TZ information");
    }

    @Test
    public void testLocalTime()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIME", TimeType.TIME, "14:30:00.000");
        }
    }

    @Test
    public void testCurrentTime()
    {
        Session localSession = Session.builder(session)
                // we use Asia/Kathmandu here to test the difference in semantic change of current_time
                // between legacy and non-legacy timestamp
                .setTimeZoneKey(KATHMANDU_ZONE_KEY)
                .setStartTime(new DateTime(2017, 3, 1, 15, 45, 0, 0, KATHMANDU_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIME", TIME_WITH_TIME_ZONE, "15:30:00.000 Asia/Kathmandu");
        }
    }

    @Test
    public void testLocalTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIMESTAMP", TimestampType.TIMESTAMP, "2017-03-01 14:30:00.000");
        }
    }

    @Test
    public void testCurrentTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStartTime(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis())
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIMESTAMP", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 " + DATE_TIME_ZONE.getID());
            localAssertion.assertFunctionString("NOW()", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 " + DATE_TIME_ZONE.getID());
        }
    }

    @Test
    public void testToUnixtime()
    {
        assertFunction("to_unixtime(TIMESTAMP'2021-07-25 17:10:00')", DOUBLE, 1.6271862E9);

        String timeZoneId = "Asia/Shanghai";
        Session localSession = Session.builder(session)
                .setSystemProperty(TIME_ZONE_ID, timeZoneId)
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunction("to_unixtime(TIMESTAMP'2021-07-25 17:10:00')", DOUBLE, 1.6272042E9);
        }
        assertFunction(String.format("to_unixtime(at_timezone(timestamp'2021-07-25 17:10:00', '%s'))", timeZoneId), DOUBLE, 1.6272042E9);

        timeZoneId = "Europe/London";
        localSession = Session.builder(session)
                .setSystemProperty(TIME_ZONE_ID, timeZoneId)
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunction("to_unixtime(TIMESTAMP'2021-07-25 17:10:00')", DOUBLE, 1.6272294E9);
        }
        assertFunction(String.format("to_unixtime(at_timezone(timestamp'2021-07-25 17:10:00', '%s'))", timeZoneId), DOUBLE, 1.6272294E9);

        timeZoneId = "America/New_York";
        localSession = Session.builder(session)
                .setSystemProperty(TIME_ZONE_ID, timeZoneId)
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunction("to_unixtime(TIMESTAMP'2021-07-25 17:10:00')", DOUBLE, 1.6272474E9);
        }
        assertFunction(String.format("to_unixtime(at_timezone(timestamp'2021-07-25 17:10:00', '%s'))", timeZoneId), DOUBLE, 1.6272474E9);
    }
}
