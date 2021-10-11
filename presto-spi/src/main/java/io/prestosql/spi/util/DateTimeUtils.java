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
import org.joda.time.LocalDateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;
import org.joda.time.format.DateTimePrinter;
import org.joda.time.format.ISODateTimeFormat;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.util.DateTimeZoneIndex.getChronology;
import static io.prestosql.spi.util.DateTimeZoneIndex.getDateTimeZone;
import static io.prestosql.spi.util.DateTimeZoneIndex.packDateTimeWithZone;
import static io.prestosql.spi.util.DateTimeZoneIndex.unpackChronology;
import static io.prestosql.spi.util.DateTimeZoneIndex.unpackDateTimeZone;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public final class DateTimeUtils
{
    private DateTimeUtils()
    {
    }

    private static final DateTimeFormatter DATE_FORMATTER = ISODateTimeFormat.date().withZoneUTC();

    public static int parseDate(String value)
    {
        return toIntExact(TimeUnit.MILLISECONDS.toDays(DATE_FORMATTER.parseMillis(value)));
    }

    public static String printDate(int days)
    {
        return DATE_FORMATTER.print(TimeUnit.DAYS.toMillis(days));
    }

    private static final DateTimeFormatter TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_TIME_ZONE_FORMATTER;
    private static final DateTimeFormatter TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER;

    static {
        DateTimeParser[] timestampWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-d").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d'T'H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d'T'H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d'T'H:m:s.SSS").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS").getParser()};
        DateTimePrinter timestampWithoutTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS").getPrinter();

        TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithoutTimeZonePrinter, timestampWithoutTimeZoneParser)
                .toFormatter()
                .withZoneUTC();

        DateTimeParser[] timestampWithTimeZoneParser = {
                DateTimeFormat.forPattern("yyyy-M-dZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:mZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:sZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS Z").getParser(),
                DateTimeFormat.forPattern("yyyy-M-dZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:mZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:sZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s ZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSSZZZ").getParser(),
                DateTimeFormat.forPattern("yyyy-M-d H:m:s.SSS ZZZ").getParser()};
        DateTimePrinter timestampWithTimeZonePrinter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ").getPrinter();
        TIMESTAMP_WITH_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();

        DateTimeParser[] timestampWithOrWithoutTimeZoneParser = Stream.concat(Stream.of(timestampWithoutTimeZoneParser), Stream.of(timestampWithTimeZoneParser))
                .toArray(DateTimeParser[]::new);
        TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder()
                .append(timestampWithTimeZonePrinter, timestampWithOrWithoutTimeZoneParser)
                .toFormatter()
                .withOffsetParsed();
    }

    /**
     * {@link LocalDateTime#getLocalMillis()}
     */
    private static final MethodHandle getLocalMillis;

    static {
        try {
            Method getLocalMillisMethod = LocalDateTime.class.getDeclaredMethod("getLocalMillis");
            getLocalMillisMethod.setAccessible(true);
            getLocalMillis = MethodHandles.lookup().unreflect(getLocalMillisMethod);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse a string (optionally containing a zone) as a value of either TIMESTAMP or TIMESTAMP WITH TIME ZONE type.
     * <p>
     * For example: {@code "2000-01-01 01:23:00"} is parsed to TIMESTAMP {@code 2000-01-01T01:23:00}
     * and {@code "2000-01-01 01:23:00 +01:23"} is parsed to TIMESTAMP WITH TIME ZONE
     * {@code 2000-01-01T01:23:00.000+01:23}.
     *
     * @return stack representation of TIMESTAMP or TIMESTAMP WITH TIME ZONE type, depending on input
     */
    public static long parseTimestampLiteral(String value)
    {
        try {
            DateTime dateTime = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(value);
            return packDateTimeWithZone(dateTime);
        }
        catch (Exception e) {
            return TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.parseMillis(value);
        }
    }

    /**
     * Parse a string (optionally containing a zone) as a value of either TIMESTAMP or TIMESTAMP WITH TIME ZONE type.
     * If the string doesn't specify a zone, it is interpreted in {@code timeZoneKey} zone.
     *
     * @return stack representation of legacy TIMESTAMP or TIMESTAMP WITH TIME ZONE type, depending on input
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long parseTimestampLiteral(TimeZoneKey timeZoneKey, String value)
    {
        DateTime dateTime = TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseDateTime(value);
        return packDateTimeWithZone(dateTime);
    }

    /**
     * Parse a string (optionally containing a zone) as a value of TIMESTAMP WITH TIME ZONE type.
     * If the string doesn't specify a zone, it is interpreted in {@code timeZoneKey} zone.
     * <p>
     * For example: {@code "2000-01-01 01:23:00"} is parsed to TIMESTAMP WITH TIME ZONE
     * {@code 2000-01-01T01:23:00 <provided zone>} and {@code "2000-01-01 01:23:00 +01:23"}
     * is parsed to TIMESTAMP WITH TIME ZONE {@code 2000-01-01T01:23:00.000+01:23}.
     *
     * @return stack representation of TIMESTAMP WITH TIME ZONE type
     */
    public static long parseTimestampWithTimeZone(TimeZoneKey timeZoneKey, String timestampWithTimeZone)
    {
        DateTime dateTime = TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER.withChronology(getChronology(timeZoneKey)).withOffsetParsed().parseDateTime(timestampWithTimeZone);
        return packDateTimeWithZone(dateTime);
    }

    /**
     * Parse a string (optionally containing a zone) as a value of TIMESTAMP type.
     * If the string specifies a zone, the zone is discarded.
     * <p>
     * For example: {@code "2000-01-01 01:23:00"} is parsed to TIMESTAMP {@code 2000-01-01T01:23:00}
     * and {@code "2000-01-01 01:23:00 +01:23"} is also parsed to TIMESTAMP {@code 2000-01-01T01:23:00.000}.
     *
     * @return stack representation of TIMESTAMP type
     */
    public static long parseTimestampWithoutTimeZone(String value)
    {
        LocalDateTime localDateTime = TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER.parseLocalDateTime(value);
        try {
            return (long) getLocalMillis.invokeExact(localDateTime);
        }
        catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parse a string (optionally containing a zone) as a value of TIMESTAMP type.
     * If the string doesn't specify a zone, it is interpreted in {@code timeZoneKey} zone.
     *
     * @return stack representation of legacy TIMESTAMP type
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long parseTimestampWithoutTimeZone(TimeZoneKey timeZoneKey, String value)
    {
        return TIMESTAMP_WITH_OR_WITHOUT_TIME_ZONE_FORMATTER.withChronology(getChronology(timeZoneKey)).parseMillis(value);
    }

    public static String printTimestampWithTimeZone(long timestampWithTimeZone)
    {
        ISOChronology chronology = unpackChronology(timestampWithTimeZone);
        long millis = unpackMillisUtc(timestampWithTimeZone);
        return TIMESTAMP_WITH_TIME_ZONE_FORMATTER.withChronology(chronology).print(millis);
    }

    public static String printTimestampWithoutTimeZone(long timestamp)
    {
        return TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.print(timestamp);
    }

    public static boolean timestampHasTimeZone(String value)
    {
        try {
            try {
                TIMESTAMP_WITH_TIME_ZONE_FORMATTER.parseMillis(value);
                return true;
            }
            catch (RuntimeException e) {
                // `.withZoneUTC()` makes `timestampHasTimeZone` return value independent of JVM zone
                TIMESTAMP_WITHOUT_TIME_ZONE_FORMATTER.withZoneUTC().parseMillis(value);
                return false;
            }
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException(format("Invalid timestamp '%s'", value));
        }
    }

    private static final DateTimeFormatter TIME_FORMATTER;
    private static final DateTimeFormatter TIME_WITH_TIME_ZONE_FORMATTER;

    static {
        DateTimeParser[] timeWithoutTimeZoneParser = {
                DateTimeFormat.forPattern("H:m").getParser(),
                DateTimeFormat.forPattern("H:m:s").getParser(),
                DateTimeFormat.forPattern("H:m:s.SSS").getParser()};
        DateTimePrinter timeWithoutTimeZonePrinter = DateTimeFormat.forPattern("HH:mm:ss.SSS").getPrinter();
        TIME_FORMATTER = new DateTimeFormatterBuilder().append(timeWithoutTimeZonePrinter, timeWithoutTimeZoneParser).toFormatter().withZoneUTC();

        DateTimeParser[] timeWithTimeZoneParser = {
                DateTimeFormat.forPattern("H:mZ").getParser(),
                DateTimeFormat.forPattern("H:m Z").getParser(),
                DateTimeFormat.forPattern("H:m:sZ").getParser(),
                DateTimeFormat.forPattern("H:m:s Z").getParser(),
                DateTimeFormat.forPattern("H:m:s.SSSZ").getParser(),
                DateTimeFormat.forPattern("H:m:s.SSS Z").getParser(),
                DateTimeFormat.forPattern("H:mZZZ").getParser(),
                DateTimeFormat.forPattern("H:m ZZZ").getParser(),
                DateTimeFormat.forPattern("H:m:sZZZ").getParser(),
                DateTimeFormat.forPattern("H:m:s ZZZ").getParser(),
                DateTimeFormat.forPattern("H:m:s.SSSZZZ").getParser(),
                DateTimeFormat.forPattern("H:m:s.SSS ZZZ").getParser()};
        DateTimePrinter timeWithTimeZonePrinter = DateTimeFormat.forPattern("HH:mm:ss.SSS ZZZ").getPrinter();
        TIME_WITH_TIME_ZONE_FORMATTER = new DateTimeFormatterBuilder().append(timeWithTimeZonePrinter, timeWithTimeZoneParser).toFormatter().withOffsetParsed();
    }

    /**
     * Parse a string (optionally containing a zone) as a value of either TIME or TIME WITH TIME ZONE type.
     * <p>
     * For example: {@code "01:23:00"} is parsed to TIME {@code 01:23:00}
     * and {@code "01:23:00 +01:23"} is parsed to TIME WITH TIME ZONE
     * {@code 01:23:00+01:23}.
     *
     * @return stack representation of TIME or TIME WITH TIME ZONE type, depending on input
     */
    public static long parseTimeLiteral(String value)
    {
        try {
            return parseTimeWithTimeZone(value);
        }
        catch (Exception e) {
            return parseTimeWithoutTimeZone(value);
        }
    }

    /**
     * Parse a string (optionally containing a zone) as a value of either TIME or TIME WITH TIME ZONE type.
     * If the string doesn't specify a zone, it is interpreted in {@code timeZoneKey} zone.
     *
     * @return stack representation of legacy TIME or TIME WITH TIME ZONE type, depending on input
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long parseTimeLiteral(TimeZoneKey timeZoneKey, String value)
    {
        try {
            return parseTimeWithTimeZone(value);
        }
        catch (Exception e) {
            return parseTimeWithoutTimeZone(timeZoneKey, value);
        }
    }

    /**
     * Parse a string containing a zone as a value of TIME WITH TIME ZONE type.
     * <p>
     * For example: {@code "01:23:00 +01:23"} is parsed to TIME WITH TIME ZONE
     * {@code 01:23:00+01:23} and {@code "01:23:00"} is rejected.
     *
     * @return stack representation of TIME WITH TIME ZONE type
     */
    public static long parseTimeWithTimeZone(String timeWithTimeZone)
    {
        DateTime dateTime = TIME_WITH_TIME_ZONE_FORMATTER.parseDateTime(timeWithTimeZone);
        return packDateTimeWithZone(dateTime);
    }

    /**
     * Parse a string (without a zone) as a value of TIME type.
     * <p>
     * For example: {@code "01:23:00"} is parsed to TIME {@code 01:23:00}
     * and {@code "01:23:00 +01:23"} is rejected.
     *
     * @return stack representation of TIME type
     */
    public static long parseTimeWithoutTimeZone(String value)
    {
        return TIME_FORMATTER.parseMillis(value);
    }

    /**
     * Parse a string (without a zone) as a value of TIME type, interpreted in {@code timeZoneKey} zone.
     *
     * @return stack representation of legacy TIME type
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static long parseTimeWithoutTimeZone(TimeZoneKey timeZoneKey, String value)
    {
        return TIME_FORMATTER.withZone(getDateTimeZone(timeZoneKey)).parseMillis(value);
    }

    public static String printTimeWithTimeZone(long timeWithTimeZone)
    {
        DateTimeZone timeZone = unpackDateTimeZone(timeWithTimeZone);
        long millis = unpackMillisUtc(timeWithTimeZone);
        return TIME_WITH_TIME_ZONE_FORMATTER.withZone(timeZone).print(millis);
    }

    public static String printTimeWithoutTimeZone(long value)
    {
        return TIME_FORMATTER.print(value);
    }

    /**
     * @deprecated applicable in legacy timestamp semantics only
     */
    @Deprecated
    public static String printTimeWithoutTimeZone(TimeZoneKey timeZoneKey, long value)
    {
        return TIME_FORMATTER.withZone(getDateTimeZone(timeZoneKey)).print(value);
    }

    public static boolean timeHasTimeZone(String value)
    {
        try {
            try {
                parseTimeWithTimeZone(value);
                return true;
            }
            catch (RuntimeException e) {
                parseTimeWithoutTimeZone(value);
                return false;
            }
        }
        catch (RuntimeException e) {
            throw new IllegalArgumentException(format("Invalid time '%s'", value));
        }
    }
}
