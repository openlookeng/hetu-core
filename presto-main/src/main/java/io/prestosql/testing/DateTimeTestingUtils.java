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
package io.prestosql.testing;

import io.prestosql.spi.type.SqlTime;
import io.prestosql.spi.type.SqlTimestamp;
import org.joda.time.DateTime;

import java.time.LocalDateTime;
import java.time.LocalTime;

import static java.lang.Math.toIntExact;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public final class DateTimeTestingUtils
{
    private DateTimeTestingUtils() {}

    public static SqlTimestamp sqlTimestampOf(
            int year,
            int monthOfYear,
            int dayOfMonth,
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond)
    {
        return sqlTimestampOf(LocalDateTime.of(year, monthOfYear, dayOfMonth, hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond)));
    }

    /**
     * Constructs standard (non-legacy) TIMESTAMP value corresponding to argument
     */
    public static SqlTimestamp sqlTimestampOf(LocalDateTime dateTime)
    {
        return new SqlTimestamp(DAYS.toMillis(dateTime.toLocalDate().toEpochDay()) + NANOSECONDS.toMillis(dateTime.toLocalTime().toNanoOfDay()));
    }

    private static SqlTimestamp sqlTimestampOf(DateTime dateTime)
    {
        return sqlTimestampOf(dateTime.getMillis());
    }

    public static SqlTimestamp sqlTimestampOf(long millis)
    {
        return new SqlTimestamp(millis);
    }

    public static SqlTime sqlTimeOf(
            int hourOfDay,
            int minuteOfHour,
            int secondOfMinute,
            int millisOfSecond)
    {
        LocalTime time = LocalTime.of(hourOfDay, minuteOfHour, secondOfMinute, millisToNanos(millisOfSecond));
        return sqlTimeOf(time);
    }

    public static SqlTime sqlTimeOf(LocalTime time)
    {
        return new SqlTime(NANOSECONDS.toMillis(time.toNanoOfDay()));
    }

    private static int millisToNanos(int millisOfSecond)
    {
        return toIntExact(MILLISECONDS.toNanos(millisOfSecond));
    }
}
