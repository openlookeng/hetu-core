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
package io.prestosql.util;

import io.prestosql.client.IntervalDayTime;
import io.prestosql.client.IntervalYearMonth;
import io.prestosql.spi.PrestoException;
import io.prestosql.sql.tree.IntervalLiteral.IntervalField;
import org.joda.time.DurationFieldType;
import org.joda.time.MutablePeriod;
import org.joda.time.Period;
import org.joda.time.ReadWritablePeriod;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;
import org.joda.time.format.PeriodParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static java.lang.String.format;

public final class DateTimePeriodUtils
{
    private DateTimePeriodUtils() {}

    private static final int YEAR_FIELD = 0;
    private static final int MONTH_FIELD = 1;
    private static final int DAY_FIELD = 3;
    private static final int HOUR_FIELD = 4;
    private static final int MINUTE_FIELD = 5;
    private static final int SECOND_FIELD = 6;
    private static final int MILLIS_FIELD = 7;

    private static final PeriodFormatter INTERVAL_DAY_SECOND_FORMATTER = cretePeriodFormatter(IntervalField.DAY, IntervalField.SECOND);
    private static final PeriodFormatter INTERVAL_DAY_MINUTE_FORMATTER = cretePeriodFormatter(IntervalField.DAY, IntervalField.MINUTE);
    private static final PeriodFormatter INTERVAL_DAY_HOUR_FORMATTER = cretePeriodFormatter(IntervalField.DAY, IntervalField.HOUR);
    private static final PeriodFormatter INTERVAL_DAY_FORMATTER = cretePeriodFormatter(IntervalField.DAY, IntervalField.DAY);

    private static final PeriodFormatter INTERVAL_HOUR_SECOND_FORMATTER = cretePeriodFormatter(IntervalField.HOUR, IntervalField.SECOND);
    private static final PeriodFormatter INTERVAL_HOUR_MINUTE_FORMATTER = cretePeriodFormatter(IntervalField.HOUR, IntervalField.MINUTE);
    private static final PeriodFormatter INTERVAL_HOUR_FORMATTER = cretePeriodFormatter(IntervalField.HOUR, IntervalField.HOUR);

    private static final PeriodFormatter INTERVAL_MINUTE_SECOND_FORMATTER = cretePeriodFormatter(IntervalField.MINUTE, IntervalField.SECOND);
    private static final PeriodFormatter INTERVAL_MINUTE_FORMATTER = cretePeriodFormatter(IntervalField.MINUTE, IntervalField.MINUTE);

    private static final PeriodFormatter INTERVAL_SECOND_FORMATTER = cretePeriodFormatter(IntervalField.SECOND, IntervalField.SECOND);

    private static final PeriodFormatter INTERVAL_YEAR_MONTH_FORMATTER = cretePeriodFormatter(IntervalField.YEAR, IntervalField.MONTH);
    private static final PeriodFormatter INTERVAL_YEAR_FORMATTER = cretePeriodFormatter(IntervalField.YEAR, IntervalField.YEAR);

    private static final PeriodFormatter INTERVAL_MONTH_FORMATTER = cretePeriodFormatter(IntervalField.MONTH, IntervalField.MONTH);

    public static long parseDayTimeInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        IntervalField end = endField.orElse(startField);

        if (startField == IntervalField.DAY && end == IntervalField.SECOND) {
            return parsePeriodMillis(INTERVAL_DAY_SECOND_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.DAY && end == IntervalField.MINUTE) {
            return parsePeriodMillis(INTERVAL_DAY_MINUTE_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.DAY && end == IntervalField.HOUR) {
            return parsePeriodMillis(INTERVAL_DAY_HOUR_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.DAY && end == IntervalField.DAY) {
            return parsePeriodMillis(INTERVAL_DAY_FORMATTER, value, startField, end);
        }

        if (startField == IntervalField.HOUR && end == IntervalField.SECOND) {
            return parsePeriodMillis(INTERVAL_HOUR_SECOND_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.HOUR && end == IntervalField.MINUTE) {
            return parsePeriodMillis(INTERVAL_HOUR_MINUTE_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.HOUR && end == IntervalField.HOUR) {
            return parsePeriodMillis(INTERVAL_HOUR_FORMATTER, value, startField, end);
        }

        if (startField == IntervalField.MINUTE && end == IntervalField.SECOND) {
            return parsePeriodMillis(INTERVAL_MINUTE_SECOND_FORMATTER, value, startField, end);
        }
        if (startField == IntervalField.MINUTE && end == IntervalField.MINUTE) {
            return parsePeriodMillis(INTERVAL_MINUTE_FORMATTER, value, startField, end);
        }

        if (startField == IntervalField.SECOND && end == IntervalField.SECOND) {
            return parsePeriodMillis(INTERVAL_SECOND_FORMATTER, value, startField, end);
        }

        throw new IllegalArgumentException("Invalid day second interval qualifier: " + startField + " to " + end);
    }

    public static long parsePeriodMillis(PeriodFormatter periodFormatter, String value, IntervalField startField, IntervalField endField)
    {
        try {
            Period period = parsePeriod(periodFormatter, value);
            return IntervalDayTime.toMillis(
                    period.getValue(DAY_FIELD),
                    period.getValue(HOUR_FIELD),
                    period.getValue(MINUTE_FIELD),
                    period.getValue(SECOND_FIELD),
                    period.getValue(MILLIS_FIELD));
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField);
        }
    }

    public static long parseYearMonthInterval(String value, IntervalField startField, Optional<IntervalField> endField)
    {
        IntervalField end = endField.orElse(startField);

        if (startField == IntervalField.YEAR && end == IntervalField.MONTH) {
            PeriodFormatter periodFormatter = INTERVAL_YEAR_MONTH_FORMATTER;
            return parsePeriodMonths(value, periodFormatter, startField, end);
        }
        if (startField == IntervalField.YEAR && end == IntervalField.YEAR) {
            return parsePeriodMonths(value, INTERVAL_YEAR_FORMATTER, startField, end);
        }

        if (startField == IntervalField.MONTH && end == IntervalField.MONTH) {
            return parsePeriodMonths(value, INTERVAL_MONTH_FORMATTER, startField, end);
        }

        throw new IllegalArgumentException("Invalid year month interval qualifier: " + startField + " to " + end);
    }

    private static long parsePeriodMonths(String value, PeriodFormatter periodFormatter, IntervalField startField, IntervalField endField)
    {
        try {
            Period period = parsePeriod(periodFormatter, value);
            return IntervalYearMonth.toMonths(
                    period.getValue(YEAR_FIELD),
                    period.getValue(MONTH_FIELD));
        }
        catch (IllegalArgumentException e) {
            throw invalidInterval(e, value, startField, endField);
        }
    }

    private static Period parsePeriod(PeriodFormatter periodFormatter, String value)
    {
        String tmpValue = value;
        boolean negative = tmpValue.startsWith("-");
        if (negative) {
            tmpValue = tmpValue.substring(1);
        }

        Period period = periodFormatter.parsePeriod(tmpValue);
        for (DurationFieldType type : period.getFieldTypes()) {
            checkArgument(period.get(type) >= 0, "Period field %s is negative", type);
        }

        if (negative) {
            period = period.negated();
        }
        return period;
    }

    private static PrestoException invalidInterval(Throwable throwable, String value, IntervalField startField, IntervalField endField)
    {
        String message;
        if (startField == endField) {
            message = format("Invalid INTERVAL %s value: %s", startField, value);
        }
        else {
            message = format("Invalid INTERVAL %s TO %s value: %s", startField, endField, value);
        }
        return new PrestoException(INVALID_FUNCTION_ARGUMENT, message, throwable);
    }

    private static PeriodFormatter cretePeriodFormatter(IntervalField startField, IntervalField endField)
    {
        IntervalField tmpEndField = endField == null ? startField : endField;

        List<PeriodParser> parsers = new ArrayList<>();

        PeriodFormatterBuilder builder = new PeriodFormatterBuilder();
        switch (startField) {
            case YEAR:
                builder.appendYears();
                parsers.add(builder.toParser());
                if (tmpEndField == IntervalField.YEAR) {
                    break;
                }
                builder.appendLiteral("-");
                // fall through

            case MONTH:
                builder.appendMonths();
                parsers.add(builder.toParser());
                if (tmpEndField != IntervalField.MONTH) {
                    throw new IllegalArgumentException("Invalid interval qualifier: " + startField + " to " + tmpEndField);
                }
                break;

            case DAY:
                builder.appendDays();
                parsers.add(builder.toParser());
                if (tmpEndField == IntervalField.DAY) {
                    break;
                }
                builder.appendLiteral(" ");
                // fall through

            case HOUR:
                builder.appendHours();
                parsers.add(builder.toParser());
                if (tmpEndField == IntervalField.HOUR) {
                    break;
                }
                builder.appendLiteral(":");
                // fall through

            case MINUTE:
                builder.appendMinutes();
                parsers.add(builder.toParser());
                if (tmpEndField == IntervalField.MINUTE) {
                    break;
                }
                builder.appendLiteral(":");
                // fall through

            case SECOND:
                builder.appendSecondsWithOptionalMillis();
                parsers.add(builder.toParser());
                break;
        }

        return new PeriodFormatter(builder.toPrinter(), new OrderedPeriodParser(parsers));
    }

    private static class OrderedPeriodParser
            implements PeriodParser
    {
        private final List<PeriodParser> parsers;

        private OrderedPeriodParser(List<PeriodParser> parsers)
        {
            this.parsers = parsers;
        }

        @Override
        public int parseInto(ReadWritablePeriod period, String text, int position, Locale locale)
        {
            int bestValidPos = position;
            ReadWritablePeriod bestValidPeriod = null;

            int bestInvalidPos = position;

            for (PeriodParser parser : parsers) {
                ReadWritablePeriod parsedPeriod = new MutablePeriod();
                int parsePos = parser.parseInto(parsedPeriod, text, position, locale);
                if (parsePos >= position) {
                    if (parsePos > bestValidPos) {
                        bestValidPos = parsePos;
                        bestValidPeriod = parsedPeriod;
                        if (parsePos >= text.length()) {
                            break;
                        }
                    }
                }
                else if (parsePos < 0) {
                    parsePos = ~parsePos;
                    if (parsePos > bestInvalidPos) {
                        bestInvalidPos = parsePos;
                    }
                }
            }

            if (bestValidPos > position || (bestValidPos == position)) {
                // Restore the state to the best valid parse.
                if (bestValidPeriod != null) {
                    period.setPeriod(bestValidPeriod);
                }
                return bestValidPos;
            }

            return ~bestInvalidPos;
        }
    }
}
