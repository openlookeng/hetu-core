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
package io.prestosql.spi.statistics;

//import io.prestosql.spi.type.*;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Int128;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.Type;

import java.util.OptionalDouble;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DecimalConversions.longDecimalToDouble;
import static io.prestosql.spi.type.DecimalConversions.shortDecimalToDouble;
import static io.prestosql.spi.type.Decimals.longTenToNth;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public final class StatsUtil
{
    private StatsUtil() {}

    public static OptionalDouble toStatsRepresentation(Type type, Object value)
    {
        requireNonNull(type, "type is null");
        requireNonNull(value, "value is null");

        if (type == BOOLEAN) {
            return OptionalDouble.of((boolean) value ? 1 : 0);
        }
        if (type == TINYINT || type == SMALLINT || type == INTEGER || type == BIGINT) {
            return OptionalDouble.of((long) value);
        }
        if (type == REAL) {
            return OptionalDouble.of(intBitsToFloat(toIntExact((Long) value)));
        }
        if (type == DOUBLE) {
            return OptionalDouble.of((double) value);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                return OptionalDouble.of(shortDecimalToDouble((long) value, longTenToNth(decimalType.getScale())));
            }
            return OptionalDouble.of(longDecimalToDouble((Int128) value, decimalType.getScale()));
        }
        if (type == DATE) {
            return OptionalDouble.of((long) value);
        }
        if (type instanceof TimestampType) {
            if (((TimestampType) type).isShort()) {
                return OptionalDouble.of((long) value);
            }
            return OptionalDouble.of(((LongTimestamp) value).getEpochMicros());
        }
        if (type instanceof TimestampWithTimeZoneType) {
            if (((TimestampWithTimeZoneType) type).isShort()) {
                return OptionalDouble.of(unpackMillisUtc((long) value));
            }
            return OptionalDouble.of(((LongTimestampWithTimeZone) value).getEpochMillis());
        }

        return OptionalDouble.empty();
    }
}
