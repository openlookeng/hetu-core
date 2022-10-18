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
package io.prestosql.cost;

import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Int128;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.LongTimestamp;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TimestampWithTimeZoneType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.util.OptionalDouble;

import static io.prestosql.metadata.CastType.CAST;
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
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

final class StatsUtil
{
    private StatsUtil() {}

    static OptionalDouble toStatsRepresentation(Metadata metadata, ConnectorSession session, Type type, Object value)
    {
        if (convertibleToDoubleWithCast(type)) {
            InterpretedFunctionInvoker functionInvoker = new InterpretedFunctionInvoker(metadata.getFunctionAndTypeManager());
            FunctionHandle cast = metadata.getFunctionAndTypeManager().lookupCast(CAST, type.getTypeSignature(), DoubleType.DOUBLE.getTypeSignature());

            return OptionalDouble.of((double) functionInvoker.invoke(cast, session, singletonList(value)));
        }

        if (DateType.DATE.equals(type)) {
            return OptionalDouble.of(((Long) value).doubleValue());
        }

        return OptionalDouble.empty();
    }

    private static boolean convertibleToDoubleWithCast(Type type)
    {
        return type instanceof DecimalType
                || DoubleType.DOUBLE.equals(type)
                || RealType.REAL.equals(type)
                || BigintType.BIGINT.equals(type)
                || IntegerType.INTEGER.equals(type)
                || SmallintType.SMALLINT.equals(type)
                || TinyintType.TINYINT.equals(type)
                || BooleanType.BOOLEAN.equals(type);
    }

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
