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
package io.hetu.core.plugin.iceberg;

import com.google.common.base.VerifyException;
import io.airlift.slice.Slice;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.Int128;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.LongTimestampWithTimeZone;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.type.UuidType;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.hetu.core.plugin.iceberg.util.Timestamps.timestampTzToMicros;
import static io.prestosql.spi.type.TimeType.TIME_MICROS;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.prestosql.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static io.prestosql.type.UuidType.trinoUuidToJavaUuid;
import static java.lang.Float.intBitsToFloat;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.expressions.Expressions.alwaysFalse;
import static org.apache.iceberg.expressions.Expressions.alwaysTrue;
import static org.apache.iceberg.expressions.Expressions.and;
import static org.apache.iceberg.expressions.Expressions.equal;
import static org.apache.iceberg.expressions.Expressions.greaterThan;
import static org.apache.iceberg.expressions.Expressions.greaterThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.in;
import static org.apache.iceberg.expressions.Expressions.isNull;
import static org.apache.iceberg.expressions.Expressions.lessThan;
import static org.apache.iceberg.expressions.Expressions.lessThanOrEqual;
import static org.apache.iceberg.expressions.Expressions.not;

public final class ExpressionConverter
{
    private ExpressionConverter() {}

    public static Expression toIcebergExpression(TupleDomain<IcebergColumnHandle> tupleDomain)
    {
        if (tupleDomain.isAll()) {
            return alwaysTrue();
        }
        if (!tupleDomain.getDomains().isPresent()) {
            return alwaysFalse();
        }
        Map<IcebergColumnHandle, Domain> domainMap = tupleDomain.getDomains().get();
        Expression expression = alwaysTrue();
        for (Map.Entry<IcebergColumnHandle, Domain> entry : domainMap.entrySet()) {
            IcebergColumnHandle columnHandle = entry.getKey();
            Domain domain = entry.getValue();
            expression = and(expression, toIcebergExpression(columnHandle.getQualifiedName(), columnHandle.getType(), domain));
        }
        return expression;
    }

    private static Expression toIcebergExpression(String columnName, Type type, Domain domain)
    {
        if (domain.isAll()) {
            return alwaysTrue();
        }
        if (domain.getValues().isNone()) {
            return domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
        }

        if (domain.getValues().isAll()) {
            return domain.isNullAllowed() ? alwaysTrue() : not(isNull(columnName));
        }

        // Skip structural types. TODO (https://github.com/trinodb/trino/issues/8759) Evaluate Apache Iceberg's support for predicate on structural types
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            // Fail fast. Ignoring expression could lead to data loss in case of deletions.
            throw new UnsupportedOperationException("Unsupported type for expression: " + type);
        }

        if (type.isOrderable()) {
            List<Range> orderedRanges = domain.getValues().getRanges().getOrderedRanges();
            List<Object> icebergValues = new ArrayList<>();
            List<Expression> rangeExpressions = new ArrayList<>();
            for (Range range : orderedRanges) {
                if (range.isSingleValue()) {
                    icebergValues.add(getIcebergLiteralValue(type, range.getLowBoundedValue()));
                }
                else {
                    rangeExpressions.add(toIcebergExpression(columnName, range));
                }
            }
            Expression ranges = or(rangeExpressions);
            Expression values = icebergValues.isEmpty() ? alwaysFalse() : in(columnName, icebergValues);
            Expression nullExpression = domain.isNullAllowed() ? isNull(columnName) : alwaysFalse();
            return or(nullExpression, or(values, ranges));
        }

        throw new VerifyException(format("Unsupported type %s with domain values %s", type, domain));
    }

    private static Expression toIcebergExpression(String columnName, Range range)
    {
        Type type = range.getType();

        if (range.isSingleValue()) {
            Object icebergValue = getIcebergLiteralValue(type, range.getSingleValue());
            return equal(columnName, icebergValue);
        }

        Expression lowBound;
        if (range.isLowUnbounded()) {
            lowBound = alwaysTrue();
        }
        else {
            Object icebergLow = getIcebergLiteralValue(type, range.getLowBoundedValue());
            if (range.isLowInclusive()) {
                lowBound = greaterThanOrEqual(columnName, icebergLow);
            }
            else {
                lowBound = greaterThan(columnName, icebergLow);
            }
        }

        Expression highBound;
        if (range.isHighUnbounded()) {
            highBound = alwaysTrue();
        }
        else {
            Object icebergHigh = getIcebergLiteralValue(type, range.getHighBoundedValue());
            if (range.isHighInclusive()) {
                highBound = lessThanOrEqual(columnName, icebergHigh);
            }
            else {
                highBound = lessThan(columnName, icebergHigh);
            }
        }

        return and(lowBound, highBound);
    }

    private static Object getIcebergLiteralValue(Type type, Object trinoNativeValue)
    {
        requireNonNull(trinoNativeValue, "trinoNativeValue is null");

        if (type instanceof BooleanType) {
            return (boolean) trinoNativeValue;
        }

        if (type instanceof IntegerType) {
            if (trinoNativeValue instanceof IntArrayBlock) {
                return ((IntArrayBlock) trinoNativeValue).getInt(0, 0);
            }
            return toIntExact((long) trinoNativeValue);
        }

        if (type instanceof BigintType) {
            if (trinoNativeValue instanceof LongArrayBlock) {
                return ((LongArrayBlock) trinoNativeValue).getLong(0, 0);
            }
            return (long) trinoNativeValue;
        }

        if (type instanceof RealType) {
            return intBitsToFloat(toIntExact((long) trinoNativeValue));
        }

        if (type instanceof DoubleType) {
            return (double) trinoNativeValue;
        }

        // TODO: Remove this conversion once we move to next iceberg version
        if (type instanceof DateType) {
            if (trinoNativeValue instanceof IntArrayBlock) {
                return ((IntArrayBlock) trinoNativeValue).getInt(0, 0);
            }
            return toIntExact(((Long) trinoNativeValue));
        }

        if (type.equals(TIME_MICROS)) {
            return ((long) trinoNativeValue) / PICOSECONDS_PER_MICROSECOND;
        }

        if (type.equals(TIMESTAMP_MICROS)) {
            return (long) trinoNativeValue;
        }

        if (type.equals(TIMESTAMP_TZ_MICROS)) {
            return timestampTzToMicros((LongTimestampWithTimeZone) trinoNativeValue);
        }

        if (type instanceof VarcharType) {
            if (trinoNativeValue instanceof VariableWidthBlock) {
                int positionCount = ((VariableWidthBlock) trinoNativeValue).getPositionCount();
                Slice slice = ((VariableWidthBlock) trinoNativeValue).getRawSlice(positionCount);
                return slice.toStringUtf8();
            }
            return ((Slice) trinoNativeValue).toStringUtf8();
        }

        if (type instanceof VarbinaryType) {
            return ByteBuffer.wrap(((Slice) trinoNativeValue).getBytes());
        }

        if (type instanceof UuidType) {
            return trinoUuidToJavaUuid(((Slice) trinoNativeValue));
        }

        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (Decimals.isShortDecimal(decimalType)) {
                return BigDecimal.valueOf(getLong(trinoNativeValue)).movePointLeft(decimalType.getScale());
            }
            return new BigDecimal(((Int128) trinoNativeValue).toBigInteger(), decimalType.getScale());
        }

        throw new UnsupportedOperationException("Unsupported type: " + type);
    }

    private static long getLong(Object trinoNativeValue)
    {
        if (trinoNativeValue instanceof LongArrayBlock) {
            return ((LongArrayBlock) trinoNativeValue).getLong(0, 0);
        }
        return (long) trinoNativeValue;
    }

    private static Expression or(Expression left, Expression right)
    {
        return Expressions.or(left, right);
    }

    private static Expression or(List<Expression> expressions)
    {
        if (expressions.isEmpty()) {
            return alwaysFalse();
        }
        if (expressions.size() == 1) {
            return getOnlyElement(expressions);
        }
        int mid = expressions.size() / 2;
        return or(or(expressions.subList(0, mid)), or(expressions.subList(mid, expressions.size())));
    }
}
