/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.prestosql.spi.heuristicindex;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.Locale;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static java.lang.Float.intBitsToFloat;

public class TypeUtils
{
    private TypeUtils() {}

    private static final String CAST_OPERATOR = "$operator$cast";

    public static Object getActualValue(Type type, Object value)
    {
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            return value;
        }
        else if (type instanceof BooleanType) {
            return value;
        }
        else if (type instanceof DoubleType) {
            return value;
        }
        else if (type instanceof DateType) {
            // keep the `long` representation of date
            return value;
        }
        else if (type instanceof RealType) {
            Long number = (Long) value;
            return intBitsToFloat(number.intValue());
        }
        else if (type instanceof VarcharType || type instanceof CharType) {
            if (value instanceof Slice) {
                return ((Slice) value).toStringUtf8();
            }
            return value;
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(value instanceof Long);
                return new BigDecimal(BigInteger.valueOf((Long) value), decimalType.getScale(), new MathContext(decimalType.getPrecision()));
            }
            Slice slice;
            if (value instanceof String) {
                slice = Slices.utf8Slice((String) value);
            }
            else {
                checkState(value instanceof Slice);
                slice = (Slice) value;
            }
            return new BigDecimal(decodeUnscaledValue(slice), decimalType.getScale(), new MathContext(decimalType.getPrecision()));
        }
        else if (type instanceof TimestampType) {
            Long time = (Long) value;
            return new Timestamp(time);
        }

        throw new UnsupportedOperationException("Not Implemented Exception: " + value + "->" + type);
    }

    public static Object extractValueFromRowExpression(RowExpression rowExpression)
    {
        if (rowExpression instanceof CallExpression) {
            CallExpression callExpression = (CallExpression) rowExpression;
            String name = ((BuiltInFunctionHandle) callExpression.getFunctionHandle()).getSignature().getNameSuffix();
            if (name.equals(CAST_OPERATOR)) {
                Object valueExtracted = extractValueFromRowExpression(callExpression.getArguments().get(0));
                String valueTypeName = valueExtracted.getClass().getTypeName().toLowerCase(Locale.ROOT);
                String expectedTypeName = rowExpression.getType().getDisplayName().toLowerCase(Locale.ROOT);
                if (!valueTypeName.contains(expectedTypeName) && expectedTypeName.equals("real")) {
                    // A cast is necessary for Real type, so use Float casting
                    return Float.parseFloat(valueExtracted.toString());
                }
                else if (!valueTypeName.contains(expectedTypeName) && expectedTypeName.equals("date")) {
                    // A cast is necessary for Date type, so use Long casting
                    LocalDate dateGiven = LocalDate.parse(valueExtracted.toString());
                    return dateGiven.toEpochDay();
                }
                return valueExtracted;
            }
        }
        else if (rowExpression instanceof ConstantExpression) {
            ConstantExpression constant = (ConstantExpression) rowExpression;
            return getActualValue(constant.getType(), constant.getValue());
        }

        throw new UnsupportedOperationException("Not Implemented Exception: " + rowExpression.toString());
    }
}
