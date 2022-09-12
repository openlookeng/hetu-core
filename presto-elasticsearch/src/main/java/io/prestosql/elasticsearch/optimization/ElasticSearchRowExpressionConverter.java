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
package io.prestosql.elasticsearch.optimization;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TimestampType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.ExpressionFormatter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.time.Instant;
import java.time.format.DateTimeFormatter;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.util.DateTimeUtils.printDate;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;

public class ElasticSearchRowExpressionConverter implements RowExpressionConverter<ElasticSearchConverterContext> {

    private final static String EMPTY_STRING = "";

    @Override
    public String visitCall(CallExpression call, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitCall(call, context);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitSpecialForm(specialForm, context);
    }

    @Override
    public String visitConstant(ConstantExpression literal, ElasticSearchConverterContext context) {
        Type type = literal.getType();

        if (literal.getValue() == null) {
            return "null";
        }
        if (type instanceof BooleanType) {
            return String.valueOf(((Boolean) literal.getValue()).booleanValue());
        }
        if (type instanceof BigintType || type instanceof TinyintType || type instanceof SmallintType || type instanceof IntegerType) {
            Number number = (Number) literal.getValue();
            return format("%d", number.longValue());
        }
        if (type instanceof DoubleType) {
            return literal.getValue().toString();
        }
        if (type instanceof RealType) {
            Long number = (Long) literal.getValue();
            return format("%f", intBitsToFloat(number.intValue()));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            if (decimalType.isShort()) {
                checkState(literal.getValue() instanceof Long);
                return decodeDecimal(BigInteger.valueOf((long) literal.getValue()), decimalType).toString();
            }
            checkState(literal.getValue() instanceof Slice);
            Slice value = (Slice) literal.getValue();
            return decodeDecimal(decodeUnscaledValue(value), decimalType).toString();
        }
        if (type instanceof VarcharType || type instanceof CharType) {
            return "'" + ((Slice) literal.getValue()).toStringUtf8() + "'";
        }
        if (type.equals(DATE)) {
            String date = printDate(Integer.parseInt(String.valueOf(literal.getValue())));
            return  ExpressionFormatter.formatStringLiteral(date);
        }
        if (type instanceof TimestampType) {
            DateTimeFormatter formatter = DateTimeFormatter.ISO_DATE_TIME;
            Long time = (Long) literal.getValue();
            return  formatter.format(Instant.ofEpochMilli(time).atZone(UTC).toLocalDateTime());
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Cannot handle the constant expression %s with value of type %s", literal.getValue(), type));

    }

    @Override
    public String visitVariableReference(VariableReferenceExpression reference, ElasticSearchConverterContext context) {
        return RowExpressionConverter.super.visitVariableReference(reference, context);
    }

    @Override
    public String visitInputReference(InputReferenceExpression reference, ElasticSearchConverterContext context) {
        return handleUnsupportedOptimize(context);
    }

    @Override
    public String visitLambda(LambdaDefinitionExpression lambda, ElasticSearchConverterContext context) {
        return handleUnsupportedOptimize(context);
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    public String handleUnsupportedOptimize(ElasticSearchConverterContext context){
        context.setConversionFailed();
        return EMPTY_STRING;
    }
}
