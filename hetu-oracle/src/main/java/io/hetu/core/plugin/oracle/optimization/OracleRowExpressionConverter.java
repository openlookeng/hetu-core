/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.oracle.optimization;

import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.util.Arrays;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.hetu.core.plugin.oracle.optimization.OraclePushDownUtils.getCastExpression;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.StandardFunctionUtils.isArrayConstructor;
import static io.prestosql.spi.function.StandardFunctionUtils.isCastFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isSubscriptFunction;
import static io.prestosql.spi.relation.SpecialForm.Form.IF;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class OracleRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private static final String AT_TIMEZONE_FUNCTION_NAME = "at_timezone";
    private static final Set<String> timeExtractFields = Arrays.stream(Time.ExtractField.values())
            .map(Time.ExtractField::name)
            .map(String::toLowerCase)
            .collect(toImmutableSet());

    public OracleRowExpressionConverter(RowExpressionService rowExpressionService)
    {
        super(rowExpressionService);
    }

    @Override
    public String visitCall(CallExpression call, Void context)
    {
        Signature signature = call.getSignature();
        String functionName = call.getSignature().getName().toLowerCase(ENGLISH);
        if (timeExtractFields.contains(functionName)) {
            if (call.getArguments().size() == 1) {
                try {
                    Time.ExtractField field = Time.ExtractField.valueOf(functionName.toUpperCase(ENGLISH));
                    return format("EXTRACT(%s FROM %s)", field, call.getArguments().get(0).accept(this, null));
                }
                catch (IllegalArgumentException e) {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal argument: " + e);
                }
            }
            else {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal argument num of function " + functionName);
            }
        }
        if (functionName.equals(AT_TIMEZONE_FUNCTION_NAME)) {
            if (call.getArguments().size() == 2) {
                return format("%s AT TIME ZONE %s",
                        call.getArguments().get(0).accept(this, null),
                        call.getArguments().get(1).accept(this, null));
            }
            else {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal argument num of function " + functionName);
            }
        }
        if (isArrayConstructor(signature)) {
            throw new PrestoException(NOT_SUPPORTED, "Oracle connector does not support array constructor");
        }
        if (isSubscriptFunction(signature)) {
            throw new PrestoException(NOT_SUPPORTED, "Oracle connector does not support subscript expression");
        }
        if (isCastFunction(signature)) {
            // deal with literal, when generic literal expression translate to rowExpression, it will be
            // translated to a 'CAST' rowExpression with a varchar type 'CONSTANT' rowExpression, in some
            // case, 'CAST' is superfluous
            RowExpression argument = call.getArguments().get(0);
            Type type = call.getType();
            if (argument instanceof ConstantExpression && argument.getType() instanceof VarcharType) {
                String value = argument.accept(this, null);
                if (type instanceof DateType) {
                    return format("date %s", value);
                }
                if (type instanceof VarcharType
                        || type instanceof CharType
                        || type instanceof VarbinaryType
                        || type instanceof DecimalType
                        || type instanceof RealType
                        || type instanceof DoubleType) {
                    return value;
                }
            }
            if (call.getType().getDisplayName().equals(LIKE_PATTERN_NAME)) {
                return call.getArguments().get(0).accept(this, null);
            }
            return getCastExpression(call.getArguments().get(0).accept(this, null), call.getType());
        }
        return super.visitCall(call, context);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, Void context)
    {
        // Oracle sql does not support if, convert IF to [case ... when ... else] expression
        if (specialForm.getForm().equals(IF)) {
            return format("(CASE WHEN %s THEN %s ELSE %s END)",
                    specialForm.getArguments().get(0).accept(this, null),
                    specialForm.getArguments().get(1).accept(this, null),
                    specialForm.getArguments().get(2).accept(this, null));
        }
        return super.visitSpecialForm(specialForm, context);
    }
}
