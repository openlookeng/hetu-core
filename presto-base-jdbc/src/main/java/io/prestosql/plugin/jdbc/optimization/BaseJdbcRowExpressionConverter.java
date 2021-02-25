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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.Signature.unmangleOperator;
import static io.prestosql.spi.function.StandardFunctionUtils.isArithmeticFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isArrayConstructor;
import static io.prestosql.spi.function.StandardFunctionUtils.isCastFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isComparisonFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isLikeFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isNegateFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isNotFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isOperator;
import static io.prestosql.spi.function.StandardFunctionUtils.isSubscriptFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isTryFunction;
import static io.prestosql.spi.sql.RowExpressionUtils.isDeterministic;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BaseJdbcRowExpressionConverter
        implements RowExpressionConverter
{
    public static final String COUNT_FUNCTION_NAME = "count";
    protected static final String LIKE_PATTERN_NAME = "LikePattern";
    private static final String INTERNAL_FUNCTION_PREFIX = "$";
    private static final String TIMESTAMP_LITERAL = "$literal$timestamp";
    private static final String DYNAMIC_FILTER_FUNCTION_NAME = "$internal$dynamic_filter_function";

    private final Map<String, Integer> blacklistFunctions;
    private final RowExpressionService rowExpressionService;

    public BaseJdbcRowExpressionConverter(RowExpressionService rowExpressionService)
    {
        this(rowExpressionService, Collections.emptyMap());
    }

    public BaseJdbcRowExpressionConverter(RowExpressionService rowExpressionService, Map<String, Integer> blacklistFunctions)
    {
        this.rowExpressionService = rowExpressionService;
        requireNonNull(blacklistFunctions, "BlackListFunctions cannot be null");
        this.blacklistFunctions = blacklistFunctions;
    }

    @Override
    public String visitCall(CallExpression call, Void context)
    {
        if (!isDeterministic(rowExpressionService.getDeterminismEvaluator(), call)) {
            throw new PrestoException(NOT_SUPPORTED, format("Unsupported not deterministic function [%s] push down.", call.getSignature().getName()));
        }
        Signature signature = call.getSignature();
        String functionName = call.getSignature().getName().toLowerCase(ENGLISH);
        if (isNotFunction(signature)) {
            return format("(NOT %s)", call.getArguments().get(0).accept(this, null));
        }
        if (isTryFunction(signature)) {
            return format("TRY(%s)", call.getArguments().get(0).accept(this, null));
        }
        if (isLikeFunction(signature)) {
            return format("(%s LIKE %s)",
                    call.getArguments().get(0).accept(this, null),
                    call.getArguments().get(1).accept(this, null));
        }
        if (isArrayConstructor(signature)) {
            String arguments = Joiner.on(",").join(call.getArguments().stream().map(expression -> expression.accept(this, null)).collect(toList()));
            return format("ARRAY[%s]", arguments);
        }
        if (isOperator(signature)) {
            return handleOperator(call);
        }
        if (functionName.equals(TIMESTAMP_LITERAL)) {
            long time = (long) ((ConstantExpression) call.getArguments().get(0)).getValue();
            return format("TIMESTAMP '%s'", new Timestamp(time));
        }
        return handleFunction(call);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, Void context)
    {
        switch (specialForm.getForm()) {
            case AND:
            case OR:
                return format("(%s %s %s)",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getForm().toString(),
                        specialForm.getArguments().get(1).accept(this, null));
            case IS_NULL:
                return format("(%s IS NULL)", specialForm.getArguments().get(0).accept(this, null));
            case NULL_IF:
                return format("NULLIF(%s, %s)",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getArguments().get(1).accept(this, null));
            case IN:
                String value = specialForm.getArguments().get(0).accept(this, null);
                String valueList = Joiner.on(", ").join(IntStream.range(1, specialForm.getArguments().size())
                        .mapToObj(i -> specialForm.getArguments().get(i))
                        .map(expression -> expression.accept(this, null))
                        .collect(toList()));
                return format("(%s IN (%s))", value, valueList);
            case BETWEEN:
                return format("(%s BETWEEN %s AND %s)",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getArguments().get(1).accept(this, null),
                        specialForm.getArguments().get(2).accept(this, null));
            case ROW_CONSTRUCTOR:
                return format("ROW (%s)", Joiner.on(", ").join(specialForm.getArguments().stream()
                        .map(expression -> expression.accept(this, null))
                        .collect(toList())));
            case COALESCE:
                String argument = Joiner.on(",")
                        .join(specialForm.getArguments().stream()
                                .map(expression -> expression.accept(this, null))
                                .collect(toList()));
                return format("COALESCE(%s)", argument);
            case IF:
                // convert IF to [case ... when ... else] expression
                return format("IF (%s, %s, %s)",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getArguments().get(1).accept(this, null),
                        specialForm.getArguments().get(2).accept(this, null));
            case SWITCH:
                int size = specialForm.getArguments().size();
                return format("(CASE %s %s ELSE %s END)",
                        specialForm.getArguments().get(0).accept(this, null),
                        Joiner.on(' ').join(IntStream.range(1, size - 1)
                                .mapToObj(i -> specialForm.getArguments().get(i).accept(this, null))
                                .collect(toList())),
                        specialForm.getArguments().get(size - 1).accept(this, null));
            case WHEN:
                return format("WHEN %s THEN %s",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getArguments().get(1).accept(this, null));
            case DEREFERENCE:
                return format("%s.%s",
                        specialForm.getArguments().get(0).accept(this, null),
                        specialForm.getArguments().get(1).accept(this, null));
            default:
                throw new PrestoException(NOT_SUPPORTED, String.format("specialForm %s not supported in filter", specialForm.getForm()));
        }
    }

    @Override
    public String visitConstant(ConstantExpression literal, Void context)
    {
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
        if (type instanceof TimestampType) {
            Long time = (Long) literal.getValue();
            return format("TIMESTAMP '%s'", new Timestamp(time));
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Cannot handle the constant expression %s with value of type %s", literal.getValue(), type));
    }

    @Override
    public String visitVariableReference(VariableReferenceExpression reference, Void context)
    {
        return reference.getName();
    }

    private String handleOperator(CallExpression call)
    {
        Signature signature = call.getSignature();

        List<RowExpression> arguments = call.getArguments();
        if (isCastFunction(signature)) {
            if (call.getType().getDisplayName().equals(LIKE_PATTERN_NAME)) {
                return arguments.get(0).accept(this, null);
            }
            return format("CAST(%s AS %s)", arguments.get(0).accept(this, null), call.getType().getDisplayName());
        }
        if (call.getArguments().size() == 1 && isNegateFunction(signature)) {
            String value = call.getArguments().get(0).accept(this, null);
            String separator = value.startsWith("-") ? " " : "";
            return format("-%s%s", separator, value);
        }
        if (arguments.size() == 2 && (isComparisonFunction(signature) || isArithmeticFunction(signature))) {
            return format(
                    "(%s %s %s)",
                    arguments.get(0).accept(this, null),
                    unmangleOperator(signature.getName()).getOperator(),
                    arguments.get(1).accept(this, null));
        }
        if (isSubscriptFunction(signature)) {
            String base = call.getArguments().get(0).accept(this, null);
            String index = call.getArguments().get(1).accept(this, null);
            return format("%s[%s]", base, index);
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Unknown operator %s in push down", signature));
    }

    private String handleFunction(CallExpression callExpression)
    {
        Signature function = callExpression.getSignature();
        List<RowExpression> arguments = callExpression.getArguments();
        if (isBlackListFunction(function)) {
            if (DYNAMIC_FILTER_FUNCTION_NAME.equals(function.getName())) {
                return "true";
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("Unsupported function in push down %s", function.getName()));
        }
        if (function.getName().equals(COUNT_FUNCTION_NAME) && callExpression.getArguments().size() == 0) {
            return "count(*)";
        }
        else {
            StringBuilder builder = new StringBuilder(function.getName());
            StringJoiner joiner = new StringJoiner(", ", "(", ")");
            for (RowExpression expression : arguments) {
                joiner.add(expression.accept(this, null));
            }
            builder.append(joiner);
            return builder.toString();
        }
    }

    private boolean isBlackListFunction(Signature signature)
    {
        if (signature.getName().contains(INTERNAL_FUNCTION_PREFIX)) {
            return true;
        }
        Integer args = blacklistFunctions.get(signature.getName());
        return args != null && (args < 0 || signature.getArgumentTypes().size() == args);
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }
}
