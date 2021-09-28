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
package io.prestosql.plugin.jdbc.optimization;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
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
import io.prestosql.spi.type.StandardTypes;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.decodeUnscaledValue;
import static io.prestosql.spi.util.DateTimeUtils.printDate;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.isDefaultFunction;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class BaseJdbcRowExpressionConverter
        implements RowExpressionConverter<JdbcConverterContext>
{
    public static final String COUNT_FUNCTION_NAME = "count";
    protected static final String LIKE_PATTERN_NAME = "LikePattern";
    private static final String INTERNAL_FUNCTION_PREFIX = "$";
    private static final String TIMESTAMP_LITERAL = "$literal$timestamp";
    private static final String DYNAMIC_FILTER_FUNCTION_NAME = "$internal$dynamic_filter_function";

    private final Map<String, Integer> blacklistFunctions;
    protected final FunctionMetadataManager functionMetadataManager;
    protected final StandardFunctionResolution standardFunctionResolution;
    protected final RowExpressionService rowExpressionService;
    protected final DeterminismEvaluator determinismEvaluator;

    public BaseJdbcRowExpressionConverter(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            RowExpressionService rowExpressionService,
            DeterminismEvaluator determinismEvaluator)
    {
        this(functionMetadataManager, standardFunctionResolution, rowExpressionService, determinismEvaluator, Collections.emptyMap());
    }

    public BaseJdbcRowExpressionConverter(
            FunctionMetadataManager functionMetadataManager,
            StandardFunctionResolution standardFunctionResolution,
            RowExpressionService rowExpressionService,
            DeterminismEvaluator determinismEvaluator,
            Map<String, Integer> blacklistFunctions)
    {
        this.functionMetadataManager = requireNonNull(functionMetadataManager, "function metadata manager is null");
        this.standardFunctionResolution = requireNonNull(standardFunctionResolution, "standardFunctionResolution is null");
        this.rowExpressionService = rowExpressionService;
        requireNonNull(blacklistFunctions, "BlackListFunctions cannot be null");
        this.blacklistFunctions = blacklistFunctions;
        this.determinismEvaluator = requireNonNull(determinismEvaluator, "determinismEvaluator is null");
    }

    @Override
    public String visitCall(CallExpression call, JdbcConverterContext context)
    {
        // remote udf verify
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (!isDefaultFunction(call)) {
            throw new PrestoException(NOT_SUPPORTED, String.format("This connector do not support push down %s.%s", functionHandle.getFunctionNamespace(), call.getDisplayName()));
        }

        context.setDefaultFunctionVisited(true);
        if (!isDeterministic(rowExpressionService.getDeterminismEvaluator(), call)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported not deterministic function push down.");
        }
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return format("(NOT %s)", call.getArguments().get(0).accept(this, context));
        }
        if (standardFunctionResolution.isTryFunction(functionHandle)) {
            return format("TRY(%s)", call.getArguments().get(0).accept(this, context));
        }
        if (standardFunctionResolution.isLikeFunction(functionHandle)) {
            return format("(%s LIKE %s)",
                    call.getArguments().get(0).accept(this, context),
                    call.getArguments().get(1).accept(this, context));
        }
        if (standardFunctionResolution.isArrayConstructor(functionHandle)) {
            String arguments = Joiner.on(",").join(call.getArguments().stream().map(expression -> expression.accept(this, context)).collect(toList()));
            return format("ARRAY[%s]", arguments);
        }
        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(functionHandle);

        if (standardFunctionResolution.isOperator(functionHandle)) {
            return handleOperator(call, functionMetadata, context);
        }
        if (functionMetadata.getName().getObjectName().equals(TIMESTAMP_LITERAL)) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            long time = (long) ((ConstantExpression) call.getArguments().get(0)).getValue();
            return format("TIMESTAMP '%s'", formatter.format(Instant.ofEpochMilli(time).atZone(UTC).toLocalDateTime()));
        }
        return handleFunction(call, functionMetadata, context);
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, JdbcConverterContext context)
    {
        switch (specialForm.getForm()) {
            case AND:
            case OR:
                return format("(%s %s %s)",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getForm().toString(),
                        specialForm.getArguments().get(1).accept(this, context));
            case IS_NULL:
                return format("(%s IS NULL)", specialForm.getArguments().get(0).accept(this, context));
            case NULL_IF:
                return format("NULLIF(%s, %s)",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getArguments().get(1).accept(this, context));
            case IN:
                String value = specialForm.getArguments().get(0).accept(this, context);
                String valueList = Joiner.on(", ").join(IntStream.range(1, specialForm.getArguments().size())
                        .mapToObj(i -> specialForm.getArguments().get(i))
                        .map(expression -> expression.accept(this, context))
                        .collect(toList()));
                return format("(%s IN (%s))", value, valueList);
            case BETWEEN:
                return format("(%s BETWEEN %s AND %s)",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getArguments().get(1).accept(this, context),
                        specialForm.getArguments().get(2).accept(this, context));
            case ROW_CONSTRUCTOR:
                return format("ROW (%s)", Joiner.on(", ").join(specialForm.getArguments().stream()
                        .map(expression -> expression.accept(this, context))
                        .collect(toList())));
            case COALESCE:
                String argument = Joiner.on(",")
                        .join(specialForm.getArguments().stream()
                                .map(expression -> expression.accept(this, context))
                                .collect(toList()));
                return format("COALESCE(%s)", argument);
            case IF:
                // convert IF to [case ... when ... else] expression
                return format("IF (%s, %s, %s)",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getArguments().get(1).accept(this, context),
                        specialForm.getArguments().get(2).accept(this, context));
            case SWITCH:
                int size = specialForm.getArguments().size();
                return format("(CASE %s %s ELSE %s END)",
                        specialForm.getArguments().get(0).accept(this, context),
                        Joiner.on(' ').join(IntStream.range(1, size - 1)
                                .mapToObj(i -> specialForm.getArguments().get(i).accept(this, context))
                                .collect(toList())),
                        specialForm.getArguments().get(size - 1).accept(this, context));
            case WHEN:
                return format("WHEN %s THEN %s",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getArguments().get(1).accept(this, context));
            case DEREFERENCE:
                return format("%s.%s",
                        specialForm.getArguments().get(0).accept(this, context),
                        specialForm.getArguments().get(1).accept(this, context));
            default:
                throw new PrestoException(NOT_SUPPORTED, String.format("specialForm %s not supported in filter", specialForm.getForm()));
        }
    }

    @Override
    public String visitConstant(ConstantExpression literal, JdbcConverterContext context)
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
        if (type.equals(DATE)) {
            String date = printDate(Integer.parseInt(String.valueOf(literal.getValue())));
            return StandardTypes.DATE + " " + ExpressionFormatter.formatStringLiteral(date);
        }
        if (type instanceof TimestampType) {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
            Long time = (Long) literal.getValue();
            return format("TIMESTAMP '%s'", formatter.format(Instant.ofEpochMilli(time).atZone(UTC).toLocalDateTime()));
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Cannot handle the constant expression %s with value of type %s", literal.getValue(), type));
    }

    @Override
    public String visitVariableReference(VariableReferenceExpression reference, JdbcConverterContext context)
    {
        return reference.getName();
    }

    private String handleOperator(CallExpression call, FunctionMetadata functionMetadata, JdbcConverterContext context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();

        List<RowExpression> arguments = call.getArguments();
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            if (call.getType().getDisplayName().equals(LIKE_PATTERN_NAME)) {
                return arguments.get(0).accept(this, context);
            }
            return format("CAST(%s AS %s)", arguments.get(0).accept(this, context), call.getType().getDisplayName());
        }
        if (call.getArguments().size() == 1 && standardFunctionResolution.isNegateFunction(functionHandle)) {
            String value = call.getArguments().get(0).accept(this, context);
            String separator = value.startsWith("-") ? " " : "";
            return format("-%s%s", separator, value);
        }
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent() && arguments.size() == 2 && (standardFunctionResolution.isComparisonFunction(functionHandle) || standardFunctionResolution.isArithmeticFunction(functionHandle))) {
            return format(
                    "(%s %s %s)",
                    arguments.get(0).accept(this, context),
                    operatorTypeOptional.get().getOperator(),
                    arguments.get(1).accept(this, context));
        }
        if (standardFunctionResolution.isSubscriptFunction(functionHandle)) {
            String base = call.getArguments().get(0).accept(this, context);
            String index = call.getArguments().get(1).accept(this, context);
            return format("%s[%s]", base, index);
        }
        throw new PrestoException(NOT_SUPPORTED, String.format("Unknown operator %s in push down", operatorTypeOptional));
    }

    private String handleFunction(CallExpression callExpression, FunctionMetadata functionMetadata, JdbcConverterContext context)
    {
        String functionName = functionMetadata.getName().getObjectName();
        List<RowExpression> arguments = callExpression.getArguments();
        if (isBlackListFunction(functionMetadata)) {
            if (DYNAMIC_FILTER_FUNCTION_NAME.equals(functionName)) {
                return "true";
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("Unsupported function in push down %s", functionName));
        }
        if (functionName.equals(COUNT_FUNCTION_NAME) && callExpression.getArguments().size() == 0) {
            return "count(*)";
        }
        else {
            StringBuilder builder = new StringBuilder(functionName);
            StringJoiner joiner = new StringJoiner(", ", "(", ")");
            for (RowExpression expression : arguments) {
                joiner.add(expression.accept(this, context));
            }
            builder.append(joiner);
            return builder.toString();
        }
    }

    private boolean isBlackListFunction(FunctionMetadata functionMetadata)
    {
        String functionName = functionMetadata.getName().getObjectName();
        if (functionName.contains(INTERNAL_FUNCTION_PREFIX)) {
            return true;
        }
        Integer args = blacklistFunctions.get(functionName);
        return args != null && (args < 0 || functionMetadata.getArgumentTypes().size() == args);
    }

    private static Number decodeDecimal(BigInteger unscaledValue, DecimalType type)
    {
        return new BigDecimal(unscaledValue, type.getScale(), new MathContext(type.getPrecision()));
    }

    protected boolean isDeterministic(DeterminismEvaluator evaluator, RowExpression call)
    {
        return evaluator.isDeterministic(call);
    }
}
