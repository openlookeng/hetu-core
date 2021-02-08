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
package io.hetu.core.plugin.hana.optimization;

import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hana.HanaConfig;
import io.hetu.core.plugin.hana.HanaConstants;
import io.hetu.core.plugin.hana.rewrite.UdfFunctionRewriteConstants;
import io.hetu.core.plugin.hana.rewrite.functioncall.ArrayConstructorCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.BuildInDirectMapFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.DateAddFunctionCallRewrite;
import io.hetu.core.plugin.hana.rewrite.functioncall.DateTimeFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.HanaUnsupportedFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.VarbinaryLiteralFunctionCallRewriter;
import io.prestosql.configmanager.ConfigSupplier;
import io.prestosql.configmanager.DefaultUdfRewriteConfigSupplier;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.spi.util.DateTimeUtils;
import io.prestosql.sql.ExpressionFormatter;
import io.prestosql.sql.builder.functioncall.FunctionWriterManager;
import io.prestosql.sql.builder.functioncall.FunctionWriterManagerGroup;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.base.FromBase64CallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.function.Signature.unmangleOperator;
import static io.prestosql.spi.function.StandardFunctionUtils.isArrayConstructor;
import static io.prestosql.spi.function.StandardFunctionUtils.isLikeFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isNotFunction;
import static io.prestosql.spi.function.StandardFunctionUtils.isOperator;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.util.DateTimeUtils.printDate;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class HanaRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private static final Set<String> hanaNotSupportFunctions =
            Stream.of("try", "try_cast", "at_timezone", "current_user", "current_path", "current_time").collect(toImmutableSet());

    private static FunctionWriterManager hanaFunctionManager;

    /**
     * Hana sql query writer
     *
     * @param hanaConfig hana config
     */
    public HanaRowExpressionConverter(RowExpressionService rowExpressionService, HanaPushDownParameter hanaConfig)
    {
        super(rowExpressionService);
        hanaFunctionManager = initFunctionManager(hanaConfig.getHanaConfig());
    }

    private FunctionWriterManager initFunctionManager(HanaConfig hanaConfig)
    {
        // add inner config udf, use the default function result string builder in the HanaConfigUdfRewriter
        ConfigSupplier configSupplier = new DefaultUdfRewriteConfigSupplier(UdfFunctionRewriteConstants.DEFAULT_VERSION_UDF_REWRITE_PATTERNS);
        DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter =
                new DefaultConnectorConfigFunctionRewriter(HanaConstants.CONNECTOR_NAME, configSupplier);

        // use the default function Signature Builder in the HanaFunctionRewriterManager
        return FunctionWriterManagerGroup.newFunctionWriterManagerInstance(HanaConstants.CONNECTOR_NAME,
                hanaConfig.getHanaSqlVersion(), getInjectFunctionCallRewritersDefault(hanaConfig), connectorConfigFunctionRewriter);
    }

    private Map<String, FunctionCallRewriter> getInjectFunctionCallRewritersDefault(HanaConfig hanaConfig)
    {
        // add the user define function re-writer
        Map<String, FunctionCallRewriter> functionCallRewriters = new HashMap<>(Collections.emptyMap());
        // 1. the base function re-writers all connector can use
        FromBase64CallRewriter fromBase64CallRewriter = new FromBase64CallRewriter();
        functionCallRewriters.put(FromBase64CallRewriter.INNER_FUNC_FROM_BASE64, fromBase64CallRewriter);

        // 2. the specific user define function re-writers
        FunctionCallRewriter varbinaryLiteralFunctionCallRewriter = new VarbinaryLiteralFunctionCallRewriter();
        functionCallRewriters.put(VarbinaryLiteralFunctionCallRewriter.INNER_FUNC_VARBINARY_LITERAL, varbinaryLiteralFunctionCallRewriter);

        FunctionCallRewriter unSupportedFunctionCallRewriter = new HanaUnsupportedFunctionCallRewriter(HanaConstants.CONNECTOR_NAME);
        functionCallRewriters.put(HanaUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_DAY2SEC, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(HanaUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_YEAR2MONTH, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(HanaUnsupportedFunctionCallRewriter.INNER_FUNC_TIME_WITH_TZ_LITERAL, unSupportedFunctionCallRewriter);

        FunctionCallRewriter dateTimeFunctionCallRewriter = new DateTimeFunctionCallRewriter(hanaConfig);
        functionCallRewriters.put(DateTimeFunctionCallRewriter.INNER_FUNC_TIME_LITERAL, dateTimeFunctionCallRewriter);
        functionCallRewriters.put(DateTimeFunctionCallRewriter.INNER_FUNC_TIMESTAMP_LITERAL, dateTimeFunctionCallRewriter);

        FunctionCallRewriter dateAddFunctionCallRewrite = new DateAddFunctionCallRewrite();
        functionCallRewriters.put(DateAddFunctionCallRewrite.BUILD_IN_FUNC_DATE_ADD, dateAddFunctionCallRewrite);

        FunctionCallRewriter buildInDirectMapFunctionCallRewriter = new BuildInDirectMapFunctionCallRewriter();
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUIDLIN_AGGR_FUNC_SUM, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_AVG, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_COUNT, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MAX, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MIN, buildInDirectMapFunctionCallRewriter);

        FunctionCallRewriter arrayConstructorCallRewriter = new ArrayConstructorCallRewriter();
        functionCallRewriters.put(ArrayConstructorCallRewriter.INNER_FUNC_ARRAY_CONSTRUCTOR, arrayConstructorCallRewriter);

        return functionCallRewriters;
    }

    protected static String functionCall(QualifiedName name, boolean isDistinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        if (hanaFunctionManager == null) {
            throw new PrestoException(NOT_SUPPORTED, "Function manager is uninitialized");
        }

        try {
            return hanaFunctionManager.getFunctionRewriteResult(name, isDistinct, argumentsList, orderBy, filter, window);
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage());
        }
    }

    private String handleCastOperator(RowExpression expression, Type dstType)
    {
        /*
        * In SqlToRowExpressionTranslator, it will translate GenericLiteral expression to a 'CONSTANT' rowExpression,
        * so the 'CAST' operator is not needed.
        * */
        String value = expression.accept(this, null);
        if (expression instanceof ConstantExpression && expression.getType() instanceof VarcharType) {
            return value;
        }

        if (dstType.getDisplayName().equals(LIKE_PATTERN_NAME)) {
            return value;
        }

        return format("CAST(%s AS %s)", value, dstType.getDisplayName().toLowerCase(ENGLISH));
    }

    private String handleOperatorFunction(CallExpression call)
    {
        OperatorType type = unmangleOperator(call.getSignature().getName());
        if (type.equals(OperatorType.CAST)) {
            return handleCastOperator(call.getArguments().get(0), call.getType());
        }

        List<String> argumentList = call.getArguments().stream().map(expr -> expr.accept(this, null)).collect(Collectors.toList());
        if (type.isArithmeticOperator()) {
            if (type.equals(OperatorType.MODULUS)) {
                return format("MOD(%s, %s)", argumentList.get(0), argumentList.get(1));
            }
            else {
                return format("(%s %s %s)", argumentList.get(0), type.getOperator(), argumentList.get(1));
            }
        }

        if (type.isComparisonOperator()) {
            final String[] hanaCompareOperators = new String[]{"=", ">", "<", ">=", "<=", "!=", "<>"};
            if (Arrays.asList(hanaCompareOperators).contains(type.getOperator())) {
                return format("(%s %s %s)", argumentList.get(0), type.getOperator(), argumentList.get(1));
            }
            else {
                String exceptionInfo = "Hana Connector does not support comparison operator " + type.getOperator();
                throw new PrestoException(NOT_SUPPORTED, exceptionInfo);
            }
        }

        if (type.equals(OperatorType.SUBSCRIPT)) {
            if (call.getArguments().size() == 2) {
                return format("MEMBER_AT(%s, %s)", argumentList.get(0), argumentList.get(1));
            }
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Illegal argument num of function " + type.getOperator());
        }

        if (call.getArguments().size() == 1 && type.equals(OperatorType.NEGATION)) {
            String value = argumentList.get(0);
            String separator = value.startsWith("-") ? " " : "";
            return format("-%s%s", separator, value);
        }

        throw new PrestoException(NOT_SUPPORTED, String.format("Unknown operator %s in push down", type.getOperator()));
    }

    @Override
    public String visitCall(CallExpression call, Void context)
    {
        Signature signature = call.getSignature();
        String functionName = call.getSignature().getName().toLowerCase(ENGLISH);

        if (hanaNotSupportFunctions.contains(functionName)) {
            throw new PrestoException(NOT_SUPPORTED, "Hana connector does not support " + functionName);
        }

        if (isOperator(signature)) {
            return handleOperatorFunction(call);
        }

        List<String> argumentList = call.getArguments().stream().map(expr -> expr.accept(this, null)).collect(Collectors.toList());
        if (isNotFunction(signature)) {
            return format("(NOT %s)", argumentList.get(0));
        }

        if (isLikeFunction(signature)) {
            return format("(%s LIKE %s)", argumentList.get(0), argumentList.get(1));
        }

        if (isArrayConstructor(signature)) {
            return format("ARRAY(%s)", Joiner.on(", ").join(argumentList));
        }

        return functionCall(new QualifiedName(Collections.singletonList(functionName)), false, argumentList, Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public String visitLambda(LambdaDefinitionExpression lambda, Void context)
    {
        throw new PrestoException(NOT_SUPPORTED, "Hana connector does not support Lambda expression");
    }

    private String visitIfExpression(SpecialForm specialForm)
    {
        String condition = specialForm.getArguments().get(0).accept(this, null);
        String trueValue = specialForm.getArguments().get(1).accept(this, null);
        RowExpression falseExpression = specialForm.getArguments().get(2);
        Optional<String> falseValue = ((falseExpression instanceof ConstantExpression) && ((ConstantExpression) falseExpression).isNull())
                ? Optional.empty() : Optional.of(falseExpression.accept(this, null));
        StringBuilder stringBuilder = new StringBuilder(HanaConstants.DEAFULT_STRINGBUFFER_CAPACITY);
        stringBuilder.append("CASE WHEN " + condition + " THEN " + trueValue);
        falseValue.ifPresent(value -> stringBuilder.append(" ELSE ").append(value));
        stringBuilder.append(" END");
        return stringBuilder.toString();
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, Void context)
    {
        if (specialForm.getForm().equals(SpecialForm.Form.DEREFERENCE)
                || specialForm.getForm().equals(SpecialForm.Form.ROW_CONSTRUCTOR)
                || specialForm.getForm().equals(SpecialForm.Form.BIND)) {
            throw new PrestoException(NOT_SUPPORTED, "Hana connector does not support" + specialForm.getForm().toString());
        }

        if (specialForm.getForm().equals(SpecialForm.Form.IF)) {
            return visitIfExpression(specialForm);
        }

        return super.visitSpecialForm(specialForm, context);
    }

    @Override
    public String visitConstant(ConstantExpression literal, Void context)
    {
        Type type = literal.getType();

        if (type.equals(DATE)) {
            String date = printDate((int) literal.getValue());
            return StandardTypes.DATE + " " + ExpressionFormatter.formatStringLiteral(date);
        }
        if (type.equals(VARBINARY)) {
            String hexValue = ((Slice) literal.getValue()).toStringUtf8();
            return format("X'%s'", hexValue);
        }
        if (type.equals(TIME)) {
            String time = DateTimeUtils.printTimeWithoutTimeZone((long) literal.getValue());
            return format("time'%s'", time);
        }
        if (type.equals(TIMESTAMP)) {
            String timestamp = DateTimeUtils.printTimeWithoutTimeZone((long) literal.getValue());
            return format("timestamp'%s'", timestamp);
        }

        return super.visitConstant(literal, context);
    }
}
