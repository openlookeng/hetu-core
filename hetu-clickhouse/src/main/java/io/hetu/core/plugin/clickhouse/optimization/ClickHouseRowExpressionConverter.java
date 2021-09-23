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
package io.hetu.core.plugin.clickhouse.optimization;

import com.google.common.base.Joiner;
import io.hetu.core.plugin.clickhouse.ClickHouseConfig;
import io.hetu.core.plugin.clickhouse.ClickHouseConstants;
import io.hetu.core.plugin.clickhouse.rewrite.BuildInDirectMapFunctionCallRewriter;
import io.hetu.core.plugin.clickhouse.rewrite.ClickHouseUnsupportedFunctionCallRewriter;
import io.hetu.core.plugin.clickhouse.rewrite.UdfFunctionRewriteConstants;
import io.hetu.core.plugin.clickhouse.rewrite.functioncall.DateParseFunctionCallRewriter;
import io.prestosql.configmanager.ConfigSupplier;
import io.prestosql.configmanager.DefaultUdfRewriteConfigSupplier;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
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
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
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
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.isDefaultFunction;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class ClickHouseRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private static final Set<String> clickHouseNotSupportFunctions =
            Stream.of("try", "try_cast", "at_timezone", "current_user", "current_path", "current_time").collect(toImmutableSet());
    private static FunctionWriterManager clickHouseFunctionManager;
    private final ClickHouseApplyRemoteFunctionPushDown clickHouseApplyRemoteFunctionPushDown;

    /**
     * ClickHouse sql query writer
     *
     * @param clickHouseConfig config
     */
    public ClickHouseRowExpressionConverter(
            DeterminismEvaluator determinismEvaluator,
            RowExpressionService rowExpressionService,
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            ClickHousePushDownParameter clickHouseConfig,
            BaseJdbcConfig baseJdbcConfig)
    {
        super(functionManager, functionResolution, rowExpressionService, determinismEvaluator);
        clickHouseFunctionManager = initFunctionManager(clickHouseConfig.getClickHouseConfig());
        clickHouseApplyRemoteFunctionPushDown = new ClickHouseApplyRemoteFunctionPushDown(baseJdbcConfig, ClickHouseConstants.CONNECTOR_NAME);
    }

    private FunctionWriterManager initFunctionManager(ClickHouseConfig clickHouseConfig)
    {
        ConfigSupplier configSupplier = new DefaultUdfRewriteConfigSupplier(UdfFunctionRewriteConstants.DEFAULT_VERSION_UDF_REWRITE_PATTERNS);
        DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter =
                new DefaultConnectorConfigFunctionRewriter(ClickHouseConstants.CONNECTOR_NAME, configSupplier);

        return FunctionWriterManagerGroup.newFunctionWriterManagerInstance(ClickHouseConstants.CONNECTOR_NAME,
                clickHouseConfig.getClickHouseSqlVersion(), getInjectFunctionCallRewritersDefault(clickHouseConfig), connectorConfigFunctionRewriter);
    }

    private Map<String, FunctionCallRewriter> getInjectFunctionCallRewritersDefault(ClickHouseConfig clickHouseConfig)
    {
        // add the user define function re-writer
        Map<String, FunctionCallRewriter> functionCallRewriters = new HashMap<>(Collections.emptyMap());
        // 1. the base function re-writers all connector can use
        FromBase64CallRewriter fromBase64CallRewriter = new FromBase64CallRewriter();
        functionCallRewriters.put(FromBase64CallRewriter.INNER_FUNC_FROM_BASE64, fromBase64CallRewriter);

        // 2. the specific user define function re-writers
        FunctionCallRewriter unSupportedFunctionCallRewriter = new ClickHouseUnsupportedFunctionCallRewriter(ClickHouseConstants.CONNECTOR_NAME);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_DAY2SEC, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_INTERVAL_LITERAL_YEAR2MONTH, unSupportedFunctionCallRewriter);
        functionCallRewriters.put(ClickHouseUnsupportedFunctionCallRewriter.INNER_FUNC_TIME_WITH_TZ_LITERAL, unSupportedFunctionCallRewriter);

        FunctionCallRewriter buildInDirectMapFunctionCallRewriter = new BuildInDirectMapFunctionCallRewriter();
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUIDLIN_AGGR_FUNC_SUM, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_AVG, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_COUNT, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MAX, buildInDirectMapFunctionCallRewriter);
        functionCallRewriters.put(BuildInDirectMapFunctionCallRewriter.BUILDIN_AGGR_FUNC_MIN, buildInDirectMapFunctionCallRewriter);

        FunctionCallRewriter dateParseFunctionCallRewriter = new DateParseFunctionCallRewriter();
        functionCallRewriters.put(DateParseFunctionCallRewriter.BUILD_IN_FUNC_DATE_PARSE, dateParseFunctionCallRewriter);

        return functionCallRewriters;
    }

    protected static String functionCall(QualifiedName name, boolean isDistinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        if (clickHouseFunctionManager == null) {
            throw new PrestoException(NOT_SUPPORTED, "Function manager is uninitialized");
        }

        try {
            return clickHouseFunctionManager.getFunctionRewriteResult(name, isDistinct, argumentsList, orderBy, filter, window);
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(NOT_SUPPORTED, e.getMessage());
        }
    }

    @Override
    public String visitCall(CallExpression call, JdbcConverterContext context)
    {
        FunctionHandle functionHandle = call.getFunctionHandle();
        // remote udf verify
        if (!isDefaultFunction(call)) {
            Optional<String> result = clickHouseApplyRemoteFunctionPushDown.rewriteRemoteFunction(call, this, context);
            if (result.isPresent()) {
                return result.get();
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("ClickHouse connector does not support remote function: %s.%s", call.getDisplayName(), call.getFunctionHandle().getFunctionNamespace()));
        }

        FunctionMetadata functionMetadata = functionMetadataManager.getFunctionMetadata(functionHandle);
        String functionName = functionMetadata.getName().getObjectName();

        if (clickHouseNotSupportFunctions.contains(functionName)) {
            throw new PrestoException(NOT_SUPPORTED, "ClickHouse connector does not support " + functionName);
        }

        if (standardFunctionResolution.isOperator(functionHandle)) {
            return handleOperatorFunction(call, functionMetadata, context);
        }

        List<String> argumentList = call.getArguments().stream().map(expr -> expr.accept(this, context)).collect(Collectors.toList());
        if (standardFunctionResolution.isNotFunction(functionHandle)) {
            return format("(NOT %s)", argumentList.get(0));
        }

        if (standardFunctionResolution.isLikeFunction(functionHandle)) {
            return format("(%s LIKE %s)", argumentList.get(0), argumentList.get(1));
        }

        /*
         * Array needs to be tested
         */
        if (standardFunctionResolution.isArrayConstructor(functionHandle)) {
            return format("ARRAY(%s)", Joiner.on(", ").join(argumentList));
        }

        return functionCall(new QualifiedName(Collections.singletonList(functionName)), false, argumentList, Optional.empty(), Optional.empty(), Optional.empty());
    }

    private String handleOperatorFunction(CallExpression call, FunctionMetadata functionMetadata, JdbcConverterContext context)
    {
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        OperatorType type = operatorTypeOptional.get();
        if (type.equals(OperatorType.CAST)) {
            return handleCastOperator(call.getArguments().get(0), call.getType(), context);
        }

        List<String> argumentList = call.getArguments().stream().map(expr -> expr.accept(this, context)).collect(Collectors.toList());
        if (type.isArithmeticOperator()) {
            return format("(%s %s %s)", argumentList.get(0), type.getOperator(), argumentList.get(1));
        }

        if (type.isComparisonOperator()) {
            final String[] clickHouseCompareOperators = new String[] {"=", ">", "<", ">=", "<=", "!=", "<>"};
            if (Arrays.asList(clickHouseCompareOperators).contains(type.getOperator())) {
                return format("(%s %s %s)", argumentList.get(0), type.getOperator(), argumentList.get(1));
            }
            else {
                String exceptionInfo = "ClickHouse Connector does not support comparison operator " + type.getOperator();
                throw new PrestoException(NOT_SUPPORTED, exceptionInfo);
            }
        }

        if (type.equals(OperatorType.SUBSCRIPT)) {
            throw new PrestoException(NOT_SUPPORTED, "ClickHouse Connector does not support subscript now");
        }

        /*
         * "Negative" needs to be tested
         */
        if (call.getArguments().size() == 1 && type.equals(OperatorType.NEGATION)) {
            String value = argumentList.get(0);
            String separator = value.startsWith("-") ? " " : "";
            return format("-%s%s", separator, value);
        }

        throw new PrestoException(NOT_SUPPORTED, String.format("Unknown operator %s in push down", type.getOperator()));
    }

    private String handleCastOperator(RowExpression expression, Type dstType, JdbcConverterContext context)
    {
        /*
         * In SqlToRowExpressionTranslator, it will translate GenericLiteral expression to a 'CONSTANT' rowExpression,
         * so the 'CAST' operator is not needed.
         * */
        String value = expression.accept(this, context);
        if (expression instanceof ConstantExpression && expression.getType() instanceof VarcharType) {
            //dstType needs to be tested here.
            //especially DateTime.
            return value;
        }

        if (dstType.getDisplayName().equals(LIKE_PATTERN_NAME)) {
            return value;
        }

        return format("CAST(%s AS %s)", value, dstType.getDisplayName().toLowerCase(ENGLISH));
    }

    @Override
    public String visitSpecialForm(SpecialForm specialForm, JdbcConverterContext context)
    {
        if (specialForm.getForm().equals(SpecialForm.Form.DEREFERENCE)
                || specialForm.getForm().equals(SpecialForm.Form.ROW_CONSTRUCTOR)
                || specialForm.getForm().equals(SpecialForm.Form.BIND)) {
            throw new PrestoException(NOT_SUPPORTED, "ClickHouse connector does not support" + specialForm.getForm().toString());
        }

        return super.visitSpecialForm(specialForm, context);
    }
}
