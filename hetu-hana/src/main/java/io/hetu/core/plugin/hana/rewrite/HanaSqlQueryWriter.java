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
package io.hetu.core.plugin.hana.rewrite;

import com.google.common.base.Joiner;
import io.hetu.core.plugin.hana.HanaConfig;
import io.hetu.core.plugin.hana.HanaConstants;
import io.hetu.core.plugin.hana.rewrite.functioncall.ArrayConstructorCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.BuildInDirectMapFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.DateAddFunctionCallRewrite;
import io.hetu.core.plugin.hana.rewrite.functioncall.DateTimeFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.HanaUnsupportedFunctionCallRewriter;
import io.hetu.core.plugin.hana.rewrite.functioncall.VarbinaryLiteralFunctionCallRewriter;
import io.prestosql.configmanager.ConfigConstants;
import io.prestosql.spi.sql.expression.Operators;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.sql.builder.BaseSqlQueryWriter;
import io.prestosql.sql.builder.functioncall.FunctionWriterManager;
import io.prestosql.sql.builder.functioncall.FunctionWriterManagerGroup;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;
import io.prestosql.sql.builder.functioncall.functions.base.FromBase64CallRewriter;
import io.prestosql.sql.builder.functioncall.functions.config.DefaultConnectorConfigFunctionRewriter;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.StandardTypes.BIGINT;
import static io.prestosql.spi.type.StandardTypes.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.CHAR;
import static io.prestosql.spi.type.StandardTypes.DATE;
import static io.prestosql.spi.type.StandardTypes.DECIMAL;
import static io.prestosql.spi.type.StandardTypes.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.INTEGER;
import static io.prestosql.spi.type.StandardTypes.REAL;
import static io.prestosql.spi.type.StandardTypes.SMALLINT;
import static io.prestosql.spi.type.StandardTypes.TINYINT;
import static io.prestosql.spi.type.StandardTypes.VARCHAR;
import static java.lang.String.format;

/**
 * Implementation of BaseSqlQueryWriter. It knows how to write Hana SQL for the Hetu's logical plan.
 *
 * @since 2019-09-10
 */
public class HanaSqlQueryWriter
        extends BaseSqlQueryWriter
{
    // The Hana connector extract function's support fields
    private static final List<Time.ExtractField> HANA_SUPPORT_EXTRACT_FIELDS_LIST = Arrays.asList(Time.ExtractField.YEAR, Time.ExtractField.MONTH, Time.ExtractField.DAY, Time.ExtractField.HOUR, Time.ExtractField.MINUTE, Time.ExtractField.SECOND);

    private FunctionWriterManager hanaFunctionRewriterManager;

    /**
     * Hana sql query writer
     *
     * @param hanaConfig hana config
     */
    public HanaSqlQueryWriter(HanaConfig hanaConfig)
    {
        super();
        functionCallManagerHandle(hanaConfig);
    }

    private void functionCallManagerHandle(HanaConfig hanaConfig)
    {
        // use the default function result string builder in the HanaConfigUdfRewriter
        DefaultConnectorConfigFunctionRewriter connectorConfigFunctionRewriter = new DefaultConnectorConfigFunctionRewriter(HanaConstants.CONNECTOR_NAME, hanaConfig.getSqlConfigFilePath());
        // use the default function Signature Builder in the HanaFunctionRewriterManager
        hanaFunctionRewriterManager = FunctionWriterManagerGroup.getFunctionWriterManagerInstance(HanaConstants.CONNECTOR_NAME, ConfigConstants.DEFAULT_VERSION_NAME, getInjectFunctionCallRewritersDefault(hanaConfig), connectorConfigFunctionRewriter);
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

    @Override
    public String cast(String expression, String type, boolean isSafe, boolean isTypeOnly)
    {
        if (isSafe) {
            throw new UnsupportedOperationException("Hana Connector does not support try_cast");
        }
        return format("CAST(%s AS %s)", expression, type);
    }

    @Override
    public String comparisonExpression(Operators.ComparisonOperator operator, String left, String right)
    {
        final String[] hanaCompareOperators = {"=", ">", "<", ">=", "<=", "!=", "<>"};
        String operatorString = operator.getValue();

        if (Arrays.asList(hanaCompareOperators).contains(operatorString)) {
            return format("(%s %s %s)", left, operatorString, right);
        }
        else {
            String exceptionInfo = "Hana Connector does not support comparison operator " + operatorString;
            throw new UnsupportedOperationException(exceptionInfo);
        }
    }

    @Override
    public String exists(String subquery)
    {
        return format("(EXISTS %s)", subquery);
    }

    @Override
    public String extract(String expression, Time.ExtractField field)
    {
        if (HANA_SUPPORT_EXTRACT_FIELDS_LIST.contains(field)) {
            return format("EXTRACT(%s FROM %s)", field, expression);
        }
        else {
            throw new UnsupportedOperationException("Hana Connector does not support extract field: " + field);
        }
    }

    /**
     * arrayConstructor should be call by the function call
     *
     * @param values array values
     */
    @Override
    public String arrayConstructor(List<String> values)
    {
        return format("ARRAY(%s)", Joiner.on(", ").join(values));
    }

    @Override
    public String subscriptExpression(String base, String index)
    {
        return format("MEMBER_AT(%s, %s)", base, index);
    }

    @Override
    public String arithmeticBinary(Operators.ArithmeticOperator operator, String left, String right)
    {
        if (operator.equals(Operators.ArithmeticOperator.MODULUS)) {
            return format("MOD(%s, %s)", left, right);
        }
        else {
            return format("(%s %s %s)", left, operator.getValue(), right);
        }
    }

    @Override
    public String atTimeZone(String value, String timezone)
    {
        throw new UnsupportedOperationException("Hana Connector does not support at time zone");
    }

    @Override
    public String lambdaArgumentDeclaration(String identifier)
    {
        throw new UnsupportedOperationException("Hana Connector does not support lambda argument declaration");
    }

    @Override
    public String currentUser()
    {
        throw new UnsupportedOperationException("Hana Connector does not support current user");
    }

    @Override
    public String currentPath()
    {
        throw new UnsupportedOperationException("Hana Connector does not support current path");
    }

    @Override
    // CHECKSTYLE:OFF:RegexpSinglelineCheck => inherit api, can't change it
    public String currentTime(Time.Function function, Integer precision)
    {
        throw new UnsupportedOperationException("Hana Connector does not support current time");
    }
    // CHECKSTYLE:ON:RegexpSinglelineCheck

    /**
     * intervalLiteral should be call by the function call
     *
     * @param signLiteral config
     * @param value hanaConfig
     * @param startField connectionFactory object
     */
    @Override
    public String intervalLiteral(Time.IntervalSign signLiteral, String value, Time.IntervalField startField, Optional<Time.IntervalField> endField)
    {
        throw new UnsupportedOperationException("Hana Connector does not support interval literal");
    }

    @Override
    public String dereferenceExpression(String base, String field)
    {
        throw new UnsupportedOperationException("Hana Connector does not support dereference expression");
    }

    @Override
    public String ifExpression(String condition, String trueValue, Optional<String> falseValue)
    {
        StringBuilder stringBuilder = new StringBuilder(HanaConstants.DEAFULT_STRINGBUFFER_CAPACITY);
        stringBuilder.append("CASE WHEN " + condition + " THEN " + trueValue);
        falseValue.ifPresent(value -> stringBuilder.append(" ELSE ").append(value));
        stringBuilder.append(" END");
        return stringBuilder.toString();
    }

    @Override
    public String filter(String value)
    {
        throw new UnsupportedOperationException("Hana Connector does not support filter");
    }

    /**
     * binaryLiteral should be call by the function call
     *
     * @param hexValue values
     */
    @Override
    public String binaryLiteral(String hexValue)
    {
        return format("X'%s'", hexValue);
    }

    @Override
    public String bindExpression(List<String> values, String function)
    {
        throw new UnsupportedOperationException("Hana Connector does not support bind expression");
    }

    @Override
    public String lambdaExpression(List<String> arguments, String body)
    {
        throw new UnsupportedOperationException("Hana Connector does not support lambda expression");
    }

    @Override
    public String tryExpression(String innerExpression)
    {
        throw new UnsupportedOperationException("Hana Connector does not support try expression");
    }

    @Override
    public String row(List<String> expressions)
    {
        throw new UnsupportedOperationException("Hana Connector does not support row");
    }

    @Override
    // CHECKSTYLE:OFF:ParameterNumber
    // => inherit api(io.prestosql.spi.sql.SqlQueryWriter.functionCall), can't change it
    public String functionCall(QualifiedName name, boolean isDistinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        // CHECKSTYLE:ON:ParameterNumber
        return this.hanaFunctionRewriterManager.getFunctionRewriteResult(name, isDistinct, argumentsList, orderBy, filter, window);
    }

    @Override
    public String timeLiteral(String value)
    {
        return format("time'%s'", value);
    }

    @Override
    public String timestampLiteral(String value)
    {
        return format("timestamp'%s'", value);
    }

    @Override
    public String genericLiteral(String type, String value)
    {
        // https://help.sap.com/viewer/
        // 4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20a1569875191014b507cf392724b7eb.html
        // -- Type Constants Section
        String lowerType = type.toLowerCase(Locale.ENGLISH);
        switch (lowerType) {
            case BIGINT:
            case SMALLINT:
            case TINYINT:
            case REAL:
            case INTEGER:
                return value;
            case DATE:
                return type + " " + this.formatStringLiteral(value);
            case BOOLEAN:
                return booleanLiteral(Boolean.parseBoolean(value));
            case DECIMAL:
                return decimalLiteral(value);
            case DOUBLE:
                return doubleLiteral(Double.parseDouble(value));
            case VARCHAR:
            case CHAR:
                return stringLiteral(value);
            default:
                String exceptionInfo = "Hana Connector does not support data type " + type;
                throw new UnsupportedOperationException(exceptionInfo);
        }
    }

    @Override
    public String decimalLiteral(String value)
    {
        return "'" + value + "'";
    }

    @Override
    public String formatWindowColumn(String functionName, List<String> args, String windows)
    {
        // the window frame has limit to rows in the windowFrame method
        if (args.size() == 0 && windows.toLowerCase(Locale.ENGLISH).contains("rows")) {
            throw new UnsupportedOperationException("Hana Connector does not support function " + functionName + " with rows, only aggregation support this!");
        }

        // Window aggregation does not support DISTINCT, the same as HeTu, do not need to verify here
        String signatureStr = this.functionCall(new QualifiedName(Collections.singletonList(functionName)), false, args, Optional.empty(), Optional.empty(), Optional.empty());
        return " " + signatureStr + " OVER " + windows;
    }

    @Override
    public String window(List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        // the window frame has limit to rows in the windowFrame method
        // in hana grammar, ROWS requires a ORDER BY clause to be specified.
        if (frame.isPresent() && frame.get().toLowerCase(Locale.ENGLISH).contains("rows") && !orderBy.isPresent()) {
            throw new UnsupportedOperationException("Hana Connector does not support rows window frame without a " + "specified ORDER BY clause");
        }
        return super.window(partitionBy, orderBy, frame);
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        String frameString = super.windowFrame(type, start, end);
        if (type.name().toLowerCase(Locale.ENGLISH).equals("range")) {
            // should verify the hana default frame and the HeTu default range
            if (frameString.toLowerCase(Locale.ENGLISH).contains("range between unbounded preceding and current row")) {
                return "";
            }
            else {
                throw new UnsupportedOperationException("Hana Connector does not support window frame: " + frameString);
            }
        }

        return frameString;
    }
}
