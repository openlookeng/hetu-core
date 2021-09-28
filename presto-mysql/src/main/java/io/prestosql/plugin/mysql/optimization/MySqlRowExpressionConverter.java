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
package io.prestosql.plugin.mysql.optimization;

import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.mysql.MySqlConstants;
import io.prestosql.plugin.mysql.optimization.function.MySqlApplyRemoteFunctionPushDown;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarbinaryType;
import io.prestosql.spi.type.VarcharType;

import java.util.Optional;

import static io.prestosql.plugin.mysql.optimization.MySqlPushDownUtils.getCastExpression;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.isDefaultFunction;

public class MySqlRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private final MySqlApplyRemoteFunctionPushDown mySqlApplyRemoteFunctionPushDown;

    public MySqlRowExpressionConverter(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, BaseJdbcConfig baseJdbcConfig)
    {
        super(functionManager, functionResolution, rowExpressionService, determinismEvaluator);
        this.mySqlApplyRemoteFunctionPushDown = new MySqlApplyRemoteFunctionPushDown(baseJdbcConfig, MySqlConstants.MYSQL_CONNECTOR_NAME);
    }

    @Override
    public String visitCall(CallExpression call, JdbcConverterContext context)
    {
        // remote udf verify
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (!isDefaultFunction(call)) {
            Optional<String> result = mySqlApplyRemoteFunctionPushDown.rewriteRemoteFunction(call, this, context);
            if (result.isPresent()) {
                return result.get();
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("MySql connector does not support remote function: %s.%s", call.getDisplayName(), call.getFunctionHandle().getFunctionNamespace()));
        }

        if (standardFunctionResolution.isArrayConstructor(functionHandle)) {
            throw new PrestoException(NOT_SUPPORTED, "MySql connector does not support array constructor");
        }
        if (standardFunctionResolution.isSubscriptFunction(functionHandle)) {
            throw new PrestoException(NOT_SUPPORTED, "MySql connector does not support subscript expression");
        }
        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            // deal with literal, when generic literal expression translate to rowExpression, it will be
            // translated to a 'CAST' rowExpression with a varchar type 'CONSTANT' rowExpression, in some
            // case, 'CAST' is superfluous
            RowExpression argument = call.getArguments().get(0);
            Type type = call.getType();
            if (argument instanceof ConstantExpression && argument.getType().equals(VARCHAR)) {
                String value = argument.accept(this, context);
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
                return call.getArguments().get(0).accept(this, context);
            }
            return getCastExpression(call.getArguments().get(0).accept(this, context), call.getType());
        }

        return super.visitCall(call, context);
    }

    @Override
    public String visitConstant(ConstantExpression literal, JdbcConverterContext context)
    {
        if (literal.getValue() == null) {
            return getCastExpression("null", literal.getType());
        }

        return super.visitConstant(literal, context);
    }
}
