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
import io.hetu.core.plugin.clickhouse.optimization.externalfunc.ClickHouseExternalFunctionHub;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.sql.builder.functioncall.ApplyRemoteFunctionPushDown;

import java.util.Map;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class ClickHouseApplyRemoteFunctionPushDown
        extends ApplyRemoteFunctionPushDown
{
    private static Map<String, String> clickHousePrimalFunctionNameMap = ClickHouseExternalFunctionHub.getPrimalFunctionNameMap();

    public ClickHouseApplyRemoteFunctionPushDown(BaseJdbcConfig baseJdbcConfig, String connectorName)
    {
        super(baseJdbcConfig, connectorName);
    }

    /**
     * rewrite the remote function to a executable function in the data source.
     */
    @Override
    public Optional<String> rewriteRemoteFunction(CallExpression callExpression, BaseJdbcRowExpressionConverter rowExpressionConverter, JdbcConverterContext jdbcConverterContext)
    {
        if (!isConnectorSupportedRemoteFunction(callExpression)) {
            return Optional.empty();
        }
        jdbcConverterContext.setRemoteUdfVisited(true);
        String displayName = ((SqlFunctionHandle) callExpression.getFunctionHandle()).getFunctionId().getFunctionName().getObjectName();
        String args = Joiner.on(",").join(callExpression.getArguments().stream().map(expression -> expression.accept(rowExpressionConverter, jdbcConverterContext)).collect(toList()));
        // The click house is case sensitive and presto only support the lower case function name,
        // we need to handle this function into lower and store its primal name.
        // The click house do not contain two different functions have the equal function name ignore case.
        // If a data base contain two different functions have the equal function name ignore case,
        // then we should register them into different function name space
        if (!clickHousePrimalFunctionNameMap.containsKey(displayName)) {
            return Optional.empty();
        }
        return Optional.of(String.format("%s(%s)", clickHousePrimalFunctionNameMap.get(displayName), args));
    }
}
