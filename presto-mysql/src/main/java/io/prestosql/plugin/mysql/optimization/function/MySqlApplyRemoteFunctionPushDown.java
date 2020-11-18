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
package io.prestosql.plugin.mysql.optimization.function;

import com.google.common.base.Joiner;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.spi.function.SqlFunctionHandle;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.sql.builder.functioncall.ApplyRemoteFunctionPushDown;

import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class MySqlApplyRemoteFunctionPushDown
        extends ApplyRemoteFunctionPushDown
{
    public MySqlApplyRemoteFunctionPushDown(BaseJdbcConfig baseJdbcConfig, String connectorName)
    {
        super(baseJdbcConfig, connectorName);
    }

    @Override
    public Optional<String> rewriteRemoteFunction(CallExpression callExpression, BaseJdbcRowExpressionConverter rowExpressionConverter, JdbcConverterContext jdbcConverterContext)
    {
        if (!isConnectorSupportedRemoteFunction(callExpression)) {
            return Optional.empty();
        }
        jdbcConverterContext.setRemoteUdfVisited(true);
        String displayName = ((SqlFunctionHandle) callExpression.getFunctionHandle()).getFunctionId().getFunctionName().getObjectName();
        String args = Joiner.on(",").join(callExpression.getArguments().stream().map(expression -> expression.accept(rowExpressionConverter, jdbcConverterContext)).collect(toList()));
        return Optional.of(String.format("%s(%s)", displayName, args));
    }
}
