/*
 * Copyright (C) 2018-2020. Autohome Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.kylin.optimization;

import io.hetu.core.plugin.kylin.KylinConstants;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcRowExpressionConverter;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;

import java.util.Optional;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.isDefaultFunction;

public class KylinRowExpressionConverter
        extends BaseJdbcRowExpressionConverter
{
    private final KylinApplyRemoteFunctionPushDown kylinApplyRemoteFunctionPushDown;

    public KylinRowExpressionConverter(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution, BaseJdbcConfig baseJdbcConfig)
    {
        super(functionManager, functionResolution, rowExpressionService, determinismEvaluator);
        this.kylinApplyRemoteFunctionPushDown = new KylinApplyRemoteFunctionPushDown(baseJdbcConfig, KylinConstants.KYLIN_CONNECTOR_NAME);
    }

    @Override
    public String visitCall(CallExpression call, JdbcConverterContext context)
    {
        // remote udf verify
        FunctionHandle functionHandle = call.getFunctionHandle();
        if (!isDefaultFunction(call)) {
            Optional<String> result = kylinApplyRemoteFunctionPushDown.rewriteRemoteFunction(call, this, context);
            if (result.isPresent()) {
                return result.get();
            }
            throw new PrestoException(NOT_SUPPORTED, String.format("kylin connector does not support remote function: %s.%s", call.getDisplayName(), call.getFunctionHandle().getFunctionNamespace()));
        }

        if (standardFunctionResolution.isArrayConstructor(functionHandle)) {
            throw new PrestoException(NOT_SUPPORTED, "kylin connector does not support array constructor");
        }

        if (standardFunctionResolution.isCastFunction(functionHandle)) {
            return call.getArguments().get(0).accept(this, context);
        }

        return super.visitCall(call, context);
    }
}
