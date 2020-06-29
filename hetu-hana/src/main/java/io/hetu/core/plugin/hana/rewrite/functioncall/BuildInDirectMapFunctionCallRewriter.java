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
package io.hetu.core.plugin.hana.rewrite.functioncall;

import io.hetu.core.plugin.hana.HanaConstants;
import io.hetu.core.plugin.hana.rewrite.RewriteUtil;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

/**
 * build-in function which can directly mapping to hana datasource. This is class is for rewrite inner function call to
 * support hana expression pushdown
 *
 * @since 2019-09-29
 */

public class BuildInDirectMapFunctionCallRewriter
        implements FunctionCallRewriter
{
    // ========= for Aggregate Functions =========

    /**
     * functioncall name of build-in direct map aggregate function max
     */
    public static final String BUILDIN_AGGR_FUNC_MAX = "max";

    /**
     * functioncall name of build-in direct min aggregate function min
     */
    public static final String BUILDIN_AGGR_FUNC_MIN = "min";

    /**
     * functioncall name of build-in direct map aggregate function count
     */
    public static final String BUILDIN_AGGR_FUNC_COUNT = "count";

    /**
     * functioncall name of build-in direct map aggregate function avg
     */
    public static final String BUILDIN_AGGR_FUNC_AVG = "avg";

    /**
     * functioncall name of build-in direct map aggregate function sum
     */
    public static final String BUIDLIN_AGGR_FUNC_SUM = "sum";

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        StringBuilder builder = new StringBuilder(HanaConstants.DEAFULT_STRINGBUFFER_CAPACITY);

        String arguments = RewriteUtil.joinExpressions(functionCallArgsPackage.getArgumentsList());
        if (functionCallArgsPackage.getArgumentsList().isEmpty() && "count".equalsIgnoreCase(functionCallArgsPackage.getName().getSuffix())) {
            arguments = "*";
        }
        if (functionCallArgsPackage.getIsDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(RewriteUtil.formatQualifiedName(functionCallArgsPackage.getName())).append('(').append(arguments);
        functionCallArgsPackage.getOrderBy().ifPresent(exp -> builder.append(' ').append(exp));
        builder.append(')');
        functionCallArgsPackage.getFilter().ifPresent(exp -> builder.append(" FILTER ").append(exp));
        functionCallArgsPackage.getWindow().ifPresent(exp -> builder.append(" OVER ").append(exp));

        return builder.toString();
    }
}
