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

import io.hetu.core.plugin.hana.rewrite.RewriteUtil;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import static java.lang.String.format;

/**
 *
 * structural datetype of array initor is rewrite to <code>array_constructor</code> function call.
 * This is class is for rewrite inner function call to support hana expression pushdown
 *
 * @since 2019-09-30
 */

public class ArrayConstructorCallRewriter
        implements FunctionCallRewriter
{
    /**
     * functioncall name of array_constructor in HeTu inner
     */
    public static final String INNER_FUNC_ARRAY_CONSTRUCTOR = "array_constructor";

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        return format("ARRAY(%s)", RewriteUtil.joinExpressions(functionCallArgsPackage.getArgumentsList()));
    }
}
