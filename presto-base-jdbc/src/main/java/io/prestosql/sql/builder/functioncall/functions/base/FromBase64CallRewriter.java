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
package io.prestosql.sql.builder.functioncall.functions.base;

import com.google.common.io.BaseEncoding;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 *
 * Inner function from_base64 is rewrite to <code>from_base64</code> function call.
 * This is class is for rewrite inner function call to support hana expression pushdown
 *
 * @since 2019-09-30
 */

public class FromBase64CallRewriter
        implements FunctionCallRewriter
{
    /**
     * functioncall name of from_base64 in Presto inner
     */
    public static final String INNER_FUNC_FROM_BASE64 = "from_base64";

    @Override
    public String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        String base64 = functionCallArgsPackage.getArgumentsList().get(0).replace("'", "");
        byte[] bytes = Base64.getDecoder().decode(base64.getBytes(StandardCharsets.UTF_8));
        return BaseEncoding.base16().encode(bytes);
    }
}
