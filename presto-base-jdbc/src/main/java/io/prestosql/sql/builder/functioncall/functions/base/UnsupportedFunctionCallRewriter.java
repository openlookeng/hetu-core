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

import io.prestosql.sql.builder.functioncall.BaseFunctionUtil;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.builder.functioncall.functions.FunctionCallRewriter;

/**
 * the Unsupported Function Call Re-writer
 *
 * @since 2019-12-25
 */
public abstract class UnsupportedFunctionCallRewriter
        implements FunctionCallRewriter
{
    private String connectorName;

    /**
     * the constructor of Unsupported Function Call Re-writer
     *
     * @param connectorName
     */
    public UnsupportedFunctionCallRewriter(String connectorName)
    {
        this.connectorName = connectorName;
    }

    @Override
    public final String rewriteFunctionCall(FunctionCallArgsPackage functionCallArgsPackage)
    {
        // throwing exception to prevent the unsupported function to push down to data source
        throw new UnsupportedOperationException(this.connectorName + "Connector is not known to be supported function call of " + BaseFunctionUtil.formatQualifiedName(functionCallArgsPackage.getName()));
    }
}
