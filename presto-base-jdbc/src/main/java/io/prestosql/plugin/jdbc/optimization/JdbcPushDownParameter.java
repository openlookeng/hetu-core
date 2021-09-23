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
package io.prestosql.plugin.jdbc.optimization;

import io.prestosql.spi.function.StandardFunctionResolution;

/**
 * Push Down parameter module
 */
public class JdbcPushDownParameter
{
    private final boolean nameCaseInsensitive;
    private final JdbcPushDownModule pushDownModule;
    private final String identifierQuote;
    private final StandardFunctionResolution functionResolution;

    public JdbcPushDownParameter(String identifierQuote, boolean nameCaseInsensitive, JdbcPushDownModule pushDownModule, StandardFunctionResolution functionResolution)
    {
        this.identifierQuote = identifierQuote;
        this.nameCaseInsensitive = nameCaseInsensitive;
        this.pushDownModule = pushDownModule;
        this.functionResolution = functionResolution;
    }

    public String getIdentifierQuote()
    {
        return identifierQuote;
    }

    public boolean getCaseInsensitiveParameter()
    {
        return nameCaseInsensitive;
    }

    public JdbcPushDownModule getPushDownModuleParameter()
    {
        return pushDownModule;
    }

    public StandardFunctionResolution getFunctionResolution()
    {
        return this.functionResolution;
    }
}
