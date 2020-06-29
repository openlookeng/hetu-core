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
package io.prestosql.sql.builder.functioncall;

import io.prestosql.spi.sql.expression.QualifiedName;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.builder.functioncall.BaseFunctionUtil.formatQualifiedName;

/**
 * the package of SqlQueryWriter's function call methods args
 *
 * @since 2019-10-29
 */
public class FunctionCallArgsPackage
{
    private QualifiedName name;

    private boolean isDistinct;

    private List<String> argumentsList;

    private Optional<String> orderBy;

    private Optional<String> filter;

    private Optional<String> window;

    /**
     * funcionCall rewrite interface api
     * @param name functioname
     * @param isDistinct distinct flag
     * @param argumentsList arguments list
     * @param orderBy order by
     * @param filter filter
     * @param window window
     */
    // CHECKSTYLE:OFF:ParameterNumber
    // inherit api(io.prestosql.spi.sql.SqlQueryWriter.functionCall), can't change it
    public FunctionCallArgsPackage(QualifiedName name, boolean isDistinct, List<String> argumentsList,
                                   Optional<String> orderBy, Optional<String> filter, Optional<String> window)
    {
        // CHECKSTYLE:ON:ParameterNumber
        this.name = name;
        this.isDistinct = isDistinct;
        this.argumentsList = argumentsList;
        this.orderBy = orderBy;
        this.filter = filter;
        this.window = window;
    }

    public QualifiedName getName()
    {
        return name;
    }

    public boolean getIsDistinct()
    {
        return isDistinct;
    }

    public List<String> getArgumentsList()
    {
        return argumentsList;
    }

    public Optional<String> getOrderBy()
    {
        return orderBy;
    }

    public Optional<String> getFilter()
    {
        return filter;
    }

    public Optional<String> getWindow()
    {
        return window;
    }

    /**
     * to build a function signature only relay on qualified name. or You can build a function signature through you way
     *
     * @param functionCallArgsPackage functionCallArgsPackage
     * @return function signature
     */
    public static String defaultBuildFunctionSignature(FunctionCallArgsPackage functionCallArgsPackage)
    {
        return formatQualifiedName(functionCallArgsPackage.getName());
    }
}
