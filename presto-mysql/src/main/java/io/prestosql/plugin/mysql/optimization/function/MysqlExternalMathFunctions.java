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
package io.prestosql.plugin.mysql.optimization.function;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.type.StandardTypes;

import java.util.Set;

public final class MysqlExternalMathFunctions
{
    public static Set<ExternalFunctionInfo> getFunctionsInfo()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .add(MYSQL_TRUNCATE_FUNCTION_INFO)
                .add(MYSQL_ABS_FUNCTION_INFO)
                .add(MYSQL_BIG_INT_ABS_FUNCTION_INFO)
                .build();
    }

    private static final ExternalFunctionInfo MYSQL_TRUNCATE_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("truncate")
                    .inputArgs(StandardTypes.DOUBLE, StandardTypes.INTEGER)
                    .returnType(StandardTypes.DOUBLE)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("return the value x that is reserved to y decimal places")
                    .build();

    private static final ExternalFunctionInfo MYSQL_ABS_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("abs")
                    .inputArgs(StandardTypes.INTEGER)
                    .returnType(StandardTypes.INTEGER)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("return the absolute value of x")
                    .build();

    private static final ExternalFunctionInfo MYSQL_BIG_INT_ABS_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("abs")
                    .inputArgs(StandardTypes.BIGINT)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("return the absolute value of x")
                    .build();

    private MysqlExternalMathFunctions()
    {
    }
}
