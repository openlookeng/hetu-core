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

public final class MysqlExternalStringFunctions
{
    public static Set<ExternalFunctionInfo> getFunctionsInfo()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .add(MYSQL_FORMAT_FUNCTION_INFO)
                .add(MYSQL_FORMAT_DECIMAL_FUNCTION_INFO)
                .add(MYSQL_LOWER_FUNCTION_INFO)
                .build();
    }

    private static final ExternalFunctionInfo MYSQL_FORMAT_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("format")
                    .inputArgs(StandardTypes.DOUBLE, StandardTypes.INTEGER)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("format the number 'num' to a format like'#,###,###.##', " +
                            "rounded to 'lo' decimal places, and returns the result as a string")
                    .build();

    private static final ExternalFunctionInfo MYSQL_FORMAT_DECIMAL_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("format")
                    .inputArgs(StandardTypes.DECIMAL, StandardTypes.INTEGER)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("format the number 'num' to a format like'#,###,###.##', " +
                            "rounded to 'lo' decimal places, and returns the result as a string")
                    .build();

    private static final ExternalFunctionInfo MYSQL_LOWER_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("lower")
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("returns the string str with all characters changed to lowercase")
                    .build();

    private MysqlExternalStringFunctions()
    {
    }
}
