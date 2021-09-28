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

public final class MysqlExternalDateTimeFunctions
{
    public static Set<ExternalFunctionInfo> getFunctionsInfo()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .add(MYSQL_MONTH_NAME_FUNCTION_INFO)
                .add(MYSQL_HOUR_FUNCTION_INFO)
                .add(MYSQL_TIMESTAMP_FUNCTION_INFO)
                .add(MYSQL_DATE_FORMAT_FUNCTION_INFO)
                .build();
    }

    private static final ExternalFunctionInfo MYSQL_MONTH_NAME_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("monthname")
                    .inputArgs(StandardTypes.DATE)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("return name of the month for the input date")
                    .build();

    private static final ExternalFunctionInfo MYSQL_HOUR_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("hour")
                    .inputArgs(StandardTypes.TIME)
                    .returnType(StandardTypes.INTEGER)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("return hour value for the input time")
                    .build();

    private static final ExternalFunctionInfo MYSQL_TIMESTAMP_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("timestamp")
                    .inputArgs(StandardTypes.TIMESTAMP, StandardTypes.TIME)
                    .returnType(StandardTypes.TIMESTAMP)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("adds time expr2 to expr1 and return the result as a timestamp value")
                    .build();

    private static final ExternalFunctionInfo MYSQL_DATE_FORMAT_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName("date_format")
                    .inputArgs(StandardTypes.TIMESTAMP, StandardTypes.VARCHAR)
                    .returnType(StandardTypes.VARCHAR)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("format the date value according to the format string")
                    .build();

    private MysqlExternalDateTimeFunctions()
    {
    }
}
