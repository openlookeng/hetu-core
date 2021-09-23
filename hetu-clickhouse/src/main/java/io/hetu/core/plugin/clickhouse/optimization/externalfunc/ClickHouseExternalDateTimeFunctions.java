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
package io.hetu.core.plugin.clickhouse.optimization.externalfunc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.function.ExternalFunctionInfo;
import io.prestosql.spi.type.StandardTypes;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class ClickHouseExternalDateTimeFunctions
{
    public static Set<ExternalFunctionInfo> getFunctionsInfo()
    {
        return ImmutableSet.<ExternalFunctionInfo>builder()
                .add(ClickHouse_toUnixTimeStamp_FUNCTION_INFO)
                .add(ClickHouse_toDateTime_FUNCTION_INFO)
                .build();
    }

    public static Map<String, String> getDateTimeHubFunctionStringMap()
    {
        // the click house is case sensitive and presto only support the lower case function name,
        // we need to handle this function into lower and store its primal name.
        // now the click house do not contain two different functions have the equal function name ignore case.
        return ImmutableMap.<String, String>builder()
                .put(TO_UNIX_TIME_STAMP_PRIMAL.toLowerCase(Locale.ENGLISH), TO_UNIX_TIME_STAMP_PRIMAL)
                .put(TO_DATE_TIME_PRIMAL.toLowerCase(Locale.ENGLISH), TO_DATE_TIME_PRIMAL)
                .build();
    }

    private static final String TO_UNIX_TIME_STAMP_PRIMAL = "toUnixTimestamp";

    private static final String TO_DATE_TIME_PRIMAL = "toDateTime";

    private static final ExternalFunctionInfo ClickHouse_toUnixTimeStamp_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName(TO_UNIX_TIME_STAMP_PRIMAL.toLowerCase(Locale.ENGLISH))
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.BIGINT)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("converts value to the number with type UInt32 -- Unix Timestamp")
                    .build();

    private static final ExternalFunctionInfo ClickHouse_toDateTime_FUNCTION_INFO =
            ExternalFunctionInfo.builder()
                    .functionName(TO_DATE_TIME_PRIMAL.toLowerCase(Locale.ENGLISH))
                    .inputArgs(StandardTypes.VARCHAR)
                    .returnType(StandardTypes.TIMESTAMP)
                    .deterministic(true)
                    .calledOnNullInput(false)
                    .description("converts value to the number with type UInt32 -- Unix Timestamp")
                    .build();

    private ClickHouseExternalDateTimeFunctions()
    {
    }
}
