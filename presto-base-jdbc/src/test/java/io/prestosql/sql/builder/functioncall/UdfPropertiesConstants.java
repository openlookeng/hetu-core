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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class UdfPropertiesConstants
{
    private UdfPropertiesConstants()
    {
    }

    /**
     * udf rewrite pattern map
     */
    public static final Map<String, String> Test_UDF_REWRITE_PATTERNS =
            new ImmutableMap.Builder<String, String>()
                    //aggregate functions
                    .put("CORR($1,$2)", "CORR($1, $2)")
                    .put("LOG10($1)", "LOG(10, $1)")
                    .build();
}
