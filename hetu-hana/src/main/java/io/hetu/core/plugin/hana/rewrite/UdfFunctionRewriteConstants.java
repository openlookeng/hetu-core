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
package io.hetu.core.plugin.hana.rewrite;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class UdfFunctionRewriteConstants
{
    private UdfFunctionRewriteConstants()
    {
    }

    /**
     * udf rewrite pattern map
     */
    public static final Map<String, String> DEFAULT_VERSION_UDF_REWRITE_PATTERNS =
            new ImmutableMap.Builder<String, String>()
                    //aggregate functions
                    .put("CORR($1,$2)", "CORR($1, $2)")
                    .put("STDDEV($1)", "STDDEV($1)")
                    .put("VARIANCE($1)", "VAR($1)")
                    // window rank functions
                    .put("RANK()", "RANK()")
                    .put("DENSE_RANK()", "DENSE_RANK()")
                    .put("ROW_NUMBER()", "ROW_NUMBER()")
                    .put("PERCENT_RANK()", "PERCENT_RANK()")
                    .put("CUME_DIST()", "CUME_DIST()")
                    // math functions
                    .put("ABS($1)", "ABS($1)")
                    .put("ACOS($1)", "ACOS($1)")
                    .put("ASIN($1)", "ASIN($1)")
                    .put("ATAN($1)", "ATAN($1)")
                    .put("ATAN2($1,$2)", "ATAN2($1, $2)")
                    .put("CEIL($1)", "CEIL($1)")
                    .put("CEILING($1)", "CEIL($1)")
                    .put("COS($1)", "COS($1)")
                    .put("EXP($1)", "EXP($1)")
                    .put("FLOOR($1)", "FLOOR($1)")
                    .put("LN($1)", "LN($1)")
                    .put("LOG10($1)", "LOG(10, $1)")
                    .put("LOG2($1)", "LOG(2, $1)")
                    .put("LOG($1,$2)", "LOG($1, $2)")
                    .put("MOD($1,$2)", "MOD($1, $2)")
                    .put("POW($1,$2)", "POWER($1, $2)")
                    .put("POWER($1,$2)", "POWER($1, $2)")
                    .put("RAND()", "RAND()")
                    .put("RANDOM()", "RAND()")
                    .put("ROUND($1)", "ROUND($1)")
                    .put("ROUND($1,$2)", "ROUND($1, $2)")
                    .put("SIGN($1)", "SIGN($1)")
                    .put("SIN($1)", "SIN($1)")
                    .put("SQRT($1)", "SQRT($1)")
                    .put("TAN($1)", "TAN($1)")
                    //character functions
                    .put("CONCAT($1,$2)", "CONCAT($1, $2)")
                    .put("LENGTH($1)", "LENGTH($1)")
                    .put("LOWER($1)", "LOWER($1)")
                    .put("LPAD($1,$2,$3)", "LPAD($1, $2, $3)")
                    .put("LTRIM($1)", "LTRIM($1)")
                    .put("REPLACE($1,$2)", "REPLACE($1, $2, '')")
                    .put("REPLACE($1,$2,$3)", "REPLACE($1, $2, $3)")
                    .put("RPAD($1,$2,$3)", "RPAD($1, $2, $3)")
                    .put("RTRIM($1)", "RTRIM($1)")
                    .put("STRPOS($1,$2)", "LOCATE($1, $2)")
                    .put("SUBSTR($1,$2,$3)", "SUBSTR($1, $2, $3)")
                    .put("SUBSTR($1,$2)", "SUBSTR($1, $2)")
                    .put("SUBSTRING($1,$2,$3)", "SUBSTRING($1, $2, $3)")
                    .put("SUBSTRING($1,$2)", "SUBSTRING($1, $2)")
                    .put("POSITION($1,$2)", "LOCATE($2, $1)")
                    .put("TRIM($1)", "TRIM($1)")
                    .put("UPPER($1)", "UPPER($1)")
                    //date functions
                    .put("YEAR($1)", "EXTRACT(YEAR FROM $1)")
                    .put("MONTH($1)", "EXTRACT(MONTH FROM $1)")
                    .put("DAY($1)", "EXTRACT(DAY FROM $1)")
                    .put("HOUR($1)", "EXTRACT(HOUR FROM $1)")
                    .put("MINUTE($1)", "EXTRACT(MINUTE FROM $1)")
                    .put("SECOND($1)", "EXTRACT(SECOND FROM $1)")
                    .put("DAY_OF_WEEK($1)", "WEEKDAY($1)")
                    .build();
}
