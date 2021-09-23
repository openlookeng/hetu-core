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
package io.hetu.core.plugin.clickhouse.rewrite;

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
                    // Statistical aggregate functions
                    .put("CORR($1,$2)", "CORR($1, $2)")
                    .put("STDDEV($1)", "stddevSamp($1)")
                    .put("stddev_pop($1)", "stddev_pop($1)")
                    .put("stddev_samp($1)", "stddevSamp($1)")
                    .put("skewness($1)", "skewPop($1)")
                    .put("kurtosis($1)", "kurtPop($1)")
                    .put("VARIANCE($1)", "varSamp($1)")
                    .put("var_samp($1)", "varSamp($1)")
                    .put("APPROX_DISTINCT($1)", "uniq($1)")
                    .put("APPROX_DISTINCT($1,$2)", "uniq($1)")
                    // math functions
                    .put("ABS($1)", "ABS($1)")
                    .put("ACOS($1)", "ACOS($1)")
                    .put("ASIN($1)", "ASIN($1)")
                    .put("ATAN($1)", "ATAN($1)")
                    .put("ATAN2($1,$2)", "ATAN2($1, $2)")
                    .put("CEIL($1)", "CEIL($1)")
                    .put("CEILING($1)", "CEIL($1)")
                    .put("COS($1)", "COS($1)")
                    .put("e()", "e()")
                    .put("EXP($1)", "EXP($1)")
                    .put("FLOOR($1)", "FLOOR($1)")
                    .put("LN($1)", "LN($1)")
                    .put("LOG10($1)", "log10($1)")
                    .put("LOG2($1)", "log2($1)")
                    .put("MOD($1,$2)", "MOD($1, $2)")
                    .put("pi()", "pi()")
                    .put("POW($1,$2)", "POW($1, $2)")
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
                    .put("LTRIM($1)", "trimLeft($1)")
                    .put("REPLACE($1,$2)", "replaceAll($1, $2, '')")
                    .put("REPLACE($1,$2,$3)", "replaceAll($1, $2, $3)")
                    .put("RTRIM($1)", "trimRight($1)")
                    .put("STRPOS($1,$2)", "position($1, $2)")
                    .put("SUBSTR($1,$2,$3)", "SUBSTR($1, $2, $3)")
                    .put("POSITION($1,$2)", "position($2, $1)")
                    .put("TRIM($1)", "trimBoth($1)")
                    .put("UPPER($1)", "UPPER($1)")
                    //date functions
                    .put("YEAR($1)", "toYear($1)")
                    .put("QUARTER($1)", "toQuarter($1)")
                    .put("MONTH($1)", "toMonth($1)")
                    .put("WEEK($1)", "toWeek($1)")
                    .put("YEAR_OF_WEEK($1)", "toISOYear($1)")
                    .put("DAY($1)", "toDayOfMonth($1)")
                    .put("HOUR($1)", "toHour($1)")
                    .put("MINUTE($1)", "toMinute($1)")
                    .put("SECOND($1)", "toSecond($1)")
                    .put("DAY_OF_WEEK($1)", "toDayOfWeek($1)")
                    .put("DAY_OF_MONTH($1)", "toDayOfMonth($1)")
                    .put("DAY_OF_YEAR($1)", "toDayOfYear($1)")
                    .put("TO_UNIXTIME($1)", "toUnixTimestamp($1)")
                    .build();
}
