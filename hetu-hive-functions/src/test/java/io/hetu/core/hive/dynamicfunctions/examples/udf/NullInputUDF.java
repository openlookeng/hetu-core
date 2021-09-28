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

package io.hetu.core.hive.dynamicfunctions.examples.udf;

public class NullInputUDF
        extends HiveFunction
{
    private static final int STR_LENGTH_WITHOUT_TRIM = 1;
    private static final int STR_LENGTH_WITH_TRIM = 2;

    public int evaluate(String str, Integer flag)
    {
        if ((null == str) || "".equals(str) || (null == flag) || ((flag != STR_LENGTH_WITHOUT_TRIM) && (flag != STR_LENGTH_WITH_TRIM))) {
            return -1;
        }

        if (flag == STR_LENGTH_WITHOUT_TRIM) {
            return str.length();
        }
        else {
            return str.trim().length();
        }
    }
}
