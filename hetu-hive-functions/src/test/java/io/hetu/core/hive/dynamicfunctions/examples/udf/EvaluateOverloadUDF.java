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

package io.hetu.core.hive.dynamicfunctions.examples.udf;

import java.util.List;
import java.util.Map;

public class EvaluateOverloadUDF
        extends HiveFunction
{
    public int evaluate(int x)
    {
        return x;
    }

    public int evaluateByInteger(Integer x)
    {
        return x;
    }

    public int evaluate(int x, int y)
    {
        return x - y;
    }

    public int evaluate(int x, int y, int z)
    {
        return x - y - z;
    }

    public int evaluate(int x, int y, int z, int m)
    {
        return x - y - z - m;
    }

    public int evaluate(int x, int y, int z, int m, int n)
    {
        return x - y - z - m - n;
    }

    public long evaluate(long x, long y)
    {
        return x - y;
    }

    public String evaluate(String x)
    {
        return x;
    }

    public Integer evaluate(Integer x, Map<Integer, Integer> map, List<Integer> list)
    {
        return x - list.get(0) - map.get(0);
    }
}
