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
package io.hetu.core.plugin.hbase.utils;

public class StartAndEndKey
{
    private final char start;
    private final char end;

    public StartAndEndKey(char start, char end)
    {
        this.start = start;
        this.end = end;
    }

    public StartAndEndKey(String startAndEnd)
    {
        String[] pairs = startAndEnd.split(Constants.SEPARATOR_START_END_KEY);
        this.start = pairs[0].charAt(0);
        this.end = pairs[1].charAt(0);
    }

    public char getStart()
    {
        return start;
    }

    public char getEnd()
    {
        return end;
    }

    @Override
    public String toString()
    {
        return "StartAndEndKey{" +
                "start=" + start +
                ", end=" + end +
                '}';
    }
}
