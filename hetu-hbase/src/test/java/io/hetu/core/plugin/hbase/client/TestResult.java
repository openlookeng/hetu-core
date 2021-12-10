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
package io.hetu.core.plugin.hbase.client;

import org.apache.hadoop.hbase.client.Result;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * TestResult
 *
 * @since 2020-03-20
 */
public class TestResult
        extends Result
{
    private int num;

    /**
     * TestResult
     */
    public TestResult(int iNum)
    {
        this.num = iNum;
    }

    @Override
    public byte[] getValue(byte[] family, byte[] qualifier)
    {
        if (Arrays.equals(family, "name".getBytes(StandardCharsets.UTF_8))
                && Arrays.equals(qualifier, "nick_name".getBytes(StandardCharsets.UTF_8))) {
            return ("nick_name_" + num).getBytes(StandardCharsets.UTF_8);
        }
        else if (Arrays.equals(family, "age".getBytes(StandardCharsets.UTF_8))
                && Arrays.equals(qualifier, "lit_age".getBytes(StandardCharsets.UTF_8))) {
            return "12".getBytes(StandardCharsets.UTF_8);
        }
        else if (Arrays.equals(family, "gender".getBytes(StandardCharsets.UTF_8))
                && Arrays.equals(qualifier, "gender".getBytes(StandardCharsets.UTF_8))) {
            return "2019-06-11".getBytes(StandardCharsets.UTF_8);
        }
        else if (Arrays.equals("t".getBytes(StandardCharsets.UTF_8), family)
                && Arrays.equals("t".getBytes(StandardCharsets.UTF_8), qualifier)) {
            return "20".getBytes(StandardCharsets.UTF_8);
        }
        else {
            return "".getBytes(StandardCharsets.UTF_8);
        }
    }

    @Override
    public byte[] getRow()
    {
        return "nick_name_1 12 2019-06-11 20".getBytes(StandardCharsets.UTF_8);
    }
}
