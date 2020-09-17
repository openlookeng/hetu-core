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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.metrics.ScanMetrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

/**
 * TestResultScanner
 *
 * @since 2020-03-20
 */
public class TestResultScanner
        implements ResultScanner
{
    private static int iNum = 5;

    @Override
    public Result next()
            throws IOException
    {
        if (iNum-- > 0) {
            return new TestResult(iNum);
        }
        else {
            Result ret = null;
            return Optional.ofNullable(ret).orElse(ret);
        }
    }

    @Override
    public Result[] next(int i)
            throws IOException
    {
        Result[] ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public boolean renewLease()
    {
        return false;
    }

    @Override
    public ScanMetrics getScanMetrics()
    {
        return null;
    }

    @Override
    public Iterator<Result> iterator()
    {
        Iterator<Result> iterator =
                new Iterator<Result>()
                {
                    @Override
                    public boolean hasNext()
                    {
                        if (iNum != 0) {
                            return true;
                        }
                        else {
                            return false;
                        }
                    }

                    @Override
                    public Result next()
                    {
                        if (iNum-- > 0) {
                            return new TestResult(iNum);
                        }
                        else {
                            Result ret = null;
                            return Optional.ofNullable(ret).orElse(ret);
                        }
                    }
                };
        return Optional.ofNullable(iterator).orElse(iterator);
    }
}
