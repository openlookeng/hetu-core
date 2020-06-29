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

import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;
import com.google.protobuf.Service;
import com.google.protobuf.ServiceException;
import io.hetu.core.plugin.hbase.utils.TestSliceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Row;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;
import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

/**
 * TestHTable
 *
 * @since 2020-03-20
 */
public class TestHTable
        implements Table
{
    @Override
    public TableName getName()
    {
        return TableName.valueOf("test_table");
    }

    @Override
    public Configuration getConfiguration()
    {
        Configuration conf = null;
        return Optional.ofNullable(conf).orElse(conf);
    }

    @Override
    public HTableDescriptor getTableDescriptor()
            throws IOException
    {
        HTableDescriptor tableDesc = null;
        return Optional.ofNullable(tableDesc).orElse(tableDesc);
    }

    @Override
    public boolean exists(Get get)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean[] existsAll(List<Get> list)
            throws IOException
    {
        return new boolean[0];
    }

    @Override
    public void batch(List<? extends Row> list, Object[] objects)
            throws IOException, InterruptedException
    {}

    @Override
    public Object[] batch(List<? extends Row> list)
            throws IOException, InterruptedException
    {
        Result[] ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public <R> void batchCallback(List<? extends Row> list, Object[] objects, Batch.Callback<R> callback)
            throws IOException, InterruptedException
    {}

    @Override
    public <R> Object[] batchCallback(List<? extends Row> list, Batch.Callback<R> callback)
            throws IOException, InterruptedException
    {
        Result[] ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public Result get(Get get)
            throws IOException
    {
        Result ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public ResultScanner getScanner(Scan scan)
            throws IOException
    {
        return new TestResultScanner();
    }

    @Override
    public ResultScanner getScanner(byte[] bytes)
            throws IOException
    {
        ResultScanner ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public ResultScanner getScanner(byte[] bytes, byte[] bytes1)
            throws IOException
    {
        ResultScanner ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public void put(Put put)
            throws IOException
    {}

    @Override
    public void put(List<Put> puts)
            throws IOException
    {
        Put put = new Put("0001".getBytes(UTF_8));
        put.addColumn(
                "name".getBytes(UTF_8), "nick_name".getBytes(UTF_8), TestSliceUtils.createSlice("name2").getBytes());
        Long longs = Long.valueOf(12);
        put.addColumn("age".getBytes(UTF_8), "lit_age".getBytes(UTF_8), longs.toString().getBytes(UTF_8));
        Integer ints = Integer.valueOf(17832);
        byte[] gender = new Date(DAYS.toMillis(ints.longValue())).toString().getBytes(UTF_8);
        put.addColumn("gender".getBytes(UTF_8), "gender".getBytes(UTF_8), gender);
        put.addColumn("t".getBytes(UTF_8), "t".getBytes(UTF_8), longs.toString().getBytes(UTF_8));
        List<Put> expected = new ArrayList<>();
        expected.add(put);
        assertEquals(expected.toString(), puts.toString());
    }

    @Override
    public boolean checkAndPut(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Put put)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean checkAndPut(
            byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Put put)
            throws IOException
    {
        return false;
    }

    @Override
    public void delete(Delete delete)
            throws IOException
    {}

    @Override
    public void delete(List<Delete> deletes)
            throws IOException
    {
        // do nothing
    }

    @Override
    public boolean checkAndDelete(byte[] bytes, byte[] bytes1, byte[] bytes2, byte[] bytes3, Delete delete)
            throws IOException
    {
        return false;
    }

    @Override
    public boolean checkAndDelete(
            byte[] bytes, byte[] bytes1, byte[] bytes2, CompareFilter.CompareOp compareOp, byte[] bytes3, Delete delete)
            throws IOException
    {
        return false;
    }

    @Override
    public void mutateRow(RowMutations rowMutations)
            throws IOException
    {}

    @Override
    public Result append(Append append)
            throws IOException
    {
        Result ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public Result increment(Increment increment)
            throws IOException
    {
        Result ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l)
            throws IOException
    {
        return 0;
    }

    @Override
    public long incrementColumnValue(byte[] bytes, byte[] bytes1, byte[] bytes2, long l, Durability durability)
            throws IOException
    {
        return 0;
    }

    @Override
    public Result[] get(List<Get> gets)
            throws IOException
    {
        Result[] results = new TestResult[5];
        int i = 0;
        for (; i < 5; i++) {
            results[i] = new TestResult(i);
        }
        return results;
    }

    @Override
    public void close()
            throws IOException
    {
        // do nothing
    }

    @Override
    public CoprocessorRpcChannel coprocessorService(byte[] bytes)
    {
        CoprocessorRpcChannel ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public <T extends Service, R> Map<byte[], R> coprocessorService(
            Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call)
            throws ServiceException, Throwable
    {
        Map ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public <T extends Service, R> void coprocessorService(
            Class<T> aClass, byte[] bytes, byte[] bytes1, Batch.Call<T, R> call, Batch.Callback<R> callback)
            throws ServiceException, Throwable
    {}

    @Override
    public long getWriteBufferSize()
    {
        return 0;
    }

    @Override
    public void setWriteBufferSize(long l)
            throws IOException
    {}

    @Override
    public <R extends Message> Map<byte[], R> batchCoprocessorService(
            Descriptors.MethodDescriptor methodDescriptor, Message message, byte[] bytes, byte[] bytes1, R r)
            throws ServiceException, Throwable
    {
        Map ret = null;
        return Optional.ofNullable(ret).orElse(ret);
    }

    @Override
    public <R extends Message> void batchCoprocessorService(
            Descriptors.MethodDescriptor methodDescriptor,
            Message message,
            byte[] bytes,
            byte[] bytes1,
            R r,
            Batch.Callback<R> callback)
            throws ServiceException, Throwable
    {}

    @Override
    public boolean checkAndMutate(
            byte[] bytes,
            byte[] bytes1,
            byte[] bytes2,
            CompareFilter.CompareOp compareOp,
            byte[] bytes3,
            RowMutations rowMutations)
            throws IOException
    {
        return false;
    }

    @Override
    public void setOperationTimeout(int i) {}

    @Override
    public int getOperationTimeout()
    {
        return 0;
    }

    @Override
    public void setRpcTimeout(int i) {}

    @Override
    public int getRpcTimeout()
    {
        return 0;
    }
}
