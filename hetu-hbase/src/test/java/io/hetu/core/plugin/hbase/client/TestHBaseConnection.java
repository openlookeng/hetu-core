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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

/**
 * TestHBaseConnection
 *
 * @since 2020-03-20
 */
public class TestHBaseConnection
        implements Connection
{
    /**
     * mockito hBaseConnection
     */
    public static ClusterConnection connection;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.client.retries.number", "1");
        connection = Mockito.mock(ClusterConnection.class);
        Mockito.when(connection.getConfiguration()).thenReturn(conf);
        Mockito.when(connection.getRpcControllerFactory()).thenReturn(Mockito.mock(RpcControllerFactory.class));
        Mockito.when(connection.getConnectionConfiguration()).thenReturn(Mockito.mock(ConnectionConfiguration.class));

        // we need a real retrying caller
        RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(conf);
        Mockito.when(connection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);
        Mockito.when(connection.getNewRpcRetryingCallerFactory(conf)).thenReturn(callerFactory);
    }

    @Override
    public Configuration getConfiguration()
    {
        Configuration conf = null;
        return Optional.ofNullable(conf).orElse(conf);
    }

    @Override
    public BufferedMutator getBufferedMutator(TableName tableName)
            throws IOException
    {
        BufferedMutator buffMutator = null;
        return Optional.ofNullable(buffMutator).orElse(buffMutator);
    }

    @Override
    public BufferedMutator getBufferedMutator(BufferedMutatorParams params)
            throws IOException
    {
        BufferedMutator buffMutator = null;
        return Optional.ofNullable(buffMutator).orElse(buffMutator);
    }

    @Override
    public RegionLocator getRegionLocator(TableName tableName)
            throws IOException
    {
        return new TestHBaseRegionLocator();
    }

    @Override
    public Admin getAdmin()
            throws IOException
    {
        return new TestHBaseAdmin();
    }

    @Override
    public void close()
            throws IOException
    {
        // do nothing
    }

    @Override
    public boolean isClosed()
    {
        return true;
    }

    @Override
    public void abort(String why, Throwable e)
    {
        // do nothing
    }

    @Override
    public boolean isAborted()
    {
        return true;
    }

    @Override
    public Table getTable(TableName tableName)
            throws IOException
    {
        return new TestHTable();
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool)
            throws IOException
    {
        Table table = null;
        return Optional.ofNullable(table).orElse(table);
    }
}
