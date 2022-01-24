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
package io.hetu.core.plugin.hbase.client;

import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.utils.TestSliceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableBuilder;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

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

    /**
     * mockito hbase admin
     */
    public static HBaseAdmin admin;

    /**
     * mockito hbase admin
     */
    public static Table htable;

    private static final Logger LOG = Logger.get(TestHBaseConnection.class);

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

        admin = Mockito.mock(HBaseAdmin.class);
        try {
            Mockito.when(admin.listNamespaceDescriptors()).thenReturn(listNamespaceDescriptors());
            Mockito.when(admin.getTableDescriptor(Mockito.any())).thenAnswer(
                    new Answer() {
                        @Override
                        public Object answer(InvocationOnMock invocationOnMock) throws Throwable
                        {
                            TableName arg = (TableName) invocationOnMock.getArguments()[0];
                            return getTableDescriptor(arg);
                        }
                    });
            Mockito.when(admin.listNamespaceDescriptors()).thenReturn(listNamespaceDescriptors());
            Mockito.when(admin.listTableDescriptorsByNamespace(Mockito.anyString())).thenAnswer(
                    new Answer() {
                        @Override
                        public Object answer(InvocationOnMock invocationOnMock) throws Throwable
                        {
                            String arg = (String) invocationOnMock.getArguments()[0];
                            return listTableDescriptorsByNamespace(arg);
                        }
                    });
            Mockito.when(admin.listNamespaceDescriptors()).thenReturn(listNamespaceDescriptors());
        }
        catch (IOException e) {
            LOG.info("Error message: " + e.getStackTrace());
        }

        htable = Mockito.mock(Table.class);
        try {
            Mockito.doAnswer((Answer) invocation -> {
                List<Put> arg = (List<Put>) invocation.getArguments()[0];
                put(arg);
                return null;
            }).when(htable).put(Mockito.anyListOf(Put.class));
        }
        catch (IOException e) {
            LOG.info("Error message: " + e.getStackTrace());
        }
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
    public void clearRegionLocationCache()
    {
        // do nothing
    }

    @Override
    public HBaseAdmin getAdmin()
            throws IOException
    {
        return admin;
    }

    /**
     * listNamespaceDescriptors
     */
    public static NamespaceDescriptor[] listNamespaceDescriptors()
            throws IOException
    {
        NamespaceDescriptor[] res = new NamespaceDescriptor[3];

        res[0] = NamespaceDescriptor.create("hbase").build();
        res[1] = NamespaceDescriptor.create("default").build();
        res[2] = NamespaceDescriptor.create("testSchema").build();

        return res;
    }

    /**
     * getTableDescriptor
     */
    public static HTableDescriptor getTableDescriptor(TableName tableName)
            throws TableNotFoundException
    {
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);
        if (!tableName.getNameAsString().equals("hbase.test_table")
                && !tableName.getNameAsString().equals("hbase.test_table4")) {
            return hTableDescriptor;
        }

        hTableDescriptor.addFamily(new HColumnDescriptor("name"));
        hTableDescriptor.addFamily(new HColumnDescriptor("age"));
        hTableDescriptor.addFamily(new HColumnDescriptor("gender"));
        hTableDescriptor.addFamily(new HColumnDescriptor("t"));
        return hTableDescriptor;
    }

    /**
     * listTableDescriptorsByNamespace
     */
    public static HTableDescriptor[] listTableDescriptorsByNamespace(final String schema)
            throws IOException
    {
        if ("hbase".equals(schema)) {
            HTableDescriptor[] tables = new HTableDescriptor[3];
            for (int iNum = 0; iNum < 3; iNum++) {
                if (iNum != 0) {
                    tables[iNum] = new HTableDescriptor(TableName.valueOf("hbase.test_table" + iNum));
                }
                else {
                    tables[iNum] = new HTableDescriptor(TableName.valueOf("hbase.test_table"));
                }
                tables[iNum].addFamily(new HColumnDescriptor("name"));
                tables[iNum].addFamily(new HColumnDescriptor("age"));
                tables[iNum].addFamily(new HColumnDescriptor("gender"));
                tables[iNum].addFamily(new HColumnDescriptor("t"));
            }

            return tables;
        }
        else {
            return new HTableDescriptor[0];
        }
    }

    /**
     * put
     */
    public static void put(List<Put> puts)
            throws IOException
    {
        Put put = new Put("0001".getBytes(UTF_8));
        put.addColumn(
                "name".getBytes(UTF_8), "nick_name".getBytes(UTF_8), TestSliceUtils.createSlice("name2").getBytes());
        Long longs = Long.valueOf(12);
        put.addColumn("age".getBytes(UTF_8), "lit_age".getBytes(UTF_8), longs.toString().getBytes(UTF_8));
        Integer ints = Integer.valueOf(17832);
        Long gender = ints.longValue();
        put.addColumn("gender".getBytes(UTF_8), "gender".getBytes(UTF_8), gender.toString().getBytes(UTF_8));
        put.addColumn("t".getBytes(UTF_8), "t".getBytes(UTF_8), longs.toString().getBytes(UTF_8));
        List<Put> expected = new ArrayList<>();
        expected.add(put);
        assertEquals(expected.toString(), puts.toString());
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
    public TableBuilder getTableBuilder(TableName tableName, ExecutorService executorService)
    {
        return null;
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
        return htable;
    }

    @Override
    public Table getTable(TableName tableName, ExecutorService pool)
            throws IOException
    {
        Table table = null;
        return Optional.ofNullable(table).orElse(table);
    }
}
