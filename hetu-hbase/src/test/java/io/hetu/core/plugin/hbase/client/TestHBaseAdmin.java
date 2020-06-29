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
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.mockito.Mockito;

import java.io.IOException;

/**
 * TestHBaseAdmin
 *
 * @since 2020-03-20
 */
public class TestHBaseAdmin
        extends HBaseAdmin
{
    /**
     * mockito hBaseConnection
     */
    public static ClusterConnection connection;

    static {
        Configuration conf = HBaseConfiguration.create();
        connection = Mockito.mock(ClusterConnection.class);
        Mockito.when(connection.getConfiguration()).thenReturn(conf);
        Mockito.when(connection.getRpcControllerFactory()).thenReturn(Mockito.mock(RpcControllerFactory.class));
        // we need a real retrying caller
        RpcRetryingCallerFactory callerFactory = new RpcRetryingCallerFactory(conf);
        Mockito.when(connection.getRpcRetryingCallerFactory()).thenReturn(callerFactory);
    }

    /**
     * TestHBaseAdmin constructor
     */
    public TestHBaseAdmin()
            throws IOException
    {
        super(connection);
    }

    /**
     * listNamespaceDescriptors
     */
    public NamespaceDescriptor[] listNamespaceDescriptors()
            throws IOException
    {
        NamespaceDescriptor[] res = new NamespaceDescriptor[3];

        res[0] = NamespaceDescriptor.create("hbase").build();
        res[1] = NamespaceDescriptor.create("default").build();
        res[2] = NamespaceDescriptor.create("testSchema").build();

        return res;
    }

    @Override
    public HTableDescriptor getTableDescriptor(TableName tableName)
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

    @Override
    public HTableDescriptor[] listTableDescriptorsByNamespace(final String schema)
            throws IOException
    {
        if ("hbase".equals(schema)) {
            HTableDescriptor[] tables = new HTableDescriptor[3];
            for (int iNum = 0; iNum < 3; iNum++) {
                if (iNum != 0) {
                    tables[iNum] = new HTableDescriptor("hbase.test_table" + iNum);
                }
                else {
                    tables[iNum] = new HTableDescriptor("hbase.test_table");
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

    @Override
    public void createTable(HTableDescriptor desc)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void disableTable(final TableName tableName)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void truncateTable(final TableName tableName, final boolean preserveSplits)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void addColumn(String tableName, HColumnDescriptor column)
            throws IOException
    {
        throw new IOException("addColumn failed");
    }

    @Override
    public void snapshot(final String snapshotName, final TableName tableName)
            throws IOException, IllegalArgumentException
    {
        // do nothing
    }

    @Override
    public void cloneSnapshot(final String snapshotName, final TableName tableName)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void deleteSnapshot(final String snapshotName)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void deleteTable(final TableName tableName)
            throws IOException
    {
        // do nothing
    }
}
