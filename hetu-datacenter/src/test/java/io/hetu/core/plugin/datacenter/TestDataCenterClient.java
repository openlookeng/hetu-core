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

package io.hetu.core.plugin.datacenter;

import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.statistics.ColumnStatistics;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestDataCenterClient
{
    private TestingPrestoServer server;

    private DataCenterConfig config;

    private OkHttpClient httpClient;

    private URI baseUri;

    private TypeManager typeManager = new TestingTypeManager();

    /**
     * setup
     *
     * @throws Exception setup failed.
     */
    @BeforeClass
    public void setup()
            throws Exception
    {
        this.server = new TestingPrestoServer();
        this.baseUri = server.getBaseUrl();
        this.server.installPlugin(new TpchPlugin());
        this.server.createCatalog("tpch", "tpch");
        this.config = new DataCenterConfig().setConnectionUrl(this.baseUri).setConnectionUser("root");
        this.httpClient = DataCenterStatementClientFactory.newHttpClient(this.config);
    }

    @AfterClass(alwaysRun = true)
    public void teardown()
            throws IOException
    {
        server.close();
        this.httpClient.dispatcher().executorService().shutdown();
        this.httpClient.connectionPool().evictAll();
    }

    @Test
    public void testShowCatalogs()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        Set<String> catalogNames = client.getCatalogNames();
        assertTrue(catalogNames.contains("tpch"));
        assertEquals(catalogNames.size(), 2);
    }

    @Test
    public void testShowSchemas()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        Set<String> schemaNames = client.getSchemaNames("tpch");
        assertTrue(schemaNames.contains("tiny"));
        assertEquals(schemaNames.size(), 9);
    }

    @Test
    public void testShowTables()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        Set<String> tableNames = client.getTableNames("tpch", "tiny");
        assertTrue(tableNames.contains("orders"));
        assertEquals(tableNames.size(), 8);
    }

    @Test
    public void testGetTable()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        DataCenterTable table = client.getTable("tpch", "tiny", "orders");
        assertTrue(table.getColumns().contains(new DataCenterColumn("orderkey", BIGINT)));
        assertEquals(table.getColumns().size(), 9);
    }

    @Test
    public void testGetColumns()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        List<DataCenterColumn> columns = client.getColumns("select * from tpch.tiny.orders");
        assertTrue(columns.contains(new DataCenterColumn("orderkey", BIGINT)));
        assertEquals(columns.size(), 9);
    }

    @Test
    public void testGetTableStatistics()
    {
        Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>();
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        columnHandles.put("orderkey", new DataCenterColumnHandle("orderkey", DOUBLE, 0));
        columnHandles.put("custkey", new DataCenterColumnHandle("custkey", DOUBLE, 1));
        columnHandles.put("orderstatus", new DataCenterColumnHandle("orderstatus", createVarcharType(1), 2));
        columnHandles.put("totalprice", new DataCenterColumnHandle("totalprice", DOUBLE, 3));
        columnHandles.put("orderdate", new DataCenterColumnHandle("orderdate", DATE, 4));
        columnHandles.put("orderpriority", new DataCenterColumnHandle("orderpriority", createVarcharType(15), 5));
        columnHandles.put("clerk", new DataCenterColumnHandle("clerk", createUnboundedVarcharType(), 6));
        columnHandles.put("shippriority", new DataCenterColumnHandle("shippriority", DOUBLE, 7));
        columnHandles.put("comment", new DataCenterColumnHandle("comment", createVarcharType(79), 8));
        TableStatistics tableStatistics = client.getTableStatistics("tpch.tiny.orders", columnHandles);
        assertEquals(tableStatistics.getRowCount().getValue(), 15000.0);
        Map<ColumnHandle, ColumnStatistics> columnStatistics = tableStatistics.getColumnStatistics();
        for (Map.Entry<ColumnHandle, ColumnStatistics> columnstatistics : columnStatistics.entrySet()) {
            ColumnHandle columnhandleKey = columnstatistics.getKey();
            ColumnStatistics columnhandleValue = columnstatistics.getValue();
            if (columnhandleKey.getColumnName().equals("orderkey")) {
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 15000.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
                assertEquals(columnhandleValue.getRange().get().getMin(), (double) 1);
                assertEquals(columnhandleValue.getRange().get().getMax(), (double) 60000);
            }
            if (columnhandleKey.getColumnName().equals("custkey")) {
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 1000.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
                assertEquals(columnhandleValue.getRange().get().getMin(), (double) 1);
                assertEquals(columnhandleValue.getRange().get().getMax(), (double) 1499);
            }
            if (columnhandleKey.getColumnName().equals("orderstatus")) {
                assertEquals(columnhandleValue.getDataSize().getValue(), 3.0);
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 3.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
            }
            if (columnhandleKey.getColumnName().equals("totalprice")) {
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 14996.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
                assertEquals(columnhandleValue.getRange().get().getMin(), 874.89);
                assertEquals(columnhandleValue.getRange().get().getMax(), 466001.28);
            }
            if (columnhandleKey.getColumnName().equals("orderdate")) {
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 2401.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
                assertEquals(columnhandleValue.getRange().get().getMin(), (double) 8035);
                assertEquals(columnhandleValue.getRange().get().getMax(), (double) 10440);
            }
            if (columnhandleKey.getColumnName().equals("orderpriority")) {
                assertEquals(columnhandleValue.getDataSize().getValue(), 42.0);
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 5.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
            }
            if (columnhandleKey.getColumnName().equals("clerk")) {
                assertEquals(columnhandleValue.getDataSize().getValue(), 15000.0);
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 1000.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
            }
            if (columnhandleKey.getColumnName().equals("shippriority")) {
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 1.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
                assertEquals(columnhandleValue.getRange().get().getMin(), (double) 0);
                assertEquals(columnhandleValue.getRange().get().getMax(), (double) 0);
            }
            if (columnhandleKey.getColumnName().equals("comment")) {
                assertEquals(columnhandleValue.getDataSize().getValue(), 727249.0);
                assertEquals(columnhandleValue.getDistinctValuesCount().getValue(), 14995.0);
                assertEquals(columnhandleValue.getNullsFraction().getValue(), 0.0);
            }
        }
    }

    @Test
    public void testGetSplits()
    {
        DataCenterClient client = new DataCenterClient(this.config, httpClient, typeManager);
        int splits = client.getSplits("random-query");
        assertEquals(splits, 5);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testPasswordWithoutSSL()
    {
        DataCenterConfig config = new DataCenterConfig().setConnectionUrl(this.baseUri)
                .setConnectionUser("root")
                .setConnectionPassword("root")
                .setSsl(false);
        DataCenterStatementClientFactory.newHttpClient(config);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testKerberosWithoutSSL()
    {
        DataCenterConfig config = new DataCenterConfig().setConnectionUrl(this.baseUri)
                .setConnectionUser("root")
                .setKerberosRemoteServiceName("kerberos")
                .setSsl(false);
        DataCenterStatementClientFactory.newHttpClient(config);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testAccessTokenWithoutSSL()
    {
        DataCenterConfig config = new DataCenterConfig().setConnectionUrl(this.baseUri)
                .setConnectionUser("root")
                .setAccessToken("token")
                .setSsl(false);
        DataCenterStatementClientFactory.newHttpClient(config);
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testDataCenterConnectorFactoryFailure()
    {
        DataCenterConnectorFactory factory = new DataCenterConnectorFactory();
        factory.create("catalog", Collections.emptyMap(), null);
    }
}
