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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.hetu.core.plugin.datacenter.client.DataCenterStatementClientFactory;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.testing.TestingTypeManager;
import okhttp3.OkHttpClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.OptionalLong;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestDataCenterMetadata
{
    private static final Logger log = Logger.get(TestDataCenterMetadata.class);

    public static final ConnectorSession SESSION = new ConnectorSession()
    {
        @Override
        public String getQueryId()
        {
            return null;
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return null;
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return null;
        }

        @Override
        public Locale getLocale()
        {
            return null;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public long getStartTime()
        {
            return 0;
        }

        @Override
        public boolean isLegacyTimestamp()
        {
            return false;
        }

        @Override
        public <T> T getProperty(String name, Class<T> type)
        {
            return null;
        }

        @Override
        public Optional<String> getCatalog()
        {
            return Optional.of("tpch");
        }
    };
    private static final DataCenterTableHandle NUMBERS_TABLE_HANDLE = new DataCenterTableHandle("tpch", "tiny",
            "orders", OptionalLong.empty());
    private TypeManager typeManager = new TestingTypeManager();

    private TestingPrestoServer server;

    private URI baseUri;

    private OkHttpClient httpClient;
    private DataCenterMetadata metadata;

    @BeforeClass
    public void setup()
            throws Exception
    {
        server = new TestingPrestoServer();
        baseUri = server.getBaseUrl();
        server.installPlugin(new TpchPlugin());
        server.createCatalog("tpch", "tpch");
        DataCenterConfig config = new DataCenterConfig().setConnectionUrl(this.baseUri).setConnectionUser("root");
        this.httpClient = DataCenterStatementClientFactory.newHttpClient(config);
        DataCenterClient client = new DataCenterClient(config, httpClient, typeManager);
        this.metadata = new DataCenterMetadata(client, config);
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
    public void testListSchemaNames()
    {
        assertEquals(metadata.listSchemaNames(SESSION),
                ImmutableSet.of("sf30000", "sf3000", "sf1000", "sf300", "tiny", "sf100000", "sf100", "sf10000", "sf1"));
    }

    @Test
    public void testGetTableHandle()
    {
        assertEquals(metadata.getTableHandle(SESSION, new SchemaTableName("tiny", "orders")), NUMBERS_TABLE_HANDLE);
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("tiny", "unknown")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "orders")));
        assertNull(metadata.getTableHandle(SESSION, new SchemaTableName("unknown", "unknown")));
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetColumnHandlesUnknownSchema()
    {
        metadata.getColumnHandles(SESSION,
                new DataCenterTableHandle("tpch", "unknown", "unknown", OptionalLong.empty()));
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testGetColumnHandlesUnknownTable()
    {
        metadata.getColumnHandles(SESSION,
                new DataCenterTableHandle("tpch", "tiny", "unknown", OptionalLong.empty()));
    }

    @Test
    public void testGetColumnHandles()
    {
        ImmutableMap.Builder builder = new ImmutableMap.Builder();

        builder.put("orderkey", new DataCenterColumnHandle("orderkey", BIGINT, 0))
                .put("custkey", new DataCenterColumnHandle("custkey", BIGINT, 1))
                .put("orderstatus", new DataCenterColumnHandle("orderstatus", createVarcharType(1), 2))
                .put("totalprice", new DataCenterColumnHandle("totalprice", DOUBLE, 3))
                .put("orderdate", new DataCenterColumnHandle("orderdate", createVarcharType(15), 4))
                .put("orderpriority", new DataCenterColumnHandle("orderpriority", createVarcharType(15), 5))
                .put("clerk", new DataCenterColumnHandle("clerk", createUnboundedVarcharType(), 6))
                .put("shippriority", new DataCenterColumnHandle("shippriority", INTEGER, 7))
                .put("comment", new DataCenterColumnHandle("comment", createVarcharType(79), 8));

        // known table
        assertEquals(metadata.getColumnHandles(SESSION, NUMBERS_TABLE_HANDLE), builder.build());
    }

    @Test
    public void getTableMetadata()
    {
        ImmutableList.Builder builder = new ImmutableList.Builder();
        builder.add(new ColumnMetadata("orderkey", BIGINT))
                .add(new ColumnMetadata("custkey", BIGINT))
                .add(new ColumnMetadata("orderstatus", createVarcharType(1)))
                .add(new ColumnMetadata("totalprice", DOUBLE))
                .add(new ColumnMetadata("orderdate", DATE))
                .add(new ColumnMetadata("orderpriority", createVarcharType(15)))
                .add(new ColumnMetadata("clerk", createVarcharType(15)))
                .add(new ColumnMetadata("shippriority", INTEGER))
                .add(new ColumnMetadata("comment", createVarcharType(79)));

        // known table
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(SESSION, NUMBERS_TABLE_HANDLE);
        assertEquals(tableMetadata.getTable(), new SchemaTableName("tiny", "orders"));
        assertEquals(tableMetadata.getColumns(), builder.build());
    }

    @Test(expectedExceptions = TableNotFoundException.class)
    public void testInvalidTableNamed()
    {
        metadata.getTableMetadata(SESSION,
                new DataCenterTableHandle("tpch", "unknown", "unknown", OptionalLong.empty()));
    }

    @Test
    public void testListTables()
    {
        // all schemas
        List<SchemaTableName> allTables = metadata.listTables(SESSION, Optional.empty());
        assertEquals(allTables.size(), 72);
        assertTrue(allTables.contains(new SchemaTableName("tiny", "orders")));

        // specific schema
        assertEquals(ImmutableSet.copyOf(metadata.listTables(SESSION, Optional.of("tiny"))),
                ImmutableSet.of(new SchemaTableName("tiny", "lineitem"), new SchemaTableName("tiny", "partsupp"),
                        new SchemaTableName("tiny", "nation"), new SchemaTableName("tiny", "part"),
                        new SchemaTableName("tiny", "supplier"), new SchemaTableName("tiny", "orders"),
                        new SchemaTableName("tiny", "region"), new SchemaTableName("tiny", "customer")));
    }

    @Test
    public void testUnknowSchema()
    {
        // unknown schema
        metadata.listTables(SESSION, Optional.of("unknown"));
    }

    @Test
    public void getColumnMetadata()
    {
        assertEquals(metadata.getColumnMetadata(SESSION, NUMBERS_TABLE_HANDLE,
                new DataCenterColumnHandle("text", createUnboundedVarcharType(), 0)),
                new ColumnMetadata("text", createUnboundedVarcharType()));

        // example connector assumes that the table handle and column handle are
        // properly formed, so it will return a metadata object for any
        // ExampleTableHandle and ExampleColumnHandle passed in.  This is on because
        // it is not possible for the Hetu Metadata system to create the handles
        // directly.
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testCreateTable()
    {
        metadata.createTable(SESSION, new ConnectorTableMetadata(new SchemaTableName("example", "foo"),
                ImmutableList.of(new ColumnMetadata("text", createUnboundedVarcharType()))), false);
    }

    @Test(expectedExceptions = PrestoException.class)
    public void testDropTableTable()
    {
        metadata.dropTable(SESSION, NUMBERS_TABLE_HANDLE);
    }
}
