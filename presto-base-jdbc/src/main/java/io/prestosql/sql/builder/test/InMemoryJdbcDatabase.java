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
package io.prestosql.sql.builder.test;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcHandleResolver;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcSplitManager;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.transaction.IsolationLevel;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static java.util.function.Function.identity;

public final class InMemoryJdbcDatabase
        implements AutoCloseable
{
    private final Connection connection;
    private final JdbcClient jdbcClient;
    private final String schemaName;
    private final ConnectorFactory connectorFactory;

    public InMemoryJdbcDatabase(Driver driver, String connectionUrl, String connectorName, String schemaName)
            throws SQLException
    {
        this(driver, connectionUrl, connectorName, schemaName, new BaseJdbcConfig(), new Properties());
    }

    public InMemoryJdbcDatabase(Driver driver, String connectionUrl, String connectorName, String schemaName, BaseJdbcConfig baseJdbcConfig, Properties connectionProperties)
            throws SQLException
    {
        this.schemaName = schemaName;
        jdbcClient = new InMemoryJdbcClient(baseJdbcConfig, driver, connectionUrl, connectionProperties);
        connection = DriverManager.getConnection(connectionUrl, connectionProperties);
        this.connectorFactory = new InMemoryJdbcConnectorFactory(this.jdbcClient, connectorName);
    }

    public void createTables()
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schemaName);
            statement.execute("CREATE TABLE " + schemaName + ".orders (orderkey bigint NOT NULL, custkey bigint NOT NULL, orderstatus varchar(1) NOT NULL, totalprice DOUBLE NOT NULL, orderdate date NOT NULL, orderpriority varchar(15) NOT NULL, clerk varchar(15) NOT NULL, shippriority integer NOT NULL, COMMENT varchar(79) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".customer (custkey bigint NOT NULL, name varchar(25) NOT NULL, address varchar(40) NOT NULL, nationkey bigint NOT NULL, phone varchar(15) NOT NULL, acctbal DOUBLE NOT NULL, mktsegment varchar(10) NOT NULL, COMMENT varchar(117) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".supplier (suppkey bigint NOT NULL, name varchar(25) NOT NULL, address varchar(40) NOT NULL, nationkey bigint NOT NULL, phone varchar(15) NOT NULL, acctbal DOUBLE NOT NULL, COMMENT varchar(101) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".region (regionkey bigint NOT NULL, name varchar(25) NOT NULL, COMMENT varchar(152) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".lineitem (orderkey bigint NOT NULL, partkey bigint NOT NULL, suppkey bigint NOT NULL, linenumber integer NOT NULL, quantity DOUBLE NOT NULL, extendedprice DOUBLE NOT NULL, discount DOUBLE NOT NULL, tax DOUBLE NOT NULL, returnflag varchar(1) NOT NULL, linestatus varchar(1) NOT NULL, shipdate date NOT NULL, commitdate date NOT NULL, receiptdate date NOT NULL, shipinstruct varchar(25) NOT NULL, shipmode varchar(10) NOT NULL, COMMENT varchar(44) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".nation (nationkey bigint NOT NULL, name varchar(25) NOT NULL, regionkey bigint NOT NULL, COMMENT varchar(152) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".part (partkey bigint NOT NULL, name varchar(55) NOT NULL, mfgr varchar(25) NOT NULL, brand varchar(10) NOT NULL, TYPE varchar(25) NOT NULL, SIZE integer NOT NULL, container varchar(10) NOT NULL, retailprice DOUBLE NOT NULL, COMMENT varchar(23) NOT NULL) ");
            statement.execute("CREATE TABLE " + schemaName + ".partsupp (partkey bigint NOT NULL, suppkey bigint NOT NULL, availqty integer NOT NULL, supplycost DOUBLE NOT NULL, COMMENT varchar(199) NOT NULL)");
        }
        connection.commit();
    }

    @Override
    public void close()
            throws SQLException
    {
        connection.close();
    }

    public Connection getConnection()
    {
        return connection;
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public ConnectorFactory getConnectorFactory()
    {
        return connectorFactory;
    }

    public JdbcTableHandle getTableHandle(ConnectorSession session, SchemaTableName table)
    {
        return jdbcClient.getTableHandle(JdbcIdentity.from(session), table)
                .orElseThrow(() -> new IllegalArgumentException("table not found: " + table));
    }

    public JdbcSplit getSplit(ConnectorSession session, JdbcTableHandle table)
    {
        ConnectorSplitSource splits = jdbcClient.getSplits(JdbcIdentity.from(session), table);
        return (JdbcSplit) getOnlyElement(getFutureValue(splits.getNextBatch(NOT_PARTITIONED, 1000)).getSplits());
    }

    public Map<String, JdbcColumnHandle> getColumnHandles(ConnectorSession session, JdbcTableHandle table)
    {
        return jdbcClient.getColumns(session, table).stream()
                .collect(toImmutableMap(column -> column.getColumnMetadata().getName(), identity()));
    }

    private static class InMemoryJdbcConnectorFactory
            implements ConnectorFactory
    {
        private final JdbcClient jdbcClient;
        private final String name;

        private InMemoryJdbcConnectorFactory(JdbcClient jdbcClient, String name)
        {
            this.jdbcClient = jdbcClient;
            this.name = name;
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new JdbcHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return new ConnectorTransactionHandle() {};
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
                {
                    return new JdbcMetadata(jdbcClient, false);
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new JdbcSplitManager(jdbcClient);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new JdbcRecordSetProvider(jdbcClient);
                }
            };
        }
    }

    private static class InMemoryJdbcClient
            extends BaseJdbcClient
    {
        public InMemoryJdbcClient(BaseJdbcConfig baseJdbcConfig, Driver driver, String connectionUrl, Properties properties)
        {
            super(baseJdbcConfig, "\"", new DriverConnectionFactory(driver, connectionUrl, Optional.empty(), Optional.empty(), properties));
        }
    }
}
