/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.tidrange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.testing.TestingTypeManager;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UuidType.UUID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTidRangeClient
{
    private TidRangeClient testingClient;

    @BeforeClass
    public void initClient()
            throws SQLException
    {
        testingClient = new TidRangeClient(new BaseJdbcConfig(), new OpenGaussClientConfig(), new TidRangeConfig(),
                mockConnectionFactory(), new SingleDataTypeManager());
    }

    @Test
    public void testTryGetSplits()
    {
        ConnectorSession session = mock(ConnectorSession.class);
        ConnectorIdentity identity = mock(ConnectorIdentity.class);
        when(identity.getUser()).thenReturn("test");
        when(identity.getExtraCredentials()).thenReturn(ImmutableMap.of());
        when(session.getIdentity()).thenReturn(identity);
        SingleDataPushDownContext pushDownContext = new SingleDataPushDownContext();
        pushDownContext.setHasJoin(false);
        ColumnHandle columnHandle = mock(ColumnHandle.class);
        when(columnHandle.getColumnName()).thenReturn("test");

        pushDownContext.setColumns(ImmutableMap.of(new Symbol("test"), columnHandle));
        pushDownContext.setTableHandle(new JdbcTableHandle(new SchemaTableName("test", "test"),
                null, null, "test"));
        pushDownContext.setFilter("a > 1");
        pushDownContext.setLimit(10L);

        Optional<List<SingleDataSplit>> splits = testingClient.tryGetSplits(session, pushDownContext);
        assertTrue(splits.isPresent());
        assertEquals(splits.get().size(), 1);
        assertEquals(splits.get().get(0).getSql(), "SELECT test AS \"test\" FROM test WHERE ctid BETWEEN '(0, 1)' AND '(2, 0)' AND a > 1 LIMIT 10");
    }

    @Test
    public void testGetSplits()
    {
        ConnectorSplitSource splits = testingClient.getSplits(new JdbcIdentity("", ImmutableMap.of()),
                new JdbcTableHandle(new SchemaTableName("test", "test0"),
                        null, null, "test0"));
        assertFalse(splits.isFinished());
        splits.getNextBatch(NOT_PARTITIONED, 30);
        assertTrue(splits.isFinished());
    }

    private TidRangeConnectionFactory mockConnectionFactory()
            throws SQLException
    {
        TidRangeConnectionFactory connectionFactory = mock(TidRangeConnectionFactory.class);
        Connection connection = mockConnection();
        when(connectionFactory.openConnection(any())).thenReturn(connection);
        when(connectionFactory.getAvailableConnections(any())).thenReturn(ImmutableList.of(1));

        return connectionFactory;
    }

    private Connection mockConnection()
            throws SQLException
    {
        Connection connection = mock(Connection.class);

        PreparedStatement emptyStatement = mock(PreparedStatement.class);
        ResultSet emptyResultSet = mock(ResultSet.class);
        when(emptyResultSet.next()).thenReturn(false);
        when(emptyStatement.executeQuery()).thenReturn(emptyResultSet);

        PreparedStatement defaultSplitStatement = mock(PreparedStatement.class);
        ResultSet defaultSplitResultSet = mock(ResultSet.class);
        when(defaultSplitResultSet.next()).thenReturn(true);
        when(defaultSplitResultSet.getLong(1)).thenReturn(10000L);
        when(defaultSplitStatement.executeQuery()).thenReturn(defaultSplitResultSet);

        PreparedStatement maxSplitStatement = mock(PreparedStatement.class);
        ResultSet maxSplitResultSet = mock(ResultSet.class);
        when(maxSplitResultSet.next()).thenReturn(true);
        when(maxSplitResultSet.getLong(1)).thenReturn(1000000000L);
        when(maxSplitStatement.executeQuery()).thenReturn(maxSplitResultSet);

        PreparedStatement blockSizeStatement = mock(PreparedStatement.class);
        ResultSet blockSizeResultSet = mock(ResultSet.class);
        when(blockSizeResultSet.next()).thenReturn(true);
        when(blockSizeResultSet.getLong(1)).thenReturn(8192L);
        when(blockSizeStatement.executeQuery()).thenReturn(blockSizeResultSet);

        when(connection.prepareStatement("SELECT * FROM pg_indexes WHERE tablename = 'test'")).thenReturn(emptyStatement);
        when(connection.prepareStatement("SELECT * FROM pg_indexes WHERE tablename = 'test0'")).thenReturn(emptyStatement);
        when(connection.prepareStatement("SHOW block_size")).thenReturn(blockSizeStatement);
        when(connection.prepareStatement("SELECT pg_relation_size('test')")).thenReturn(defaultSplitStatement);
        when(connection.prepareStatement("SELECT pg_relation_size('test0')")).thenReturn(maxSplitStatement);

        return connection;
    }

    private static class SingleDataTypeManager
            extends TestingTypeManager
    {
        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of(BOOLEAN, BIGINT, INTEGER, DOUBLE, VARCHAR, VARBINARY, TIMESTAMP, DATE, HYPER_LOG_LOG, JSON, UUID);
        }
    }
}
