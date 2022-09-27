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

package io.hetu.core.plugin.singledata;

import io.hetu.core.plugin.opengauss.OpenGaussClient;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_UNSUPPORTED_EXPRESSION;

public abstract class BaseSingleDataClient
        extends OpenGaussClient
{
    public BaseSingleDataClient(BaseJdbcConfig config, OpenGaussClientConfig openGaussConfig,
            ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, openGaussConfig, connectionFactory, typeManager);
    }

    public abstract Optional<List<SingleDataSplit>> tryGetSplits(ConnectorSession session, SingleDataPushDownContext context);

    @Override
    public abstract Connection getConnection(JdbcIdentity identity, JdbcSplit split) throws SQLException;

    @Override
    public abstract PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split,
            JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns) throws SQLException;

    @Override
    public abstract ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle);

    public boolean checkJoinPushDown(JdbcTableHandle leftTableHandle, JdbcTableHandle rightTableHandle, String query)
    {
        return false;
    }

    // disable original limit push down
    @Override
    public boolean supportsLimit()
    {
        return false;
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return false;
    }

    // SingleData Connector doesn't support modify data
    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "INSERT DATA is not supported in this catalog");
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "CREATE TABLE is not supported in this catalog");
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "CREATE TABLE is not supported in this catalog");
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "RENAME TABLE is not supported in this catalog");
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "DROP TABLE is not supported in this catalog");
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "CREATE SCHEMA is not supported in this catalog");
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "DROP SCHEMA is not supported in this catalog");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "ADD COLUMN is not supported in this catalog");
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumnHandle, String newColumnName)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "RENAME COLUMN is not supported in this catalog");
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        throw new PrestoException(JDBC_UNSUPPORTED_EXPRESSION, "DROP COLUMN is not supported in this catalog");
    }
}
