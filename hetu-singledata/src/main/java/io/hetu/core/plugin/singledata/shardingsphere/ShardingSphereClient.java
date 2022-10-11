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

package io.hetu.core.plugin.singledata.shardingsphere;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.plugin.opengauss.OpenGaussClientConfig;
import io.hetu.core.plugin.singledata.BaseSingleDataClient;
import io.hetu.core.plugin.singledata.SingleDataSplit;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildPushDownSql;
import static io.hetu.core.plugin.singledata.SingleDataUtils.buildQueryByTableHandle;
import static io.hetu.core.plugin.singledata.SingleDataUtils.rewriteQueryWithColumns;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.lang.String.format;
import static java.sql.DatabaseMetaData.columnNoNulls;

public class ShardingSphereClient
        extends BaseSingleDataClient
{
    private static final Logger LOGGER = Logger.get(ShardingSphereClient.class);

    private static final String DATA_TYPE_COLUMN_LABEL = "DATA_TYPE";
    private static final String TYPE_NAME_COLUMN_LABEL = "TYPE_NAME";
    private static final String COLUMN_SIZE_COLUMN_LABEL = "COLUMN_SIZE";
    private static final String DECIMAL_DIGITS_COLUMN_LABEL = "DECIMAL_DIGITS";
    private static final String COLUMN_NAME_COLUMN_LABEL = "COLUMN_NAME";
    private static final String NULLABLE_COLUMN_LABEL = "NULLABLE";

    private final ShardingSphereAdaptor shardingSphereAdaptor;

    @Inject
    public ShardingSphereClient(BaseJdbcConfig config, OpenGaussClientConfig openGaussConfig,
            ConnectionFactory connectionFactory, TypeManager typeManager)
    {
        super(config, openGaussConfig, connectionFactory, typeManager);
        checkState(connectionFactory instanceof ShardingSphereAdaptor, "Need ShardingSphereAdaptor");
        this.shardingSphereAdaptor = (ShardingSphereAdaptor) connectionFactory;
    }

    @Override
    public Collection<String> listSchemas(Connection connection)
    {
        return shardingSphereAdaptor.getSchemaNames();
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return schema.map(schemaName ->
                shardingSphereAdaptor.getTableNames(schemaName).stream()
                        .map(tableName -> new SchemaTableName(schemaName, tableName))
                        .collect(Collectors.toList()))
                .orElse(Collections.emptyList());
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        List<SchemaTableName> tableNames = getTableNames(identity, Optional.of(schemaTableName.getSchemaName()));
        return tableNames.contains(schemaTableName)
                ? Optional.of(new JdbcTableHandle(schemaTableName, null, schemaTableName.getSchemaName(), schemaTableName.getTableName()))
                : Optional.empty();
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        try {
            String logicalTableName = tableHandle.getTableName();
            ActualDataNode actualDataNode = shardingSphereAdaptor.getOneActualDataNode(logicalTableName);
            try (Connection connection = shardingSphereAdaptor.openResourceConnection(actualDataNode.getResourceName())) {
                JdbcTableHandle actualTableHandle = new JdbcTableHandle(tableHandle.getSchemaTableName(),
                        connection.getCatalog(), actualDataNode.getSchemaName(), actualDataNode.getTableName());
                try (ResultSet resultSet = optimizedGetColumns(connection, actualTableHandle)) {
                    List<JdbcColumnHandle> columnHandles = getColumnsFromResultSet(session, connection, resultSet,
                            getArrayColumnDimensions(connection, actualTableHandle));
                    if (columnHandles.isEmpty()) {
                        throw new PrestoException(JDBC_ERROR, format("No supported columns found in dataSource [%s] table [%s]",
                                actualDataNode.getResourceName(), actualDataNode.getTableName()));
                    }
                    return ImmutableList.copyOf(columnHandles);
                }
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public Optional<List<SingleDataSplit>> tryGetSplits(ConnectorSession session, SingleDataPushDownContext context)
    {
        String pushDownSql = buildPushDownSql(context);
        try {
            return Optional.of(shardingSphereAdaptor.getSplitsByQuery(pushDownSql));
        }
        catch (Exception e) {
            LOGGER.debug("Try get SingleData Split failed, %s", e.getMessage());
        }
        return Optional.empty();
    }

    @Override
    public boolean checkJoinPushDown(JdbcTableHandle leftTableHandle, JdbcTableHandle rightTableHandle, String query)
    {
        return shardingSphereAdaptor.checkJoin(leftTableHandle, rightTableHandle, query);
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle)
    {
        String query = buildQueryByTableHandle(tableHandle);
        return new FixedSplitSource(shardingSphereAdaptor.getSplitsByQuery(query));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
    {
        checkState(split instanceof SingleDataSplit, "Need SingleDataSplit");
        return shardingSphereAdaptor.openResourceConnection(((SingleDataSplit) split).getDataSourceName());
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split,
            JdbcTableHandle tableHandle, List<JdbcColumnHandle> columns) throws SQLException
    {
        checkState(split instanceof SingleDataSplit, "Need SingleDataSplit");
        String sql = rewriteQueryWithColumns(((SingleDataSplit) split).getSql(), columns);
        LOGGER.debug("Actual Sql: [%s]", sql);
        return connection.prepareStatement(sql);
    }

    private List<JdbcColumnHandle> getColumnsFromResultSet(
            ConnectorSession session,
            Connection connection,
            ResultSet resultSet,
            Map<String, Integer> arrayColumnDimensions)
            throws SQLException
    {
        ImmutableList.Builder<JdbcColumnHandle> columnHandles = ImmutableList.builder();
        while (resultSet.next()) {
            String columnName = resultSet.getString(COLUMN_NAME_COLUMN_LABEL);
            JdbcTypeHandle typeHandle = new JdbcTypeHandle(
                    resultSet.getInt(DATA_TYPE_COLUMN_LABEL),
                    Optional.of(resultSet.getString(TYPE_NAME_COLUMN_LABEL)),
                    resultSet.getInt(COLUMN_SIZE_COLUMN_LABEL),
                    resultSet.getInt(DECIMAL_DIGITS_COLUMN_LABEL),
                    Optional.ofNullable(arrayColumnDimensions.get(columnName)));
            Optional<ColumnMapping> prestoType = toPrestoType(session, connection, typeHandle);
            if (prestoType.isPresent()) {
                boolean nullable = (resultSet.getInt(NULLABLE_COLUMN_LABEL) != columnNoNulls);
                columnHandles.add(new JdbcColumnHandle(columnName, typeHandle, prestoType.get().getType(), nullable));
            }
            else {
                LOGGER.warn("Column [%s] is unsupported by hetu, it will be skipped", columnName);
            }
        }
        return columnHandles.build();
    }
}
