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
package io.hetu.core.plugin.clickhouse;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.plugin.clickhouse.optimization.ClickHousePushDownParameter;
import io.hetu.core.plugin.clickhouse.optimization.ClickHouseQueryGenerator;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SuppressFBWarnings;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.charWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.longDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.shortDecimalWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varbinaryWriteFunction;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharColumnMapping;
import static io.prestosql.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.util.Collections.nCopies;
import static java.util.Locale.ENGLISH;

public class ClickHouseClient
        extends BaseJdbcClient
{
    private static final Logger logger = Logger.get(ClickHouseClient.class);

    private String[] tableTypes;

    private ClickHouseConfig clickHouseConfig;

    private BaseJdbcConfig baseJdbcConfig;

    /**
     * If disabled, do not accept sub-query push down.
     */
    private final JdbcPushDownModule pushDownModule;

    /**
     * constructor
     *
     * @param config Base config
     * @param clickHouseConfig clickhouse config
     * @param connectionFactory connectionFactory
     */
    @Inject
    public ClickHouseClient(BaseJdbcConfig config, ClickHouseConfig clickHouseConfig, @StatsCollecting ConnectionFactory connectionFactory, ExternalFunctionHub externalFunctionHub)
    {
        super(config, "", connectionFactory, externalFunctionHub);
        tableTypes = clickHouseConfig.getTableTypes().split(",");
        this.pushDownModule = config.getPushDownModule();
        this.baseJdbcConfig = config;
        this.clickHouseConfig = clickHouseConfig;
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> columnMapping;

        String jdbcTypeName = "";
        if (typeHandle.getJdbcTypeName().isPresent()) {
            jdbcTypeName = typeHandle.getJdbcTypeName().get().toUpperCase(ENGLISH);
        }
        switch (typeHandle.getJdbcType()) {
            case Types.TINYINT:
                if (jdbcTypeName.equals("UINT8")) {
                    columnMapping = Optional.of(smallintColumnMapping());
                }
                else {
                    columnMapping = Optional.of(tinyintColumnMapping());
                }
                break;
            case Types.INTEGER:
                if (jdbcTypeName.equals("UINT32")) {
                    columnMapping = Optional.of(bigintColumnMapping());
                }
                else {
                    columnMapping = Optional.of(integerColumnMapping());
                }
                break;
            case Types.SMALLINT:
                if (jdbcTypeName.equals("UINT16")) {
                    columnMapping = Optional.of(integerColumnMapping());
                }
                else {
                    columnMapping = Optional.of(smallintColumnMapping());
                }
                break;
            case Types.BIGINT:
                if (jdbcTypeName.equals("UINT64")) {
                    logger.warn("openLooKeng doesn't support UInt64, it will convert to Int64");
                }
                columnMapping = Optional.of(bigintColumnMapping());
                break;
            case Types.VARCHAR:
                columnMapping = Optional.of(varcharColumnMapping(createUnboundedVarcharType()));
                break;
            case Types.DATE:
            case Types.TIMESTAMP:
            case Types.OTHER:
            default:
                columnMapping = super.toPrestoType(session, connection, typeHandle);
        }
        if (!columnMapping.isPresent()) {
            throw new UnsupportedOperationException(
                    "openLooKeng does not support the clickhouse type " + typeHandle.getJdbcTypeName().orElse("")
                            + '(' + typeHandle.getColumnSize() + ", " + typeHandle.getDecimalDigits()
                            + ')');
        }
        return columnMapping;
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (isVarcharType(type)) {
            return WriteMapping.sliceMapping("String", varcharWriteFunction());
        }
        else if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("String", varbinaryWriteFunction());
        }
        else if (type instanceof CharType) {
            return WriteMapping.sliceMapping("FixedString(" + ((CharType) type).getLength() + ')',
                    charWriteFunction());
        }
        else if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            String dataType = format("decimal(%s, %s)", decimalType.getPrecision(), decimalType.getScale());
            if (decimalType.isShort()) {
                return WriteMapping.longMapping(dataType, shortDecimalWriteFunction(decimalType));
            }
            return WriteMapping.sliceMapping(dataType, longDecimalWriteFunction(decimalType));
        }
        else if (BOOLEAN.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "There is no separate type for boolean values. Use UInt8 type, restricted to the values 0 or 1.");
        }
        else if (TIME_WITH_TIME_ZONE.equals(type) || TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            throw new PrestoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
        }

        return super.toWriteMapping(session, type);
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        try (ResultSet resultSet = connection.getMetaData().getSchemas()) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString(1);
                schemaNames.add(schemaName);
            }
            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "openLooKeng ClickHouse connector failed to list schemas");
        }
    }

    @Override
    protected ResultSet getTables(Connection connection, Optional<String> schemaName, Optional<String> tableName)
            throws SQLException
    {
        DatabaseMetaData metadata = connection.getMetaData();
        Optional<String> escape = Optional.ofNullable(metadata.getSearchStringEscape());
        return metadata.getTables(connection.getCatalog(), escapeNamePattern(schemaName, escape).orElse(null), escapeNamePattern(tableName, escape).orElse(null), tableTypes);
    }

    /**
     * create table function should be disabled.
     */
    @Override
    protected JdbcOutputTableHandle createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, String tableName)
    {
        throw new UnsupportedOperationException("ClickHouse Connector does not support create table.");
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            String columnName = column.getName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }

            String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
            String sql = format("ALTER TABLE %s ADD column %s", quoted(null, schema, handle.getTableName()), getColumnSql(session, column, columnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "openLooKeng ClickHouse connector failed to add column, check table engine and sql");
        }
    }

    private String getColumnSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder(ClickHouseConstants.DEAFULT_STRINGBUFFER_CAPACITY)
                .append(quoted(columnName))
                .append(" ")
                .append(toWriteMapping(session, column.getType()).getDataType());
        return sb.toString();
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
            String sql = format("ALTER TABLE %s DROP column %s", quoted(null, schema, handle.getTableName()), column.getColumnName());
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "openLooKeng ClickHouse connector failed to drop column");
        }
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, JdbcTableHandle handle)
    {
        String sql = format(
                "SELECT max(modification_time) as modified_time FROM system.parts WHERE database='%s' and table='%s'",
                handle.getSchemaTableName().getSchemaName(),
                handle.getSchemaTableName().getTableName());

        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                PreparedStatement preparedStatement = getPreparedStatement(connection, sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                String updateTime = resultSet.getString("modified_time");
                SimpleDateFormat parser = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
                return parser.parse(updateTime).getTime();
            }
            else {
                // In the case where for some reason the table doesn't exist anymore in system.parts
                return -1L;
            }
        }
        catch (Exception e) {
            // We want to make sure the query doesn't fail because of star-tree not being able to get last modified time
            logger.error("Exception thrown while trying to get modified time", e);
            return -1L;
        }
    }

    @Override
    public boolean isPreAggregationSupported(ConnectorSession session)
    {
        return true;
    }

    @Override
    protected void renameTable(JdbcIdentity identity, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String newSchemaName = newTable.getSchemaName();
            String newTableName = newTable.getTableName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newSchemaName = newSchemaName.toUpperCase(ENGLISH);
                newTableName = newTableName.toUpperCase(ENGLISH);
            }

            String schema = checkCatalog(catalogName, schemaName);
            String newSchema = checkCatalog(catalogName, newSchemaName);
            String sql = format("RENAME TABLE %s TO %s", quoted(null, schema, tableName), quoted(null, newSchema, newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "openLooKeng ClickHouse connector failed to rename table");
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String inputNewColumnName)
    {
        String newColumnName = inputNewColumnName;
        try (Connection connection = connectionFactory.openConnection(identity)) {
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                newColumnName = newColumnName.toUpperCase(ENGLISH);
            }
            String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
            String sql = format(
                    "ALTER TABLE %s RENAME COLUMN %s TO %s",
                    quoted(null, schema, handle.getTableName()),
                    jdbcColumn.getColumnName(),
                    newColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();

        JdbcIdentity identity = JdbcIdentity.from(session);
        if (!getSchemaNames(identity).contains(schemaTableName.getSchemaName())) {
            throw new PrestoException(NOT_FOUND, "Schema not found: " + schemaTableName.getSchemaName());
        }

        try (Connection connection = connectionFactory.openConnection(identity)) {
            boolean uppercase = connection.getMetaData().storesUpperCaseIdentifiers();
            String remoteSchema = toRemoteSchemaName(identity, connection, schemaTableName.getSchemaName());
            String remoteTable = toRemoteTableName(identity, connection, remoteSchema, schemaTableName.getTableName());
            String temporaryTableName = generateTemporaryTableName();
            String catalog = connection.getCatalog();

            ImmutableList.Builder<String> columnNames = ImmutableList.builder();
            ImmutableList.Builder<Type> columnTypes = ImmutableList.builder();
            ImmutableList.Builder<String> columnList = ImmutableList.builder();
            for (ColumnMetadata column : tableMetadata.getColumns()) {
                String columnName = column.getName();
                if (uppercase) {
                    columnName = columnName.toUpperCase(ENGLISH);
                }
                columnNames.add(columnName);
                columnTypes.add(column.getType());
                columnList.add(getColumnSql(session, column, columnName));
            }

            String schema = checkCatalog(catalog, remoteSchema);
            String sql = format(
                    "CREATE TABLE %s engine=Log AS SELECT * FROM %s WHERE 0 = 1",
                    quoted(null, schema, temporaryTableName),
                    quoted(null, schema, remoteTable));
            execute(connection, sql);

            return new JdbcOutputTableHandle(
                    catalog,
                    remoteSchema,
                    remoteTable,
                    columnNames.build(),
                    columnTypes.build(),
                    temporaryTableName);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
        String sql = format(
                "INSERT INTO %s VALUES (%s)",
                quoted(null, schema, handle.getTemporaryTableName()),
                join(",", nCopies(handle.getColumnNames().size(), "?")));
        return sql;
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException
    {
        if (table.getGeneratedSql().isPresent()) {
            // openLooKeng: If the sub-query is pushed down, use it as the table
            return new ClickHouseQueryBuilder(identifierQuote, true).buildSql(
                    this,
                    session,
                    connection,
                    null,
                    null,
                    table.getGeneratedSql().get().getSql(),
                    columns,
                    table.getConstraint(),
                    split.getAdditionalPredicate(),
                    tryApplyLimit(table.getLimit()));
        }
        return new ClickHouseQueryBuilder(identifierQuote).buildSql(
                this,
                session,
                connection,
                table.getCatalogName(),
                table.getSchemaName(),
                table.getTableName(),
                columns,
                table.getConstraint(),
                split.getAdditionalPredicate(),
                tryApplyLimit(table.getLimit()));
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
        String temporaryTable = quoted(null, schema, handle.getTemporaryTableName());
        String targetTable = quoted(null, schema, handle.getTableName());
        String insertSql = format("INSERT INTO %s SELECT * FROM %s", targetTable, temporaryTable);
        String cleanupSql = "DROP TABLE " + temporaryTable;

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, insertSql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        try (Connection connection = getConnection(identity, handle)) {
            execute(connection, cleanupSql);
        }
        catch (SQLException e) {
            logger.warn(e, "Failed to cleanup temporary table: %s", temporaryTable);
        }
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle handle)
    {
        String schema = checkCatalog(handle.getCatalogName(), handle.getSchemaName());
        String sql = "DROP TABLE " + quoted(null, schema, handle.getTableName());

        try (Connection connection = connectionFactory.openConnection(identity)) {
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
    }

    @Override
    protected Optional<BiFunction<String, Long, String>> limitFunction()
    {
        return Optional.of((sql, limit) -> sql + " LIMIT " + limit);
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return true;
    }

    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution)
    {
        ClickHousePushDownParameter pushDownParameter = new ClickHousePushDownParameter(getIdentifierQuote(), this.caseInsensitiveNameMatching, pushDownModule, clickHouseConfig, functionResolution);
        return Optional.of(new ClickHouseQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution, pushDownParameter, baseJdbcConfig));
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSetMetaData metadata = statement.getMetaData();
            ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                String columnName = metadata.getColumnLabel(i);
                String typeName = metadata.getColumnTypeName(i);
                int precision = metadata.getPrecision(i);
                int dataType = metadata.getColumnType(i);
                int scale = metadata.getScale(i);

                boolean isNullAble = metadata.isNullable(i) != ResultSetMetaData.columnNoNulls;
                if (dataType == Types.DECIMAL) {
                    String loweredColumnName = columnName.toLowerCase(ENGLISH);
                    Type type = types.get(loweredColumnName);
                    if (type instanceof DecimalType) {
                        DecimalType decimalType = (DecimalType) type;
                        precision = decimalType.getPrecision();
                        scale = decimalType.getScale();
                    }
                }

                JdbcTypeHandle typeHandle = new JdbcTypeHandle(dataType, Optional.ofNullable(typeName), precision, scale, Optional.empty());
                Optional<ColumnMapping> columnMapping;
                try {
                    columnMapping = toPrestoType(session, connection, typeHandle);
                }
                catch (UnsupportedOperationException ex) {
                    // User configured to fail the query if the data type is not supported
                    return Collections.emptyMap();
                }
                // skip unsupported column types
                if (columnMapping.isPresent()) {
                    Type type = columnMapping.get().getType();
                    JdbcColumnHandle handle = new JdbcColumnHandle(columnName, typeHandle, type, isNullAble);
                    builder.put(columnName.toLowerCase(ENGLISH), handle);
                }
                else {
                    return Collections.emptyMap();
                }
            }
            return builder.build();
        }
        catch (SQLException | PrestoException e) {
            logger.error("in clickhouse push down, clickhouse data source error msg[%s] rewrite sql[%s]", e.getMessage(), sql);
            return Collections.emptyMap();
        }
    }

    /**
     * Clickhouse does not support operations on catalog.schema.table
     */
    private String checkCatalog(String catalog, String remoteSchema)
    {
        String schema;
        if (catalog != null && remoteSchema != null) {
            schema = remoteSchema;
        }
        else {
            schema = catalog == null ? remoteSchema : catalog;
        }
        return schema;
    }
}
