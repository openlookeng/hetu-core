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
package io.hetu.core.plugin.hana;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hana.optimization.HanaPushDownParameter;
import io.hetu.core.plugin.hana.optimization.HanaQueryGenerator;
import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DoubleWriteFunction;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.StatsCollecting;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownModule;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SuppressFBWarnings;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;

import javax.inject.Inject;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.BiFunction;

import static io.hetu.core.plugin.hana.HanaConstants.DEAFULT_STRINGBUFFER_CAPACITY;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

/**
 * Implementation of HanaClient. It describes table, schemas and columns behaviours.
 * It allows to change the QueryBuilder to a custom one as well.
 *
 * @since 2019-07-10
 */
public class HanaClient
        extends BaseJdbcClient
{
    private static final int HANA_SMALLDECIMAL_DEFAULT_SCALE = 16;

    private final Logger logger = Logger.get(HanaClient.class);

    private String[] tableTypes;

    private String schemaPattern;

    private HanaConfig hanaConfig;

    private BaseJdbcConfig baseJdbcConfig;

    /**
     * If disabled, do not accept sub-query push down.
     */
    private final JdbcPushDownModule pushDownModule;

    /**
     * constructor
     *
     * @param config config
     * @param hanaConfig hanaConfig
     * @param connectionFactory connectionFactory object
     */
    @Inject
    public HanaClient(BaseJdbcConfig config, HanaConfig hanaConfig, @StatsCollecting ConnectionFactory connectionFactory)
    {
        super(config, "", connectionFactory);
        tableTypes = hanaConfig.getTableTypes().split(",");
        schemaPattern = hanaConfig.getSchemaPattern();
        this.pushDownModule = config.getPushDownModule();
        this.baseJdbcConfig = config;
        this.hanaConfig = hanaConfig;
    }

    @Override
    protected Collection<String> listSchemas(Connection connection)
    {
        String realSchemaPattern = schemaPattern;

        try {
            if (realSchemaPattern != null && connection.getMetaData().storesUpperCaseIdentifiers()) {
                realSchemaPattern = realSchemaPattern.toUpperCase(ENGLISH);
            }
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to list schemas");
        }

        try (ResultSet resultSet = connection.getMetaData().getSchemas(connection.getCatalog(), realSchemaPattern)) {
            ImmutableSet.Builder<String> schemaNames = ImmutableSet.builder();
            while (resultSet.next()) {
                String schemaName = resultSet.getString("TABLE_SCHEM");
                // skip internal schemas
                if (!"information_schema".equalsIgnoreCase(schemaName)) {
                    schemaNames.add(schemaName);
                }
            }

            return schemaNames.build();
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to list schemas");
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

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session))) {
            String columnName = column.getName();
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                columnName = columnName.toUpperCase(ENGLISH);
            }
            String sql = format("ALTER TABLE %s ADD (%s)", quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()), getColumnSql(session, column, columnName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to add column");
        }
    }

    private String getColumnSql(ConnectorSession session, ColumnMetadata column, String columnName)
    {
        StringBuilder sb = new StringBuilder(DEAFULT_STRINGBUFFER_CAPACITY).append(quoted(columnName)).append(" ").append(toWriteMapping(session, column.getType()).getDataType());
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String sql = format("ALTER TABLE %s DROP (%s)", quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()), column.getColumnName());
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to drop column");
        }
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        try (Connection connection = connectionFactory.openConnection(identity)) {
            String oldColumnName = jdbcColumn.getColumnName();
            String realNewColumnName = newColumnName;
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                realNewColumnName = realNewColumnName.toUpperCase(ENGLISH);
                oldColumnName = oldColumnName.toUpperCase(ENGLISH);
            }
            String sql = format("RENAME COLUMN %s.%s TO %s", quoted(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName()), oldColumnName, realNewColumnName);
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to rename column");
        }
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
            String sql = format("RENAME TABLE %s TO %s", quoted(catalogName, schemaName, tableName), quoted(catalogName, newSchemaName, newTableName));
            execute(connection, sql);
        }
        catch (SQLException e) {
            throw new PrestoException(JDBC_ERROR, "Hetu Hana connector failed to rename table");
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
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        Connection connection = connectionFactory.openConnection(identity);
        return connection;
    }

    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution)
    {
        HanaPushDownParameter pushDownParameter = new HanaPushDownParameter(getIdentifierQuote(), this.caseInsensitiveNameMatching, pushDownModule, hanaConfig, functionResolution);
        return Optional.of(new HanaQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution, pushDownParameter));
    }

    @SuppressFBWarnings("SQL_PREPARED_STATEMENT_GENERATED_FROM_NONCONSTANT_STRING")
    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        try (Connection connection = connectionFactory.openConnection(JdbcIdentity.from(session));
                PreparedStatement statement = connection.prepareStatement(sql)) {
            ResultSetMetaData metadata = statement.getMetaData();
            ImmutableMap.Builder<String, ColumnHandle> builder = new ImmutableMap.Builder<>();
            Map<String, Type> nodeMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
            nodeMap.putAll(types);
            for (int i = 1; i <= metadata.getColumnCount(); i++) {
                String columnName = metadata.getColumnLabel(i);
                String typeName = metadata.getColumnTypeName(i);
                int precision = metadata.getPrecision(i);
                int dataType = metadata.getColumnType(i);
                int scale = metadata.getScale(i);
                // The hana datasource decimal's scale with zero value which means dynamic scale in hana,
                // but in hetu it mean no number of digits in fractional part.
                // When we are dealing with decimal type columns,
                // use hetu's precision and scale define from hetu plan tree node's output column type.
                if (dataType == Types.DECIMAL) {
                    Type type = nodeMap.get(columnName);
                    if (type instanceof DecimalType) {
                        DecimalType decimalType = (DecimalType) type;
                        precision = decimalType.getPrecision();
                        scale = decimalType.getScale();
                    }
                }
                boolean isNullAble = metadata.isNullable(i) != ResultSetMetaData.columnNoNulls;

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
        catch (SQLException e1) {
            // No need to raise an error.
            // This method is used inside applySubQuery method to extract the column types from a sub-query.
            // Returning empty map will indicate that something wrong and let the Hetu to execute the query as usual.
            logger.warn("in hana push down, hana data source error msg[%s] rewrite sql[%s]", e1.getMessage(), sql);
            return Collections.emptyMap();
        }
        catch (PrestoException e2) {
            // No need to raise an error.
            // This method is used inside applySubQuery method to extract the column types from a sub-query.
            // Returning empty map will indicate that something wrong and let the Hetu to execute the query as usual.
            logger.warn("in hana push down, hana connector error msg[%s] rewrite sql[%s]", e2.getMessage(), sql);
            return Collections.emptyMap();
        }
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        // web address, can not change it
        // CHECKSTYLE:OFF:LineLength
        // https://help.sap.com/viewer/4fe29514fd584807ac9f2a04f6754767/2.0.03/en-US/20a1569875191014b507cf392724b7eb.html
        // CHECKSTYLE:ON:LineLength
        Optional<ColumnMapping> columnMapping;
        int columnSize = typeHandle.getColumnSize();

        switch (typeHandle.getJdbcType()) {
            case Types.DECIMAL:
                int decimalDigits = typeHandle.getDecimalDigits();
                // Map decimal(p, -s) (negative scale) to
                // decimal(p+s, 0).
                int precision = columnSize + max(-decimalDigits, 0);
                if (precision > Decimals.MAX_PRECISION) {
                    columnMapping = Optional.empty();
                    break;
                }
                DecimalType decimalType = createDecimalType(precision, max(decimalDigits, 0));
                if (precision == HANA_SMALLDECIMAL_DEFAULT_SCALE && decimalType.getScale() == 0) {
                    // hana smalldecimal type, or decimal p==HANA_SMALLDECIMAL_DEFAULT_SCALE, s==0
                    columnMapping = Optional.of(hanaSmallDecimalColumnMapping());
                    break;
                }
                else {
                    columnMapping = super.toPrestoType(session, connection, typeHandle);
                    break;
                }
            default:
                columnMapping = super.toPrestoType(session, connection, typeHandle);
        }
        return columnMapping;
    }

    private static ColumnMapping hanaSmallDecimalColumnMapping()
    {
        // JDBC driver can return BigDecimal with lower scale than column's scale when there are trailing zeroes
        return ColumnMapping.doubleMapping(DOUBLE, (resultSet,
                        columnIndex) -> hanaEncodeShortScaledShortDecimalValue(resultSet.getBigDecimal(columnIndex)),
                hanaShortDecimalWriteFunction());
    }

    private static double hanaEncodeShortScaledShortDecimalValue(BigDecimal value)
    {
        return value.doubleValue();
    }

    private static DoubleWriteFunction hanaShortDecimalWriteFunction()
    {
        return (statement, index, value) -> {
            BigDecimal bigDecimal = new BigDecimal(Double.toString(value));
            statement.setBigDecimal(index, bigDecimal);
        };
    }
}
