/*
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
package io.prestosql.plugin.jdbc;

import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.optimization.BaseJdbcQueryGenerator;
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.plugin.splitmanager.SplitStatLog;
import io.prestosql.plugin.splitmanager.TableSplitConfig;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.function.ExternalFunctionHub;
import io.prestosql.spi.function.FunctionMetadataManager;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpressionService;
import io.prestosql.spi.sql.QueryGenerator;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

public abstract class ForwardingJdbcClient
        implements JdbcClient
{
    protected abstract JdbcClient getDelegate();

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return getDelegate().schemaExists(identity, schema);
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return getDelegate().getSchemaNames(identity);
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return getDelegate().getTableNames(identity, schema);
    }

    @Override
    public String getIdentifierQuote()
    {
        return getDelegate().getIdentifierQuote();
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        return getDelegate().getTableHandle(identity, schemaTableName);
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getDelegate().getColumns(session, tableHandle);
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return getDelegate().toPrestoType(session, connection, typeHandle);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return getDelegate().toWriteMapping(session, type);
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle layoutHandle)
    {
        return getDelegate().getSplits(identity, layoutHandle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return getDelegate().getConnection(identity, split);
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        getDelegate().abortReadConnection(connection);
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return getDelegate().buildSql(session, connection, split, tableHandle, columnHandles);
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return getDelegate().beginCreateTable(session, tableMetadata);
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().commitCreateTable(identity, handle);
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return getDelegate().beginInsertTable(session, tableMetadata);
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().finishInsertTable(identity, handle);
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        getDelegate().dropTable(identity, jdbcTableHandle);
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        getDelegate().rollbackCreateTable(identity, handle);
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return getDelegate().buildInsertSql(handle);
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return getDelegate().getConnection(identity, handle);
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return getDelegate().getPreparedStatement(connection, sql);
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return getDelegate().getTableStatistics(session, handle, tupleDomain);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        getDelegate().createSchema(session, schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        getDelegate().dropSchema(session, schemaName);
    }

    @Override
    public boolean supportsLimit()
    {
        return getDelegate().supportsLimit();
    }

    @Override
    public boolean isLimitGuaranteed()
    {
        return getDelegate().isLimitGuaranteed();
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        getDelegate().addColumn(session, handle, column);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        getDelegate().dropColumn(identity, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        getDelegate().renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        getDelegate().renameTable(identity, handle, newTableName);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        getDelegate().createTable(session, tableMetadata);
    }

    @Override
    public Collection<String> getCatalogNames(JdbcIdentity identity)
    {
        return getDelegate().getCatalogNames(identity);
    }

    /**
     * Hetu's push down requires to get output columns of the given sql query.
     * The returned list of columns does not necessarily match with the underlying table schema.
     * It interprets all the selected values as a separate column.
     * For example `SELECT CAST(MAX(price) AS varchar) as max_price FORM orders GROUP BY customer`
     * query will return a map with a single {@link ColumnHandle} for a VARCHAR max_price column.
     *
     * @param session the connector session for the JdbcClient
     * @param sql the sub-query to process
     * @param types Presto types of intermediate symbols
     * @return a Map of output symbols treated as columns along with their {@link ColumnHandle}s
     */
    @Override
    public Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        return getDelegate().getColumns(session, sql, types);
    }

    // default method to check if execution plan caching is supported by this connector
    @Override
    public boolean isExecutionPlanCacheSupported()
    {
        return getDelegate().isExecutionPlanCacheSupported();
    }

    /**
     * Hetu's query push down expects the JDBC connectors to provide a {@link QueryGenerator}
     * to write SQL queries for the respective databases. By default, this method provides the
     * {@link BaseJdbcQueryGenerator} which writes Presto ANSI SQL queries.
     * <p>
     * Override this method in the JDBC client of supporting database and return a {@link QueryGenerator}
     * object which knows how to write database specific SQL queries.
     *
     * @return the optional SQL query writer which can write database specific SQL queries
     */
    @Override
    public Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution)
    {
        return getDelegate().getQueryGenerator(determinismEvaluator, rowExpressionService, functionManager, functionResolution);
    }

    /**
     * return external function hub
     */
    @Override
    public Optional<ExternalFunctionHub> getExternalFunctionHub()
    {
        return Optional.empty();
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getDelegate().getDeleteRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return getDelegate().applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return getDelegate().executeDelete(session, handle);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return getDelegate().executeUpdate(session, handle);
    }

    @Override
    public OptionalLong deleteTable(ConnectorSession session, ConnectorTableHandle handle)
    {
        return getDelegate().deleteTable(session, handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getDelegate().beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        getDelegate().finishDelete(session, tableHandle, fragments);
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        return getDelegate().beginUpdate(session, tableHandle, updatedColumnTypes);
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        getDelegate().finishUpdate(session, tableHandle, fragments);
    }

    @Override
    public String buildDeleteSql(ConnectorTableHandle handle)
    {
        return getDelegate().buildDeleteSql(handle);
    }

    @Override
    public String buildUpdateSql(ConnectorSession session, ConnectorTableHandle handle, int updateColumnNum, List<String> updatedColumns)
    {
        return getDelegate().buildUpdateSql(session, handle, updateColumnNum, updatedColumns);
    }

    @Override
    public void setDeleteSql(PreparedStatement statement, Block rowIds, int position)
    {
        getDelegate().setDeleteSql(statement, rowIds, position);
    }

    @Override
    public void setUpdateSql(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, List<Block> columnValueAndRowIdBlock, int position, List<String> updatedColumns)
    {
        getDelegate().setUpdateSql(session, tableHandle, statement, columnValueAndRowIdBlock, position, updatedColumns);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return getDelegate().getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns);
    }

    @Override
    public List<SplitStatLog> getSplitStatic(JdbcIdentity identity, List<JdbcSplit> jdbcSplitList)
    {
        return getDelegate().getSplitStatic(identity, jdbcSplitList);
    }

    @Override
    public Long[] getSplitFieldMinAndMaxValue(TableSplitConfig conf, Connection connection, JdbcTableHandle tableHandle)
    {
        return getDelegate().getSplitFieldMinAndMaxValue(conf, connection, tableHandle);
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getDelegate().getTableModificationTime(session, tableHandle);
    }

    @Override
    public boolean isPreAggregationSupported(ConnectorSession session)
    {
        return getDelegate().isPreAggregationSupported(session);
    }
}
