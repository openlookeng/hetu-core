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
import io.prestosql.plugin.jdbc.optimization.JdbcConverterContext;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult;
import io.prestosql.plugin.splitmanager.SplitStatLog;
import io.prestosql.plugin.splitmanager.TableSplitConfig;
import io.prestosql.spi.PrestoException;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;

public interface JdbcClient
{
    default boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return getSchemaNames(identity).contains(schema);
    }

    String getIdentifierQuote();

    Set<String> getSchemaNames(JdbcIdentity identity);

    List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema);

    Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName);

    List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle);

    Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle);

    WriteMapping toWriteMapping(ConnectorSession session, Type type);

    ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle tableHandle);

    Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException;

    default void abortReadConnection(Connection connection)
            throws SQLException
    {
        // most drivers do not need this
    }

    PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle table, List<JdbcColumnHandle> columns)
            throws SQLException;

    boolean supportsLimit();

    boolean isLimitGuaranteed();

    void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column);

    void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column);

    void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName);

    void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName);

    void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle);

    JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata);

    void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle);

    void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle);

    void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle);

    String buildInsertSql(JdbcOutputTableHandle handle);

    Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException;

    PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException;

    TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain);

    void createSchema(ConnectorSession session, String schemaName);

    void dropSchema(ConnectorSession session, String schemaName);

    /**
     * Hetu requires catalog names to create dynamic catalogs.
     *
     * @param identity JDBC identity
     * @return the list of catalogs in the remote data source
     */
    default Collection<String> getCatalogNames(JdbcIdentity identity)
    {
        return Collections.emptyList();
    }

    /**
     * Hetu's query push down requires to get output columns of the given sql query.
     * The returned list of columns does not necessarily match with the underlying table schema.
     * It interprets all the selected values as a separate column.
     * For example `SELECT CAST(MAX(price) AS varchar) as max_price FORM orders GROUP BY customer`
     * query will return a map with a single {@link ColumnHandle} for a VARCHAR max_price column.
     *
     * @param session the connector session for the JdbcClient
     * @param sql the sub-query to process
     * @param types Hetu types of intermediate symbols
     * @return a Map of output symbols treated as columns along with their {@link ColumnHandle}s
     */
    default Map<String, ColumnHandle> getColumns(ConnectorSession session, String sql, Map<String, Type> types)
    {
        return Collections.emptyMap();
    }

    /**
     * Hetu's query push down expects the JDBC connectors to provide a {@link QueryGenerator}
     * to write SQL queries for the respective databases.
     *
     * @return the optional SQL query writer which can write database specific SQL Queries
     */
    default Optional<QueryGenerator<JdbcQueryGeneratorResult, JdbcConverterContext>> getQueryGenerator(DeterminismEvaluator determinismEvaluator, RowExpressionService rowExpressionService, FunctionMetadataManager functionManager, StandardFunctionResolution functionResolution)
    {
        return Optional.empty();
    }

    /**
     * Hetu can only cache execution plans for supported connectors.
     * By default, most JDBC connectors will be able to support to execution plan caching
     */
    default boolean isExecutionPlanCacheSupported()
    {
        return true;
    }

    /**
     * return external function hub
     */
    default Optional<ExternalFunctionHub> getExternalFunctionHub()
    {
        return Optional.empty();
    }

    default ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates or deletes");
    }

    default Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.empty();
    }

    default OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return OptionalLong.empty();
    }

    default OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return OptionalLong.empty();
    }

    default OptionalLong deleteTable(ConnectorSession session, ConnectorTableHandle handle)
    {
        return OptionalLong.empty();
    }

    default ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support beginDelete");
    }

    default void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support finishDelete");
    }

    default ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support beginDelete");
    }

    default void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support finishUpdate");
    }

    default String buildDeleteSql(ConnectorTableHandle handle)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support buildDeleteSql");
    }

    default String buildUpdateSql(ConnectorSession session, ConnectorTableHandle handle, int setNum, List<String> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support buildUpdateSql");
    }

    default void setDeleteSql(PreparedStatement statement, Block rowIds, int position)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setDeleteSql");
    }

    default void setUpdateSql(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, List<Block> columnValueAndRowIdBlock, int position, List<String> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support setUpdateSql");
    }

    default ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        throw new PrestoException(NOT_SUPPORTED, "This connector does not support updates or deletes");
    }

    default List<SplitStatLog> getSplitStatic(JdbcIdentity identity, List<JdbcSplit> jdbcSplitList)
    {
        return Collections.emptyList();
    }

    default Long[] getSplitFieldMinAndMaxValue(TableSplitConfig conf, Connection connection, JdbcTableHandle tableHandle)
    {
        return null;
    }

    default long getTableModificationTime(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        throw new PrestoException(NOT_SUPPORTED, "this connector does not support table modification times");
    }

    default boolean isPreAggregationSupported(ConnectorSession session)
    {
        return false;
    }
}
