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
package io.prestosql.plugin.jdbc.jmx;

import io.airlift.slice.Slice;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ForwardingJdbcClient;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcOutputTableHandle;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.plugin.jdbc.WriteMapping;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.type.Type;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StatisticsAwareJdbcClient
        extends ForwardingJdbcClient
{
    private final JdbcClientStats stats = new JdbcClientStats();
    private final JdbcClient delegate;

    public StatisticsAwareJdbcClient(JdbcClient delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    protected JdbcClient getDelegate()
    {
        return delegate;
    }

    @Override
    public String getIdentifierQuote()
    {
        return delegate.getIdentifierQuote();
    }

    @Managed
    @Flatten
    public JdbcClientStats getStats()
    {
        return stats;
    }

    @Override
    public boolean schemaExists(JdbcIdentity identity, String schema)
    {
        return stats.schemaExists.wrap(() -> getDelegate().schemaExists(identity, schema));
    }

    @Override
    public Set<String> getSchemaNames(JdbcIdentity identity)
    {
        return stats.getSchemaNames.wrap(() -> getDelegate().getSchemaNames(identity));
    }

    @Override
    public List<SchemaTableName> getTableNames(JdbcIdentity identity, Optional<String> schema)
    {
        return stats.getTableNames.wrap(() -> getDelegate().getTableNames(identity, schema));
    }

    @Override
    public Optional<JdbcTableHandle> getTableHandle(JdbcIdentity identity, SchemaTableName schemaTableName)
    {
        return stats.getTableHandle.wrap(() -> getDelegate().getTableHandle(identity, schemaTableName));
    }

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return stats.getColumns.wrap(() -> getDelegate().getColumns(session, tableHandle));
    }

    @Override
    public Optional<ColumnMapping> toPrestoType(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        return stats.toPrestoType.wrap(() -> getDelegate().toPrestoType(session, connection, typeHandle));
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        return stats.toWriteMapping.wrap(() -> getDelegate().toWriteMapping(session, type));
    }

    @Override
    public ConnectorSplitSource getSplits(JdbcIdentity identity, JdbcTableHandle layoutHandle)
    {
        return stats.getSplits.wrap(() -> getDelegate().getSplits(identity, layoutHandle));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcSplit split)
            throws SQLException
    {
        return stats.getConnectionWithSplit.wrap(() -> getDelegate().getConnection(identity, split));
    }

    @Override
    public void abortReadConnection(Connection connection)
            throws SQLException
    {
        stats.abortReadConnection.wrap(() -> getDelegate().abortReadConnection(connection));
    }

    @Override
    public PreparedStatement buildSql(ConnectorSession session, Connection connection, JdbcSplit split, JdbcTableHandle tableHandle, List<JdbcColumnHandle> columnHandles)
            throws SQLException
    {
        return stats.buildSql.wrap(() -> getDelegate().buildSql(session, connection, split, tableHandle, columnHandles));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        stats.addColumn.wrap(() -> getDelegate().addColumn(session, handle, column));
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        stats.dropColumn.wrap(() -> getDelegate().dropColumn(identity, handle, column));
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        stats.renameColumn.wrap(() -> getDelegate().renameColumn(identity, handle, jdbcColumn, newColumnName));
    }

    @Override
    public void renameTable(JdbcIdentity identity, JdbcTableHandle handle, SchemaTableName newTableName)
    {
        stats.renameTable.wrap(() -> getDelegate().renameTable(identity, handle, newTableName));
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        stats.createTable.wrap(() -> getDelegate().createTable(session, tableMetadata));
    }

    @Override
    public JdbcOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return stats.beginCreateTable.wrap(() -> getDelegate().beginCreateTable(session, tableMetadata));
    }

    @Override
    public void commitCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.commitCreateTable.wrap(() -> getDelegate().commitCreateTable(identity, handle));
    }

    @Override
    public JdbcOutputTableHandle beginInsertTable(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        return stats.beginInsertTable.wrap(() -> getDelegate().beginInsertTable(session, tableMetadata));
    }

    @Override
    public void finishInsertTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.finishInsertTable.wrap(() -> getDelegate().finishInsertTable(identity, handle));
    }

    @Override
    public void dropTable(JdbcIdentity identity, JdbcTableHandle jdbcTableHandle)
    {
        stats.dropTable.wrap(() -> getDelegate().dropTable(identity, jdbcTableHandle));
    }

    @Override
    public void rollbackCreateTable(JdbcIdentity identity, JdbcOutputTableHandle handle)
    {
        stats.rollbackCreateTable.wrap(() -> getDelegate().rollbackCreateTable(identity, handle));
    }

    @Override
    public String buildInsertSql(JdbcOutputTableHandle handle)
    {
        return stats.buildInsertSql.wrap(() -> getDelegate().buildInsertSql(handle));
    }

    @Override
    public Connection getConnection(JdbcIdentity identity, JdbcOutputTableHandle handle)
            throws SQLException
    {
        return stats.getConnectionWithHandle.wrap(() -> getDelegate().getConnection(identity, handle));
    }

    @Override
    public PreparedStatement getPreparedStatement(Connection connection, String sql)
            throws SQLException
    {
        return stats.getPreparedStatement.wrap(() -> getDelegate().getPreparedStatement(connection, sql));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        return stats.getTableStatistics.wrap(() -> getDelegate().getTableStatistics(session, handle, tupleDomain));
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName)
    {
        stats.createSchema.wrap(() -> getDelegate().createSchema(session, schemaName));
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        stats.dropSchema.wrap(() -> getDelegate().dropSchema(session, schemaName));
    }

    @Override
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return stats.getDeleteRowIdColumnHandle.wrap(() -> getDelegate().getDeleteRowIdColumnHandle(session, tableHandle));
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return stats.applyDelete.wrap(() -> getDelegate().applyDelete(session, handle));
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return stats.executeDelete.wrap(() -> getDelegate().executeDelete(session, handle));
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return stats.executeUpdate.wrap(() -> getDelegate().executeUpdate(session, handle));
    }

    @Override
    public OptionalLong deleteTable(ConnectorSession session, ConnectorTableHandle handle)
    {
        return stats.deleteTable.wrap(() -> getDelegate().deleteTable(session, handle));
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return stats.beginDelete.wrap(() -> getDelegate().beginDelete(session, tableHandle));
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        stats.finishDelete.wrap(() -> getDelegate().finishDelete(session, tableHandle, fragments));
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        return stats.finishDelete.wrap(() -> getDelegate().beginUpdate(session, tableHandle, updatedColumnTypes));
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        stats.finishDelete.wrap(() -> getDelegate().finishUpdate(session, tableHandle, fragments));
    }

    @Override
    public String buildDeleteSql(ConnectorTableHandle handle)
    {
        return stats.buildDeleteSql.wrap(() -> getDelegate().buildDeleteSql(handle));
    }

    @Override
    public String buildUpdateSql(ConnectorSession session, ConnectorTableHandle handle, int updateColumnNum, List<String> updatedColumns)
    {
        return stats.buildUpdateSql.wrap(() -> getDelegate().buildUpdateSql(session, handle, updateColumnNum, updatedColumns));
    }

    @Override
    public void setDeleteSql(PreparedStatement statement, Block rowIds, int position)
    {
        stats.setDeleteSql.wrap(() -> getDelegate().setDeleteSql(statement, rowIds, position));
    }

    @Override
    public void setUpdateSql(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, List<Block> columnValueAndRowIdBlock, int position, List<String> updatedColumns)
    {
        stats.setUpdateSql.wrap(() -> getDelegate().setUpdateSql(session, tableHandle, statement, columnValueAndRowIdBlock, position, updatedColumns));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return stats.getUpdateRowIdColumnHandle.wrap(() -> getDelegate().getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns));
    }

    public static final class JdbcClientStats
    {
        private final JdbcApiStats schemaExists = new JdbcApiStats();
        private final JdbcApiStats getSchemaNames = new JdbcApiStats();
        private final JdbcApiStats getTableNames = new JdbcApiStats();
        private final JdbcApiStats getTableHandle = new JdbcApiStats();
        private final JdbcApiStats getColumns = new JdbcApiStats();
        private final JdbcApiStats toPrestoType = new JdbcApiStats();
        private final JdbcApiStats toWriteMapping = new JdbcApiStats();
        private final JdbcApiStats getSplits = new JdbcApiStats();
        private final JdbcApiStats getConnectionWithSplit = new JdbcApiStats();
        private final JdbcApiStats getConnectionWithHandle = new JdbcApiStats();
        private final JdbcApiStats abortReadConnection = new JdbcApiStats();
        private final JdbcApiStats buildSql = new JdbcApiStats();
        private final JdbcApiStats beginCreateTable = new JdbcApiStats();
        private final JdbcApiStats commitCreateTable = new JdbcApiStats();
        private final JdbcApiStats beginInsertTable = new JdbcApiStats();
        private final JdbcApiStats finishInsertTable = new JdbcApiStats();
        private final JdbcApiStats dropTable = new JdbcApiStats();
        private final JdbcApiStats rollbackCreateTable = new JdbcApiStats();
        private final JdbcApiStats buildInsertSql = new JdbcApiStats();
        private final JdbcApiStats getPreparedStatement = new JdbcApiStats();
        private final JdbcApiStats getTableStatistics = new JdbcApiStats();
        private final JdbcApiStats createSchema = new JdbcApiStats();
        private final JdbcApiStats dropSchema = new JdbcApiStats();
        private final JdbcApiStats addColumn = new JdbcApiStats();
        private final JdbcApiStats dropColumn = new JdbcApiStats();
        private final JdbcApiStats renameColumn = new JdbcApiStats();
        private final JdbcApiStats renameTable = new JdbcApiStats();
        private final JdbcApiStats createTable = new JdbcApiStats();
        public final JdbcApiStats deleteTable = new JdbcApiStats();
        public final JdbcApiStats getDeleteRowIdColumnHandle = new JdbcApiStats();
        public final JdbcApiStats applyDelete = new JdbcApiStats();
        public final JdbcApiStats executeDelete = new JdbcApiStats();
        public final JdbcApiStats beginDelete = new JdbcApiStats();
        public final JdbcApiStats finishDelete = new JdbcApiStats();
        public final JdbcApiStats buildDeleteSql = new JdbcApiStats();
        public final JdbcApiStats buildUpdateSql = new JdbcApiStats();
        public final JdbcApiStats setDeleteSql = new JdbcApiStats();
        public final JdbcApiStats setUpdateSql = new JdbcApiStats();
        public final JdbcApiStats getUpdateRowIdColumnHandle = new JdbcApiStats();
        public final JdbcApiStats executeUpdate = new JdbcApiStats();

        @Managed
        @Nested
        public JdbcApiStats getSchemaExists()
        {
            return schemaExists;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetSchemaNames()
        {
            return getSchemaNames;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetTableNames()
        {
            return getTableNames;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetTableHandle()
        {
            return getTableHandle;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetColumns()
        {
            return getColumns;
        }

        @Managed
        @Nested
        public JdbcApiStats getToPrestoType()
        {
            return toPrestoType;
        }

        @Managed
        @Nested
        public JdbcApiStats getToWriteMapping()
        {
            return toWriteMapping;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetSplits()
        {
            return getSplits;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetConnectionWithSplit()
        {
            return getConnectionWithSplit;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetConnectionWithHandle()
        {
            return getConnectionWithHandle;
        }

        @Managed
        @Nested
        public JdbcApiStats getAbortReadConnection()
        {
            return abortReadConnection;
        }

        @Managed
        @Nested
        public JdbcApiStats getBuildSql()
        {
            return buildSql;
        }

        @Managed
        @Nested
        public JdbcApiStats getBeginCreateTable()
        {
            return beginCreateTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getCommitCreateTable()
        {
            return commitCreateTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getBeginInsertTable()
        {
            return beginInsertTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getFinishInsertTable()
        {
            return finishInsertTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getDropTable()
        {
            return dropTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getRollbackCreateTable()
        {
            return rollbackCreateTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getBuildInsertSql()
        {
            return buildInsertSql;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetPreparedStatement()
        {
            return getPreparedStatement;
        }

        @Managed
        @Nested
        public JdbcApiStats getGetTableStatistics()
        {
            return getTableStatistics;
        }

        @Managed
        @Nested
        public JdbcApiStats getCreateSchema()
        {
            return createSchema;
        }

        @Managed
        @Nested
        public JdbcApiStats getDropSchema()
        {
            return dropSchema;
        }

        @Managed
        @Nested
        public JdbcApiStats getAddColumn()
        {
            return addColumn;
        }

        @Managed
        @Nested
        public JdbcApiStats getDropColumn()
        {
            return dropColumn;
        }

        @Managed
        @Nested
        public JdbcApiStats getRenameColumn()
        {
            return renameColumn;
        }

        @Managed
        @Nested
        public JdbcApiStats getRenameTable()
        {
            return renameTable;
        }

        @Managed
        @Nested
        public JdbcApiStats getCreateTable()
        {
            return createTable;
        }
    }
}
