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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.type.Type;

import java.sql.PreparedStatement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

public class TransactionScopeCachingJdbcClient
        extends ForwardingJdbcClient
{
    private final Map<JdbcTableHandle, List<JdbcColumnHandle>> getColumnsCache = new ConcurrentHashMap<>();

    private final JdbcClient delegate;

    public TransactionScopeCachingJdbcClient(JdbcClient delegate)
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

    @Override
    public List<JdbcColumnHandle> getColumns(ConnectorSession session, JdbcTableHandle tableHandle)
    {
        return getColumnsCache.computeIfAbsent(tableHandle, ignored -> super.getColumns(session, tableHandle));
    }

    @Override
    public void addColumn(ConnectorSession session, JdbcTableHandle handle, ColumnMetadata column)
    {
        getColumnsCache.remove(handle);
        super.addColumn(session, handle, column);
    }

    @Override
    public void renameColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle jdbcColumn, String newColumnName)
    {
        getColumnsCache.remove(handle);
        super.renameColumn(identity, handle, jdbcColumn, newColumnName);
    }

    @Override
    public void dropColumn(JdbcIdentity identity, JdbcTableHandle handle, JdbcColumnHandle column)
    {
        getColumnsCache.remove(handle);
        super.dropColumn(identity, handle, column);
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
    public ColumnHandle getDeleteRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return super.getDeleteRowIdColumnHandle(session, tableHandle);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return super.applyDelete(session, handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return super.executeDelete(session, handle);
    }

    @Override
    public OptionalLong executeUpdate(ConnectorSession session, ConnectorTableHandle handle)
    {
        return super.executeUpdate(session, handle);
    }

    @Override
    public OptionalLong deleteTable(ConnectorSession session, ConnectorTableHandle handle)
    {
        return super.deleteTable(session, handle);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return super.beginDelete(session, tableHandle);
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        super.finishDelete(session, tableHandle, fragments);
    }

    @Override
    public ConnectorTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, List<Type> updatedColumnTypes)
    {
        return super.beginUpdate(session, tableHandle, updatedColumnTypes);
    }

    @Override
    public void finishUpdate(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments)
    {
        super.finishUpdate(session, tableHandle, fragments);
    }

    @Override
    public String buildDeleteSql(ConnectorTableHandle handle)
    {
        return super.buildDeleteSql(handle);
    }

    @Override
    public String buildUpdateSql(ConnectorSession session, ConnectorTableHandle handle, int updateColumnNum, List<String> updatedColumns)
    {
        return super.buildUpdateSql(session, handle, updateColumnNum, updatedColumns);
    }

    @Override
    public void setDeleteSql(PreparedStatement statement, Block rowIds, int position)
    {
        super.setDeleteSql(statement, rowIds, position);
    }

    @Override
    public void setUpdateSql(ConnectorSession session, ConnectorTableHandle tableHandle, PreparedStatement statement, List<Block> columnValueAndRowIdBlock, int position, List<String> updatedColumns)
    {
        super.setUpdateSql(session, tableHandle, statement, columnValueAndRowIdBlock, position, updatedColumns);
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle, List<ColumnHandle> updatedColumns)
    {
        return super.getUpdateRowIdColumnHandle(session, tableHandle, updatedColumns);
    }
}
