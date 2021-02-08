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
package io.hetu.core.plugin.hbase.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.statistics.ComputedStatistics;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * HBaseConnectorMetadata
 *
 * @since 2020-03-30
 */
public class HBaseConnectorMetadata
        implements ConnectorMetadata
{
    private static final Logger LOG = Logger.get(HBaseConnectorMetadata.class);

    private HBaseConnection hbaseConn;
    private final AtomicReference<Runnable> rollbackAction = new AtomicReference<>();

    /**
     * constructor
     *
     * @param hbaseConn hbaseConn
     */
    @Inject
    public HBaseConnectorMetadata(HBaseConnection hbaseConn)
    {
        this.hbaseConn = hbaseConn;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return hbaseConn.listSchemaNames();
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        ConnectorTableHandle connectorTableHandle = null;
        // namespace not exist
        if (!listSchemaNames(connectorSession).contains(schemaTableName.getSchemaName().toLowerCase(Locale.ENGLISH))) {
            return Optional.ofNullable(connectorTableHandle).orElse(connectorTableHandle);
        }

        HBaseTable table = hbaseConn.getTable(schemaTableName);
        if (table == null) {
            return Optional.ofNullable(connectorTableHandle).orElse(connectorTableHandle);
        }

        return new HBaseTableHandle(
                table.getSchema(),
                table.getTable(),
                table.getRowId(),
                table.isExternal(),
                table.getSerializerClassName(),
                table.getHbaseTableName(),
                table.getIndexColumns(),
                OptionalLong.empty());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        hbaseConn.createSchema(schemaName, properties);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        hbaseConn.dropSchema(schemaName);
    }

    /**
     * addColumn
     * ALTER TABLE name ADD COLUMN column_name data_type WITH ( family = 'f', qualifier = 'q')
     *
     * @param session session
     * @param tableHandle tableHandle
     * @param column column
     */
    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HBaseTableHandle handle = (HBaseTableHandle) tableHandle;
        hbaseConn.addColumn(handle, column);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession connectorSession, ConnectorTableHandle table)
    {
        HBaseTableHandle handle = (HBaseTableHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.getSchema(), handle.getTable());
        ConnectorTableMetadata metadata = getTableMetadata(tableName);

        if (metadata == null) {
            throw new TableNotFoundException(tableName);
        }
        return metadata;
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        ConnectorTableMetadata tableMetadata = null;
        if (!hbaseConn.listSchemaNames().contains(tableName.getSchemaName())) {
            return Optional.ofNullable(tableMetadata).orElse(tableMetadata);
        }

        // Need to validate that SchemaTableName is a table
        HBaseTable table = hbaseConn.getTable(tableName);
        if (table == null) {
            return Optional.ofNullable(tableMetadata).orElse(tableMetadata);
        }

        return new ConnectorTableMetadata(tableName, table.getColumnMetadatas(), table.getTableProperties());
    }

    @Override
    public ConnectorOutputTableHandle beginCreateTable(
            ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        checkNoRollback();
        SchemaTableName tableName = tableMetadata.getTable();
        HBaseTable table = hbaseConn.createTable(tableMetadata);
        // support create table xxx as select * from yyy
        HBaseTableHandle handle =
                new HBaseTableHandle(
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        table.getRowId(),
                        table.isExternal(),
                        table.getSerializerClassName(),
                        table.getHbaseTableName(),
                        table.getIndexColumns(),
                        TupleDomain.all(),
                        table.getColumns(),
                        table.getRowIdOrdinal(),
                        OptionalLong.empty());
        setRollback(() -> rollbackCreateTable(table));

        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    @Override
    public ConnectorInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        checkNoRollback();

        HBaseTableHandle tmpHandle = (HBaseTableHandle) tableHandle;
        HBaseTable table = hbaseConn.getTable(tmpHandle.getFullTableName());
        HBaseTableHandle handle =
                new HBaseTableHandle(
                        table.getSchema(),
                        table.getTable(),
                        table.getRowIdOrdinal(),
                        table.getColumns(),
                        table.getSerializerClassName(),
                        table.getHbaseTableName(),
                        OptionalLong.empty());

        return handle;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(
            ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics)
    {
        clearRollback();
        return Optional.empty();
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        hbaseConn.createTable(tableMetadata);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HBaseTable table = hbaseConn.getTable(((HBaseTableHandle) tableHandle).toSchemaTableName());
        if (table != null) {
            hbaseConn.dropTable(table);
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HBaseTableHandle handle = (HBaseTableHandle) tableHandle;
        hbaseConn.renameTable(handle.toSchemaTableName(), newTableName);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        HBaseTableHandle handle = (HBaseTableHandle) connectorTableHandle;

        HBaseTable table = hbaseConn.getTable(handle.toSchemaTableName());
        if (table == null) {
            throw new TableNotFoundException(handle.toSchemaTableName());
        }

        return table.getColumnsMap();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle, ColumnHandle columnHandle)
    {
        ColumnMetadata columnMetadata = null;

        if (columnHandle instanceof HBaseColumnHandle) {
            columnMetadata = ((HBaseColumnHandle) columnHandle).getColumnMetadata();
            return columnMetadata;
        }
        else {
            return Optional.ofNullable(columnMetadata).orElse(columnMetadata);
        }
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(
            ConnectorSession connectorSession, SchemaTablePrefix schemaTablePrefix)
    {
        requireNonNull(schemaTablePrefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(connectorSession, schemaTablePrefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    /**
     * listTables: read from metastore, and not visit hbase
     *
     * @param session session
     * @param filterSchema filterSchema
     * @return List
     */
    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        Set<String> schemaNames = filterSchema.<Set<String>>map(ImmutableSet::of).orElseGet(hbaseConn::getSchemaNames);

        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        Map<String, List<SchemaTableName>> tables = hbaseConn.getAllTablesFromMetadata();
        for (String schemaName : schemaNames) {
            if (tables.get(schemaName) != null) {
                builder.addAll(tables.get(schemaName));
            }
        }
        return builder.build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.isEmpty()) {
            return listTables(session, prefix.getSchema());
        }
        // Make sure requested table exists, returning the single table of it does
        SchemaTableName table = prefix.toSchemaTableName();
        if (getTableHandle(session, table) != null) {
            return ImmutableList.of(table);
        }

        return ImmutableList.of();
    }

    private void checkNoRollback()
    {
        checkState(rollbackAction.get() == null, "Cannot begin a new write while in an existing one");
    }

    private void clearRollback()
    {
        rollbackAction.set(null);
    }

    /**
     * transaction rollback
     */
    public void rollback()
    {
        Runnable rollbackAction0 = this.rollbackAction.getAndSet(null);
        if (rollbackAction0 != null) {
            rollbackAction0.run();
        }
    }

    private void setRollback(Runnable action)
    {
        checkState(rollbackAction.compareAndSet(null, action), "Should not have to override existing rollback action");
    }

    private void rollbackCreateTable(HBaseTable table)
    {
        hbaseConn.dropTable(table);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session, ConnectorTableHandle connectorTableHandle, Constraint constraint)
    {
        HBaseTableHandle tableHandle;

        if (connectorTableHandle instanceof HBaseTableHandle) {
            tableHandle = (HBaseTableHandle) connectorTableHandle;
        }
        else {
            return Optional.empty();
        }
        TupleDomain<ColumnHandle> oldDomain = tableHandle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        HBaseTableHandle newTableHandle =
                new HBaseTableHandle(
                        tableHandle.getSchema(),
                        tableHandle.getTable(),
                        tableHandle.getRowId(),
                        tableHandle.isExternal(),
                        tableHandle.getSerializerClassName(),
                        tableHandle.getHbaseTableName(),
                        tableHandle.getFullTableName(),
                        newDomain,
                        tableHandle.getColumns(),
                        tableHandle.getRowIdOrdinal(),
                        tableHandle.getLimit());
        return Optional.of(new ConstraintApplicationResult<>(newTableHandle, constraint.getSummary()));
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session, ConnectorTableHandle connectorTableHandle, long limit)
    {
        HBaseTableHandle tableHandle;

        if (connectorTableHandle instanceof HBaseTableHandle) {
            tableHandle = (HBaseTableHandle) connectorTableHandle;
        }
        else {
            return Optional.empty();
        }

        if (tableHandle.getLimit().isPresent() && tableHandle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        HBaseTableHandle newTableHandle =
                new HBaseTableHandle(
                        tableHandle.getSchema(),
                        tableHandle.getTable(),
                        tableHandle.getRowId(),
                        tableHandle.isExternal(),
                        tableHandle.getSerializerClassName(),
                        tableHandle.getHbaseTableName(),
                        tableHandle.getFullTableName(),
                        tableHandle.getConstraint(),
                        tableHandle.getColumns(),
                        tableHandle.getRowIdOrdinal(),
                        OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(newTableHandle, false));
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HBaseTableHandle hBaseTableHandle = (HBaseTableHandle) tableHandle;
        HBaseTable table = hbaseConn.getTable(hBaseTableHandle.getFullTableName());
        HBaseColumnHandle handle = table.getColumns().get(table.getRowIdOrdinal());
        return new HBaseColumnHandle(
                handle.getName(),
                handle.getFamily(),
                handle.getQualifier(),
                handle.getType(),
                handle.getOrdinal(),
                handle.getComment(),
                true);
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        if (!(tableHandle instanceof HBaseTableHandle)) {
            throw new IllegalArgumentException("tableHandle must be type of HBaseTableHandle");
        }
        return tableHandle;
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle deleteHandle)
    {
        if (deleteHandle instanceof HBaseTableHandle) {
            HBaseTableHandle hBaseTableHandle = (HBaseTableHandle) deleteHandle;
            HBaseTable table = hbaseConn.getTable(hBaseTableHandle.getFullTableName());
            if (table == null) {
                throw new TableNotFoundException(hBaseTableHandle.toSchemaTableName());
            }
            try {
                TableName tableName = TableName.valueOf(table.getHbaseTableName().get());
                hbaseConn.getHbaseAdmin().disableTable(tableName);
                hbaseConn.getHbaseAdmin().truncateTable(tableName, false);
            }
            catch (IOException e) {
                LOG.error("HBase delete table failed... cause by %s", e);
            }
        }

        return OptionalLong.empty();
    }

    @Override
    public void finishDelete(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<Slice> fragments) {}

    @Override
    public boolean supportsMetadataDelete(
            ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorTableLayoutHandle tableLayoutHandle)
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    /**
     * Hetu can only cache execution plans for supported connectors.
     * This method overrides {@link ConnectorMetadata} returns true to indicate
     * execution plan caching is enabled for HBase connectors.
     *
     * @param session Hetu session
     * @param handle Connector specific table handle
     */
    @Override
    public boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return true;
    }
}
