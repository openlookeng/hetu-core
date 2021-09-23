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

package io.hetu.core.plugin.datacenter;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.hetu.core.plugin.datacenter.client.DataCenterClient;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.statistics.TableStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static java.util.Objects.requireNonNull;

/**
 * Data center metadata, use {@link DataCenterClient} to get the metadata.
 *
 * @since 2020-02-11
 */
public class DataCenterMetadata
        implements ConnectorMetadata
{
    private static final int CATALOG_SPLIT_SIZE = 2;

    private static final int CATALOG_POSITION = 1;

    private final DataCenterClient dataCenterClient;

    private final boolean isQueryPushDownEnabled;

    /**
     * This the maximum remote header size defined in DataCenter config file
     */
    private final long maxRemoteHeaderSize;

    /**
     * Constructor of data center metadata.
     *
     * @param dataCenterClient data center client.
     * @param dataCenterConfig data center config.
     */
    public DataCenterMetadata(DataCenterClient dataCenterClient, DataCenterConfig dataCenterConfig)
    {
        this.dataCenterClient = requireNonNull(dataCenterClient, "client is null");
        this.isQueryPushDownEnabled = dataCenterConfig.isQueryPushDownEnabled();
        this.maxRemoteHeaderSize = dataCenterConfig.getRemoteHttpServerMaxRequestHeaderSize().toBytes();
    }

    /**
     * Extract remote catalog, like if the remote catalog is "dc.tpcds", it will remove "dc." and return "tpcds"
     *
     * @param session connector session.
     * @return the catalog name.
     */
    private static String extractRemoteCatalog(ConnectorSession session)
    {
        if (!session.getCatalog().isPresent()) {
            throw new PrestoException(NOT_FOUND, "catalog is not found in session");
        }
        String catalogName = session.getCatalog().get();
        String[] subCatalogs = catalogName.split("\\.");
        if (subCatalogs.length == CATALOG_SPLIT_SIZE) {
            return subCatalogs[CATALOG_POSITION];
        }
        else {
            return catalogName;
        }
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(dataCenterClient.getSchemaNames(extractRemoteCatalog(session)));
    }

    @Override
    public DataCenterTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String catalog = extractRemoteCatalog(session);
        // Validate if table exists
        try {
            Set<String> tables = dataCenterClient.getTableNames(catalog, tableName.getSchemaName());
            if (!tables.contains(tableName.getTableName())) {
                return null;
            }
            return new DataCenterTableHandle(catalog, tableName.getSchemaName(), tableName.getTableName(),
                    OptionalLong.empty());
        }
        catch (SchemaNotFoundException ex) {
            return null;
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName schemaTableName = ((DataCenterTableHandle) tableHandle).toSchemaTableName();

        DataCenterTable table = dataCenterClient.getTable(extractRemoteCatalog(session),
                schemaTableName.getSchemaName(), schemaTableName.getTableName());
        if (table == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return new ConnectorTableMetadata(schemaTableName, table.getColumnsMetadata());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(dataCenterClient.getSchemaNames(extractRemoteCatalog(session))));

        String catalogName = extractRemoteCatalog(session);
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String schemaName : schemaNames) {
            for (String tableName : dataCenterClient.getTableNames(catalogName, schemaName)) {
                builder.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return builder.build();
    }

    /**
     * List the tables of this schema.
     *
     * @param session connection session.
     * @param prefix schema table prefix.
     * @return the list of schema table name.
     */
    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (!prefix.getTable().isPresent()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        DataCenterTableHandle dataCenterTableHandle = (DataCenterTableHandle) tableHandle;

        DataCenterTable table = dataCenterClient.getTable(extractRemoteCatalog(session),
                dataCenterTableHandle.getSchemaName(), dataCenterTableHandle.getTableName());
        if (table == null) {
            throw new TableNotFoundException(dataCenterTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : table.getColumnsMetadata()) {
            columnHandles.put(column.getName(), new DataCenterColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.build();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session,
            SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        String catalog = extractRemoteCatalog(session);

        for (SchemaTableName tableName : listTables(session, prefix)) {
            DataCenterTable table = dataCenterClient.getTable(catalog, tableName.getSchemaName(),
                    tableName.getTableName());
            // table can disappear during listing operation
            if (table != null) {
                columns.put(tableName, table.getColumnsMetadata());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((DataCenterColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session,
            ConnectorTableHandle table, long limit)
    {
        DataCenterTableHandle handle = (DataCenterTableHandle) table;

        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }

        handle = new DataCenterTableHandle(handle.getCatalogName(), handle.getSchemaName(), handle.getTableName(),
                OptionalLong.of(limit));

        return Optional.of(new LimitApplicationResult<>(handle, true));
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint, boolean includeColumnStatistics)
    {
        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, tableHandle);
        String tableFullName = tableHandle.getSchemaPrefixedTableName();
        return dataCenterClient.getTableStatistics(tableFullName, columnHandles);
    }

    @Override
    public boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return true;
    }
}
