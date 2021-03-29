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

package io.hetu.core.materializedview.metadata;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.hetu.core.materializedview.connector.MaterializedViewColumnHandle;
import io.hetu.core.materializedview.connector.MaterializedViewHandle;
import io.hetu.core.materializedview.utils.MaterializedViewDateUtils;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.type.TypeManager;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * MvConnector metadata
 *
 * @since 2020-03-28
 */
public class MaterializedViewConnectorMetadata
        implements ConnectorMetadata
{
    private MaterializedViewMetastore metastore;
    private TypeManager typeManager;

    /**
     * Constructor of connector metadata
     */
    @Inject
    public MaterializedViewConnectorMetadata(MaterializedViewMetastoreFactory metastoreFactory, TypeManager typeManager, HetuMetastore hetuMetastore)
    {
        this.metastore = metastoreFactory.create(hetuMetastore);
        metastore.init();
        this.typeManager = typeManager;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return new ArrayList<>(metastore.getSchemas());
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        metastore.addSchema(schemaName);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        metastore.dropSchema(schemaName);
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession connectorSession, SchemaTableName schemaTableName)
    {
        MaterializedView materializedView = metastore.getMaterializedView(schemaTableName);
        return materializedView == null ? null : new MaterializedViewHandle(schemaTableName.getSchemaName(), materializedView.getViewName());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(
            ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle)
    {
        MaterializedViewHandle handle = (MaterializedViewHandle) connectorTableHandle;

        MaterializedView mv = metastore.getMaterializedView(handle.toSchemaTableName());

        return mv == null ? null : mv.getColumns().stream().collect(Collectors.toMap(x -> x.getColumnName(), x -> x));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession connectorSession, ConnectorTableHandle connectorTableHandle, ColumnHandle columnHandle)
    {
        ColumnMetadata columnMetadata = null;

        if (columnHandle instanceof MaterializedViewColumnHandle) {
            MaterializedViewColumnHandle mvColumnHandle = (MaterializedViewColumnHandle) columnHandle;
            columnMetadata = new ColumnMetadata(mvColumnHandle.getColumnName(), typeManager.getType(mvColumnHandle.getColumnTypeSignature()));
        }

        return Optional.ofNullable(columnMetadata).orElse(columnMetadata);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        MaterializedViewHandle handle = (MaterializedViewHandle) table;
        SchemaTableName tableName = new SchemaTableName(handle.getSchema(), handle.getView());
        MaterializedView mv = metastore.getMaterializedView(tableName);

        List<ColumnMetadata> columns = new ArrayList<>(mv.getColumns().size());
        for (MaterializedViewColumnHandle columnHandle : mv.getColumns()) {
            columns.add(new ColumnMetadata(columnHandle.getColumnName(), typeManager.getType(columnHandle.getColumnTypeSignature())));
        }
        return mv == null ? null : new ConnectorTableMetadata(tableName, columns);
    }

    @Override
    public void dropMaterializedView(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        MaterializedViewHandle handle = (MaterializedViewHandle) tableHandle;
        metastore.dropMaterializedView(handle.getSchema(), handle.getView());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> filterSchema)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        List<String> schemaList = filterSchema.isPresent() ? ImmutableList.of(filterSchema.get()) : listSchemaNames(session);
        for (String schemaName : schemaList) {
            for (String viewName : metastore.getAllMaterializedView(schemaName)) {
                tableNames.add(new SchemaTableName(schemaName, viewName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        // support presto view and hive view
        MaterializedView materializedView = metastore.getMaterializedView(viewName);
        if (materializedView == null) {
            return Optional.empty();
        }
        // get column type from view
        List<ConnectorViewDefinition.ViewColumn> viewColumns = new ArrayList<>();
        for (MaterializedViewColumnHandle columnHandle : materializedView.getColumns()) {
            ConnectorViewDefinition.ViewColumn vc = new ConnectorViewDefinition.ViewColumn(columnHandle.getColumnName(),
                    columnHandle.getColumnTypeSignature());
            viewColumns.add(vc);
        }

        return Optional.of(new ConnectorMaterializedViewDefinition(
                materializedView.getViewSql(),
                Optional.ofNullable(materializedView.getDataCatalog()),
                Optional.ofNullable(materializedView.getDataSchema()),
                viewColumns,
                Optional.ofNullable(materializedView.getOwner()),
                materializedView.isRunAsInvoker(),
                materializedView.getFullSql(),
                materializedView.getMetaCatalog(),
                materializedView.getCurrentCatalog(),
                materializedView.getCurrentSchema(),
                materializedView.getStatus()));
    }

    @Override
    public ConnectorOutputTableHandle beginCreateMaterializedView(
            ConnectorTableMetadata tableMetadata,
            ConnectorMaterializedViewDefinition definition)
    {
        List<MaterializedViewColumnHandle> columns = new ArrayList<>();
        long columnId = 0L;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            columns.add(new MaterializedViewColumnHandle(column.getName(), columnId++, column.getType().getTypeSignature()));
        }

        metastore.addMaterializedView(tableMetadata.getTable().getSchemaName(),
                new MaterializedView(
                        tableMetadata.getTable().getTableName(),
                        definition.getOriginalSql(),
                        definition.getFullSql(),
                        definition.getCatalog().orElse(null),
                        definition.getSchema().orElse(null),
                        definition.getMetaCatalog(),
                        definition.getCurrentCatalog(),
                        definition.getCurrentSchema(),
                        columns,
                        definition.getOwner().orElse(null),
                        definition.isRunAsInvoker(),
                        MaterializedViewDateUtils.getStringFromDate(new Date()),
                        MaterializedViewStatus.getStatusString(MaterializedViewStatus.DISABLE)));
        return null;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateMaterializedView(SchemaTableName name)
    {
        metastore.setViewStatus(name, MaterializedViewStatus.ENABLE);
        return Optional.empty();
    }
}
