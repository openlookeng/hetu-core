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

import io.hetu.core.materializedview.connector.MaterializedViewColumnHandle;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.SchemaAlreadyExistsException;
import io.prestosql.spi.connector.SchemaNotFoundException;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableAlreadyExistsException;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.metastore.model.CatalogEntity;
import io.prestosql.spi.metastore.model.ColumnEntity;
import io.prestosql.spi.metastore.model.DatabaseEntity;
import io.prestosql.spi.metastore.model.TableEntity;
import io.prestosql.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.hetu.core.materializedview.utils.MaterializedViewConstants.CATALOG_NAME;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.COLUMN_ID;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.CURRENT_CATALOG;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.CURRENT_SCHEMA;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.DATA_CATALOG;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.FULL_SQL;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.LAST_REFRESH_TIME;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.RUN_AS_INVOKER;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.STATUS;
import static io.hetu.core.materializedview.utils.MaterializedViewStatus.getStatusString;

/**
 * use HetuMetastore
 *
 * @since 2020-03-26
 */
public class HetuMaterializedViewMetastore
        implements MaterializedViewMetastore
{
    private static final String TABLE_TYPE = "materialized_view";
    private final HetuMetastore hetuMetastore;
    private final Map<String, Map<String, MaterializedView>> views;

    public HetuMaterializedViewMetastore(HetuMetastore hetuMetastore)
    {
        this.hetuMetastore = hetuMetastore;
        this.views = new HashMap<>();
    }

    @Override
    public void init()
    {
        views.clear();
        if (!hetuMetastore.getCatalog(CATALOG_NAME).isPresent()) {
            CatalogEntity catalogEntity = new CatalogEntity();
            catalogEntity.setName(CATALOG_NAME);
            catalogEntity.setCreateTime(new Date().getTime());
            hetuMetastore.createCatalog(catalogEntity);
        }
        else {
            List<DatabaseEntity> databaseEntities = hetuMetastore.getAllDatabases(CATALOG_NAME);
            for (DatabaseEntity databaseEntity : databaseEntities) {
                String schema = databaseEntity.getName();
                Map<String, MaterializedView> viewMap = new HashMap<>();
                List<TableEntity> tableEntities = hetuMetastore.getAllTables(CATALOG_NAME, schema);
                for (TableEntity tableEntity : tableEntities) {
                    viewMap.put(tableEntity.getName(), tableEntityToMaterializedView(tableEntity));
                }
                views.put(schema, viewMap);
            }
        }
    }

    @Override
    public void addMaterializedView(String schema, MaterializedView materializedView)
    {
        synchronized (views) {
            if (!views.containsKey(schema)) {
                throw new SchemaNotFoundException(schema);
            }

            Map<String, MaterializedView> viewMap = views.get(schema);
            String view = materializedView.getViewName();
            if (viewMap.containsKey(view)) {
                throw new TableAlreadyExistsException(new SchemaTableName(schema, view));
            }

            TableEntity tableEntity = materializedViewToTableEntity(materializedView);
            hetuMetastore.createTable(tableEntity);
            viewMap.put(view, materializedView);
        }
    }

    private TableEntity materializedViewToTableEntity(MaterializedView materializedView)
    {
        TableEntity tableEntity = new TableEntity();
        tableEntity.setCatalogName(materializedView.getMetaCatalog());
        tableEntity.setDatabaseName(materializedView.getDataSchema());
        tableEntity.setName(materializedView.getViewName());
        tableEntity.setOwner(materializedView.getOwner());
        tableEntity.setType(TABLE_TYPE);
        tableEntity.setViewOriginalText(materializedView.getViewSql());
        tableEntity.setCreateTime(new Date().getTime());

        List<ColumnEntity> columns = tableEntity.getColumns();
        for (MaterializedViewColumnHandle columnHandle : materializedView.getColumns()) {
            ColumnEntity columnEntity = new ColumnEntity();
            columnEntity.setName(columnHandle.getColumnName());
            columnEntity.setType(columnHandle.getColumnTypeSignature().toString());
            Map<String, String> columnParameters = columnEntity.getParameters();
            columnParameters.put(COLUMN_ID, String.valueOf(columnHandle.getColumnId()));

            columns.add(columnEntity);
        }
        tableEntity.setColumns(columns);

        Map<String, String> tableParameters = tableEntity.getParameters();
        tableParameters.put(FULL_SQL, materializedView.getFullSql());
        tableParameters.put(DATA_CATALOG, materializedView.getDataCatalog());
        tableParameters.put(CURRENT_CATALOG, materializedView.getCurrentCatalog());
        tableParameters.put(CURRENT_SCHEMA, materializedView.getCurrentSchema());
        tableParameters.put(RUN_AS_INVOKER, String.valueOf(materializedView.isRunAsInvoker()));
        tableParameters.put(LAST_REFRESH_TIME, materializedView.getLastRefreshTime());
        tableParameters.put(STATUS, materializedView.getStatus());

        return tableEntity;
    }

    @Override
    public void dropMaterializedView(String schema, String view)
    {
        synchronized (views) {
            if (!views.containsKey(schema)) {
                throw new SchemaNotFoundException(schema);
            }

            Map<String, MaterializedView> viewMap = views.get(schema);
            if (!viewMap.containsKey(view)) {
                throw new TableNotFoundException(new SchemaTableName(schema, view));
            }

            hetuMetastore.dropTable(CATALOG_NAME, schema, view);
            viewMap.remove(view);
        }
    }

    @Override
    public void addSchema(String schemaName)
    {
        DatabaseEntity databaseEntity = new DatabaseEntity();
        databaseEntity.setCatalogName(CATALOG_NAME);
        databaseEntity.setName(schemaName);
        databaseEntity.setCreateTime(new Date().getTime());

        synchronized (views) {
            if (views.containsKey(schemaName)) {
                throw new SchemaAlreadyExistsException(schemaName);
            }

            hetuMetastore.createDatabase(databaseEntity);
            views.put(schemaName, new HashMap<>());
        }
    }

    @Override
    public void dropSchema(String schemaName)
    {
        synchronized (views) {
            if (!views.containsKey(schemaName)) {
                throw new SchemaNotFoundException(schemaName);
            }
            hetuMetastore.dropDatabase(CATALOG_NAME, schemaName);
            views.remove(schemaName);
        }
    }

    @Override
    public void setViewStatus(SchemaTableName viewName, MaterializedViewStatus status)
    {
        String schema = viewName.getSchemaName();
        String view = viewName.getTableName();

        synchronized (views) {
            if (!views.containsKey(schema)) {
                throw new SchemaNotFoundException(schema);
            }

            Map<String, MaterializedView> viewMap = views.get(schema);
            if (!viewMap.containsKey(view)) {
                throw new TableNotFoundException(viewName);
            }

            Optional<TableEntity> tableEntity = hetuMetastore.getTable(CATALOG_NAME, schema, view);
            if (!tableEntity.isPresent()) {
                throw new TableNotFoundException(viewName);
            }

            Map<String, String> tableParameters = tableEntity.get().getParameters();
            String statusStr = getStatusString(status);
            if (!tableParameters.get(STATUS).equals(statusStr)) {
                tableParameters.put(STATUS, statusStr);
                hetuMetastore.alterTable(CATALOG_NAME, schema, view, tableEntity.get());
                viewMap.get(view).setStatus(status);
            }
        }
    }

    @Override
    public Set<String> getSchemas()
    {
        synchronized (views) {
            return views.keySet();
        }
    }

    @Override
    public List<String> getAllMaterializedView(String schemaName)
    {
        synchronized (views) {
            if (!views.containsKey(schemaName)) {
                return new ArrayList<>();
            }

            Map<String, MaterializedView> viewMap = views.get(schemaName);
            return new ArrayList<>(viewMap.keySet());
        }
    }

    @Override
    public MaterializedView getMaterializedView(SchemaTableName viewName)
    {
        String schema = viewName.getSchemaName();
        String view = viewName.getTableName();
        synchronized (views) {
            if (!views.containsKey(schema)) {
                return null;
            }

            Map<String, MaterializedView> viewMap = views.get(schema);
            if (!viewMap.containsKey(view)) {
                return null;
            }

            return viewMap.get(view);
        }
    }

    private MaterializedView tableEntityToMaterializedView(TableEntity tableEntity)
    {
        List<MaterializedViewColumnHandle> columns = new ArrayList<>(tableEntity.getColumns().size());
        for (ColumnEntity columnEntity : tableEntity.getColumns()) {
            String columnName = columnEntity.getName();
            long columnId = Long.parseLong(columnEntity.getParameters().get(COLUMN_ID));
            TypeSignature columnTypeSignature = TypeSignature.parseTypeSignature(columnEntity.getType());
            columns.add(new MaterializedViewColumnHandle(columnName, columnId, columnTypeSignature));
        }

        Map<String, String> tableParameters = tableEntity.getParameters();

        return new MaterializedView(
                tableEntity.getName(),
                tableEntity.getViewOriginalText(),
                tableParameters.get(FULL_SQL),
                tableParameters.get(DATA_CATALOG),
                tableEntity.getDatabaseName(),
                tableEntity.getCatalogName(),
                tableParameters.get(CURRENT_CATALOG),
                tableParameters.get(CURRENT_SCHEMA),
                columns,
                tableEntity.getOwner(),
                Boolean.parseBoolean(tableParameters.get(RUN_AS_INVOKER)),
                tableParameters.get(LAST_REFRESH_TIME),
                tableParameters.get(STATUS));
    }
}
