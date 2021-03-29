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
package io.hetu.core.materializedview;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.hetu.core.materializedview.conf.MaterializedViewConfig;
import io.hetu.core.materializedview.metadata.MaterializedView;
import io.hetu.core.materializedview.metadata.MaterializedViewConnectorMetadata;
import io.hetu.core.materializedview.metadata.MaterializedViewConnectorMetadataFactory;
import io.hetu.core.materializedview.metadata.MaterializedViewMetastore;
import io.hetu.core.materializedview.metadata.MaterializedViewMetastoreFactory;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.ConnectorViewDefinition.ViewColumn;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.metastore.HetuMetastore;
import io.prestosql.spi.type.testing.TestingTypeManager;
import io.prestosql.testing.TestingConnectorSession;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.hetu.core.materializedview.utils.MaterializedViewConstants.TYPE_LOCAL;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestMvConnectorMetadata
{
    private static final String SCHEMA_NAME1 = "test1";
    private static final String SCHEMA_NAME2 = "test2";
    private static final String TABLE_NAME = "table";
    private static final String COLUMN_NAME = "column";
    private static final String CATALOG_NAME = "memory";
    private static final String SCHEMA_NAME = "demo";
    private static final String META_CATALOG_NAME = "mv";
    private static final String CURRENT_CATALOG_NAME = "hive";
    private static final String CURRENT_SCHEMA_NAME = "default";
    private static final String EXECUTE_SQL = "select column from test1.table";
    private static final String FULL_SQL = "create materialized view mv.demo.view as select column from test1.table";
    private static final String STATUS = "ENABLE";
    private static final String OWNER = "root";
    private MaterializedViewConnectorMetadata metadata;
    private MockMaterializedViewMetastore mockMvMetastore;
    private ConnectorSession session;

    @BeforeClass
    public void setUp()
    {
        MaterializedViewConfig config = new MaterializedViewConfig();
        config.setMetastoreType(TYPE_LOCAL);

        mockMvMetastore = new MockMaterializedViewMetastore();
        MaterializedViewMetastoreFactory mockMetastoreFactory = mock(MaterializedViewMetastoreFactory.class);
        when(mockMetastoreFactory.create(any())).thenReturn(mockMvMetastore);

        HetuMetastore hetuMetastore = mock(HetuMetastore.class);
        MaterializedViewConnectorMetadataFactory factory = new MaterializedViewConnectorMetadataFactory(mockMetastoreFactory, new TestingTypeManager(), hetuMetastore);
        metadata = factory.create();
        session = new TestingConnectorSession(ImmutableList.of());

        metadata.createSchema(session, SCHEMA_NAME1, ImmutableMap.of());
    }

    @Test
    public void testSchema()
    {
        //listSchemaNames
        //createSchema
        //dropSchema
        metadata.createSchema(session, SCHEMA_NAME2, ImmutableMap.of());
        List<String> schemas = metadata.listSchemaNames(session);
        assertEquals(schemas.size(), 2);
        assertTrue(schemas.contains(SCHEMA_NAME1));
        assertTrue(schemas.contains(SCHEMA_NAME2));
        metadata.dropSchema(session, SCHEMA_NAME2);
        schemas = metadata.listSchemaNames(session);
        assertEquals(schemas.size(), 1);
        assertTrue(schemas.contains(SCHEMA_NAME1));
    }

    @Test
    public void testViews()
    {
        ColumnMetadata columnMetadata = new ColumnMetadata(COLUMN_NAME, VARCHAR);
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(new SchemaTableName(SCHEMA_NAME1, TABLE_NAME), ImmutableList.of(columnMetadata));
        ViewColumn viewColumn = new ViewColumn(COLUMN_NAME, VARCHAR.getTypeSignature());
        ConnectorMaterializedViewDefinition definition = new ConnectorMaterializedViewDefinition(
                EXECUTE_SQL,
                Optional.of(CATALOG_NAME),
                Optional.of(SCHEMA_NAME),
                ImmutableList.of(viewColumn),
                Optional.of(OWNER),
                false,
                FULL_SQL,
                META_CATALOG_NAME,
                CURRENT_CATALOG_NAME,
                CURRENT_SCHEMA_NAME,
                STATUS);
        metadata.beginCreateMaterializedView(tableMetadata, definition);
        SchemaTableName schemaTableName = new SchemaTableName(SCHEMA_NAME1, TABLE_NAME);
        metadata.finishCreateMaterializedView(schemaTableName);

        Optional<ConnectorViewDefinition> viewDefinition = metadata.getView(session, schemaTableName);
        assertTrue(viewDefinition.isPresent());
        ConnectorMaterializedViewDefinition materializedViewDefinition = (ConnectorMaterializedViewDefinition) viewDefinition.get();
        assertEquals(materializedViewDefinition.toString(), definition.toString());

        ConnectorTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
        ConnectorTableMetadata getTableMetadata = metadata.getTableMetadata(session, tableHandle);
        assertEquals(getTableMetadata.toString(), tableMetadata.toString());
        Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
        ColumnMetadata getColumnMetadata = metadata.getColumnMetadata(session, tableHandle, columnHandles.get(COLUMN_NAME));
        assertEquals(getColumnMetadata, columnMetadata);

        List<SchemaTableName> tables = metadata.listTables(session, Optional.empty());
        assertEquals(tables.size(), 1);
        assertEquals(tables.get(0), schemaTableName);

        metadata.dropMaterializedView(session, tableHandle);
        tables = metadata.listTables(session, Optional.empty());
        assertEquals(tables.size(), 0);
    }

    class MockMaterializedViewMetastore
            implements MaterializedViewMetastore
    {
        private Map<String, Map<String, MaterializedView>> views;

        @Override
        public void init()
        {
            views = new HashMap<>();
        }

        @Override
        public void addMaterializedView(String schema, MaterializedView materializedView)
        {
            synchronized (views) {
                Map<String, MaterializedView> viewMap = views.get(schema);
                viewMap.put(materializedView.getViewName(), materializedView);
            }
        }

        @Override
        public void dropMaterializedView(String schema, String view)
        {
            synchronized (views) {
                Map<String, MaterializedView> viewMap = views.get(schema);
                viewMap.remove(view);
            }
        }

        @Override
        public void addSchema(String schemaName)
        {
            synchronized (views) {
                if (views.containsKey(schemaName)) {
                    return;
                }
                Map<String, MaterializedView> viewMap = new HashMap<>();
                views.put(schemaName, viewMap);
            }
        }

        @Override
        public void dropSchema(String schemaName)
        {
            synchronized (views) {
                views.remove(schemaName);
            }
        }

        @Override
        public void setViewStatus(SchemaTableName viewName, MaterializedViewStatus status)
        {
            synchronized (views) {
                Map<String, MaterializedView> viewMap = views.get(viewName.getSchemaName());
                MaterializedView materializedView = viewMap.get(viewName.getTableName());
                materializedView.setStatus(status);
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
            List<String> viewList = new ArrayList<>();
            synchronized (views) {
                Map<String, MaterializedView> viewMap = views.get(schemaName);
                for (MaterializedView view : viewMap.values()) {
                    viewList.add(view.getViewName());
                }
            }
            return viewList;
        }

        @Override
        public MaterializedView getMaterializedView(SchemaTableName viewName)
        {
            synchronized (views) {
                Map<String, MaterializedView> viewMap = views.get(viewName.getSchemaName());
                return viewMap.get(viewName.getTableName());
            }
        }
    }
}
