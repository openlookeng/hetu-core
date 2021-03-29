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

import io.hetu.core.materializedview.conf.MaterializedViewConfig;
import io.hetu.core.materializedview.metadata.LocalMaterializedViewMetastore;
import io.hetu.core.materializedview.metadata.MaterializedView;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.List;
import java.util.Set;

import static io.hetu.core.materializedview.TestUtils.CURRENT_SCHEMA;
import static io.hetu.core.materializedview.TestUtils.DATA_SCHEMA;
import static io.hetu.core.materializedview.TestUtils.TEST_FILE;
import static io.hetu.core.materializedview.TestUtils.VIEW_NAME;
import static io.hetu.core.materializedview.TestUtils.createMaterializedView;
import static io.hetu.core.materializedview.utils.MaterializedViewConstants.TYPE_LOCAL;
import static io.hetu.core.materializedview.utils.MaterializedViewStatus.getStatusString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalMvMetastore
{
    private LocalMaterializedViewMetastore localMaterializedViewMetastore;
    private final String newName = "test_view";

    @BeforeClass
    public void setUp()
    {
        MaterializedViewConfig config = new MaterializedViewConfig();
        config.setMetastoreType(TYPE_LOCAL);
        config.setMetastorePath(TEST_FILE);
        localMaterializedViewMetastore = new LocalMaterializedViewMetastore(config);
        localMaterializedViewMetastore.addSchema(DATA_SCHEMA);
        localMaterializedViewMetastore.addMaterializedView(DATA_SCHEMA, createMaterializedView(VIEW_NAME));
    }

    @Test
    public void testSchema()
    {
        localMaterializedViewMetastore.addSchema(CURRENT_SCHEMA);
        Set<String> schemas = localMaterializedViewMetastore.getSchemas();
        assertEquals(schemas.size(), 2);
        assertTrue(schemas.contains(CURRENT_SCHEMA));

        localMaterializedViewMetastore.dropSchema(CURRENT_SCHEMA);
        schemas = localMaterializedViewMetastore.getSchemas();
        assertEquals(schemas.size(), 1);
        assertFalse(schemas.contains(CURRENT_SCHEMA));
    }

    @Test
    public void testMaterializedView()
    {
        SchemaTableName viewName = new SchemaTableName(DATA_SCHEMA, newName);

        List<String> views = localMaterializedViewMetastore.getAllMaterializedView(DATA_SCHEMA);
        assertEquals(views.size(), 1);

        MaterializedView view = createMaterializedView(newName);
        localMaterializedViewMetastore.addMaterializedView(DATA_SCHEMA, view);
        views = localMaterializedViewMetastore.getAllMaterializedView(DATA_SCHEMA);
        assertEquals(views.size(), 2);
        assertTrue(views.contains(newName));
        MaterializedView actualView = localMaterializedViewMetastore.getMaterializedView(viewName);
        assertEquals(actualView.toString(), view.toString());

        localMaterializedViewMetastore.setViewStatus(viewName, MaterializedViewStatus.ENABLE);
        actualView = localMaterializedViewMetastore.getMaterializedView(viewName);
        assertEquals(actualView.getStatus(), getStatusString(MaterializedViewStatus.ENABLE));

        localMaterializedViewMetastore.dropMaterializedView(DATA_SCHEMA, newName);
        views = localMaterializedViewMetastore.getAllMaterializedView(DATA_SCHEMA);
        assertEquals(views.size(), 1);
        assertFalse(views.contains(newName));
    }

    @AfterClass
    public void down()
    {
        new File(TEST_FILE).delete();
    }
}
