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

import io.hetu.core.materializedview.connector.MaterializedViewColumnHandle;
import io.hetu.core.materializedview.metadata.MaterializedView;
import io.hetu.core.materializedview.utils.MaterializedViewDateUtils;
import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static io.hetu.core.materializedview.TestUtils.CURRENT_CATALOG;
import static io.hetu.core.materializedview.TestUtils.CURRENT_SCHEMA;
import static io.hetu.core.materializedview.TestUtils.DATA_CATALOG;
import static io.hetu.core.materializedview.TestUtils.DATA_SCHEMA;
import static io.hetu.core.materializedview.TestUtils.DATE;
import static io.hetu.core.materializedview.TestUtils.FULL_SQL;
import static io.hetu.core.materializedview.TestUtils.META_CATALOG;
import static io.hetu.core.materializedview.TestUtils.OWNER;
import static io.hetu.core.materializedview.TestUtils.VIEW_NAME;
import static io.hetu.core.materializedview.TestUtils.VIEW_SQL;
import static io.hetu.core.materializedview.utils.MaterializedViewDateUtils.getStringFromDate;
import static io.hetu.core.materializedview.utils.MaterializedViewStatus.DISABLE;
import static io.hetu.core.materializedview.utils.MaterializedViewStatus.ENABLE;
import static io.hetu.core.materializedview.utils.MaterializedViewStatus.getStatusFromString;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMaterializedView
{
    @Test
    public void testMaterializedView()
    {
        MaterializedView materializedView = createMaterializedView(VIEW_NAME);
        assertEquals(VIEW_NAME, materializedView.getViewName());
        assertEquals(VIEW_SQL, materializedView.getViewSql());
        assertEquals(FULL_SQL, materializedView.getFullSql());
        assertEquals(DATA_CATALOG, materializedView.getDataCatalog());
        assertEquals(DATA_SCHEMA, materializedView.getDataSchema());
        assertEquals(META_CATALOG, materializedView.getMetaCatalog());
        assertEquals(CURRENT_CATALOG, materializedView.getCurrentCatalog());
        assertEquals(CURRENT_SCHEMA, materializedView.getCurrentSchema());
        assertTrue(materializedView.getColumns().equals(createMaterializedViewColumns()));
        assertEquals(OWNER, materializedView.getOwner());
        assertFalse(materializedView.isRunAsInvoker());
        assertEquals(materializedView.getLastRefreshTime(), getStringFromDate(DATE));
        assertEquals(getStatusFromString(materializedView.getStatus()), DISABLE);

        materializedView.setStatus(ENABLE);
        assertEquals(getStatusFromString(materializedView.getStatus()), ENABLE);
    }

    private MaterializedView createMaterializedView(String viewName)
    {
        return new MaterializedView(
                viewName,
                VIEW_SQL,
                FULL_SQL,
                DATA_CATALOG,
                DATA_SCHEMA,
                META_CATALOG,
                CURRENT_CATALOG,
                CURRENT_SCHEMA,
                createMaterializedViewColumns(),
                OWNER,
                false,
                MaterializedViewDateUtils.getStringFromDate(DATE),
                MaterializedViewStatus.getStatusString(DISABLE));
    }

    private List<MaterializedViewColumnHandle> createMaterializedViewColumns()
    {
        List<MaterializedViewColumnHandle> columns = new ArrayList<>();
        columns.add(new MaterializedViewColumnHandle("column0", 0, TypeSignature.parseTypeSignature("char")));
        columns.add(new MaterializedViewColumnHandle("column1", 1, TypeSignature.parseTypeSignature("int")));
        return columns;
    }
}
