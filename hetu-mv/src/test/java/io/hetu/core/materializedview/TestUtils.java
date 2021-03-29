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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static io.hetu.core.materializedview.utils.MaterializedViewStatus.DISABLE;

public class TestUtils
{
    public static final String TEST_FILE = "testLocalMvMetadata.ini";

    public static final String VIEW_NAME = "mv.test.view";
    public static final String FULL_SQL = "create materialized view mv.test.view as select * from hive.default.test";
    public static final String VIEW_SQL = "select * from hive.default.test";
    public static final String DATA_CATALOG = "memory";
    public static final String DATA_SCHEMA = "test";
    public static final String META_CATALOG = "mv";
    public static final String CURRENT_CATALOG = "hive";
    public static final String CURRENT_SCHEMA = "default";
    public static final String OWNER = "test";
    public static final Date DATE = new Date();

    private TestUtils()
    {
    }

    public static MaterializedView createMaterializedView(String viewName)
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

    public static List<MaterializedViewColumnHandle> createMaterializedViewColumns()
    {
        List<MaterializedViewColumnHandle> columns = new ArrayList<>();
        columns.add(new MaterializedViewColumnHandle("column0", 0, TypeSignature.parseTypeSignature("char")));
        columns.add(new MaterializedViewColumnHandle("column1", 1, TypeSignature.parseTypeSignature("int")));
        return columns;
    }
}
