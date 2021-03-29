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

package io.hetu.core.materializedview.utils;

/**
 * Constants of mv
 *
 * @since 2020-02-26
 */
public class MaterializedViewConstants
{
    public static final int INDENTFACTOR_4 = 4;

    public static final String TYPE_LOCAL = "local";

    public static final String TYPE_HETU = "hetuMetastore";

    public static final String CATALOG_NAME = "mv";

    public static final String FULL_SQL = "full_sql";

    public static final String DATA_CATALOG = "data_catalog";

    public static final String CURRENT_CATALOG = "current_catalog";

    public static final String CURRENT_SCHEMA = "current_schema";

    public static final String RUN_AS_INVOKER = "run_as_invoker";

    public static final String LAST_REFRESH_TIME = "last_refresh_time";

    public static final String STATUS = "status";

    public static final String COLUMN_ID = "column_id";

    private MaterializedViewConstants()
    {
    }
}
