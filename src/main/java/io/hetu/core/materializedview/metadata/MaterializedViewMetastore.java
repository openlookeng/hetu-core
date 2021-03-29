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

import io.hetu.core.materializedview.utils.MaterializedViewStatus;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.List;
import java.util.Set;

/**
 * MvMetastore
 *
 * @since 2020-03-25
 */
public interface MaterializedViewMetastore
{
    void init();

    void addMaterializedView(String schema, MaterializedView materializedView);

    void dropMaterializedView(String schema, String view);

    void addSchema(String schemaName);

    void dropSchema(String schemaName);

    void setViewStatus(SchemaTableName viewName, MaterializedViewStatus status);

    Set<String> getSchemas();

    List<String> getAllMaterializedView(String schemaName);

    MaterializedView getMaterializedView(SchemaTableName viewName);
}
