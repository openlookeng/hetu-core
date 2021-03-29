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

package io.hetu.core.materializedview.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Objects;

/**
 * Material view handle
 *
 * @since 2020-03-28
 */
public class MaterializedViewHandle
        implements ConnectorTableHandle
{
    private final String schema;
    private final String view;

    /**
     * Constructor of view handle
     *
     * @param schema schemaName
     * @param view viewName
     */
    @JsonCreator
    public MaterializedViewHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("view") String view)
    {
        this.schema = schema;
        this.view = view;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getView()
    {
        return view;
    }

    /**
     * get full view name
     *
     * @return schemaTableName
     */
    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schema, view);
    }

    @Override
    public String toString()
    {
        return schema + ":" + view;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, view);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        MaterializedViewHandle other = (MaterializedViewHandle) obj;
        return Objects.equals(this.schema, other.getSchema())
                && Objects.equals(this.view, other.getView());
    }
}
