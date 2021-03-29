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
package io.prestosql.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

public class ConnectorMaterializedViewDefinition
        extends ConnectorViewDefinition
{
    private final String fullSql; // the full sql for creating materialized view
    private final String metaCatalog; // the catalog which stores materialized view metadata
    private final String currentCatalog; // the catalog which current session used
    private final String currentSchema; // the schema which current session used
    private final String status; // the status of the materialized view

    @JsonCreator
    public ConnectorMaterializedViewDefinition(
            @JsonProperty("executeSql") String executeSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<ViewColumn> columns,
            @JsonProperty("owner") Optional<String> owner,
            @JsonProperty("runAsInvoker") boolean runAsInvoker,
            @JsonProperty("fullSql") String fullSql,
            @JsonProperty("metaCatalog") String metaCatalog,
            @JsonProperty("currentCatalog") String currentCatalog,
            @JsonProperty("currentSchema") String currentSchema,
            @JsonProperty("status") String status)
    {
        super(executeSql, catalog, schema, columns, owner, runAsInvoker);
        this.fullSql = fullSql;
        this.metaCatalog = metaCatalog;
        this.currentCatalog = currentCatalog;
        this.currentSchema = currentSchema;
        this.status = status;
    }

    @JsonProperty
    public String getFullSql()
    {
        return this.fullSql;
    }

    @JsonProperty
    public String getMetaCatalog()
    {
        return this.metaCatalog;
    }

    @JsonProperty
    public String getCurrentCatalog()
    {
        return this.currentCatalog;
    }

    @JsonProperty
    public String getCurrentSchema()
    {
        return this.currentSchema;
    }

    @JsonProperty
    public String getStatus()
    {
        return this.status;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        getOwner().ifPresent(value -> joiner.add("owner=" + value));
        joiner.add("runAsInvoker=" + isRunAsInvoker());
        joiner.add("columns=" + getColumns());
        getCatalog().ifPresent(value -> joiner.add("catalog=" + value));
        getSchema().ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("originalSql=[" + getOriginalSql() + "]");
        joiner.add("fullSql=[" + getFullSql() + "]");
        joiner.add("metaCatalog=" + getMetaCatalog());
        joiner.add("currentCatalog=" + getCurrentCatalog());
        joiner.add("currentSchema=" + getCurrentSchema());
        joiner.add("status=" + getStatus());
        return getClass().getSimpleName() + joiner.toString();
    }
}
