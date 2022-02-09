/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.datacenter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;

import java.util.Objects;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

/**
 * Data center table handle.
 *
 * @since 2020-02-11
 */
public final class DataCenterTableHandle
        implements ConnectorTableHandle
{
    private static final String SPLIT_DOT = ".";

    private final String catalogName;

    private final String schemaName;

    private final String tableName;

    private final OptionalLong limit;

    private final String pushDownSql;

    /**
     * Constructor of data center table handle.
     *
     * @param catalogName catalog name.
     * @param schemaName schema name.
     * @param tableName table name.
     * @param limit the limit number of this query need.
     */
    public DataCenterTableHandle(String catalogName, String schemaName, String tableName, OptionalLong limit)
    {
        this(catalogName,
                requireNonNull(schemaName, "schemaName is null"),
                requireNonNull(tableName, "tableName is null"),
                requireNonNull(limit, "limit is null"),
                "");
    }

    /**
     * Constructor of data center table handle.
     *
     * @param catalogName catalog name.
     * @param schemaName schema name.
     * @param tableName table name.
     * @param limit the limit number of this query need.
     * @param pushDownSql the sub query statement that want to be pushed down to remote data center.
     */
    @JsonCreator
    public DataCenterTableHandle(@JsonProperty("catalogName") String catalogName,
            @JsonProperty("schemaName") String schemaName, @JsonProperty("tableName") String tableName,
            @JsonProperty("limit") OptionalLong limit, @JsonProperty("subQuery") String pushDownSql)
    {
        this.catalogName = catalogName;
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.pushDownSql = pushDownSql;
    }

    @Override
    public ConnectorTableHandle createFrom(ConnectorTableHandle connectorTableHandle)
    {
        DataCenterTableHandle dataCenterTableHandle = (DataCenterTableHandle) connectorTableHandle;
        return new DataCenterTableHandle(catalogName, schemaName, dataCenterTableHandle.tableName, dataCenterTableHandle.getLimit(), dataCenterTableHandle.getPushDownSql());
    }

    @JsonProperty
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    /**
     * Merge schema and table name as a instance.
     *
     * @return schema and table name.
     */
    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String getSchemaPrefixedTableName()
    {
        return catalogName + SPLIT_DOT + schemaName + SPLIT_DOT + tableName;
    }

    @JsonProperty
    public String getPushDownSql()
    {
        return pushDownSql;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogName, schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        DataCenterTableHandle other = (DataCenterTableHandle) obj;
        return Objects.equals(this.catalogName, other.catalogName) && Objects.equals(this.schemaName, other.schemaName)
                && Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        if (!pushDownSql.isEmpty()) {
            Joiner.on(SPLIT_DOT).skipNulls().appendTo(builder, catalogName, "{" + pushDownSql + "}");
        }
        else {
            Joiner.on(SPLIT_DOT).skipNulls().appendTo(builder, catalogName, schemaName, tableName);
        }
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        return builder.toString();
    }
}
