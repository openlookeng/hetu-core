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
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public class CreateIndexMetadata
{
    public static final String LEVEL_PROP_KEY = "level";
    public static final Index.Level LEVEL_DEFAULT = Index.Level.STRIPE;

    private final String indexName;
    private final String tableName;
    private final String indexType;
    private final List<Map.Entry<String, Type>> indexColumns;
    private final List<String> partitions;
    private final Properties properties;
    private final String user;
    private final Index.Level createLevel;

    @JsonCreator
    public CreateIndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("indexType") String indexType,
            @JsonProperty("indexColumns") List<Map.Entry<String, Type>> indexColumns,
            @JsonProperty("partitions") List<String> partitions,
            @JsonProperty("properties") Properties properties,
            @JsonProperty("user") String user,
            @JsonProperty("createLevel") Index.Level createLevel)
    {
        this.indexName = checkNotEmpty(indexName, "indexName");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.indexType = requireNonNull(indexType, "indexType is null");
        this.indexColumns = indexColumns;
        this.partitions = partitions;
        this.properties = properties;
        this.user = requireNonNull(user, "user is null");
        this.createLevel = createLevel == null ? LEVEL_DEFAULT : createLevel;
    }

    @JsonProperty
    public Index.Level getCreateLevel()
    {
        return createLevel;
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getIndexType()
    {
        return indexType;
    }

    @JsonProperty
    public List<Map.Entry<String, Type>> getIndexColumns()
    {
        return indexColumns;
    }

    @JsonProperty
    public List<String> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
    public Properties getProperties()
    {
        return properties;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("CreateIndexMetadata{");
        sb.append("indexName='").append(indexName).append('\'');
        sb.append("tableName='").append(tableName).append('\'');
        sb.append("indexType=").append(indexType).append('\'');
        if (!indexColumns.isEmpty()) {
            sb.append(", indexColumns=").append(indexColumns);
        }
        if (partitions != null) {
            sb.append(", partitions='").append(partitions).append('\'');
        }
        if (!properties.isEmpty()) {
            sb.append(", properties=").append(properties);
        }
        sb.append("user=").append(user).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, tableName, indexType, indexColumns, partitions, properties, user);
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
        CreateIndexMetadata other = (CreateIndexMetadata) obj;
        return Objects.equals(this.indexName, other.indexName)
                && Objects.equals(this.tableName, other.tableName)
                && Objects.equals(this.indexType, other.indexType)
                && Objects.equals(this.indexColumns, other.indexColumns)
                && Objects.equals(this.partitions, other.partitions)
                && Objects.equals(this.properties, other.properties)
                && Objects.equals(this.user, other.user);
    }
}
