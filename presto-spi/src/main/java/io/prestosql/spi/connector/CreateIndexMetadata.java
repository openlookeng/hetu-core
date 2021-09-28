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
package io.prestosql.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public class CreateIndexMetadata
{
    public static final String LEVEL_PROP_KEY = "level";
    public static final String AUTOLOAD_PROP_KEY = "autoload";
    public static final Map<String, List<String>> INDEX_SUPPORTED_TYPES = ImmutableMap.<String, List<String>>builder()
            .put("bloom", ImmutableList.of(
                    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"))
            .put("bitmap", ImmutableList.of(
                    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"))
            .put("minmax", ImmutableList.of(
                    "integer", "smallint", "bigint", "tinyint", "varchar", "char", "boolean", "double", "real", "date", "decimal"))
            .put("btree", ImmutableList.of(
                    "integer", "smallint", "bigint", "tinyint", "varchar", "double", "real", "date", "decimal"))
            .build();

    private final String indexName;
    private final String tableName;
    private final String indexType;
    private final List<Pair<String, Type>> indexColumns;
    private final List<String> partitions;
    private final Properties properties;
    private final String user;
    private volatile Level createLevel;
    private volatile long indexSize;

    @JsonCreator
    public CreateIndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("indexType") String indexType,
            @JsonProperty("indexSize") long indexSize,
            @JsonProperty("indexColumns") List<Pair<String, Type>> indexColumns,
            @JsonProperty("partitions") List<String> partitions,
            @JsonProperty("properties") Properties properties,
            @JsonProperty("user") String user,
            @JsonProperty("createLevel") Level createLevel)
    {
        this.indexName = checkNotEmpty(indexName, "indexName");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.indexType = requireNonNull(indexType, "indexType is null").toUpperCase(Locale.ENGLISH);
        this.indexSize = indexSize;
        this.indexColumns = indexColumns;
        this.partitions = partitions;
        this.properties = properties;
        this.user = requireNonNull(user, "user is null");
        this.createLevel = requireNonNull(createLevel, "createLevel is null");
        properties.setProperty(LEVEL_PROP_KEY, createLevel.toString());
    }

    @JsonProperty
    public Level getCreateLevel()
    {
        return createLevel;
    }

    /**
     * When index creation has gathered enough information (index type, partitioned table or not, etc),
     * this method will be called to finalize creationLevel
     * <p>
     * The level is based on index type, and whether the table is partitioned
     *
     * @param tableIsPartitioned if this table is partitioned
     */
    public synchronized void decideIndexLevel(boolean tableIsPartitioned)
    {
        if (createLevel == Level.UNDEFINED) {
            if (indexType.toUpperCase(Locale.ROOT).equals("BTREE")) {
                this.createLevel = tableIsPartitioned ? Level.PARTITION : Level.TABLE;
            }
            else {
                this.createLevel = Level.STRIPE;
            }
            properties.setProperty(LEVEL_PROP_KEY, createLevel.toString());
        }
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
    public long getIndexSize()
    {
        return indexSize;
    }

    public void setIndexSize(long size)
    {
        this.indexSize = size;
    }

    @JsonProperty
    public List<Pair<String, Type>> getIndexColumns()
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
        sb.append("indexSize=").append(indexSize).append('\'');
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
        return Objects.hash(indexName, tableName, indexType, indexSize, indexColumns, partitions, properties, user);
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
                && Objects.equals(this.indexSize, other.indexSize)
                && Objects.equals(this.indexColumns, other.indexColumns)
                && Objects.equals(this.partitions, other.partitions)
                && Objects.equals(this.properties, other.properties)
                && Objects.equals(this.user, other.user);
    }

    public enum Level
    {
        STRIPE,
        PARTITION,
        TABLE,
        // If the index creation level is not specified, it will be decided on-the-fly. See decideIndexLevel().
        UNDEFINED
    }
}
