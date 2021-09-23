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
import io.prestosql.spi.type.Type;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public class UpdateIndexMetadata
{
    private final String indexName;
    private final Properties properties;
    private final String user;
    private final Map<String, Type> columnTypes;

    @JsonCreator
    public UpdateIndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("properties") Properties properties,
            @JsonProperty("user") String user,
            @JsonProperty("columnTypes") Map<String, Type> columnTypes)
    {
        this.indexName = checkNotEmpty(indexName, "indexName");
        this.properties = properties;
        this.user = requireNonNull(user, "user is null");
        this.columnTypes = columnTypes;
    }

    @JsonProperty
    public String getIndexName()
    {
        return indexName;
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

    @JsonProperty
    public Map<String, Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, properties, user);
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
        UpdateIndexMetadata other = (UpdateIndexMetadata) obj;
        return Objects.equals(this.indexName, other.indexName)
                && Objects.equals(this.properties, other.properties)
                && Objects.equals(this.user, other.user);
    }

    @Override
    public String toString()
    {
        return "UpdateIndexMetadata{" +
                "indexName='" + indexName + '\'' +
                ", properties=" + properties +
                ", user='" + user + '\'' +
                ", columnTypes=" + columnTypes +
                '}';
    }
}
