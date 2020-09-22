/*
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

import java.util.List;
import java.util.Objects;

import static io.prestosql.spi.connector.SchemaUtil.checkNotEmpty;
import static java.util.Objects.requireNonNull;

public class CreateIndexMetadata
{
    private final String indexName;
    private final String tableName;
    private final String indexType;
    private final List<String> indexColumns;
    private final String expression;
    private final List<String> properties;

    @JsonCreator
    public CreateIndexMetadata(
            @JsonProperty("indexName") String indexName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("indexType") String indexType,
            @JsonProperty("indexColumns") List<String> indexColumns,
            @JsonProperty("expression") String expression,
            @JsonProperty("properties") List<String> properties)
    {
        checkNotEmpty(indexName, "indexName");
        this.indexName = indexName;
        requireNonNull(tableName, "tableName is null");
        this.tableName = tableName;
        requireNonNull(indexType, "indexType is null");
        this.indexType = indexType;
        this.indexColumns = indexColumns;
        this.expression = expression;
        this.properties = ImmutableList.copyOf(requireNonNull(properties, "properties is null"));
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
    public List<String> getIndexColumns()
    {
        return indexColumns;
    }

    @JsonProperty
    public String getExpression()
    {
        return expression;
    }

    @JsonProperty
    public List<String> getProperties()
    {
        return properties;
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
        if (expression != null) {
            sb.append(", expression='").append(expression).append('\'');
        }
        if (!properties.isEmpty()) {
            sb.append(", properties=").append(properties);
        }
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(indexName, tableName, indexType, indexColumns, expression, properties);
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
        return Objects.equals(this.indexName, other.indexName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.indexType, other.indexType) &&
                Objects.equals(this.indexColumns, other.indexColumns) &&
                Objects.equals(this.expression, other.expression) &&
                Objects.equals(this.properties, other.properties);
    }
}
