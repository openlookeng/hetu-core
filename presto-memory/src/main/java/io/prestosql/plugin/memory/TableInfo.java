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
package io.prestosql.plugin.memory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.util.Objects.requireNonNull;

public class TableInfo
{
    // Codec object to serialize/deserialize this class. Stored in metadata.
    private static final JsonCodec<TableInfo> TABLE_INFO_JSON_CODEC = jsonCodec(TableInfo.class);

    private final long id;
    private final String schemaName;
    private final String tableName;
    private final List<MemoryColumnHandle> columns;
    private final Map<HostAddress, MemoryDataFragment> dataFragments;
    private final long modificationTime;

    @JsonCreator
    public TableInfo(
            @JsonProperty("id") long id,
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("columns") List<MemoryColumnHandle> columns,
            @JsonProperty("dataFragments") Map<HostAddress, MemoryDataFragment> dataFragments,
            @JsonProperty("modificationTime") long modificationTime)
    {
        this.id = requireNonNull(id, "handle is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.columns = ImmutableList.copyOf(columns);
        this.dataFragments = ImmutableMap.copyOf(dataFragments);
        this.modificationTime = modificationTime;
    }

    @JsonProperty
    public long getId()
    {
        return id;
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

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    public List<ColumnMetadata> getColumns(TypeManager typeManager)
    {
        return columns.stream()
                .map(columnInfo -> columnInfo.getMetadata(typeManager))
                .collect(Collectors.toList());
    }

    @JsonProperty
    public List<MemoryColumnHandle> getColumns()
    {
        return columns;
    }

    public MemoryColumnHandle getColumn(ColumnHandle handle)
    {
        return columns.stream()
                .filter(column -> column.equals(handle))
                .findFirst()
                .get();
    }

    @JsonProperty
    public Map<HostAddress, MemoryDataFragment> getDataFragments()
    {
        return dataFragments;
    }

    @JsonProperty
    public long getModificationTime()
    {
        return modificationTime;
    }

    public String serialize()
    {
        return Base64.getEncoder().encodeToString(TABLE_INFO_JSON_CODEC.toJsonBytes(this));
    }

    public static TableInfo deserialize(String serializedTableInfo)
    {
        return TABLE_INFO_JSON_CODEC.fromJson(Base64.getDecoder().decode(serializedTableInfo));
    }
}
