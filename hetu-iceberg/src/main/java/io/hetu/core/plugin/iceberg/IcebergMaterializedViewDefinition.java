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
package io.hetu.core.plugin.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.prestosql.spi.connector.ConnectorMaterializedViewDefinition;
import io.prestosql.spi.type.TypeId;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_VIEW_DATA;
import static io.prestosql.plugin.hive.HiveUtil.checkCondition;
import static java.util.Objects.requireNonNull;

/*
 * Serializable version of ConnectorMaterializedViewDefinition stored by iceberg connector
 */
public class IcebergMaterializedViewDefinition
{
    private static final String MATERIALIZED_VIEW_PREFIX = "/* Presto Materialized View: ";
    private static final String MATERIALIZED_VIEW_SUFFIX = " */";

    private static final JsonCodec<IcebergMaterializedViewDefinition> materializedViewCodec =
            new JsonCodecFactory(new ObjectMapperProvider()).jsonCodec(IcebergMaterializedViewDefinition.class);

    private final String originalSql;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final List<Column> columns;
    private final Optional<String> comment;

    public static String encodeMaterializedViewData(IcebergMaterializedViewDefinition definition)
    {
        byte[] bytes = materializedViewCodec.toJsonBytes(definition);
        String data = Base64.getEncoder().encodeToString(bytes);
        return MATERIALIZED_VIEW_PREFIX + data + MATERIALIZED_VIEW_SUFFIX;
    }

    public static IcebergMaterializedViewDefinition decodeMaterializedViewData(String data)
    {
        String dataStr = data;
        checkCondition(dataStr.startsWith(MATERIALIZED_VIEW_PREFIX), HIVE_INVALID_VIEW_DATA, "Materialized View data missing prefix: %s", dataStr);
        checkCondition(dataStr.endsWith(MATERIALIZED_VIEW_SUFFIX), HIVE_INVALID_VIEW_DATA, "Materialized View data missing suffix: %s", dataStr);
        dataStr = dataStr.substring(MATERIALIZED_VIEW_PREFIX.length());
        dataStr = dataStr.substring(0, dataStr.length() - MATERIALIZED_VIEW_SUFFIX.length());
        byte[] bytes = Base64.getDecoder().decode(dataStr);
        return materializedViewCodec.fromJson(bytes);
    }

    public static IcebergMaterializedViewDefinition fromConnectorMaterializedViewDefinition(ConnectorMaterializedViewDefinition definition)
    {
        return new IcebergMaterializedViewDefinition(
                definition.getOriginalSql(),
                definition.getCatalog(),
                definition.getSchema(),
                definition.getColumns().stream()
                        .map(column -> new Column(column.getName(), column.getType()))
                        .collect(toImmutableList()),
                definition.getComment());
    }

    @JsonCreator
    public IcebergMaterializedViewDefinition(
            @JsonProperty("originalSql") String originalSql,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("comment") Optional<String> comment)
    {
        this.originalSql = requireNonNull(originalSql, "originalSql is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.columns = Collections.unmodifiableList(new ArrayList<>(requireNonNull(columns, "columns is null")));
        this.comment = requireNonNull(comment, "comment is null");

        if (!catalog.isPresent() && schema.isPresent()) {
            throw new IllegalArgumentException("catalog must be present if schema is present");
        }
        if (columns.isEmpty()) {
            throw new IllegalArgumentException("columns list is empty");
        }
    }

    @JsonProperty
    public String getOriginalSql()
    {
        return originalSql;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public String toString()
    {
        StringJoiner joiner = new StringJoiner(", ", "[", "]");
        joiner.add("originalSql=[" + originalSql + "]");
        catalog.ifPresent(value -> joiner.add("catalog=" + value));
        schema.ifPresent(value -> joiner.add("schema=" + value));
        joiner.add("columns=" + columns);
        comment.ifPresent(value -> joiner.add("comment=" + value));
        return getClass().getSimpleName() + joiner;
    }

    public static final class Column
    {
        private final String name;
        private final TypeId type;

        @JsonCreator
        public Column(
                @JsonProperty("name") String name,
                @JsonProperty("type") TypeId type)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public TypeId getType()
        {
            return type;
        }

        @Override
        public String toString()
        {
            return name + " " + type;
        }
    }
}
