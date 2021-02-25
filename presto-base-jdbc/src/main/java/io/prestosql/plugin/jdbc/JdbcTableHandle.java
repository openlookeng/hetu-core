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
package io.prestosql.plugin.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import io.prestosql.plugin.jdbc.optimization.JdbcQueryGeneratorResult.GeneratedSql;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class JdbcTableHandle
        implements ConnectorTableHandle
{
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final TupleDomain<ColumnHandle> constraint;
    private final OptionalLong limit;
    // Hetu: If query is push down use pushDown sql to build sql and use columnHandles directly
    private final Optional<GeneratedSql> generatedSql;

    public JdbcTableHandle(SchemaTableName schemaTableName, @Nullable String catalogName, @Nullable String schemaName, String tableName)
    {
        this(schemaTableName, catalogName, schemaName, tableName, TupleDomain.all(), OptionalLong.empty());
    }

    /**
     * Hetu requires subQuery in the @JsonCreator constructor which is called from {@link JdbcMetadata} and TestJdbcRecordSetProvider.
     * This overloaded constructor is added to avoid modifying all those places.
     *
     * @param schemaTableName
     * @param catalogName
     * @param schemaName
     * @param tableName
     * @param constraint
     */
    public JdbcTableHandle(
            SchemaTableName schemaTableName,
            @Nullable String catalogName,
            @Nullable String schemaName,
            String tableName,
            TupleDomain<ColumnHandle> constraint,
            OptionalLong limit)
    {
        this(schemaTableName, catalogName, schemaName, tableName, constraint, limit, Optional.empty());
    }

    @JsonCreator
    public JdbcTableHandle(
            @JsonProperty("schemaTableName") SchemaTableName schemaTableName,
            @JsonProperty("catalogName") @Nullable String catalogName,
            @JsonProperty("schemaName") @Nullable String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("sql") Optional<GeneratedSql> generatedSql)
    {
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.generatedSql = generatedSql;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @JsonProperty
    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @JsonProperty
    @Nullable
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
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
    }

    @JsonProperty
    public Optional<GeneratedSql> getGeneratedSql()
    {
        return generatedSql;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    /**
     * Hetu DC Connector uses {@link JdbcTableHandle}.
     * Overriding this method makes all JdbcConnectors using {@link JdbcTableHandle}
     * to return the qualified name of the table which is used in SqlQueryBuilder.
     *
     * @return qualified name of the table
     */
    @Override
    public String getSchemaPrefixedTableName()
    {
        return schemaTableName.toString();
    }

    /**
     * Hetu execution plan caching functionality requires a method to update
     * {@link ConnectorTableHandle} from a previous execution plan with new info from
     * a new query. This is a workaround for hetu-main module to modify jdbc connector table
     * handles in a generic way without needing access to classes in hetu-base-jdbc package or
     * knowledge of connector specific constructor implementations. Connectors must override this
     * method to support execution plan caching.
     *
     * @param oldConnectorTableHandle connector table handle containing information
     * to be passed into a new {@link JdbcTableHandle}
     * @return new {@link JdbcTableHandle} containing the constraints, limit,
     * and subquery from an old {@link JdbcTableHandle}
     */
    @Override
    public ConnectorTableHandle createFrom(ConnectorTableHandle oldConnectorTableHandle)
    {
        JdbcTableHandle oldJdbcTableHandle = (JdbcTableHandle) oldConnectorTableHandle;
        return new JdbcTableHandle(schemaTableName, catalogName, schemaName, tableName, oldJdbcTableHandle.getConstraint(),
                oldJdbcTableHandle.getLimit(), oldJdbcTableHandle.getGeneratedSql());
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
        JdbcTableHandle o = (JdbcTableHandle) obj;
        return Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaTableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        if (generatedSql.isPresent()) {
            Joiner.on(".").skipNulls().appendTo(builder, catalogName, generatedSql.get());
        }
        else {
            Joiner.on(".").skipNulls().appendTo(builder, catalogName, schemaName, tableName);
        }
        limit.ifPresent(value -> builder.append(" limit=").append(value));
        return builder.toString();
    }
}
