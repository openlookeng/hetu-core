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
package io.hetu.core.plugin.hbase.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * HBaseTableHandle
 *
 * @since 2020-03-30
 */
public class HBaseTableHandle
        implements ConnectorInsertTableHandle, ConnectorOutputTableHandle, ConnectorTableHandle
{
    private final boolean external;
    private final String rowId;
    private final Optional<String> hbaseTableName;
    private final String schema;
    private final String serializerClassName;
    private final String table;
    private final String indexColumns;
    private final TupleDomain<ColumnHandle> constraint;
    private final List<HBaseColumnHandle> columns;
    private final int rowIdOrdinal;
    private final OptionalLong limit;

    /**
     * constructor
     *
     * @param schema schema
     * @param table table
     * @param rowId rowId
     * @param external external
     * @param serializerClassName serializerClassName
     * @param hbaseTableName hbaseTableName
     * @param indexColumns indexColumns
     * @param constraint constraint
     * @param columns columns
     * @param rowIdOrdinal rowIdOrdinal
     * @param limit limit
     */
    @JsonCreator
    public HBaseTableHandle(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("external") boolean external,
            @JsonProperty("serializerClassName") String serializerClassName,
            @JsonProperty("hbaseTableName") Optional<String> hbaseTableName,
            @JsonProperty("indexColumns") String indexColumns,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("columns") List<HBaseColumnHandle> columns,
            @JsonProperty("rowIdOrdinal") int rowIdOrdinal,
            @JsonProperty("limit") OptionalLong limit)
    {
        this.external = external;
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.hbaseTableName = hbaseTableName;
        this.schema = requireNonNull(schema, "schema is null");
        this.serializerClassName = requireNonNull(serializerClassName, "serializerClassName is null");
        this.table = requireNonNull(table, "table is null");
        this.indexColumns = indexColumns;
        this.constraint = constraint;
        this.columns = columns;
        this.rowIdOrdinal = rowIdOrdinal;
        this.limit = requireNonNull(limit, "limit is null");
    }

    /**
     * HBaseTableHandle
     *
     * @param schema schema
     * @param table table
     * @param rowIdOrdinal rowIdOrdinal
     * @param columns columns
     * @param serializerClassName serializerClassName
     * @param hbaseTableName hbaseTableName
     * @param limit limit
     */
    public HBaseTableHandle(
            String schema,
            String table,
            int rowIdOrdinal,
            List<HBaseColumnHandle> columns,
            String serializerClassName,
            Optional<String> hbaseTableName,
            OptionalLong limit)
    {
        this(
                schema,
                table,
                "",
                false,
                serializerClassName,
                hbaseTableName,
                "",
                TupleDomain.all(),
                columns,
                rowIdOrdinal,
                limit);
    }

    /**
     * constructor
     *
     * @param schema schema
     * @param table table
     * @param rowId rowId
     * @param external external
     * @param serializerClassName serializerClassName
     * @param hbaseTableName hbaseTableName
     * @param indexColumns indexColumns
     * @param limit limit
     */
    public HBaseTableHandle(
            String schema,
            String table,
            String rowId,
            boolean external,
            String serializerClassName,
            Optional<String> hbaseTableName,
            String indexColumns,
            OptionalLong limit)
    {
        this(
                schema,
                table,
                rowId,
                external,
                serializerClassName,
                hbaseTableName,
                indexColumns,
                TupleDomain.all(),
                ImmutableList.of(),
                0,
                limit);
    }

    @JsonProperty
    public Optional<String> getHbaseTableName()
    {
        return hbaseTableName;
    }

    @JsonProperty
    public String getRowId()
    {
        return rowId;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return serializerClassName;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return this.constraint;
    }

    @JsonProperty
    public int getRowIdOrdinal()
    {
        return rowIdOrdinal;
    }

    @JsonProperty
    public List<HBaseColumnHandle> getColumns()
    {
        return this.columns;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public boolean isExternal()
    {
        return external;
    }

    @JsonProperty
    public String getIndexColumns()
    {
        return indexColumns;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    /**
     * toSchemaTableName
     *
     * @return SchemaTableName
     */
    public SchemaTableName toSchemaTableName()
    {
        return new SchemaTableName(schema, table);
    }

    /**
     * getFullTableName
     *
     * @return String
     */
    @JsonIgnore
    public String getFullTableName()
    {
        return HBaseTable.getFullTableName(schema, table);
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
     * to be passed into a new {@link HBaseTableHandle}
     * @return new {@link HBaseTableHandle} containing the constraints, limit,
     * and subquery from an old {@link HBaseTableHandle}
     */
    @Override
    public ConnectorTableHandle createFrom(ConnectorTableHandle oldConnectorTableHandle)
    {
        HBaseTableHandle oldHBaseConnectorTableHandle = (HBaseTableHandle) oldConnectorTableHandle;
        return new HBaseTableHandle(oldHBaseConnectorTableHandle.getSchema(),
                oldHBaseConnectorTableHandle.getTable(),
                oldHBaseConnectorTableHandle.getRowId(),
                oldHBaseConnectorTableHandle.isExternal(),
                oldHBaseConnectorTableHandle.getSerializerClassName(),
                oldHBaseConnectorTableHandle.getHbaseTableName(),
                oldHBaseConnectorTableHandle.getIndexColumns(),
                oldHBaseConnectorTableHandle.getConstraint(),
                oldHBaseConnectorTableHandle.getColumns(),
                oldHBaseConnectorTableHandle.getRowIdOrdinal(),
                oldHBaseConnectorTableHandle.getLimit());
    }

    @Override
    public String getTableName()
    {
        return String.format("%s.%s", schema, table);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schema, table, rowId, external, serializerClassName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if ((obj == null) || (!(obj instanceof HBaseTableHandle))) {
            return false;
        }

        HBaseTableHandle other = (HBaseTableHandle) obj;
        return Objects.equals(this.schema, other.schema)
                && Objects.equals(this.table, other.table)
                && Objects.equals(this.rowId, other.rowId)
                && Objects.equals(this.external, other.external)
                && Objects.equals(this.serializerClassName, other.serializerClassName)
                && Objects.equals(this.indexColumns, other.indexColumns)
                && Objects.equals(this.hbaseTableName, other.hbaseTableName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schema", schema)
                .add("table", table)
                .add("rowId", rowId)
                .add("internal", external)
                .add("serializerClassName", serializerClassName)
                .add("indexColumns", indexColumns)
                .add("hbaseTableName", hbaseTableName)
                .toString();
    }
}
