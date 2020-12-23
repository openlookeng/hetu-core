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
package io.hetu.core.plugin.hbase.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.conf.HBaseTableProperties;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.utils.Constants;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * HBaseTable
 *
 * @since 2020-03-30
 */
public class HBaseTable
{
    private static final Logger LOG = Logger.get(HBaseTable.class);
    private final boolean external;
    private final Integer rowIdOrdinal;
    private String schema;
    private final String serializerClassName;
    private final List<ColumnMetadata> columnMetadatas;
    private final boolean indexed;
    private final List<HBaseColumnHandle> columns;
    private final String rowId;
    private String table;
    private final SchemaTableName schemaTableName;
    private String indexColumns;
    private Optional<String> hbaseTableName;
    private Optional<String> splitByChar;
    private Map<String, ColumnHandle> columnsMap;

    /**
     * constructor
     *
     * @param schema schema
     * @param table table
     * @param columns columns
     * @param rowId rowId
     * @param external external
     * @param serializerClassName serializerClassName
     * @param indexColumns indexColumns
     * @param hbaseTableName hbaseTableName
     * @param splitByChar splitByChar
     */
    @JsonCreator
    public HBaseTable(
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("columns") List<HBaseColumnHandle> columns,
            @JsonProperty("rowId") String rowId,
            @JsonProperty("external") boolean external,
            @JsonProperty("serializerClassName") Optional<String> serializerClassName,
            @JsonProperty("indexColumns") Optional<String> indexColumns,
            @JsonProperty("hbaseTableName") Optional<String> hbaseTableName,
            @JsonProperty("splitByChar") Optional<String> splitByChar)
    {
        this.external = external;
        this.rowId = requireNonNull(rowId, "rowId is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.table = requireNonNull(table, "table is null");
        this.columns = requireNonNull(columns, "columns are null");
        this.serializerClassName = serializerClassName.orElse("");
        this.indexColumns = indexColumns.orElse("");
        this.hbaseTableName = hbaseTableName;
        this.splitByChar = splitByChar;

        boolean flag = false;
        int rowIdOrdinal0 = 0;

        // Extract the ColumnMetadata from the handles for faster access
        columnMetadatas = new ArrayList<>(columns.size());
        for (HBaseColumnHandle column : this.columns) {
            columnMetadatas.add(column.getColumnMetadata());
            flag |= column.isIndexed();
            if (column.getName().equals(this.rowId)) {
                rowIdOrdinal0 = column.getOrdinal();
            }
        }

        this.rowIdOrdinal = rowIdOrdinal0;
        this.indexed = flag;
        this.schemaTableName = new SchemaTableName(this.schema, this.table);
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
    public String getTable()
    {
        return table;
    }

    /**
     * getIndexTableName
     *
     * @return index tableName
     */
    @JsonIgnore
    public String getIndexTableName()
    {
        return "";
    }

    /**
     * getMetricsTableName
     *
     * @return metrics tableName
     */
    @JsonIgnore
    public String getMetricsTableName()
    {
        return "";
    }

    /**
     * getFullTableName
     *
     * @return full tableName
     */
    @JsonIgnore
    public String getFullTableName()
    {
        return getFullTableName(schema, table);
    }

    /**
     * getColumnsMap
     *
     * @return columns map
     */
    @JsonProperty
    public Map<String, ColumnHandle> getColumnsMap()
    {
        return this.columnsMap;
    }

    /**
     * getSplitByChar
     *
     * @return all chars range
     */
    @JsonProperty
    public Optional<String> getSplitByChar()
    {
        return splitByChar;
    }

    /**
     * get table properties
     *
     * @return all table properties
     */
    @JsonProperty
    public Map<String, Object> getTableProperties()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put(HBaseTableProperties.EXTERNAL, isExternal());
        properties.put(HBaseTableProperties.INDEX_COLUMNS, getIndexColumns());
        properties.put(HBaseTableProperties.SPLIT_BY_CHAR, getSplitByChar().orElse(""));
        properties.put(HBaseTableProperties.HBASE_TABLE_NAME, getHbaseTableName().orElse(""));
        properties.put(HBaseTableProperties.ROW_ID, getRowId());
        properties.put(HBaseTableProperties.SERIALIZER, getSerializerClassName());
        return properties;
    }

    /**
     * setHbaseTableName
     *
     * @param hbaseTableName hbaseTableName
     */
    public void setHbaseTableName(Optional<String> hbaseTableName)
    {
        this.hbaseTableName = hbaseTableName;
    }

    /**
     * setColumnsToMap
     *
     * @param map column map
     */
    public void setColumnsToMap(Map<String, ColumnHandle> map)
    {
        this.columnsMap = map;
    }

    /**
     * getFullTableName
     *
     * @param schema schema
     * @param table table
     * @return String
     */
    @JsonIgnore
    public static String getFullTableName(String schema, String table)
    {
        // hbase table schema:table
        return schema + Constants.POINT + table;
    }

    /**
     * getFullTableName
     *
     * @param tableName tableName
     * @return String
     */
    @JsonIgnore
    public static String getFullTableName(SchemaTableName tableName)
    {
        return getFullTableName(tableName.getSchemaName(), tableName.getTableName());
    }

    @JsonProperty
    public List<HBaseColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public String getSerializerClassName()
    {
        return serializerClassName;
    }

    @JsonIgnore
    public List<ColumnMetadata> getColumnMetadatas()
    {
        return columnMetadatas;
    }

    @JsonProperty
    public boolean isExternal()
    {
        return external;
    }

    @JsonIgnore
    public boolean isIndexed()
    {
        return indexed;
    }

    @JsonIgnore
    public int getRowIdOrdinal()
    {
        return this.rowIdOrdinal;
    }

    @JsonIgnore
    public String getIndexColumns()
    {
        return indexColumns;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public void setTable(String table)
    {
        this.table = table;
    }

    @JsonIgnore
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schema)
                .add("tableName", table)
                .add("columns", columns)
                .add("rowIdName", rowId)
                .add("external", external)
                .add("serializerClassName", serializerClassName)
                .add("hbaseTableName", hbaseTableName)
                .add("splitByChar", splitByChar)
                .toString();
    }

    /**
     * parseHbaseTableToJson
     *
     * @return JSONObject
     * @throws JSONException JSONException
     */
    public JSONObject parseHbaseTableToJson()
            throws JSONException
    {
        JSONObject jo = new JSONObject();
        jo.put("schema", schema);
        jo.put("external", external);
        jo.put("rowIdOrdinal", rowIdOrdinal);
        jo.put("serializerClassName", serializerClassName);
        jo.put("indexed", indexed);
        jo.put("rowId", rowId);
        jo.put("table", table);
        jo.put("indexColumns", indexColumns);
        jo.put("hbaseTableName", hbaseTableName.orElse(""));
        jo.put("splitByChar", splitByChar.orElse(""));

        JSONArray jac = new JSONArray();
        JSONObject joc;
        for (HBaseColumnHandle hc : columns) {
            joc = hc.parseToJson();
            jac.put(joc);
        }

        jo.accumulate("columns", jac);

        return jo;
    }
}
