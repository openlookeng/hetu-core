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
package io.hetu.core.plugin.hbase.client;

import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.Type;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.hetu.core.plugin.hbase.utils.ValueSetUtils.createValueSet;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

/**
 * TestUtils
 *
 * @since 2020-03-20
 */
public class TestUtils
{
    private TestUtils() {}

    /**
     * createColumnList
     */
    public static List<HBaseColumnHandle> createColumnList()
    {
        List<HBaseColumnHandle> list = new ArrayList<>();

        list.add(createHBaseColumnRowId("rowkey"));
        list.add(createHBaseColumnHandle("a", "f_a", "q_a", 1));
        list.add(createHBaseColumnHandle("b", "f_b", "q_d", 2));
        list.add(createHBaseColumnHandle("c", "f_c", "q_d", 3));
        list.add(createHBaseColumnHandle("d", "f_d", "q_d", 4));

        return list;
    }

    /**
     * createHBaseColumnRowId
     */
    public static HBaseColumnHandle createHBaseColumnRowId(String name)
    {
        HBaseColumnHandle hBaseColumnHandle =
                new HBaseColumnHandle(name, Optional.empty(), Optional.empty(), VARCHAR, 0, "HBase row ID", false);
        return hBaseColumnHandle;
    }

    /**
     * createHBaseColumnHandle
     */
    public static HBaseColumnHandle createHBaseColumnHandle(String name, String family, String qualifer, int ordinal)
    {
        HBaseColumnHandle hBaseColumnHandle =
                new HBaseColumnHandle(
                        name,
                        Optional.of(family),
                        Optional.of(qualifer),
                        VARCHAR,
                        ordinal,
                        "HBase column Optional[" + family + "]:Optional[" + qualifer + "]. Indexed: false",
                        false);
        return hBaseColumnHandle;
    }

    /**
     * createHBaseColumnHandle
     */
    public static HBaseColumnHandle createHBaseColumnHandle(String name, String family, String qualifier)
    {
        HBaseColumnHandle hBaseColumnHandle =
                new HBaseColumnHandle(
                        name,
                        Optional.of(family),
                        Optional.of(qualifier),
                        VARCHAR,
                        0,
                        "HBase column Optional[" + family + "]:Optional[" + qualifier + "]. Indexed: false",
                        false);
        return hBaseColumnHandle;
    }

    /**
     * createDomain
     */
    public static Domain createDomain(int i)
    {
        return Domain.create(createValueSet(i), false);
    }

    /**
     * createTupleDomain
     */
    public static TupleDomain createTupleDomain(int i)
    {
        Map<ColumnHandle, Domain> map = new HashMap<>();
        map.put(createHBaseColumnHandle("rowkey", "f", "rowkey"), createDomain(i));

        return TupleDomain.withColumnDomains(map);
    }

    /**
     * createHBaseTableHandle
     */
    public static HBaseTableHandle createHBaseTableHandle()
    {
        return new HBaseTableHandle(
                "hbase",
                "test_table",
                "rowkey",
                false,
                "io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer",
                Optional.of("test_table"),
                "",
                OptionalLong.empty());
    }

    /**
     * createHBaseTableHandle
     */
    public static HBaseTableHandle createHBaseTableHandle(String schema, String table)
    {
        return new HBaseTableHandle(
                schema,
                table,
                "rowkey",
                false,
                "StringRowSerializer",
                Optional.of(table),
                "",
                OptionalLong.empty());
    }

    /**
     * createConnectorTableMeta
     */
    public static ConnectorTableMetadata createConnectorTableMeta(String table)
    {
        ConnectorTableMetadata ctm =
                new ConnectorTableMetadata(
                        new SchemaTableName("hbase", table), createColumnMetadataList(), createProperties());

        return ctm;
    }

    /**
     * createConnectorTableMeta
     */
    public static ConnectorTableMetadata createConnectorTableMeta()
    {
        ConnectorTableMetadata ctm =
                new ConnectorTableMetadata(
                        new SchemaTableName("hbase", "test_table"), createColumnMetadataList(), createProperties());

        return ctm;
    }

    /**
     * createColumnMetadataList
     */
    public static List<ColumnMetadata> createColumnMetadataList()
    {
        List<ColumnMetadata> list = new ArrayList<>();

        ColumnMetadata cm = new ColumnMetadata("rowkey", VARCHAR);
        list.add(cm);

        return list;
    }

    /**
     * createProperties
     */
    public static Map<String, Object> createProperties()
    {
        Map<String, Object> properties = new HashMap<>();
        properties.put("column_mapping", "rowkey:f:rowkey,name:name:nick_name,age:age:lit_age,gender:gender:gender");
        properties.put("index_columns", "rowkey");
        properties.put("external", false);
        properties.put("serializer", "StringRowSerializer");
        properties.put("split_by_char", "0~9,a~z,A~Z");

        return properties;
    }

    /**
     * loadHBaseTableFromFile
     *
     * @throws NullPointerException JSONException
     */
    public static Map<String, HBaseTable> loadHBaseTableFromFile(JSONObject object)
    {
        Map<String, HBaseTable> hTableMetaMemory = new HashMap<>();
        Iterator<String> iterator = object.keys();

        try {
            while (iterator.hasNext()) {
                String key = iterator.next();
                JSONObject jHtable = object.getJSONObject(key);
                boolean external = jHtable.getBoolean("external");
                String schema = jHtable.getString("schema");
                String serializerClassName = jHtable.getString("serializerClassName");
                String table = jHtable.getString("table");
                String rowId = jHtable.getString("rowId");
                String hBaseTableName = jHtable.getString("hbaseTableName");

                List<HBaseColumnHandle> columns = new ArrayList<>();
                JSONArray jaColumns = jHtable.getJSONArray("columns");
                if (jaColumns.length() > 0) {
                    for (int i = 0; i < jaColumns.length(); i++) {
                        JSONObject jColumns = jaColumns.getJSONObject(i);
                        HBaseColumnHandle columnHandle =
                                new HBaseColumnHandle(
                                        jColumns.getString("name"),
                                        Optional.of(jColumns.getString("family")),
                                        Optional.of(jColumns.getString("qualifer")),
                                        createTypeByName(jColumns.getString("type")),
                                        jColumns.getInt("ordinal"),
                                        jColumns.getString("comment"),
                                        jColumns.getBoolean("indexed"));
                        columns.add(columnHandle);
                    }
                }

                HBaseTable hBaseTable =
                        new HBaseTable(
                                schema,
                                table,
                                columns,
                                rowId,
                                external,
                                Optional.of(serializerClassName),
                                Optional.of(table),
                                Optional.of(""),
                                Optional.of(""));
                hTableMetaMemory.put(key, hBaseTable);
            }
        }
        catch (NullPointerException | JSONException e) {
            e.printStackTrace();
        }
        return hTableMetaMemory;
    }

    /**
     * createTypeByName
     *
     * @throws ClassNotFoundException IllegalArgumentException IllegalAccessException
     */
    public static Type createTypeByName(String type)
    {
        try {
            Class clazz = Class.forName(type);
            Field[] fields = clazz.getFields();

            for (Field field : fields) {
                if (type.equals(field.getType().getName()) && (field.get(clazz) instanceof Type)) {
                    return (Type) field.get(clazz);
                }
            }
        }
        catch (ClassNotFoundException | IllegalArgumentException | IllegalAccessException e) {
            e.printStackTrace();
        }

        Type typeNull = null;
        return Optional.ofNullable(typeNull).orElse(typeNull);
    }
}
