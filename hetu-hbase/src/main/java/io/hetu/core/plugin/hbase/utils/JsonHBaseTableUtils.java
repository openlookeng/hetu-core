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
package io.hetu.core.plugin.hbase.utils;

import io.airlift.log.Logger;
import io.hetu.core.plugin.hbase.connector.HBaseColumnHandle;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.prestosql.spi.connector.ColumnHandle;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * JsonHBaseTableUtils
 *
 * @since 2020-03-30
 */
public class JsonHBaseTableUtils
{
    private static final Logger LOG = Logger.get(JsonHBaseTableUtils.class);

    private JsonHBaseTableUtils() {}

    /**
     * loadHBaseTablesFromJson
     *
     * @param hbaseTables hbaseTables map
     * @param jsonObject jsonObject
     */
    public static void loadHBaseTablesFromJson(Map<String, HBaseTable> hbaseTables, JSONObject jsonObject)
    {
        if (jsonObject == null) {
            return;
        }

        Iterator<String> iterator = jsonObject.keys();
        String key;
        List<HBaseColumnHandle> columns;
        JSONObject jsonHtable;
        Map<String, ColumnHandle> columnHandleMap;

        try {
            while (iterator.hasNext()) {
                columnHandleMap = new ConcurrentHashMap<>();
                key = iterator.next();
                jsonHtable = jsonObject.getJSONObject(key);
                columns = new ArrayList<>();
                JSONArray jaColumns = jsonHtable.getJSONArray("columns");
                if (jaColumns.length() > 0) {
                    for (int i = 0; i < jaColumns.length(); i++) {
                        JSONObject jsonColumns = jaColumns.getJSONObject(i);
                        HBaseColumnHandle columnHandle =
                                new HBaseColumnHandle(
                                        jsonColumns.getString("name"),
                                        Optional.of(jsonColumns.getString("family")),
                                        Optional.of(jsonColumns.getString("qualifer")),
                                        Utils.createTypeByName(jsonColumns.getString("type")),
                                        jsonColumns.getInt("ordinal"),
                                        jsonColumns.getString("comment"),
                                        jsonColumns.getBoolean("indexed"));
                        columns.add(columnHandle);
                        columnHandleMap.put(columnHandle.getName(), columnHandle);
                    }
                }

                HBaseTable hbaseTable =
                        new HBaseTable(
                                jsonHtable.getString("schema"),
                                jsonHtable.getString("table"),
                                columns,
                                jsonHtable.getString("rowId"),
                                jsonHtable.getBoolean("external"),
                                Optional.of(jsonHtable.getString("serializerClassName")),
                                Optional.of(jsonHtable.getString("indexColumns")),
                                Optional.of(jsonHtable.getString("hbaseTableName")));
                hbaseTable.setColumnsToMap(columnHandleMap);
                hbaseTables.put(key, hbaseTable);
            }
        }
        catch (JSONException e) {
            LOG.error("loadHbaseTableFromFile faild... cause by %s", e);
        }
    }

    /**
     * hbaseTablesMapToJson
     *
     * @param hbaseTablesMap hbaseTables Map
     * @return JSONObject
     */
    public static JSONObject hbaseTablesMapToJson(Map<String, HBaseTable> hbaseTablesMap)
    {
        JSONObject hbaseTablesJson = new JSONObject();

        try {
            for (Map.Entry<String, HBaseTable> entry : hbaseTablesMap.entrySet()) {
                hbaseTablesJson.put(entry.getKey(), entry.getValue().parseHbaseTableToJson());
            }
        }
        catch (JSONException e) {
            LOG.error("hbaseTablesMapToJson faild... cause by %s", e);
        }

        return hbaseTablesJson;
    }
}
