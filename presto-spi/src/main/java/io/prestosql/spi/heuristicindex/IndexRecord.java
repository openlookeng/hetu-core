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
package io.prestosql.spi.heuristicindex;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.prestosql.spi.metastore.model.TableEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class IndexRecord
{
    // An index record in hetu-metastore's table entity's parameter map:
    // "HINDEX|<columns>|<indexType>" -> "name|user|properties|partitions|lastModifiedTime"
    public static final String INDEX_METASTORE_PREFIX = "HINDEX";
    public static final String METASTORE_DELIMITER = "|";
    public static final String INPROGRESS_PROPERTY_KEY = "CreationInProgress";

    public final String name;
    public final String user;
    public final String catalog;
    public final String schema;
    public final String table;
    public final String qualifiedTable;
    public final String[] columns;
    public final String indexType;
    public final List<String> properties;
    public final List<String> partitions;
    public final long lastModifiedTime;

    public IndexRecord(String name, String user, String qualifiedTable, String[] columns, String indexType, List<String> properties, List<String> partitions)
    {
        this.name = name;
        this.user = user == null ? "" : user;
        String[] qualifiedNames = qualifiedTable.split("\\.");
        if (qualifiedNames.length != 3) {
            throw new IllegalArgumentException(String.format("Invalid table name: %s", qualifiedTable));
        }
        this.qualifiedTable = qualifiedTable;
        this.catalog = qualifiedNames[0];
        this.schema = qualifiedNames[1];
        this.table = qualifiedNames[2];
        this.columns = columns;
        this.indexType = indexType.toUpperCase(Locale.ENGLISH);
        this.properties = properties;
        this.partitions = partitions;
        this.lastModifiedTime = System.currentTimeMillis();
    }

    /**
     * Construct IndexRecord from line of csv record
     */
    public IndexRecord(String csvRecord)
    {
        String[] records = csvRecord.split("\\|", Integer.MAX_VALUE);
        this.name = records[0];
        this.user = records[1];
        String[] qualifiedNames = records[2].split("\\.");
        if (qualifiedNames.length != 3) {
            throw new IllegalArgumentException(String.format("Invalid table name: %s", records[2]));
        }
        this.qualifiedTable = records[2];
        this.catalog = qualifiedNames[0];
        this.schema = qualifiedNames[1];
        this.table = qualifiedNames[2];
        this.columns = records[3].split(",");
        this.indexType = records[4];
        this.properties = Arrays.stream(records[5].split(",")).filter(s -> !s.equals("")).collect(Collectors.toList());
        this.partitions = Arrays.stream(records[6].split(",")).filter(s -> !s.equals("")).collect(Collectors.toList());
        this.lastModifiedTime = Long.parseLong(records[7]);
    }

    /**
     * Construct IndexRecord from a MapEntry in TableEntity's parameters map in Hetu MetaStore
     */
    public IndexRecord(TableEntity tableEntity, Map.Entry<String, String> metastoreEntry)
    {
        this.catalog = tableEntity.getCatalogName();
        this.schema = tableEntity.getDatabaseName();
        this.table = tableEntity.getName();
        this.qualifiedTable = catalog + "." + schema + "." + table;

        // parse key (delimited strings)
        String[] keyParts = metastoreEntry.getKey().split("\\" + METASTORE_DELIMITER);
        if (keyParts.length != 3) {
            throw new IllegalArgumentException(String.format("Invalid hindex metastore key: %s", metastoreEntry.getKey()));
        }
        this.columns = keyParts[1].split(",");
        this.indexType = keyParts[2];

        // parse value (JSON)
        Gson gson = new Gson();
        JsonParser parser = new JsonParser();
        JsonObject values = parser.parse(metastoreEntry.getValue()).getAsJsonObject();
        this.name = values.get("name").getAsString();
        this.user = values.get("user").getAsString();
        this.properties = new ArrayList<>();
        values.get("properties").getAsJsonArray().forEach(e -> this.properties.add(e.getAsString()));
        this.partitions = new ArrayList<>();
        values.get("partitions").getAsJsonArray().forEach(e -> this.partitions.add(e.getAsString()));
        this.lastModifiedTime = values.get("lastModifiedTime").getAsLong();
    }

    public String serializeKey()
    {
        return String.format("%s|%s|%s", INDEX_METASTORE_PREFIX, String.join(",", columns), indexType);
    }

    public String serializeValue()
    {
        Gson gson = new Gson();
        Map<String, Object> content = new HashMap<>();
        content.put("name", name);
        content.put("user", user);
        content.put("properties", properties);
        content.put("partitions", partitions);
        content.put("lastModifiedTime", lastModifiedTime);

        return gson.toJson(content);
    }

    public boolean isInProgressRecord()
    {
        return this.properties.stream().anyMatch(property -> property.startsWith(INPROGRESS_PROPERTY_KEY));
    }

    public String getProperty(String key)
    {
        for (String property : properties) {
            if (property.toLowerCase(Locale.ROOT).startsWith(key.toLowerCase(Locale.ROOT))) {
                String[] entry = property.split("=");
                return entry[1];
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof IndexRecord)) {
            return false;
        }
        IndexRecord that = (IndexRecord) o;
        return Objects.equals(name, that.name) &&
                Objects.equals(user, that.user) &&
                Objects.equals(qualifiedTable, that.qualifiedTable) &&
                Arrays.equals(columns, that.columns) &&
                Objects.equals(indexType, that.indexType);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(name, user, qualifiedTable, columns, indexType);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    @Override
    public String toString()
    {
        return "IndexRecord{" +
                "name='" + name + '\'' +
                ", table='" + qualifiedTable + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", indexType='" + indexType + '\'' +
                ", lastModifiedTime=" + lastModifiedTime +
                '}';
    }
}
