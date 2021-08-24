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

import static io.prestosql.spi.connector.CreateIndexMetadata.AUTOLOAD_PROP_KEY;
import static java.util.Objects.requireNonNull;

public class IndexRecord
{
    // An index record in hetu-metastore's table entity's parameter map:
    // "HINDEX|<columns>|<indexType>" -> "name|user|properties|indexsize|partitions|lastModifiedTime"
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
    public final long indexSize;
    public final List<String> properties;
    public final List<String> partitions;
    public final long lastModifiedTime;

    public IndexRecord(String name, String user, String qualifiedTable, String[] columns, String indexType, long indexSize, List<String> properties, List<String> partitions)
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
        this.columns = Arrays.stream(columns).map(String::toLowerCase).toArray(String[]::new);
        this.indexType = indexType.toUpperCase(Locale.ENGLISH);
        this.indexSize = indexSize;
        this.properties = properties;
        this.partitions = partitions;
        this.lastModifiedTime = System.currentTimeMillis();
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
        JsonParser parser = new JsonParser();
        JsonObject values = parser.parse(metastoreEntry.getValue()).getAsJsonObject();

        this.name = requireNonNull(values.get("name"), "attribute 'name' should not be null").getAsString();
        this.user = requireNonNull(values.get("user"), "attribute 'user' should not be null").getAsString();
        this.indexSize = values.has("indexSize") ? values.get("indexSize").getAsLong() : 0;
        this.properties = new ArrayList<>();
        values.get("properties").getAsJsonArray().forEach(e -> this.properties.add(e.getAsString()));
        this.partitions = new ArrayList<>();
        values.get("partitions").getAsJsonArray().forEach(e -> this.partitions.add(e.getAsString()));
        this.lastModifiedTime = requireNonNull(values.get("lastModifiedTime"), "attribute 'lastModifiedTime' should not be null").getAsLong();
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
        content.put("indexSize", indexSize);
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

    public boolean isAutoloadEnabled()
    {
        return Boolean.parseBoolean(getProperty(AUTOLOAD_PROP_KEY));
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
                indexSize == that.indexSize &&
                Objects.equals(indexType, that.indexType);
    }

    @Override
    public int hashCode()
    {
        int result = Objects.hash(name, user, catalog, schema, table, qualifiedTable, indexType, indexSize, properties, partitions, lastModifiedTime);
        result = 31 * result + Arrays.hashCode(columns);
        return result;
    }

    @Override
    public String toString()
    {
        return "IndexRecord{" +
                "name='" + name + '\'' +
                ", user='" + user + '\'' +
                ", catalog='" + catalog + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", qualifiedTable='" + qualifiedTable + '\'' +
                ", columns=" + Arrays.toString(columns) +
                ", indexType='" + indexType + '\'' +
                ", indexSize=" + indexSize +
                ", properties=" + properties +
                ", partitions=" + partitions +
                ", lastModifiedTime=" + lastModifiedTime +
                '}';
    }
}
