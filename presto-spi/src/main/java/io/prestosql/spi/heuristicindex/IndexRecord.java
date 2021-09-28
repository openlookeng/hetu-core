/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.metastore.model.TableEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

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
    public final List<String> propertiesAsList;
    public final Properties propertiesAsProperties;
    public final List<String> partitions;
    public final long lastModifiedTime;

    public long memoryUsage;
    public long diskUsage;

    public void setMemoryUsage(long memoryUsage)
    {
        this.memoryUsage = memoryUsage;
    }

    public void setDiskUsage(long diskUsage)
    {
        this.diskUsage = diskUsage;
    }

    public IndexRecord(String name, String user, String qualifiedTable, String[] columns, String indexType, long indexSize, List<String> propertiesAsList, List<String> partitions)
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
        this.partitions = partitions;
        this.lastModifiedTime = System.currentTimeMillis();
        this.propertiesAsList = propertiesAsList;
        this.propertiesAsProperties = stringToProperties(propertiesAsList);
    }

    private Properties stringToProperties(List<String> properties)
    {
        Properties curProperties = new Properties();
        for (String prop : properties) {
            String key = prop.substring(0, prop.indexOf("="));
            String val = prop.substring(prop.indexOf("=") + 1);
            curProperties.setProperty(key, val);
        }
        return curProperties;
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

        this.name = requireNonNull(values.get("name"), "attribute 'name' should not be null. Index is in an invalid state, recreate the index to resolve the issue.").getAsString();
        this.user = requireNonNull(values.get("user"), "attribute 'user' should not be null. Index is in an invalid state, recreate the index to resolve the issue.").getAsString();
        this.indexSize = values.has("indexSize") ? values.get("indexSize").getAsLong() : 0;
        this.propertiesAsList = new ArrayList<>();
        requireNonNull(values.get("properties"), "attribute 'properties' should not be null. Index is in an invalid state, recreate the index to resolve the issue.").getAsJsonArray().forEach(e -> this.propertiesAsList.add(e.getAsString()));
        this.propertiesAsProperties = stringToProperties(this.propertiesAsList);
        this.partitions = new ArrayList<>();
        requireNonNull(values.get("partitions"), "attribute 'partitions' should not be null. Index is in an invalid state, recreate the index to resolve the issue.").getAsJsonArray().forEach(e -> this.partitions.add(e.getAsString()));
        this.lastModifiedTime = requireNonNull(values.get("lastModifiedTime"), "attribute 'lastModifiedTime' should not be null. Index is in an invalid state, recreate the index to resolve the issue.").getAsLong();
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
        content.put("properties", propertiesAsList);
        content.put("partitions", partitions);
        content.put("lastModifiedTime", lastModifiedTime);

        return gson.toJson(content);
    }

    public boolean isInProgressRecord()
    {
        return this.propertiesAsProperties.containsKey(INPROGRESS_PROPERTY_KEY);
    }

    public Properties getProperties()
    {
        return propertiesAsProperties;
    }

    public String getProperty(String key)
    {
        for (String property : this.propertiesAsList) {
            if (property.toLowerCase(Locale.ROOT).startsWith(key.toLowerCase(Locale.ROOT))) {
                String[] entry = property.split("=");
                return entry[1];
            }
        }
        return null;
    }

    public CreateIndexMetadata.Level getLevel()
    {
        String levelStr = this.getProperty(CreateIndexMetadata.LEVEL_PROP_KEY);

        if (levelStr == null || levelStr.isEmpty()) {
            throw new RuntimeException("IndexRecord's level property not found. The Index is in an invalid state and should be dropped.");
        }

        return CreateIndexMetadata.Level.valueOf(levelStr.toUpperCase(Locale.ROOT));
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
        int result = Objects.hash(name, user, catalog, schema, table, qualifiedTable, indexType, indexSize, propertiesAsList, partitions, lastModifiedTime);
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
                ", properties=" + propertiesAsList +
                ", partitions=" + partitions +
                ", lastModifiedTime=" + lastModifiedTime +
                '}';
    }
}
