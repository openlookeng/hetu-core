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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import io.airlift.log.Logger;
import io.prestosql.metadata.Split;
import io.prestosql.sql.tree.QualifiedName;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class SplitKey
{
    private static final Logger log = Logger.get(SplitKey.class);

    private final String catalog;
    private final String schema;
    private final String table;
    private long start;
    private long end;
    private long lastModifiedTime;
    private QualifiedName qualifiedTableName;
    private String path;

    public SplitKey(
            Split split,
            String catalog,
            String schema,
            String table)
    {
        requireNonNull(split, "split is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = schema;
        this.table = requireNonNull(table, "table is null");
        this.qualifiedTableName = QualifiedName.of(catalog, schema, table);

        this.start = split.getConnectorSplit().getStartIndex();
        this.end = split.getConnectorSplit().getEndIndex();
        this.path = split.getConnectorSplit().getFilePath();
        this.lastModifiedTime = split.getConnectorSplit().getLastModifiedTime();
    }

    @JsonCreator
    public SplitKey(
            @JsonProperty("catalog") String catalog,
            @JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("path") String path,
            @JsonProperty("startIndex") long start,
            @JsonProperty("endIndex") long end,
            @JsonProperty("lastModifiedTime") long lastModifiedTime)
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(lastModifiedTime >= 0, "lastModifiedTime must be positive");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema or database is null");
        this.table = requireNonNull(table, "table is null");
        this.path = requireNonNull(path, "path is null");
        this.qualifiedTableName = QualifiedName.of(catalog, schema, table);
        this.start = start;
        this.end = end;
        this.lastModifiedTime = lastModifiedTime;
    }

    @JsonProperty
    public String getCatalog()
    {
        return catalog;
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

    @JsonProperty
    public long getStartIndex()
    {
        return start;
    }

    @JsonProperty
    public long getEndIndex()
    {
        return end;
    }

    @JsonProperty
    public long getLastModifiedTime()
    {
        return lastModifiedTime;
    }

    @JsonProperty
    public String getPath()
    {
        return path;
    }

    public QualifiedName getQualifiedTableName()
    {
        return qualifiedTableName;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SplitKey)) {
            return false;
        }
        SplitKey splitKey = (SplitKey) o;
        return start == splitKey.start &&
                end == splitKey.end &&
                lastModifiedTime == splitKey.lastModifiedTime &&
                Objects.equals(catalog, splitKey.catalog) &&
                Objects.equals(schema, splitKey.schema) &&
                Objects.equals(table, splitKey.table) &&
                Objects.equals(qualifiedTableName, splitKey.qualifiedTableName) &&
                Objects.equals(path, splitKey.path);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, schema, table, start, end, lastModifiedTime, qualifiedTableName, path);
    }

    @Override
    public String toString()
    {
        //this method is also used for Serde when use in Map.
        //any changes to this method also requires appropriate changes in fromSerializedString() method
        return "SplitKey{" +
                "catalog='" + catalog + '\'' +
                ", schema='" + schema + '\'' +
                ", table='" + table + '\'' +
                ", start=" + start +
                ", end=" + end +
                ", lastModifiedTime=" + lastModifiedTime +
                ", qualifiedTableName=" + qualifiedTableName +
                ", path='" + path + '\'' +
                '}';
    }

    private static SplitKey fromSerializedString(String key) throws IOException
    {
        String splitKeyString = key;
        if (!splitKeyString.startsWith("SplitKey{")) {
            log.error("Cannot create SplitKey object from serialized value %s", splitKeyString);
            throw new IOException("Cannot create SplitKey instance from serialized value");
        }
        try {
            String catalog = null;
            String schema = null;
            String table = null;
            String path = null;
            long start = -1;
            long end = -1;
            long lastModifiedTime = 0;
            splitKeyString = splitKeyString.replace("SplitKey{", "").replace("}", "");
            String[] variableAndValues = splitKeyString.split(",");
            for (String variableAndValue : variableAndValues) {
                String[] tokens = variableAndValue.split("=", 2);
                String variable = tokens[0].trim();
                String value = tokens[1].replace("'", "").trim();
                switch (variable) {
                    case "catalog":
                        catalog = value;
                        break;
                    case "schema":
                        schema = value;
                        break;
                    case "table":
                        table = value;
                        break;
                    case "start":
                        start = Long.parseLong(value);
                        break;
                    case "end":
                        end = Long.parseLong(value);
                        break;
                    case "lastModifiedTime":
                        lastModifiedTime = Long.parseLong(value);
                        break;
                    case "path":
                        path = value;
                        break;
                }
            }
            return new SplitKey(catalog, schema, table, path, start, end, lastModifiedTime);
        }
        catch (Exception ex) {
            log.error(ex, "Unable to create SplitKey from serialized value %s", key);
            throw new IOException(ex);
        }
    }

    public static class KeyDeserializer
            extends com.fasterxml.jackson.databind.KeyDeserializer
    {
        @Override
        public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException
        {
            return fromSerializedString(key);
        }
    }
}
