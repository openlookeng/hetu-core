/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.cache.elements;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;
import io.prestosql.sql.tree.Query;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.stream.Collectors.toMap;

public class CachedDataKey
{
    public static final CachedDataKey NULL_KEY = CachedDataKey.builder().build();
    private final String name;
    private final String query;
    //private final Query query;
    private final Set<String> tables = new HashSet<>();
    private final Map<String, TypeId> columnTypes = new HashMap<>();
    private final Set<String> rules = new HashSet<>();

    @JsonCreator
    public CachedDataKey(@JsonProperty("name") String name,
                         @JsonProperty("query") String query,
                         @JsonProperty("tables") Set<String> tables,
                         @JsonProperty("columns") Map<String, TypeId> columnTypes,
                         @JsonProperty("rules") Set<String> rules)
    {
        this.name = name;
        this.query = query;
        if (tables != null) {
            this.tables.addAll(tables);
        }
        if (columnTypes != null) {
            this.columnTypes.putAll(columnTypes);
        }
        if (rules != null) {
            this.rules.addAll(rules);
        }
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Set<String> getTables()
    {
        return tables;
    }

    @JsonProperty
    public Map<String, TypeId> getColumnTypes()
    {
        return columnTypes;
    }

    @JsonProperty
    public Set<String> getRules()
    {
        return rules;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, tables, columnTypes, rules);
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
        CachedDataKey o = (CachedDataKey) obj;

        return Objects.equals(query, o.query)
                && Objects.equals(tables, o.tables)
                && Objects.equals(columnTypes, o.columnTypes)
                && Objects.equals(rules, o.rules);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("Name", name)
                .add("Query", query)
                .add("Tables", tables)
                .add("ColumnsWithType", columnTypes)
                .add("Rules", rules)
                .toString();
    }

    public static class Builder
    {
        private String name = "";
        private String query;
        private Set<String> tables = new HashSet<>();
        private Map<String, TypeId> columnTypes = new HashMap<>();
        private Set<String> rules = new HashSet<>();

        public Builder setQuery(Query query)
        {
            this.query = query.toString();
            return this;
        }

        public Builder addTableNames(String...tableNames)
        {
            for (String table : tableNames) {
                tables.add(table);
            }
            return this;
        }

        public Builder addTableName(String tableName)
        {
            tables.add(tableName);
            return this;
        }

        public Builder addColumn(String columnName, Type type)
        {
            this.columnTypes.put(columnName, type.getTypeId());
            return this;
        }

        public Builder addColumns(Map<String, Type> colTypes)
        {
            this.columnTypes.putAll(colTypes.entrySet()
                    .stream()
                    .collect(
                            toMap(es -> es.getKey(),
                                    es -> es.getValue().getTypeId())));
            return this;
        }

        public Builder addRules(String...rules)
        {
            for (String rule : rules) {
                addRule(rule);
            }
            return this;
        }

        public Builder addRule(String rule)
        {
            rules.add(rule);
            return this;
        }

        public Builder setName(String name)
        {
            this.name = name;
            return this;
        }

        public CachedDataKey build()
        {
            return new CachedDataKey(name, query, tables, columnTypes, rules);
        }
    }
}
