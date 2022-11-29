/*
 * Copyright (c) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.prestosql.cache.elements;

import io.prestosql.spi.type.Type;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.WithQuery;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class CachedDataKey
{
    private final String name;
    private final Query query;
    private final Set<String> tableNames;
    private final Map<String, Type> columnTypes;
    private final Set<String> rules;

    public CachedDataKey(String name, Query query, Set<String> tables, Map<String, Type> columnTypes, Set<String> rules)
    {
        this.name = name;
        this.query = query;
        this.tableNames = tables;
        this.columnTypes = columnTypes;
        this.rules = rules;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(query, tableNames, columnTypes, rules);
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
                && Objects.equals(tableNames, o.tableNames)
                && Objects.equals(columnTypes, o.columnTypes)
                && Objects.equals(rules, o.rules);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("Query", query)
                .add("Tables", tableNames)
                .add("columnType", columnTypes)
                .add("rules", rules)
                .toString();
    }

    public static class Builder
    {
        private String name = "";
        private Query query;
        private Set<String> tables = new HashSet<>();
        private Map<String, Type> columnTypes = new HashMap<>();
        private Set<String> rules = new HashSet<>();

        public Builder setQuery(Query query)
        {
            this.query = query;
            return this;
        }

        public Builder addTableNames(String...tableNames)
        {
            for(String table : tableNames)
            {
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
            this.columnTypes.put(columnName, type);
            return this;
        }

        public Builder addColumns(Map<String, Type> colTypes)
        {
            this.columnTypes.putAll(colTypes);
            return this;
        }

        public Builder addRules(String...rules)
        {
            for(String rule : rules)
            {
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
