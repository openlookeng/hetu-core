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
package io.prestosql.plugin.jdbc.optimization;

import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.Selection;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class JdbcQueryGeneratorContext
{
    private final Optional<CatalogName> catalogName;
    private final Optional<SchemaTableName> schemaTableName;
    // cresponding to catalogName/schemaName/tableName to JdbcTableHandle
    private final String remoteCatalogName;
    private final String remoteSchemaName;
    private final String remoteTableName;
    private final Optional<ConnectorTransactionHandle> transaction;
    private final LinkedHashMap<String, Selection> selections;
    private final Set<String> groupByColumns;
    private final Optional<String> from;
    private final Optional<String> filter;
    private final OptionalLong limit;
    private final Optional<List<OrderBy>> orderBy;
    private final boolean hasPushDown;
    private final GroupIdNodeInfo groupIdNodeInfo;

    private JdbcQueryGeneratorContext(
            Optional<CatalogName> catalogName,
            Optional<SchemaTableName> schemaTableName,
            String remoteCatalogName,
            String remoteSchemaName,
            String remoteTableName,
            Optional<ConnectorTransactionHandle> transaction,
            Map<String, Selection> selections,
            Optional<String> from,
            Set<String> groupByColumns,
            Optional<String> filter,
            OptionalLong limit,
            Optional<List<OrderBy>> orderBy,
            GroupIdNodeInfo groupIdNodeInfo,
            boolean hasPushDown)
    {
        this.catalogName = catalogName;
        this.schemaTableName = schemaTableName;
        this.remoteCatalogName = remoteCatalogName;
        this.remoteSchemaName = remoteSchemaName;
        this.remoteTableName = requireNonNull(remoteTableName, "table name is null");
        this.transaction = transaction;
        this.selections = new LinkedHashMap<>(requireNonNull(selections, "selections can't be null"));
        this.from = requireNonNull(from, "from can't be null");
        this.groupByColumns = new HashSet<>(requireNonNull(groupByColumns, "groupByColumns can't be null. It could be empty if not available."));
        this.filter = requireNonNull(filter);
        this.limit = requireNonNull(limit, "limit is null");
        this.orderBy = orderBy;
        this.groupIdNodeInfo = groupIdNodeInfo;
        this.hasPushDown = hasPushDown;
    }

    public Optional<CatalogName> getCatalogName()
    {
        return catalogName;
    }

    public Optional<SchemaTableName> getSchemaTableName()
    {
        return schemaTableName;
    }

    public String getRemoteCatalogName()
    {
        return remoteCatalogName;
    }

    public String getRemoteSchemaName()
    {
        return remoteSchemaName;
    }

    public String getRemoteTableName()
    {
        return remoteTableName;
    }

    public Optional<ConnectorTransactionHandle> getTransaction()
    {
        return transaction;
    }

    public LinkedHashMap<String, Selection> getSelections()
    {
        return selections;
    }

    public Optional<String> getFrom()
    {
        return from;
    }

    public Set<String> getGroupByColumns()
    {
        return groupByColumns;
    }

    public Optional<String> getFilter()
    {
        return filter;
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    public Optional<List<OrderBy>> getOrderBy()
    {
        return orderBy;
    }

    public GroupIdNodeInfo getGroupIdNodeInfo()
    {
        return groupIdNodeInfo;
    }

    public boolean isHasPushDown()
    {
        return hasPushDown;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("selections", selections)
                .add("from", from)
                .add("filter", filter)
                .add("limit", limit)
                .add("groupByColumns", groupByColumns)
                .add("orderingSchema", orderBy)
                .toString();
    }

    public static class GroupIdNodeInfo
    {
        private boolean isGroupByComplexOperation;
        private Map<Symbol, String> groupingElementStore;

        GroupIdNodeInfo()
        {
            this.groupingElementStore = new HashMap<>();
        }

        public boolean isGroupByComplexOperation()
        {
            return isGroupByComplexOperation;
        }

        public void setGroupByComplexOperation(boolean groupByComplexOperation)
        {
            isGroupByComplexOperation = groupByComplexOperation;
        }

        public Map<Symbol, String> getGroupingElementStore()
        {
            return groupingElementStore;
        }

        public void setGroupingElementStore(Map<Symbol, String> groupingElementStore)
        {
            this.groupingElementStore = groupingElementStore;
        }
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder buildFrom(JdbcQueryGeneratorContext context)
    {
        return new Builder(context);
    }

    public static Builder buildAsNewTable(JdbcQueryGeneratorContext context)
    {
        return new Builder(context.getCatalogName(), context.getSchemaTableName(), context.getRemoteCatalogName(),
                context.getRemoteSchemaName(), context.getRemoteTableName(), context.getTransaction(), context.getGroupIdNodeInfo());
    }

    public static final class Builder
    {
        private Optional<CatalogName> catalogName;
        private Optional<SchemaTableName> schemaTableName;
        private String remoteCatalogName;
        private String remoteSchemaName;
        private String remoteTableName;
        private Optional<ConnectorTransactionHandle> transaction;
        private LinkedHashMap<String, Selection> selections = new LinkedHashMap<>();
        private Set<String> groupByColumns = new HashSet<>();
        private Optional<String> from = Optional.empty();
        private Optional<String> filter = Optional.empty();
        private OptionalLong limit = OptionalLong.empty();
        private Optional<List<OrderBy>> orderBy = Optional.empty();
        private GroupIdNodeInfo groupIdNodeInfo = new GroupIdNodeInfo();
        private boolean hasPushDown;

        public Builder() {}

        private Builder(JdbcQueryGeneratorContext context)
        {
            this.catalogName = context.getCatalogName();
            this.schemaTableName = context.getSchemaTableName();
            this.remoteCatalogName = context.getRemoteCatalogName();
            this.remoteSchemaName = context.getRemoteSchemaName();
            this.remoteTableName = context.getRemoteTableName();
            this.transaction = context.getTransaction();
            this.selections = context.getSelections();
            this.groupByColumns = context.getGroupByColumns();
            this.from = context.getFrom();
            this.filter = context.getFilter();
            this.limit = context.getLimit();
            this.orderBy = context.getOrderBy();
            this.hasPushDown = context.isHasPushDown();
            this.groupIdNodeInfo = context.getGroupIdNodeInfo();
        }

        private Builder(
                Optional<CatalogName> catalogName,
                Optional<SchemaTableName> schemaTableName,
                String remoteCatalogName,
                String remoteSchemaName,
                String remoteTableName,
                Optional<ConnectorTransactionHandle> transaction,
                GroupIdNodeInfo groupIdNodeInfo)
        {
            this.catalogName = catalogName;
            this.schemaTableName = schemaTableName;
            this.remoteCatalogName = remoteCatalogName;
            this.remoteSchemaName = remoteSchemaName;
            this.remoteTableName = remoteTableName;
            this.transaction = transaction;
            this.groupIdNodeInfo = groupIdNodeInfo;
        }

        public Builder setCatalogName(Optional<CatalogName> catalogName)
        {
            this.catalogName = catalogName;
            return this;
        }

        public Builder setSchemaTableName(Optional<SchemaTableName> schemaTableName)
        {
            this.schemaTableName = schemaTableName;
            return this;
        }

        public Builder setRemoteCatalogName(String catalogname)
        {
            this.remoteCatalogName = catalogname;
            return this;
        }

        public Builder setRemoteSchemaName(String schemaName)
        {
            this.remoteSchemaName = schemaName;
            return this;
        }

        public Builder setRemoteTablename(String tableName)
        {
            this.remoteTableName = tableName;
            return this;
        }

        public Builder setTransaction(Optional<ConnectorTransactionHandle> transaction)
        {
            this.transaction = transaction;
            return this;
        }

        public Builder setSelections(LinkedHashMap<String, Selection> selections)
        {
            this.selections = selections;
            return this;
        }

        public Builder setGroupByColumns(Set<String> groupByColumns)
        {
            this.groupByColumns = groupByColumns;
            return this;
        }

        public Builder setFrom(Optional<String> from)
        {
            this.from = from;
            return this;
        }

        public Builder setFilter(Optional<String> filter)
        {
            this.filter = filter;
            return this;
        }

        public Builder setLimit(OptionalLong limit)
        {
            this.limit = limit;
            return this;
        }

        public Builder setOrderBy(Optional<List<OrderBy>> orderBy)
        {
            this.orderBy = orderBy;
            return this;
        }

        public Builder setHasPushDown(boolean hasPushDown)
        {
            this.hasPushDown = hasPushDown;
            return this;
        }

        public Builder setOutputColumns(List<Symbol> outputColumns)
        {
            LinkedHashMap<String, Selection> newSelections = new LinkedHashMap<>();
            for (Symbol out : outputColumns) {
                // If column is group id column, skip it
                if (groupIdNodeInfo.getGroupingElementStore().containsKey(out)) {
                    continue;
                }
                newSelections.put(out.getName(), requireNonNull(selections.get(out.getName()),
                        "Cannot find the selection " + out.getName() + " in the original context."));
            }
            this.selections = newSelections;
            return this;
        }

        public Builder setGroupIdNodeInfo(GroupIdNodeInfo groupIdNodeInfo)
        {
            this.groupIdNodeInfo = groupIdNodeInfo;
            return this;
        }

        public JdbcQueryGeneratorContext build()
        {
            return new JdbcQueryGeneratorContext(
                    catalogName,
                    schemaTableName,
                    remoteCatalogName,
                    remoteSchemaName,
                    remoteTableName,
                    transaction,
                    selections,
                    from,
                    groupByColumns,
                    filter,
                    limit,
                    orderBy,
                    groupIdNodeInfo,
                    hasPushDown);
        }
    }
}
