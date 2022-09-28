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

package io.hetu.core.plugin.singledata.optimization;

import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.hetu.core.plugin.singledata.SingleDataUtils.quote;
import static java.util.stream.Collectors.joining;

public class SingleDataPushDownContext
{
    private JdbcTableHandle tableHandle;
    private CatalogName catalogName;
    private ConnectorTransactionHandle transaction;
    private Map<Symbol, ColumnHandle> columns;
    private Optional<String> filter = Optional.empty();
    private Optional<RowExpression> noPushableFilter = Optional.empty();
    private OptionalLong limit = OptionalLong.empty();
    private String joinSql;
    private boolean hasJoin;
    private boolean hasFilter;

    public SingleDataPushDownContext() {}

    public JdbcTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public SingleDataPushDownContext setTableHandle(JdbcTableHandle tableHandle)
    {
        this.tableHandle = tableHandle;
        return this;
    }

    public CatalogName getCatalogName()
    {
        return catalogName;
    }

    public SingleDataPushDownContext setCatalogName(CatalogName catalogName)
    {
        this.catalogName = catalogName;
        return this;
    }

    public ConnectorTransactionHandle getTransaction()
    {
        return transaction;
    }

    public SingleDataPushDownContext setTransaction(ConnectorTransactionHandle transaction)
    {
        this.transaction = transaction;
        return this;
    }

    public Map<Symbol, ColumnHandle> getColumns()
    {
        return columns;
    }

    public SingleDataPushDownContext setColumns(Map<Symbol, ColumnHandle> columns)
    {
        this.columns = columns;
        return this;
    }

    public Optional<String> getFilter()
    {
        return filter;
    }

    public SingleDataPushDownContext setFilter(String filter)
    {
        this.filter = Optional.of(filter);
        return this;
    }

    public String getJoinSql()
    {
        return joinSql;
    }

    public SingleDataPushDownContext setJoinSql(String joinSql)
    {
        this.joinSql = joinSql;
        return this;
    }

    public boolean isHasJoin()
    {
        return hasJoin;
    }

    public SingleDataPushDownContext setHasJoin(boolean hasJoin)
    {
        this.hasJoin = hasJoin;
        return this;
    }

    public OptionalLong getLimit()
    {
        return limit;
    }

    public SingleDataPushDownContext setLimit(long limit)
    {
        this.limit = OptionalLong.of(limit);
        return this;
    }

    public boolean isHasFilter()
    {
        return hasFilter;
    }

    public SingleDataPushDownContext setHasFilter(boolean hasFilter)
    {
        this.hasFilter = hasFilter;
        return this;
    }

    public Optional<RowExpression> getNoPushableFilter()
    {
        return noPushableFilter;
    }

    public SingleDataPushDownContext setNoPushableFilter(RowExpression noPushableFilter)
    {
        this.noPushableFilter = Optional.of(noPushableFilter);
        return this;
    }

    public String getSelectionExpression()
    {
        if (columns == null || columns.isEmpty()) {
            return "null";
        }
        return columns.entrySet().stream().map(entry -> entry.getValue().getColumnName() + " AS " + quote(entry.getKey().getName())).collect(joining(", "));
    }
}
