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

package io.hetu.core.plugin.singledata;

import com.google.common.base.Joiner;
import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcTableHandle;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.joining;

public class SingleDataUtils
{
    public static final String OPENGAUSS_DRIVER_NAME = "org.opengauss.Driver";

    private static final String QUOTE = "\"";

    public static String rewriteQueryWithColumns(String query, List<JdbcColumnHandle> columns)
    {
        StringBuilder selectClause = new StringBuilder("SELECT ");
        if (columns.isEmpty()) {
            selectClause.append("null");
        }
        else {
            selectClause.append(columns.stream().map(JdbcColumnHandle::getColumnName).map(SingleDataUtils::quote).collect(joining(", ")));
        }
        selectClause.append(" FROM (").append(query).append(") AS hetu_temp_table");
        return selectClause.toString();
    }

    public static String quote(String name)
    {
        return QUOTE + name.replace(QUOTE, QUOTE + QUOTE) + QUOTE;
    }

    public static String buildPushDownSql(SingleDataPushDownContext context)
    {
        if (context.isHasJoin()) {
            return context.getJoinSql();
        }
        StringBuilder query = new StringBuilder("SELECT ")
                .append(context.getSelectionExpression())
                .append(" FROM ")
                .append(context.getTableHandle().getTableName());
        context.getFilter().ifPresent(filter -> query.append(" WHERE ").append(filter));
        context.getLimit().ifPresent(limit -> query.append(" LIMIT ").append(limit));
        return query.toString();
    }

    public static String buildQueryByTableHandle(JdbcTableHandle tableHandle)
    {
        StringBuilder builder = new StringBuilder("SELECT * FROM ");
        Joiner.on(".").skipNulls().appendTo(builder, tableHandle.getCatalogName(), tableHandle.getSchemaName(), tableHandle.getTableName());
        return builder.toString();
    }

    public static SingleDataSplit getSingleDataSplit(@Nullable String dataSourceName, String sql)
    {
        return new SingleDataSplit(null, null, "", "", "", "",
                System.nanoTime(), 1, Optional.empty(), dataSourceName, sql);
    }

    private SingleDataUtils() {}
}
