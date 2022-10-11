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

package io.hetu.core.plugin.singledata.tidrange;

import io.hetu.core.plugin.singledata.optimization.SingleDataPushDownContext;

public class TidRangeUtils
{
    public static String getRelationSizeSql(String tableName)
    {
        return "SELECT pg_relation_size('" + tableName + "')";
    }

    public static String getTidRangeFilter(long start, long end)
    {
        return "ctid BETWEEN '(" + start + ", 1)' AND '(" + end + ", 0)'";
    }

    public static String getIndexSql(String tableName)
    {
        return "SELECT * FROM pg_indexes WHERE tablename = '" + tableName + "'";
    }

    public static String getSplitSqlFromContext(SingleDataPushDownContext context, String splitFilter)
    {
        StringBuilder sqlBuilder = new StringBuilder("SELECT ")
                .append(context.getSelectionExpression())
                .append(" FROM ")
                .append(context.getTableHandle().getTableName())
                .append(" WHERE ")
                .append(splitFilter);
        if (context.getFilter().isPresent()) {
            sqlBuilder.append(" AND ").append(context.getFilter().get());
        }
        if (context.getLimit().isPresent()) {
            sqlBuilder.append(" LIMIT ").append(context.getLimit().getAsLong());
        }
        return sqlBuilder.toString();
    }

    private TidRangeUtils() {}
}
