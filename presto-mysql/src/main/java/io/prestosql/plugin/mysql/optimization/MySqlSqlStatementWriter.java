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
package io.prestosql.plugin.mysql.optimization;

import io.prestosql.plugin.jdbc.optimization.BaseJdbcSqlStatementWriter;
import io.prestosql.plugin.jdbc.optimization.JdbcPushDownParameter;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.type.DoubleType;
import io.prestosql.spi.type.RealType;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.StringJoiner;

public class MySqlSqlStatementWriter
        extends BaseJdbcSqlStatementWriter
{
    public MySqlSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        super(pushDownParameter);
    }

    /**
     * MySql doesn't support NULLS FIRST & NULLS LAST in ORDER BY section, so
     * use ISNULL() replace it.
     * e.g.
     * select * from table order by id ASC NULLS FIRST -> select * from table order by ISNULL(id) DESC, id ASC
     * select * from table order by id ASC NULLS LAST -> select * from table order by ISNULL(id) ASC, id ASC
     *
     * @param table input table
     * @param orderings input ordering scheme
     * @return order by section
     */
    @Override
    public String orderBy(String table, List<OrderBy> orderings)
    {
        StringJoiner joiner = new StringJoiner(", ");
        for (OrderBy orderBy : orderings) {
            StringJoiner orderItem = new StringJoiner(" ");
            String orderByColumn = orderBy.getSymbol();
            orderItem.add("ISNULL(" + orderByColumn + ")");
            SortOrder sortOrder = orderBy.getType();
            orderItem.add(sortOrder.isNullsFirst() ? "DESC" : "ASC");
            joiner.merge(orderItem);
            orderItem = new StringJoiner(" ");
            orderItem.add(orderByColumn);
            orderItem.add(sortOrder.isAscending() ? "ASC" : "DESC");
            joiner.merge(orderItem);
        }
        return table + " ORDER BY " + joiner.toString();
    }

    @Override
    public String castAggregationType(String aggregationExpression, RowExpressionConverter converter, Type returnType)
    {
        if (returnType instanceof DoubleType || returnType instanceof RealType) {
            return aggregationExpression;
        }
        return super.castAggregationType(aggregationExpression, converter, returnType);
    }
}
