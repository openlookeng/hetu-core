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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionConverter;
import io.prestosql.spi.sql.SqlStatementWriter;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;

import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.DERIVED_TABLE_PREFIX;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.JOIN_LEFT_TABLE_PREFIX;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.JOIN_RIGHT_TABLE_PREFIX;
import static io.prestosql.plugin.jdbc.optimization.JdbcPlanOptimizerUtils.parentheses;

public class BaseJdbcSqlStatementWriter
        implements SqlStatementWriter
{
    private static final String COUNT_FUNCTION_NAME = "count";
    private final boolean nameCaseInsensitive;
    private final JdbcPushDownParameter jdbcPushDownParameter;

    public BaseJdbcSqlStatementWriter(JdbcPushDownParameter pushDownParameter)
    {
        this.nameCaseInsensitive = pushDownParameter.getCaseInsensitiveParameter();
        this.jdbcPushDownParameter = pushDownParameter;
    }

    @Override
    public String select(List<Selection> selections)
    {
        StringBuilder builder = new StringBuilder("SELECT ");
        if (selections == null || selections.size() == 0) {
            builder.append("null");
        }
        else {
            StringJoiner joiner = new StringJoiner(", ");
            for (Selection selection : selections) {
                if (selection.isAliased(!nameCaseInsensitive)) {
                    joiner.add(selection.getExpression() + " AS " + selection.getAlias());
                }
                else {
                    joiner.add(selection.getExpression());
                }
            }
            builder.append(joiner);
        }
        return builder.toString();
    }

    @Override
    public String from(String selections, String from)
    {
        return selections + " FROM " + from;
    }

    @Override
    public String filter(String table, String predicate)
    {
        return table + " WHERE " + predicate;
    }

    @Override
    public String groupBy(String table, Set<String> groupBy)
    {
        StringJoiner joiner = new StringJoiner(", ");
        for (String symbol : groupBy) {
            joiner.add(symbol);
        }
        return table + " GROUP BY " + joiner.toString();
    }

    @Override
    public String orderBy(String table, List<OrderBy> orderings)
    {
        StringJoiner joiner = new StringJoiner(", ");
        for (OrderBy orderBy : orderings) {
            StringJoiner orderItem = new StringJoiner(" ");
            orderItem.add(orderBy.getSymbol());
            SortOrder sortOrder = orderBy.getType();
            orderItem.add(sortOrder.isAscending() ? "ASC" : "DESC");
            orderItem.add(sortOrder.isNullsFirst() ? "NULLS FIRST" : "NULLS LAST");
            joiner.merge(orderItem);
        }
        return table + " ORDER BY " + joiner.toString();
    }

    @Override
    public String limit(String table, long count)
    {
        return table + " LIMIT " + count;
    }

    @Override
    public String windowFrame(Types.WindowFrameType type, String start, Optional<String> end)
    {
        StringBuilder builder = new StringBuilder();

        builder.append(type.toString()).append(' ');

        if (end.isPresent()) {
            builder.append("BETWEEN ")
                    .append(start)
                    .append(" AND ")
                    .append(end.get());
        }
        else {
            builder.append(start);
        }

        return builder.toString();
    }

    @Override
    public String window(String functionName, List<String> functionArgs, List<String> partitionBy, Optional<String> orderBy, Optional<String> frame)
    {
        List<String> parts = new ArrayList<>();
        if (!partitionBy.isEmpty()) {
            parts.add("PARTITION BY " + Joiner.on(", ").join(partitionBy));
        }
        orderBy.ifPresent(parts::add);
        frame.ifPresent(parts::add);
        String windows = '(' + Joiner.on(' ').join(parts) + ')';

        String arguments = (functionArgs.size() == 0 && functionName.equals(COUNT_FUNCTION_NAME)) ? "*" : Joiner.on(", ").join(functionArgs);
        return functionName + '(' + arguments + ')' + " OVER " + windows;
    }

    @Override
    public String aggregation(String functionName, List<String> arguments, boolean isDistinct)
    {
        String params = (arguments.size() == 0 && functionName.equals(COUNT_FUNCTION_NAME)) ? "*" : Joiner.on(", ").join(arguments);
        if (isDistinct) {
            params = "DISTINCT " + params;
        }
        return functionName + parentheses(params);
    }

    @Override
    public String castAggregationType(String aggregationExpression, RowExpressionConverter converter, Type returnType)
    {
        VariableReferenceExpression aggVariable = new VariableReferenceExpression(aggregationExpression, returnType);
        return converter.visitCall(new CallExpression(OperatorType.CAST.name(), jdbcPushDownParameter.getFunctionResolution().castFunction(returnType.getTypeSignature(), returnType.getTypeSignature()), returnType, ImmutableList.of(aggVariable), Optional.empty()), new JdbcConverterContext());
    }

    @Override
    public String join(String joinType, String leftTable, String rightTable, List<String> criteria, Optional<String> filter, int identifier)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(parentheses(leftTable)).append(' ').append(JOIN_LEFT_TABLE_PREFIX).append(identifier)
                .append(' ').append(joinType).append(' ')
                .append(parentheses(rightTable)).append(' ').append(JOIN_RIGHT_TABLE_PREFIX).append(identifier);

        if (!criteria.isEmpty() || filter.isPresent()) {
            builder.append(" ON ");
            StringJoiner joiner = new StringJoiner(" AND ");
            criteria.forEach(joiner::add);
            filter.ifPresent(joiner::add);
            builder.append(joiner);
        }
        return parentheses(builder.toString());
    }

    @Override
    public String union(List<String> relations, int identifier)
    {
        StringJoiner joiner = new StringJoiner(" UNION ALL ", "(", ") " + DERIVED_TABLE_PREFIX + identifier);
        for (String relation : relations) {
            joiner.add(parentheses(relation));
        }
        return joiner.toString();
    }

    @Override
    public String groupingsSets(List<List<String>> groupSets)
    {
        StringJoiner joiner = new StringJoiner(", ", "(", ")");
        for (List<String> group : groupSets) {
            joiner.add(parentheses(Joiner.on(", ").join(group)));
        }
        return " GROUPING SETS " + joiner.toString();
    }
}
