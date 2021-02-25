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
package io.prestosql.spi.sql;

import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Types;
import io.prestosql.spi.type.Type;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public interface SqlStatementWriter
{
    /**
     * Write Select aliased symbols.
     * Output format: SELECT selections
     *
     * @param selections aliased selections
     * @return a SELECT statement
     */
    String select(List<Selection> selections);

    /**
     * from expression
     * Output format: selections FROM from
     *
     * @param selections selections
     * @param from the sub-query
     * @return a SELECT statement
     */
    String from(String selections, String from);

    /**
     * Write a FILTER statement.
     * Output format: SELECT symbols FROM from WHERE predicate
     *
     * @param table input table
     * @param predicate the condition
     * @return the FILTER statement
     */
    String filter(String table, String predicate);

    /**
     * write a GROUP BY statement
     * Output format: table GROUP BY groupBy
     *
     * @param table table
     * @param groupBy group by symbols
     * @return the GROUP BY statement
     */
    String groupBy(String table, Set<String> groupBy);

    /**
     * Write an ORDER BY statement.
     * Output format: SELECT symbols FROM from ORDER BY x ASC, y DESC NULLS FIRST
     *
     * @param table table
     * @param orderings the ordering symbols
     * @return the ORDER BY statement
     */
    String orderBy(String table, List<OrderBy> orderings);

    /**
     * Write a LIMIT statement.
     * Output format: SELECT symbols FROM from LIMIT count
     *
     * @param table the table
     * @param count the limit count
     * @return the LIMIT statement
     */
    String limit(String table, long count);

    /**
     * Write a window frame statement.
     * Output format: ROWS|RANGE BETWEEN frame_start AND frame_end
     *
     * @param type frame type
     * @param start frame start
     * @param end frame end
     * @return the window frame statement
     */
    String windowFrame(Types.WindowFrameType type, String start, Optional<String> end);

    /**
     * Write a window clause.
     * Output format: window_function(expression) OVER ([PARTITION BY part_list] [ORDER BY order_list] [{ ROWS|RANGE} BETWEEN frame_start AND frame_end])
     *
     * @param functionName window function name
     * @param functionArgs window function arguments
     * @param partitionBy partition by statement
     * @param orderBy order by statement
     * @param frame window frame statement
     * @return the window clause
     */
    String window(String functionName, List<String> functionArgs, List<String> partitionBy, Optional<String> orderBy, Optional<String> frame);

    /**
     * write a aggregation expression
     *
     * @param functionName aggregation function name
     * @param arguments aggregation function arguments
     * @param isDistinct is distinct
     * @return the aggregation expression
     */
    String aggregation(String functionName, List<String> arguments, boolean isDistinct);

    /**
     * write a join statement
     *
     * @param joinType joinType
     * @param leftTable leftTable
     * @param rightTable rightTable
     * @param criteria join criteria
     * @param filter join filter
     * @param identifier join table identifier
     * @return the join statement
     */
    String join(String joinType, String leftTable, String rightTable, List<String> criteria, Optional<String> filter, int identifier);

    /**
     * write a union statement
     *
     * @param relations union relations
     * @param identifier union table identifier
     * @return the union statement
     */
    String union(List<String> relations, int identifier);

    /**
     * write a grouping sets expression
     *
     * @param groupSets grouping sets
     * @return the grouping sets expression
     */
    String groupingsSets(List<List<String>> groupSets);

    /**
     * this interface is use for deal with connector's aggregation function return a different type from hetu's aggregation
     * function, CAST connector's type to hetu type. Return original expression by default.
     *
     * @param aggregationExpression aggregation function expression
     * @param converter rowExpression converter
     * @param returnType expected type
     * @return a aggregation expression with type cast
     */
    default String castAggregationType(String aggregationExpression, RowExpressionConverter converter, Type returnType)
    {
        return aggregationExpression;
    }
}
