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

import io.prestosql.spi.sql.expression.Operators;
import io.prestosql.spi.sql.expression.OrderBy;
import io.prestosql.spi.sql.expression.QualifiedName;
import io.prestosql.spi.sql.expression.Selection;
import io.prestosql.spi.sql.expression.Time;
import io.prestosql.spi.sql.expression.Types;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public interface SqlQueryWriter
{
    /////////////////////////////// Following methods are for SQL expressions. ///////////////////////////////

    String groupByIdElement(List<List<String>> groSets);

    String row(List<String> expressions);

    String atTimeZone(String value, String timezone);

    String currentUser();

    String currentPath();

    String currentTime(Time.Function function, Integer precision);

    String extract(String expression, Time.ExtractField field);

    String booleanLiteral(boolean value);

    String stringLiteral(String value);

    String charLiteral(String value);

    String binaryLiteral(String hexValue);

    String parameter(Optional<List<String>> parameters, int position);

    String arrayConstructor(List<String> values);

    String subscriptExpression(String base, String index);

    String longLiteral(long value);

    String doubleLiteral(double value);

    String decimalLiteral(String value);

    String genericLiteral(String type, String value);

    String timeLiteral(String value);

    String timestampLiteral(String value);

    String nullLiteral();

    String intervalLiteral(Time.IntervalSign signLiteral, String value, Time.IntervalField startField, Optional<Time.IntervalField> endField);

    String subqueryExpression(String query);

    String exists(String subquery);

    String identifier(String value, boolean delimited);

    String lambdaArgumentDeclaration(String identifier);

    String dereferenceExpression(String base, String field);

    String fieldReference(int fieldIndex);

    String functionCall(QualifiedName name, boolean distinct, List<String> argumentsList, Optional<String> orderBy, Optional<String> filter, Optional<String> window);

    String lambdaExpression(List<String> arguments, String body);

    String bindExpression(List<String> values, String function);

    String logicalBinaryExpression(Operators.LogicalOperator operator, String left, String right);

    String notExpression(String value);

    String comparisonExpression(Operators.ComparisonOperator operator, String left, String right);

    String isNullPredicate(String value);

    String isNotNullPredicate(String value);

    String nullIfExpression(String first, String second);

    String ifExpression(String condition, String trueValue, Optional<String> falseValue);

    String tryExpression(String innerExpression);

    String coalesceExpression(List<String> operands);

    String arithmeticUnary(Operators.Sign sign, String value);

    String arithmeticBinary(Operators.ArithmeticOperator operator, String left, String right);

    String likePredicate(String value, String pattern, Optional<String> escape);

    String allColumns(Optional<QualifiedName> prefix);

    String cast(String expression, String type, boolean safe, boolean typeOnly);

    String searchedCaseExpression(List<String> whenCaluses, Optional<String> defaultValue);

    String simpleCaseExpression(String operand, List<String> whenCaluses, Optional<String> defaultValue);

    String whenClause(String operand, String result);

    String betweenPredicate(String value, String min, String max);

    String inPredicate(String value, String valueList);

    String inListExpression(List<String> values);

    String filter(String value);

    String formatWindowColumn(String functionName, List<String> args, String windows);

    String window(List<String> partitionBy, Optional<String> orderBy, Optional<String> frame);

    String windowFrame(Types.WindowFrameType type, String start, Optional<String> end);

    String frameBound(Types.FrameBoundType type, Optional<String> value);

    String quantifiedComparisonExpression(Operators.ComparisonOperator operator, Types.Quantifier quantifier, String value, String subquery);

    String groupingOperation(List<String> groupingColumns);

    String formatStringLiteral(String s);

    String joinExpressions(List<String> expressions);

    String orderBy(List<OrderBy> orders);

    String qualifiedName(String tableName, String symbolName);

    String queryAlias(String id);

    String formatIdentifier(Optional<Map<String, Selection>> qualifiedNames, String identifier);

    String formatQualifiedName(QualifiedName name);

    String formatBinaryExpression(String operator, String left, String right);

    String toNativeType(String type);

    boolean isBlacklistedFunction(String qualifiedName, int noOfArgs);
    ///////////////////////////// Following methods are for SQL statements /////////////////////////////

    /**
     * Select aliased symbols form the sub-query.
     * Output format: SELECT expression AS alias FROM from
     *
     * @param symbols aliased selections
     * @param from the sub-query
     * @return a SELECT statement
     */
    String select(List<Selection> symbols, String from);

    /**
     * Write a JOIN statement.
     * Output format: SELECT symbols FROM left JOIN TYPE right ON criteria
     *
     * @param symbols selecting symbols
     * @param type join type
     * @param left left side SQL query
     * @param right right side SQL query
     * @param criteria list of JOIN conditions
     * @param filter optional JOIN filter
     * @return the JOIN statement
     */
    String join(List<Selection> symbols, Types.JoinType type, String left, String leftId, String right, String rightId, List<String> criteria, Optional<String> filter);

    /**
     * Write an AGGREGATION statement.
     * Output format: SELECT expression AS alias FROM from GROUP BY groupingKeys
     *
     * @param symbols selecting symbols
     * @param groupingKeysOp grouping keys
     * @param groupIdElementOp group Id Element
     * @param from sub-query
     * @return the AGGREGATION statement
     */
    String aggregation(List<Selection> symbols, Optional<List<String>> groupingKeysOp, Optional<String> groupIdElementOp, String from);

    /**
     * Write a LIMIT statement.
     * Output format: SELECT symbols FROM from LIMIT count
     *
     * @param symbols selecting symbols
     * @param count the limit count
     * @param from sub-query
     * @return the LIMIT statement
     */
    String limit(List<Selection> symbols, long count, String from);

    /**
     * Write a FILTER statement.
     * Output format: SELECT symbols FROM from WHERE predicate
     *
     * @param symbols selecting symbols
     * @param predicate the condition
     * @param from sub-query
     * @return the FILTER statement
     */
    String filter(List<Selection> symbols, String predicate, String from);

    /**
     * Write an ORDER BY statement.
     * Output format: SELECT symbols FROM from ORDER BY x ASC, y DESC NULLS FIRST
     *
     * @param symbols selecting symbols
     * @param orderings the ordering symbols
     * @param from sub-query
     * @return the ORDER BY statement
     */
    String sort(List<Selection> symbols, List<OrderBy> orderings, String from);

    /**
     * Write an ORDER BY statement with LIMIT.
     * Output format: SELECT symbols FROM from ORDER BY x ASC, y DESC NULLS FIRST LIMIT count
     *
     * @param symbols selecting symbols
     * @param orderings the ordering symbols
     * @param count the limit count
     * @param from sub-query
     * @return the ORDER BY with LIMIT statement
     */
    String topN(List<Selection> symbols, List<OrderBy> orderings, long count, String from);

    /**
     * Write an UNION | INTERSECT | EXCEPT clause
     *
     * @param symbols selecting symbols
     * @param type set operator support
     * @param relations query of operator
     * @return
     */
    String setOperator(List<Selection> symbols, Types.SetOperator type, List<String> relations);
}
