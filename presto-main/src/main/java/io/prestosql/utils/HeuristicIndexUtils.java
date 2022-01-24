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
package io.prestosql.utils;

import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HeuristicIndexUtils
{
    private HeuristicIndexUtils()
    {
    }

    /**
     * Given one of these expressions types:
     * - partition=1
     * - partition=1 or partition=2
     * - partition in (1,2)
     * - partition=1 or partition in (2)
     *
     * extract the partitions as a list of key=value pairs
     * @param expression
     * @return
     */
    public static List<String> extractPartitions(Expression expression)
    {
        if (expression instanceof ComparisonExpression) {
            ComparisonExpression exp = (ComparisonExpression) expression;

            if (exp.getOperator() == ComparisonExpression.Operator.EQUAL) {
                return Collections.singletonList(parsePartitionName(exp.getLeft().toString()) + "=" + parsePartitionValue(exp.getRight().toString()));
            }
        }
        else if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression exp = (LogicalBinaryExpression) expression;

            if (exp.getOperator() == LogicalBinaryExpression.Operator.OR) {
                Expression left = exp.getLeft();
                Expression right = exp.getRight();
                return Stream.concat(extractPartitions(left).stream(), extractPartitions(right).stream()).collect(Collectors.toList());
            }
        }
        else if (expression instanceof InPredicate) {
            Expression valueList = ((InPredicate) expression).getValueList();
            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;
                List<String> res = new LinkedList<>();
                for (Expression expr : inListExpression.getValues()) {
                    res.add(parsePartitionName(((InPredicate) expression).getValue().toString()) + "=" + parsePartitionValue(expr.toString()));
                }
                if (res.size() > 0) {
                    return res;
                }
            }
        }

        throw new ParsingException("Unsupported WHERE expression. Only in-predicate/equality-expressions are supported " +
                "e.g. partition=1 or partition=2/partition in (1,2)");
    }

    private static String parsePartitionValue(String rightVal)
    {
        // quoted value, e.g. 'value'
        if (rightVal.matches("^'.*'$")) {
            return rightVal.substring(1, rightVal.length() - 1);
        }
        // typed value
        // e.g. DATE '1234' or BIGINT '5'
        else if (rightVal.matches("^.*\\s'.*'$")) {
            return rightVal.replaceAll("^.*\\s*'(.*)'$", "$1");
        }

        return rightVal;
    }

    private static String parsePartitionName(String leftVal)
    {
        // quoted, e.g. "a"
        if (leftVal.startsWith("\"")) {
            return leftVal.substring(1, leftVal.length() - 1).trim();
        }

        // unquoted
        return leftVal;
    }
}
