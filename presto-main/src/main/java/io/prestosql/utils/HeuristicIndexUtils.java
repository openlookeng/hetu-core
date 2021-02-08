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
package io.prestosql.utils;

import io.prestosql.sql.parser.ParsingException;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LogicalBinaryExpression;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HeuristicIndexUtils
{
    private HeuristicIndexUtils()
    {
    }

    public static List<String> extractPartitions(Expression expression)
    {
        if (expression instanceof ComparisonExpression) {
            ComparisonExpression exp = (ComparisonExpression) expression;

            if (exp.getOperator() != ComparisonExpression.Operator.EQUAL) {
                throw new ParsingException("Unsupported WHERE expression. Only equality expressions are supported with OR operator, " +
                        "e.g. partition=1, partition=1 OR partition=2");
            }
            // check predicate column is validate partition column.

            return Collections.singletonList(exp.getLeft().toString() + "=" + parseSpecialPartitionValues(exp.getRight().toString()));
        }
        else if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression exp = (LogicalBinaryExpression) expression;

            if (exp.getOperator() != LogicalBinaryExpression.Operator.OR) {
                throw new ParsingException("Unsupported WHERE expression. Only equality expressions are supported with OR operator. " +
                        "e.g. partition=1, partition=1 OR partition=2");
            }

            Expression left = exp.getLeft();
            Expression right = exp.getRight();
            return Stream.concat(extractPartitions(left).stream(), extractPartitions(right).stream()).collect(Collectors.toList());
        }
        else {
            throw new ParsingException("Unsupported WHERE expression. Only equality expressions are supported with OR operator. " +
                    "e.g. partition=1, partition=1 OR partition=2");
        }
    }

    private static String parseSpecialPartitionValues(String rightVal)
    {
        if (rightVal.matches("^'.*'$")) {
            return rightVal.substring(1, rightVal.length() - 1);
        }

        else if (rightVal.matches("^date\\s'.*'$")) {
            return rightVal.replaceAll("^date\\s*'(.*)'$", "$1");
        }

        return rightVal;
    }
}
