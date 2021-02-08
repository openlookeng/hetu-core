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

package io.hetu.core.heuristicindex.filter;

import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.prestosql.sql.tree.ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN_OR_EQUAL;

public class HeuristicIndexFilter
        implements IndexFilter
{
    Map<String, List<IndexMetadata>> indices;

    public HeuristicIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        this.indices = indices;
    }

    @Override
    public boolean matches(Object expression)
    {
        // Only push ComparisonExpression to the actual indices
        if (expression instanceof ComparisonExpression) {
            return matchAny((ComparisonExpression) expression);
        }

        if (expression instanceof BetweenPredicate) {
            BetweenPredicate betweenPredicate = (BetweenPredicate) expression;
            ComparisonExpression left = new ComparisonExpression(GREATER_THAN_OR_EQUAL, betweenPredicate.getValue(), betweenPredicate.getMin());
            ComparisonExpression right = new ComparisonExpression(LESS_THAN_OR_EQUAL, betweenPredicate.getValue(), betweenPredicate.getMax());
            return matches(left) && matches(right);
        }

        if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;
            final LogicalBinaryExpression.Operator operator = lbExpression.getOperator();
            if (operator == LogicalBinaryExpression.Operator.AND) {
                return matches(lbExpression.getLeft()) && matches(lbExpression.getRight());
            }
            else if (operator == LogicalBinaryExpression.Operator.OR) {
                return matches(lbExpression.getLeft()) || matches(lbExpression.getRight());
            }
            else {
                throw new IllegalArgumentException("Unsupported logical expression type: " + operator);
            }
        }

        if (expression instanceof InPredicate) {
            Expression valueList = ((InPredicate) expression).getValueList();
            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;
                for (Expression expr : inListExpression.getValues()) {
                    ComparisonExpression oneValueCompExp = new ComparisonExpression(
                            ComparisonExpression.Operator.EQUAL, ((InPredicate) expression).getValue(), expr);
                    if (matchAny(oneValueCompExp)) {
                        return true;
                    }
                }
                // None of the values in the IN-valueList matches any index
                return false;
            }
        }

        // Not able to apply index filtering, just don't filter
        return true;
    }

    @Override
    public <I> Iterator<I> lookUp(Object expression)
    {
        return Collections.emptyIterator();
    }

    // Apply the indices on the expression. Currently only ComparisonExpression is supported
    private boolean matchAny(ComparisonExpression compExp)
    {
        if (!(compExp.getLeft() instanceof SymbolReference)) {
            return true;
        }

        String columnName = ((SymbolReference) compExp.getLeft()).getName();
        List<IndexMetadata> selectedIndices = HeuristicIndexSelector.select(compExp, indices.get(columnName));

        for (IndexMetadata indexMetadata : selectedIndices) {
            if (indexMetadata == null || indexMetadata.getIndex() == null) {
                // Invalid index. Don't filter out
                return true;
            }

            try {
                if (indexMetadata.getIndex().matches(compExp)) {
                    return true;
                }
            }
            catch (IllegalArgumentException e) {
                // Unable to apply the index. Don't filter out
                return true;
            }
        }

        // None of the index matches the expression
        return false;
    }
}
