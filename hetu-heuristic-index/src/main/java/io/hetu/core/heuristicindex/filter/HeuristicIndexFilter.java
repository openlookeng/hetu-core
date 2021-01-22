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

import io.hetu.core.common.algorithm.SequenceUtils;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexLookUpException;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
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
            LogicalBinaryExpression.Operator operator = lbExpression.getOperator();
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
    public <I extends Comparable<I>> Iterator<I> lookUp(Object expression)
            throws IndexLookUpException
    {
        if (expression instanceof ComparisonExpression || expression instanceof InPredicate || expression instanceof BetweenPredicate) {
            return lookUpAll((Expression) expression);
        }

        if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression lbExpression = (LogicalBinaryExpression) expression;
            LogicalBinaryExpression.Operator operator = lbExpression.getOperator();
            if (operator == LogicalBinaryExpression.Operator.AND) {
                Iterator<I> iterator1 = lookUp(lbExpression.getLeft());
                Iterator<I> iterator2 = lookUp(lbExpression.getRight());

                if (iterator1 == null && iterator2 == null) {
                    return null;
                }
                else if (iterator1 == null) {
                    return iterator2;
                }
                else if (iterator2 == null) {
                    return iterator1;
                }
                else {
                    return SequenceUtils.intersect(iterator1, iterator2);
                }
            }
            else if (operator == LogicalBinaryExpression.Operator.OR) {
                Iterator<I> iterator1 = lookUp(lbExpression.getLeft());
                Iterator<I> iterator2 = lookUp(lbExpression.getRight());
                if (iterator1 == null || iterator2 == null) {
                    throw new IndexLookUpException();
                }
                return SequenceUtils.union(iterator1, iterator2);
            }
        }

        throw new IndexLookUpException();
    }

    private static Expression extractExpression(Expression expression)
    {
        if (expression instanceof Cast) {
            // extract the inner expression for CAST expressions
            return extractExpression(((Cast) expression).getExpression());
        }
        else {
            return expression;
        }
    }

    // Apply the indices on the expression. Currently only ComparisonExpression is supported
    private boolean matchAny(ComparisonExpression compExp)
    {
        Expression left = extractExpression(compExp.getLeft());

        if (!(left instanceof SymbolReference)) {
            return true;
        }

        String columnName = ((SymbolReference) left).getName();
        List<IndexMetadata> selectedIndices = HeuristicIndexSelector.select(compExp, indices.get(columnName));

        if (selectedIndices == null || selectedIndices.isEmpty()) {
            return true;
        }

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
            catch (UnsupportedOperationException e) {
                // Unable to apply the index. Don't filter out
                return true;
            }
        }

        // None of the index matches the expression
        return false;
    }

    private <T extends Comparable<T>> Iterator<T> lookUpAll(Expression expression)
    {
        Expression left = null;

        if (expression instanceof ComparisonExpression) {
            left = extractExpression(((ComparisonExpression) expression).getLeft());
        }

        if (expression instanceof BetweenPredicate) {
            left = extractExpression(((BetweenPredicate) expression).getValue());
        }

        if (expression instanceof InPredicate) {
            left = extractExpression(((InPredicate) expression).getValue());
        }

        if (!(left instanceof SymbolReference)) {
            return null;
        }

        List<IndexMetadata> selectedIndex = HeuristicIndexSelector.select(expression, indices.get(((SymbolReference) left).getName()));

        if (selectedIndex.isEmpty()) {
            return null;
        }

        List<Iterator<T>> iterators = new ArrayList<>(selectedIndex.size());

        for (IndexMetadata indexMetadata : selectedIndex) {
            iterators.add((indexMetadata.getIndex()).lookUp(expression));
        }

        return SequenceUtils.union(iterators);
    }
}
