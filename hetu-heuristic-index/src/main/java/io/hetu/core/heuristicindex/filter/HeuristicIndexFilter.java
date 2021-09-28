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

package io.hetu.core.heuristicindex.filter;

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.algorithm.SequenceUtils;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexLookUpException;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class HeuristicIndexFilter
        implements IndexFilter
{
    Map<String, List<IndexMetadata>> indices;

    /**
     * Construct the filter with indexes.
     *
     * @param indices A map of column name to list of index. Only one column is supported for now.
     */
    public HeuristicIndexFilter(Map<String, List<IndexMetadata>> indices)
    {
        this.indices = indices;
    }

    @Override
    public boolean matches(Object expression)
    {
        // Only push ComparisonExpression to the actual indices
        if (expression instanceof CallExpression) {
            return matchAny((CallExpression) expression);
        }

        if (expression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) expression;
            switch (specialForm.getForm()) {
                case BETWEEN:
                    Signature sigLeft = Signature.internalOperator(OperatorType.GREATER_THAN_OR_EQUAL,
                            specialForm.getType().getTypeSignature(),
                            specialForm.getArguments().get(1).getType().getTypeSignature());
                    Signature sigRight = Signature.internalOperator(OperatorType.LESS_THAN_OR_EQUAL,
                            specialForm.getType().getTypeSignature(),
                            specialForm.getArguments().get(2).getType().getTypeSignature());
                    // todo remote udf, we should get FunctionHandle from FunctionAndTypeManager
                    CallExpression left = new CallExpression(OperatorType.GREATER_THAN_OR_EQUAL.name(), new BuiltInFunctionHandle(sigLeft), specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), specialForm.getArguments().get(1)), Optional.empty());
                    CallExpression right = new CallExpression(OperatorType.LESS_THAN_OR_EQUAL.name(), new BuiltInFunctionHandle(sigRight), specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), specialForm.getArguments().get(2)), Optional.empty());
                    return matches(left) && matches(right); // break it to (>= left and <= right)
                case IN:
                    Signature sigEqual = Signature.internalOperator(OperatorType.EQUAL,
                            specialForm.getType().getTypeSignature(),
                            specialForm.getArguments().get(1).getType().getTypeSignature());
                    for (RowExpression exp : specialForm.getArguments().subList(1, specialForm.getArguments().size())) {
                        // todo remote udf, we should get FunctionHandle from FunctionAndTypeManager
                        if (matches(new CallExpression(OperatorType.EQUAL.name(), new BuiltInFunctionHandle(sigEqual), specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), exp), Optional.empty()))) {
                            return true;
                        }
                    }
                    // None of the values in the IN-valueList matches any index
                    return false;
                case AND:
                    return matches(specialForm.getArguments().get(0)) && matches(specialForm.getArguments().get(1));
                case OR:
                    return matches(specialForm.getArguments().get(0)) || matches(specialForm.getArguments().get(1));
            }
        }

        // Not able to apply index filtering, just don't filter
        return true;
    }

    @Override
    public <I extends Comparable<I>> Iterator<I> lookUp(Object expression)
            throws IndexLookUpException
    {
        if (expression instanceof CallExpression) {
            return lookUpAll((RowExpression) expression);
        }
        if (expression instanceof SpecialForm) {
            SpecialForm specialForm = (SpecialForm) expression;
            switch (specialForm.getForm()) {
                case IN:
                case BETWEEN:
                    return lookUpAll((RowExpression) expression);
                case AND:
                    Iterator<I> iteratorAnd1 = lookUp(specialForm.getArguments().get(0));
                    Iterator<I> iteratorAnd2 = lookUp(specialForm.getArguments().get(1));

                    if (iteratorAnd1 == null && iteratorAnd2 == null) {
                        return null;
                    }
                    else if (iteratorAnd1 == null) {
                        return iteratorAnd2;
                    }
                    else if (iteratorAnd2 == null) {
                        return iteratorAnd1;
                    }
                    else {
                        return SequenceUtils.intersect(iteratorAnd1, iteratorAnd2);
                    }
                case OR:
                    Iterator<I> iteratorOr1 = lookUp(specialForm.getArguments().get(0));
                    Iterator<I> iteratorOr2 = lookUp(specialForm.getArguments().get(1));
                    if (iteratorOr1 == null || iteratorOr2 == null) {
                        throw new IndexLookUpException();
                    }
                    return SequenceUtils.union(iteratorOr1, iteratorOr2);
            }
        }

        throw new IndexLookUpException();
    }

    // Apply the indices on the expression. Currently only ComparisonExpression is supported
    private boolean matchAny(CallExpression callExp)
    {
        if (callExp.getArguments().size() != 2) {
            return true;
        }
        RowExpression varRef = callExp.getArguments().get(0);

        if (!(varRef instanceof VariableReferenceExpression)) {
            return true;
        }
        String columnName = ((VariableReferenceExpression) varRef).getName();

        List<IndexMetadata> selectedIndices = HeuristicIndexSelector.select(callExp, indices.get(columnName));

        if (selectedIndices == null || selectedIndices.isEmpty()) {
            return true;
        }

        for (IndexMetadata indexMetadata : selectedIndices) {
            if (indexMetadata == null || indexMetadata.getIndex() == null) {
                // Invalid index. Don't filter out
                return true;
            }

            try {
                if (indexMetadata.getIndex().matches(callExp)) {
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

    /**
     * Lookup all index available according to the expression and union the result.
     * <p>
     * It returns {@code null} as the special value for "universe" result U such that,
     * for any other results A: U \and A == A, U \or A == U.
     * <p>
     * If any of the index throws {@code IndexLookUpException} during lookup, it immediately break and return null.
     */
    private <T extends Comparable<T>> Iterator<T> lookUpAll(RowExpression expression)
    {
        RowExpression varRef = null;

        if (expression instanceof CallExpression) {
            varRef = ((CallExpression) expression).getArguments().get(0);
        }

        if (expression instanceof SpecialForm &&
                (((SpecialForm) expression).getForm() == SpecialForm.Form.BETWEEN || ((SpecialForm) expression).getForm() == SpecialForm.Form.IN)) {
            varRef = ((SpecialForm) expression).getArguments().get(0);
        }

        if (!(varRef instanceof VariableReferenceExpression)) {
            return null;
        }

        List<IndexMetadata> selectedIndex = HeuristicIndexSelector.select(expression, indices.get(((VariableReferenceExpression) varRef).getName()));

        if (selectedIndex.isEmpty()) {
            return null;
        }

        try {
            List<Iterator<T>> iterators = selectedIndex.parallelStream()
                    .map(indexMetadata -> {
                        try {
                            return (Iterator<T>) indexMetadata.getIndex().lookUp(expression);
                        }
                        catch (IndexLookUpException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .collect(Collectors.toList());

            return SequenceUtils.union(iterators);
        }
        catch (RuntimeException re) {
            return null;
        }
    }
}
