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

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.IndexFilter;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
                    CallExpression left = new CallExpression(sigLeft, specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), specialForm.getArguments().get(1)));
                    CallExpression right = new CallExpression(sigRight, specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), specialForm.getArguments().get(2)));
                    return matches(left) && matches(right);
                case IN:
                    Signature sigEqual = Signature.internalOperator(OperatorType.EQUAL,
                            specialForm.getType().getTypeSignature(),
                            specialForm.getArguments().get(1).getType().getTypeSignature());
                    for (RowExpression exp : specialForm.getArguments().subList(1, specialForm.getArguments().size())) {
                        if (matches(new CallExpression(sigEqual, specialForm.getType(), ImmutableList.of(specialForm.getArguments().get(0), exp)))) {
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
    public <I> Iterator<I> lookUp(Object expression)
    {
        return Collections.emptyIterator();
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
}
