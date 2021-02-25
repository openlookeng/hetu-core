/*
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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;

    public AggregationFunctionMatcher(ExpectedValueProvider<FunctionCall> callMaker)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof AggregationNode)) {
            return result;
        }

        AggregationNode aggregationNode = (AggregationNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        for (Map.Entry<Symbol, Aggregation> assignment : aggregationNode.getAggregations().entrySet()) {
            Aggregation aggregation = assignment.getValue();
            if (aggregationMatches(aggregation, expectedCall)) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", aggregationNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private static boolean aggregationMatches(Aggregation aggregation, FunctionCall expectedCall)
    {
        if (expectedCall.getWindow().isPresent()) {
            return false;
        }
        if (!Objects.equals(expectedCall.getName(), QualifiedName.of(aggregation.getSignature().getName())) ||
                !Objects.equals(expectedCall.getFilter(), aggregation.getFilter()) ||
                !Objects.equals(expectedCall.getOrderBy().map(OrderingSchemeUtils::fromOrderBy), aggregation.getOrderingScheme()) ||
                !Objects.equals(expectedCall.isDistinct(), aggregation.isDistinct()) ||
                expectedCall.getArguments().size() != aggregation.getArguments().size()) {
            return false;
        }
        for (int i = 0; i < aggregation.getArguments().size(); i++) {
            if (isExpression(aggregation.getArguments().get(i))) {
                if (!Objects.equals(expectedCall.getArguments().get(i), castToExpression(aggregation.getArguments().get(i)))) {
                    return false;
                }
            }
            else {
                if (aggregation.getArguments().get(i) instanceof VariableReferenceExpression && expectedCall.getArguments().get(i) instanceof SymbolReference) {
                    if (expectedCall.getArguments().get(i) instanceof AnySymbolReference) {
                        return true;
                    }
                    if (((SymbolReference) expectedCall.getArguments().get(i)).getName() != ((VariableReferenceExpression) aggregation.getArguments().get(i)).getName()) {
                        return false;
                    }
                }
                else {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public String toString()
    {
        return callMaker.toString();
    }
}
