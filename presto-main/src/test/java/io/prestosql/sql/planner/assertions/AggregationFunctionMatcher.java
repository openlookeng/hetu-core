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

import com.google.common.collect.Streams;
import io.prestosql.Session;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.OrderingSchemeUtils.sortItemToSortOrder;
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
            if (aggregationMatches(aggregation, expectedCall, metadata)) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", aggregationNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private static boolean aggregationMatches(Aggregation aggregation, FunctionCall expectedCall, Metadata metadata)
    {
        if (expectedCall.getWindow().isPresent()) {
            return false;
        }
        if (!Objects.equals(expectedCall.getName().getSuffix(), aggregation.getFunctionCall().getDisplayName()) ||
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
                return verifyAggregation(metadata.getFunctionAndTypeManager(), aggregation, expectedCall);
            }
        }
        return true;
    }

    private static boolean verifyAggregation(FunctionAndTypeManager functionAndTypeManager, Aggregation aggregation, FunctionCall expectedCall)
    {
        return functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName().getObjectName().equalsIgnoreCase(expectedCall.getName().getSuffix()) &&
                aggregation.getArguments().size() == expectedCall.getArguments().size() &&
                Streams.zip(
                        aggregation.getArguments().stream(),
                        expectedCall.getArguments().stream(),
                        (actualArgument, expectedArgument) -> isEquivalent(Optional.of(expectedArgument), Optional.of(actualArgument))).allMatch(Boolean::booleanValue) &&
                isSymbolEquivalent(expectedCall.getFilter(), aggregation.getFilter()) &&
                expectedCall.isDistinct() == aggregation.isDistinct() &&
                verifyAggregationOrderBy(aggregation.getOrderingScheme(), expectedCall.getOrderBy());
    }

    private static boolean verifyAggregationOrderBy(Optional<OrderingScheme> orderingScheme, Optional<OrderBy> expectedSortOrder)
    {
        if (orderingScheme.isPresent() && expectedSortOrder.isPresent()) {
            return verifyAggregationOrderBy(orderingScheme.get(), expectedSortOrder.get());
        }
        return orderingScheme.isPresent() == expectedSortOrder.isPresent();
    }

    private static boolean verifyAggregationOrderBy(OrderingScheme orderingScheme, OrderBy expectedSortOrder)
    {
        if (orderingScheme.getOrderBy().size() != expectedSortOrder.getSortItems().size()) {
            return false;
        }
        for (int i = 0; i < expectedSortOrder.getSortItems().size(); i++) {
            Symbol orderingSymbol = orderingScheme.getOrderBy().get(i);
            if (expectedSortOrder.getSortItems().get(i).getSortKey().equals(new SymbolReference(orderingSymbol.getName())) &&
                    sortItemToSortOrder(expectedSortOrder.getSortItems().get(i)).equals(orderingScheme.getOrdering(orderingSymbol))) {
                continue;
            }
            return false;
        }
        return true;
    }

    private static boolean isSymbolEquivalent(Optional<Expression> expression, Optional<Symbol> symbol)
    {
        // Function's argument provided by FunctionCallProvider is SymbolReference that already resolved from symbolAliases.
        if (symbol.isPresent() && expression.isPresent()) {
            return expression.get().equals(new SymbolReference(symbol.get().getName()));
        }
        return symbol.isPresent() == expression.isPresent();
    }

    private static boolean isEquivalent(Optional<Expression> expression, Optional<RowExpression> rowExpression)
    {
        // Function's argument provided by FunctionCallProvider is SymbolReference that already resolved from symbolAliases.
        if (rowExpression.isPresent() && expression.isPresent()) {
            if (isExpression(rowExpression.get())) {
                return expression.get().equals(castToExpression(rowExpression.get()));
            }
            checkArgument(rowExpression.get() instanceof VariableReferenceExpression, "can only process variableReference");
            return expression.get().equals(new SymbolReference(((VariableReferenceExpression) rowExpression.get()).getName()));
        }
        return rowExpression.isPresent() == expression.isPresent();
    }

    @Override
    public String toString()
    {
        return callMaker.toString();
    }
}
