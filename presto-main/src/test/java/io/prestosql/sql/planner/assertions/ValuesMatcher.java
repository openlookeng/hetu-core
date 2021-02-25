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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.DoubleLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.StringLiteral;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public class ValuesMatcher
        implements Matcher
{
    private final Map<String, Integer> outputSymbolAliases;
    private final Optional<Integer> expectedOutputSymbolCount;
    private final Optional<List<List<Expression>>> expectedRows;

    public ValuesMatcher(
            Map<String, Integer> outputSymbolAliases,
            Optional<Integer> expectedOutputSymbolCount,
            Optional<List<List<Expression>>> expectedRows)
    {
        this.outputSymbolAliases = ImmutableMap.copyOf(outputSymbolAliases);
        this.expectedOutputSymbolCount = requireNonNull(expectedOutputSymbolCount, "expectedOutputSymbolCount is null");
        this.expectedRows = requireNonNull(expectedRows, "expectedRows is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return (node instanceof ValuesNode) &&
                expectedOutputSymbolCount.map(Integer.valueOf(node.getOutputSymbols().size())::equals).orElse(true);
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        ValuesNode valuesNode = (ValuesNode) node;

        if (!expectedRows.map(rows -> rows.equals(valuesNode.getRows()
                .stream()
                .map(rowExpressions -> rowExpressions.stream()
                        .map(rowExpression -> {
                            if (isExpression(rowExpression)) {
                                return castToExpression(rowExpression);
                            }
                            ConstantExpression expression = (ConstantExpression) rowExpression;
                            if (expression.getType().getJavaType() == boolean.class) {
                                return new BooleanLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == long.class) {
                                return new LongLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == double.class) {
                                return new DoubleLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == Slice.class) {
                                return new StringLiteral(String.valueOf(expression.getValue()));
                            }
                            return new GenericLiteral(expression.getType().toString(), String.valueOf(expression.getValue()));
                        })
                        .collect(toImmutableList()))
                .collect(toImmutableList())))
                .orElse(true)) {
            return NO_MATCH;
        }

        return match(SymbolAliases.builder()
                .putAll(Maps.transformValues(outputSymbolAliases, index -> toSymbolReference(valuesNode.getOutputSymbols().get(index))))
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("outputSymbolAliases", outputSymbolAliases)
                .add("expectedOutputSymbolCount", expectedOutputSymbolCount)
                .add("expectedRows", expectedRows)
                .toString();
    }
}
