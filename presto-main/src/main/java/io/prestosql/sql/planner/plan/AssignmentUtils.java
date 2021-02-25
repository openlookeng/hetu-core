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
package io.prestosql.sql.planner.plan;

import com.google.common.collect.Maps;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;

import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Arrays.asList;

public class AssignmentUtils
{
    private AssignmentUtils() {}

    public static Assignments identityAsSymbolReferences(Collection<Symbol> symbols)
    {
        Assignments.Builder builder = Assignments.builder();
        symbols.forEach(symbol -> builder.put(symbol, castToRowExpression(toSymbolReference(symbol))));
        return builder.build();
    }

    public static Assignments identityAsSymbolReferences(Symbol... symbols)
    {
        return identityAsSymbolReferences(asList(symbols));
    }

    public static Assignments identityAssignments(TypeProvider typeProvider, Collection<Symbol> symbols)
    {
        Assignments.Builder builder = Assignments.builder();
        symbols.forEach(symbol -> builder.put(symbol, new VariableReferenceExpression(symbol.getName(), typeProvider.get(symbol))));
        return builder.build();
    }

    public static Assignments identityAssignments(TypeProvider typeProvider, Symbol... symbols)
    {
        return identityAssignments(typeProvider, asList(symbols));
    }

    public static boolean isIdentity(Assignments assignments, Symbol output)
    {
        RowExpression value = assignments.get(output);
        if (isExpression(value)) {
            Expression expression = castToExpression(value);
            return expression instanceof SymbolReference && ((SymbolReference) expression).getName().equals(output.getName());
        }
        return value instanceof VariableReferenceExpression && ((VariableReferenceExpression) value).getName().equals(output.getName());
    }

    public static Assignments rewrite(Assignments assignments, Function<Expression, Expression> rewrite)
    {
        return assignments.entrySet().stream()
                .map(entry -> {
                    if (isExpression(entry.getValue())) {
                        return Maps.immutableEntry(entry.getKey(), castToRowExpression(rewrite.apply(castToExpression(entry.getValue()))));
                    }
                    else {
                        return Maps.immutableEntry(entry.getKey(), entry.getValue());
                    }
                })
                .collect(toAssignments());
    }

    private static Collector<Map.Entry<Symbol, RowExpression>, Assignments.Builder, Assignments> toAssignments()
    {
        return Collector.of(
                Assignments::builder,
                (builder, entry) -> builder.put(entry.getKey(), entry.getValue()),
                (left, right) -> {
                    left.putAll(right.build());
                    return left;
                },
                Assignments.Builder::build);
    }
}
