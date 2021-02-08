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
package io.prestosql.spi.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.relation.RowExpression;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collector;

import static java.lang.String.format;
import static java.util.Map.Entry.comparingByKey;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class Assignments
{
    public static Builder builder()
    {
        return new Builder();
    }

    public static Builder builder(Map<Symbol, RowExpression> assignments)
    {
        return new Builder().putAll(assignments);
    }

    public static Assignments copyOf(Map<Symbol, RowExpression> assignments)
    {
        return builder()
                .putAll(assignments)
                .build();
    }

    public static Assignments of()
    {
        return builder().build();
    }

    public static Assignments of(Symbol symbol, RowExpression expression)
    {
        return builder().put(symbol, expression).build();
    }

    public static Assignments of(Symbol symbol1, RowExpression expression1, Symbol symbol2, RowExpression expression2)
    {
        return builder().put(symbol1, expression1).put(symbol2, expression2).build();
    }

    private final Map<Symbol, RowExpression> assignments;

    @JsonCreator
    public Assignments(@JsonProperty("assignments") Map<Symbol, RowExpression> assignments)
    {
        this.assignments = ImmutableMap.copyOf(requireNonNull(assignments, "assignments is null"));
    }

    public List<Symbol> getOutputs()
    {
        return ImmutableList.copyOf(assignments.keySet());
    }

    @JsonProperty("assignments")
    public Map<Symbol, RowExpression> getMap()
    {
        return assignments;
    }

    public Assignments filter(Collection<Symbol> symbols)
    {
        return filter(symbols::contains);
    }

    public Assignments filter(Predicate<Symbol> predicate)
    {
        return assignments.entrySet().stream()
                .filter(entry -> predicate.apply(entry.getKey()))
                .collect(toAssignments());
    }

    private Collector<Entry<Symbol, RowExpression>, Builder, Assignments> toAssignments()
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

    public Collection<RowExpression> getExpressions()
    {
        return assignments.values();
    }

    public Set<Symbol> getSymbols()
    {
        return assignments.keySet();
    }

    public Set<Entry<Symbol, RowExpression>> entrySet()
    {
        return assignments.entrySet();
    }

    public RowExpression get(Symbol symbol)
    {
        return assignments.get(symbol);
    }

    public int size()
    {
        return assignments.size();
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void forEach(BiConsumer<Symbol, RowExpression> consumer)
    {
        assignments.forEach(consumer);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Assignments that = (Assignments) o;

        return assignments.equals(that.assignments);
    }

    @Override
    public int hashCode()
    {
        return assignments.hashCode();
    }

    public static class Builder
    {
        private final Map<Symbol, RowExpression> assignments = new LinkedHashMap<>();

        public Builder putAll(Assignments assignments)
        {
            return putAll(assignments.getMap());
        }

        public Builder putAllSorted(Assignments assignments)
        {
            return putAllSorted(assignments.getMap());
        }

        public Builder putAll(Map<Symbol, RowExpression> assignments)
        {
            for (Entry<Symbol, RowExpression> assignment : assignments.entrySet()) {
                put(assignment.getKey(), assignment.getValue());
            }
            return this;
        }

        public Builder put(Symbol symbol, RowExpression expression)
        {
            if (assignments.containsKey(symbol)) {
                RowExpression assignment = assignments.get(symbol);
                if (!assignment.equals(expression)) {
                    throw new IllegalStateException(format("Variable %s already has assignment %s, while adding %s", symbol, assignment, expression));
                }
            }
            assignments.put(symbol, expression);
            return this;
        }

        public Builder put(Entry<Symbol, RowExpression> assignment)
        {
            put(assignment.getKey(), assignment.getValue());
            return this;
        }

        public Builder putAllSorted(Map<Symbol, RowExpression> assignments)
        {
            Map<Symbol, RowExpression> sortedAssigments = assignments
                    .entrySet()
                    .stream()
                    .sorted(comparingByKey())
                    .collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue,
                                (e1, e2) -> e2, LinkedHashMap::new));
            putAll(sortedAssigments);
            return this;
        }

        public Assignments build()
        {
            return new Assignments(assignments);
        }
    }
}
