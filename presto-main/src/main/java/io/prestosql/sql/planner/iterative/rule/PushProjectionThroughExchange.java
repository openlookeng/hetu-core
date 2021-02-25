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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.RowExpressionVariableInliner;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.exchange;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;

/**
 * Transforms:
 * <pre>
 *  Project(x = e1, y = e2)
 *    Exchange()
 *      Source(a, b, c)
 *  </pre>
 * to:
 * <pre>
 *  Exchange()
 *    Project(x = e1, y = e2)
 *      Source(a, b, c)
 *  </pre>
 * Or if Exchange needs symbols from Source for partitioning or as hash symbol to:
 * <pre>
 *  Project(x, y)
 *    Exchange()
 *      Project(x = e1, y = e2, a)
 *        Source(a, b, c)
 *  </pre>
 * To avoid looping this optimizer will not be fired if upper Project contains just symbol references.
 */
public class PushProjectionThroughExchange
        implements Rule<ProjectNode>
{
    private static final Capture<ExchangeNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .matching(project -> !isSymbolToSymbolProjection(project))
            .with(source().matching(exchange().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        ExchangeNode exchange = captures.get(CHILD);
        Set<Symbol> partitioningColumns = exchange.getPartitioningScheme().getPartitioning().getColumns();

        ImmutableList.Builder<PlanNode> newSourceBuilder = ImmutableList.builder();
        ImmutableList.Builder<List<Symbol>> inputsBuilder = ImmutableList.builder();
        for (int i = 0; i < exchange.getSources().size(); i++) {
            Map<Symbol, VariableReferenceExpression> outputToInputMap = extractExchangeOutputToInput(exchange, i, context.getSymbolAllocator().getTypes());

            Assignments.Builder projections = Assignments.builder();
            ImmutableList.Builder<Symbol> inputs = ImmutableList.builder();

            // Need to retain the partition keys for the exchange
            partitioningColumns.stream()
                    .map(outputToInputMap::get)
                    .forEach(nameReference -> {
                        Symbol symbol = new Symbol(nameReference.getName());
                        projections.put(symbol, nameReference);
                        inputs.add(symbol);
                    });

            if (exchange.getPartitioningScheme().getHashColumn().isPresent()) {
                // Need to retain the hash symbol for the exchange
                projections.put(exchange.getPartitioningScheme().getHashColumn().get(), castToRowExpression(toSymbolReference(exchange.getPartitioningScheme().getHashColumn().get())));
                inputs.add(exchange.getPartitioningScheme().getHashColumn().get());
            }

            if (exchange.getOrderingScheme().isPresent()) {
                // need to retain ordering columns for the exchange
                exchange.getOrderingScheme().get().getOrderBy().stream()
                        // do not project the same symbol twice as ExchangeNode verifies that source input symbols match partitioning scheme outputLayout
                        .filter(symbol -> !partitioningColumns.contains(symbol))
                        .map(outputToInputMap::get)
                        .forEach(nameReference -> {
                            Symbol symbol = new Symbol(nameReference.getName());
                            projections.put(symbol, nameReference);
                            inputs.add(symbol);
                        });
            }

            for (Map.Entry<Symbol, RowExpression> projection : project.getAssignments().entrySet()) {
                checkArgument(!isExpression(projection.getValue()), "Cannot contain OriginalExpression after AddExchange");
                Map<VariableReferenceExpression, VariableReferenceExpression> variableOutputToInputMap = new LinkedHashMap<>();
                outputToInputMap.forEach(((symbol, variable) -> variableOutputToInputMap.put(
                        new VariableReferenceExpression(symbol.getName(), context.getSymbolAllocator().getTypes().get(symbol)), variable)));
                RowExpression translatedExpression = RowExpressionVariableInliner
                        .inlineVariables(variableOutputToInputMap, projection.getValue());
                Symbol symbol = context.getSymbolAllocator().newSymbol(translatedExpression);
                projections.put(symbol, translatedExpression);
                inputs.add(symbol);
            }
            newSourceBuilder.add(new ProjectNode(context.getIdAllocator().getNextId(), exchange.getSources().get(i), projections.build()));
            inputsBuilder.add(inputs.build());
        }

        // Construct the output symbols in the same order as the sources
        ImmutableList.Builder<Symbol> outputBuilder = ImmutableList.builder();
        partitioningColumns.forEach(outputBuilder::add);
        exchange.getPartitioningScheme().getHashColumn().ifPresent(outputBuilder::add);
        if (exchange.getOrderingScheme().isPresent()) {
            exchange.getOrderingScheme().get().getOrderBy().stream()
                    .filter(symbol -> !partitioningColumns.contains(symbol))
                    .forEach(outputBuilder::add);
        }
        for (Map.Entry<Symbol, RowExpression> projection : project.getAssignments().entrySet()) {
            outputBuilder.add(projection.getKey());
        }

        // outputBuilder contains all partition and hash symbols so simply swap the output layout
        PartitioningScheme partitioningScheme = new PartitioningScheme(
                exchange.getPartitioningScheme().getPartitioning(),
                outputBuilder.build(),
                exchange.getPartitioningScheme().getHashColumn(),
                exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                exchange.getPartitioningScheme().getBucketToPartition());

        PlanNode result = new ExchangeNode(
                exchange.getId(),
                exchange.getType(),
                exchange.getScope(),
                partitioningScheme,
                newSourceBuilder.build(),
                inputsBuilder.build(),
                exchange.getOrderingScheme());

        // we need to strip unnecessary symbols (hash, partitioning columns).
        return Result.ofPlanNode(restrictOutputs(context.getIdAllocator(), result, ImmutableSet.copyOf(project.getOutputSymbols())).orElse(result));
    }

    private static boolean isSymbolToSymbolProjection(ProjectNode project)
    {
        return project.getAssignments().getExpressions().stream().allMatch(e -> {
            if (isExpression(e)) {
                return castToExpression(e) instanceof SymbolReference;
            }
            else {
                return e instanceof InputReferenceExpression || e instanceof VariableReferenceExpression;
            }
        });
    }

    private static Map<Symbol, VariableReferenceExpression> extractExchangeOutputToInput(ExchangeNode exchange, int sourceIndex, TypeProvider types)
    {
        Map<Symbol, VariableReferenceExpression> outputToInputMap = new HashMap<>();
        for (int i = 0; i < exchange.getOutputSymbols().size(); i++) {
            Symbol inputSymbol = exchange.getInputs().get(sourceIndex).get(i);
            outputToInputMap.put(exchange.getOutputSymbols().get(i), new VariableReferenceExpression(inputSymbol.getName(), types.get(inputSymbol)));
        }
        return outputToInputMap;
    }
}
