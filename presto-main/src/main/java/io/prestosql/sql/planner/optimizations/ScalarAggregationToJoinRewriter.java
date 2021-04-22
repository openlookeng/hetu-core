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
package io.prestosql.sql.planner.optimizations;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Expression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

// TODO: move this class to TransformCorrelatedScalarAggregationToJoin when old optimizer is gone
public class ScalarAggregationToJoinRewriter
{
    private final FunctionResolution functionResolution;
    private final PlanSymbolAllocator planSymbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;
    private final PlanNodeDecorrelator planNodeDecorrelator;

    public ScalarAggregationToJoinRewriter(Metadata metadata, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        this.planSymbolAllocator = requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, lookup);
    }

    public PlanNode rewriteScalarAggregation(LateralJoinNode lateralJoinNode, AggregationNode aggregation)
    {
        List<Symbol> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> source = planNodeDecorrelator.decorrelateFilters(lookup.resolve(aggregation.getSource()), correlation);
        if (!source.isPresent()) {
            return lateralJoinNode;
        }

        Symbol nonNull = planSymbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
        Assignments scalarAggregationSourceAssignments = Assignments.builder()
                .putAll(AssignmentUtils.identityAsSymbolReferences(source.get().getNode().getOutputSymbols()))
                .put(nonNull, castToRowExpression(TRUE_LITERAL))
                .build();
        ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                idAllocator.getNextId(),
                source.get().getNode(),
                scalarAggregationSourceAssignments);

        return rewriteScalarAggregation(
                lateralJoinNode,
                aggregation,
                scalarAggregationSourceWithNonNullableSymbol,
                source.get().getCorrelatedPredicates(),
                nonNull);
    }

    private PlanNode rewriteScalarAggregation(
            LateralJoinNode lateralJoinNode,
            AggregationNode scalarAggregation,
            PlanNode scalarAggregationSource,
            Optional<Expression> joinExpression,
            Symbol nonNull)
    {
        AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                idAllocator.getNextId(),
                lateralJoinNode.getInput(),
                planSymbolAllocator.newSymbol("unique", BIGINT));

        JoinNode leftOuterJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                inputWithUniqueColumns,
                scalarAggregationSource,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(inputWithUniqueColumns.getOutputSymbols())
                        .addAll(scalarAggregationSource.getOutputSymbols())
                        .build(),
                joinExpression.map(OriginalExpressionUtils::castToRowExpression),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Optional<AggregationNode> aggregationNode = createAggregationNode(
                scalarAggregation,
                leftOuterJoin,
                nonNull);

        if (!aggregationNode.isPresent()) {
            return lateralJoinNode;
        }

        Optional<ProjectNode> subqueryProjection = searchFrom(lateralJoinNode.getSubquery(), lookup)
                .where(ProjectNode.class::isInstance)
                .recurseOnlyWhen(EnforceSingleRowNode.class::isInstance)
                .findFirst();

        List<Symbol> aggregationOutputSymbols = getTruncatedAggregationSymbols(lateralJoinNode, aggregationNode.get());

        if (subqueryProjection.isPresent()) {
            Assignments assignments = Assignments.builder()
                    .putAll(AssignmentUtils.identityAsSymbolReferences(aggregationOutputSymbols))
                    .putAll(subqueryProjection.get().getAssignments())
                    .build();

            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    assignments);
        }
        else {
            return new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    AssignmentUtils.identityAsSymbolReferences(aggregationOutputSymbols));
        }
    }

    private static List<Symbol> getTruncatedAggregationSymbols(LateralJoinNode lateralJoinNode, AggregationNode aggregationNode)
    {
        Set<Symbol> applySymbols = new HashSet<>(lateralJoinNode.getOutputSymbols());
        return aggregationNode.getOutputSymbols().stream()
                .filter(applySymbols::contains)
                .collect(toImmutableList());
    }

    private Optional<AggregationNode> createAggregationNode(
            AggregationNode scalarAggregation,
            JoinNode leftOuterJoin,
            Symbol nonNullableAggregationSourceSymbol)
    {
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            Symbol symbol = entry.getKey();
            if (functionResolution.isCountFunction(entry.getValue().getFunctionHandle())) {
                aggregations.put(symbol, new Aggregation(
                        new CallExpression(
                                "count",
                                functionResolution.countFunction(planSymbolAllocator.getTypes().get(nonNullableAggregationSourceSymbol)),
                                BIGINT,
                                ImmutableList.of(castToRowExpression(toSymbolReference(nonNullableAggregationSourceSymbol)))),
                        ImmutableList.of(castToRowExpression(toSymbolReference(nonNullableAggregationSourceSymbol))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        aggregation.getMask()));
            }
            else {
                aggregations.put(symbol, aggregation);
            }
        }

        return Optional.of(new AggregationNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                aggregations.build(),
                singleGroupingSet(leftOuterJoin.getLeft().getOutputSymbols()),
                ImmutableList.of(),
                scalarAggregation.getStep(),
                scalarAggregation.getHashSymbol(),
                Optional.empty(),
                scalarAggregation.getAggregationType(),
                scalarAggregation.getFinalizeSymbol()));
    }
}
