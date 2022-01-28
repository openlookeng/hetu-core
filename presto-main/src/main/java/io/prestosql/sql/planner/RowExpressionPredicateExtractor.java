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
package io.prestosql.sql.planner;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import io.prestosql.Session;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.OperatorNotFoundException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.type.InternalTypeManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.relation.SpecialForm.Form.IS_NULL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReference;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReferenceMap;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReferences;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.specialForm;
import static java.util.Objects.requireNonNull;

public class RowExpressionPredicateExtractor
{
    private final RowExpressionDomainTranslator domainTranslator;
    private final TypeManager typeManager;
    private final Metadata metadata;
    private final PlanSymbolAllocator planSymbolAllocator;
    private final boolean useTableProperties;
    private final LogicalRowExpressions logicalRowExpressions;

    public RowExpressionPredicateExtractor(RowExpressionDomainTranslator domainTranslator, Metadata metadata, PlanSymbolAllocator planSymbolAllocator, boolean useTableProperties)
    {
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.metadata = metadata;
        this.typeManager = new InternalTypeManager(metadata.getFunctionAndTypeManager());
        this.planSymbolAllocator = planSymbolAllocator;
        this.useTableProperties = useTableProperties;
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
    }

    public RowExpression extract(PlanNode node, Session session)
    {
        return node.accept(new Visitor(domainTranslator, metadata, session, typeManager, planSymbolAllocator, useTableProperties), null);
    }

    private static class Visitor
            extends InternalPlanVisitor<RowExpression, Void>
    {
        private final RowExpressionDomainTranslator domainTranslator;
        private final LogicalRowExpressions logicalRowExpressions;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;
        private final Metadata metadata;
        private final Session session;
        private final TypeManager typeManager;
        private final PlanSymbolAllocator planSymbolAllocator;
        private final boolean useTableProperties;

        public Visitor(RowExpressionDomainTranslator domainTranslator, Metadata metadata, Session session, TypeManager typeManager,
                PlanSymbolAllocator planSymbolAllocator, boolean useTableProperties)
        {
            this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
            this.metadata = metadata;
            this.session = session;
            this.typeManager = requireNonNull(typeManager);
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
            this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
            this.planSymbolAllocator = planSymbolAllocator;
            this.useTableProperties = useTableProperties;
        }

        @Override
        public RowExpression visitPlan(PlanNode node, Void context)
        {
            return TRUE_CONSTANT;
        }

        @Override
        public RowExpression visitAggregation(AggregationNode node, Void context)
        {
            // GROUP BY () always produces a group, regardless of whether there's any
            // input (unlike the case where there are group by keys, which produce
            // no output if there's no input).
            // Therefore, we can't say anything about the effective predicate of the
            // output of such an aggregation.
            if (node.getGroupingKeys().isEmpty()) {
                return TRUE_CONSTANT;
            }

            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            return pullExpressionThroughVariables(underlyingPredicate, toVariableReferences(node.getGroupingKeys(), planSymbolAllocator.getTypes()));
        }

        @Override
        public RowExpression visitFilter(FilterNode node, Void context)
        {
            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            RowExpression predicate = node.getPredicate();

            // Remove non-deterministic conjuncts
            predicate = logicalRowExpressions.filterDeterministicConjuncts(predicate);

            Optional<RowExpression> staticFilters = DynamicFilters.extractStaticFilters(Optional.of(predicate), metadata);
            predicate = staticFilters.isPresent() ? staticFilters.get() : TRUE_CONSTANT;

            return logicalRowExpressions.combineConjuncts(predicate, underlyingPredicate);
        }

        @Override
        public RowExpression visitExchange(ExchangeNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> {
                Map<VariableReferenceExpression, VariableReferenceExpression> mappings = new HashMap<>();
                for (int i = 0; i < node.getInputs().get(source).size(); i++) {
                    mappings.put(
                            toVariableReference(node.getOutputSymbols().get(i), planSymbolAllocator.getTypes()),
                            toVariableReference(node.getInputs().get(source).get(i), planSymbolAllocator.getTypes()));
                }
                return mappings.entrySet();
            });
        }

        @Override
        public RowExpression visitProject(ProjectNode node, Void context)
        {
            // TODO: add simple algebraic solver for projection translation (right now only considers identity projections)

            RowExpression underlyingPredicate = node.getSource().accept(this, context);

            Map<VariableReferenceExpression, RowExpression> map = toVariableReferenceMap(node.getAssignments().getMap(), planSymbolAllocator.getTypes());

            List<RowExpression> projectionEqualities = map.entrySet().stream()
                    .filter(this::notIdentityAssignment)
                    .filter(this::canCompareEquity)
                    .map(this::toEquality)
                    .collect(toImmutableList());

            return pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(
                    ImmutableList.<RowExpression>builder()
                            .addAll(projectionEqualities)
                            .add(underlyingPredicate)
                            .build()),
                    toVariableReferences(node.getOutputSymbols(), planSymbolAllocator.getTypes()));
        }

        @Override
        public RowExpression visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitTableScan(TableScanNode node, Void context)
        {
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
            Map<ColumnHandle, VariableReferenceExpression> variableAssignments = new LinkedHashMap<>();
            assignments.forEach((key, value) -> variableAssignments.put(key, toVariableReference(value, planSymbolAllocator.getTypes())));

            TupleDomain<ColumnHandle> predicate = node.getEnforcedConstraint();
            if (useTableProperties) {
                predicate = metadata.getTableProperties(session, node.getTable()).getPredicate();
            }

            return domainTranslator.toPredicate(predicate.simplify().transform(column -> variableAssignments.containsKey(column) ? variableAssignments.get(column) : null));
        }

        @Override
        public RowExpression visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitValues(ValuesNode node, Void context)
        {
            if (node.getOutputSymbols().isEmpty()) {
                return TRUE_CONSTANT;
            }

            ImmutableMap.Builder<VariableReferenceExpression, Domain> domains = ImmutableMap.builder();

            for (int column = 0; column < node.getOutputSymbols().size(); column++) {
                Symbol symbol = node.getOutputSymbols().get(column);
                Type type = planSymbolAllocator.getTypes().get(symbol);

                ImmutableList.Builder<Object> builder = ImmutableList.builder();
                boolean hasNull = false;
                boolean nonDeterministic = false;
                for (int row = 0; row < node.getRows().size(); row++) {
                    RowExpression value = node.getRows().get(row).get(column);

                    if (!LogicalRowExpressions.isDeterministic(new RowExpressionDeterminismEvaluator(metadata), value)) {
                        nonDeterministic = true;
                        break;
                    }

                    RowExpressionInterpreter interpreter = RowExpressionInterpreter.rowExpressionInterpreter(value, metadata, session.toConnectorSession());
                    Object evaluated = interpreter.evaluate();

                    if (evaluated instanceof RowExpressionInterpreter) {
                        return TRUE_CONSTANT;
                    }

                    if (evaluated == null) {
                        hasNull = true;
                    }
                    else {
                        builder.add(evaluated);
                    }
                }

                if (nonDeterministic) {
                    // We can't describe a predicate for this column because at least
                    // one cell is non-deterministic, so skip it.
                    continue;
                }

                List<Object> values = builder.build();

                Domain domain = Domain.none(type);

                if (!values.isEmpty()) {
                    domain = domain.union(Domain.multipleValues(type, values));
                }

                if (hasNull) {
                    domain = domain.union(Domain.onlyNull(type));
                }

                domains.put(new VariableReferenceExpression(symbol.getName(), type), domain);
            }

            // simplify to avoid a large expression if there are many rows in ValuesNode
            return domainTranslator.toPredicate(TupleDomain.withColumnDomains(domains.build()).simplify());
        }

        private Multimap<VariableReferenceExpression, VariableReferenceExpression> outputMap(UnionNode node, int sourceIndex)
        {
            Multimap<Symbol, Symbol> map = FluentIterable.from(node.getOutputSymbols())
                    .toMap(output -> node.getSymbolMapping().get(output).get(sourceIndex))
                    .asMultimap()
                    .inverse();

            Multimap<VariableReferenceExpression, VariableReferenceExpression> multimap = LinkedHashMultimap.create();

            map.forEach((key, vaule) -> multimap.put(toVariableReference(key, planSymbolAllocator.getTypes()),
                    toVariableReference(vaule, planSymbolAllocator.getTypes())));
            return multimap;
        }

        @Override
        public RowExpression visitUnion(UnionNode node, Void context)
        {
            return deriveCommonPredicates(node, source -> outputMap(node, source).entries());
        }

        @Override
        public RowExpression visitJoin(JoinNode node, Void context)
        {
            RowExpression leftPredicate = node.getLeft().accept(this, context);
            RowExpression rightPredicate = node.getRight().accept(this, context);

            List<RowExpression> joinConjuncts = node.getCriteria().stream()
                    .map(this::toRowExpression)
                    .collect(toImmutableList());

            List<VariableReferenceExpression> nodeOutput = toVariableReferences(node.getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeLeftOutput = toVariableReferences(node.getLeft().getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeRightOutput = toVariableReferences(node.getRight().getOutputSymbols(), planSymbolAllocator.getTypes());

            switch (node.getType()) {
                case INNER:
                    return pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(leftPredicate)
                            .add(rightPredicate)
                            .add(logicalRowExpressions.combineConjuncts(joinConjuncts))
                            .add(node.getFilter().orElse(TRUE_CONSTANT))
                            .build()), nodeOutput);
                case LEFT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, nodeOutput))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), nodeOutput, nodeRightOutput::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, nodeOutput, nodeRightOutput::contains))
                            .build());
                case RIGHT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(rightPredicate, nodeOutput))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), nodeOutput, nodeLeftOutput::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, nodeOutput, nodeLeftOutput::contains))
                            .build());
                case FULL:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(leftPredicate), nodeOutput, nodeLeftOutput::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), nodeOutput, nodeRightOutput::contains))
                            .addAll(pullNullableConjunctsThroughOuterJoin(joinConjuncts, nodeOutput, nodeLeftOutput::contains, nodeRightOutput::contains))
                            .build());
                default:
                    throw new UnsupportedOperationException("Unknown join type: " + node.getType());
            }
        }

        private Iterable<RowExpression> pullNullableConjunctsThroughOuterJoin(List<RowExpression> conjuncts, Collection<VariableReferenceExpression> outputVariables, Predicate<VariableReferenceExpression>... nullVariableScopes)
        {
            // Conjuncts without any symbol dependencies cannot be applied to the effective predicate (e.g. FALSE literal)
            return conjuncts.stream()
                    .map(expression -> pullExpressionThroughVariables(expression, outputVariables))
                    .map(expression -> VariablesExtractor.extractAll(expression).isEmpty() ? TRUE_CONSTANT : expression)
                    .map(expressionOrNullVariables(nullVariableScopes))
                    .collect(toImmutableList());
        }

        public Function<RowExpression, RowExpression> expressionOrNullVariables(final Predicate<VariableReferenceExpression>... nullVariableScopes)
        {
            return expression -> {
                ImmutableList.Builder<RowExpression> resultDisjunct = ImmutableList.builder();
                resultDisjunct.add(expression);

                for (Predicate<VariableReferenceExpression> nullVariableScope : nullVariableScopes) {
                    List<VariableReferenceExpression> variables = VariablesExtractor.extractUnique(expression).stream()
                            .filter(nullVariableScope)
                            .collect(toImmutableList());

                    if (Iterables.isEmpty(variables)) {
                        continue;
                    }

                    ImmutableList.Builder<RowExpression> nullConjuncts = ImmutableList.builder();
                    for (VariableReferenceExpression variable : variables) {
                        nullConjuncts.add(specialForm(IS_NULL, BOOLEAN, variable));
                    }

                    resultDisjunct.add(LogicalRowExpressions.and(nullConjuncts.build()));
                }

                return LogicalRowExpressions.or(resultDisjunct.build());
            };
        }

        @Override
        public RowExpression visitSemiJoin(SemiJoinNode node, Void context)
        {
            // Filtering source does not change the effective predicate over the output symbols
            return node.getSource().accept(this, context);
        }

        @Override
        public RowExpression visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            RowExpression leftPredicate = node.getLeft().accept(this, context);
            RowExpression rightPredicate = node.getRight().accept(this, context);

            List<VariableReferenceExpression> nodeOutput = toVariableReferences(node.getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeRightOutput = toVariableReferences(node.getRight().getOutputSymbols(), planSymbolAllocator.getTypes());

            switch (node.getType()) {
                case INNER:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, nodeOutput))
                            .add(pullExpressionThroughVariables(rightPredicate, nodeOutput))
                            .build());
                case LEFT:
                    return logicalRowExpressions.combineConjuncts(ImmutableList.<RowExpression>builder()
                            .add(pullExpressionThroughVariables(leftPredicate, nodeOutput))
                            .addAll(pullNullableConjunctsThroughOuterJoin(extractConjuncts(rightPredicate), nodeOutput, nodeRightOutput::contains))
                            .build());
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        private RowExpression toRowExpression(JoinNode.EquiJoinClause equiJoinClause)
        {
            RowExpression left = toVariableReference(equiJoinClause.getLeft(), planSymbolAllocator.getTypes());
            RowExpression right = toVariableReference(equiJoinClause.getRight(), planSymbolAllocator.getTypes());
            return buildEqualsExpression(metadata, left, right);
        }

        private RowExpression deriveCommonPredicates(PlanNode node, Function<Integer, Collection<Map.Entry<VariableReferenceExpression, VariableReferenceExpression>>> mapping)
        {
            // Find the predicates that can be pulled up from each source
            List<Set<RowExpression>> sourceOutputConjuncts = new ArrayList<>();
            for (int i = 0; i < node.getSources().size(); i++) {
                RowExpression underlyingPredicate = node.getSources().get(i).accept(this, null);

                List<RowExpression> equalities = mapping.apply(i).stream()
                        .filter(this::notIdentityAssignment)
                        .filter(this::canCompareEquity)
                        .map(this::toEquality)
                        .collect(toImmutableList());

                sourceOutputConjuncts.add(ImmutableSet.copyOf(extractConjuncts(pullExpressionThroughVariables(logicalRowExpressions.combineConjuncts(
                        ImmutableList.<RowExpression>builder()
                                .addAll(equalities)
                                .add(underlyingPredicate)
                                .build()),
                        toVariableReferences(node.getOutputSymbols(), planSymbolAllocator.getTypes())))));
            }

            // Find the intersection of predicates across all sources
            // TODO: use a more precise way to determine overlapping conjuncts (e.g. commutative predicates)
            Iterator<Set<RowExpression>> iterator = sourceOutputConjuncts.iterator();
            Set<RowExpression> potentialOutputConjuncts = iterator.next();
            while (iterator.hasNext()) {
                potentialOutputConjuncts = Sets.intersection(potentialOutputConjuncts, iterator.next());
            }

            return logicalRowExpressions.combineConjuncts(potentialOutputConjuncts);
        }

        private boolean notIdentityAssignment(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            return !entry.getKey().equals(entry.getValue());
        }

        private boolean canCompareEquity(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            try {
                metadata.getFunctionAndTypeManager().resolveOperator(EQUAL, ImmutableList.of(entry.getKey().getType(), entry.getValue().getType()));
                return true;
            }
            catch (OperatorNotFoundException e) {
                return false;
            }
        }

        private RowExpression toEquality(Map.Entry<VariableReferenceExpression, ? extends RowExpression> entry)
        {
            return buildEqualsExpression(metadata, entry.getKey(), entry.getValue());
        }

        private static CallExpression buildEqualsExpression(Metadata metadata, RowExpression left, RowExpression right)
        {
            return call(EQUAL.name(), metadata.getFunctionAndTypeManager().resolveOperatorFunctionHandle(EQUAL, fromTypes(left.getType(), right.getType())), BOOLEAN, left, right);
        }

        private RowExpression pullExpressionThroughVariables(RowExpression expression, Collection<VariableReferenceExpression> variables)
        {
            RowExpressionEqualityInference equalityInference = new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(expression)
                    .build();

            ImmutableList.Builder<RowExpression> effectiveConjuncts = ImmutableList.builder();
            for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(expression)) {
                if (determinismEvaluator.isDeterministic(conjunct)) {
                    RowExpression rewritten = equalityInference.rewriteExpression(conjunct, in(variables));
                    if (rewritten != null) {
                        effectiveConjuncts.add(rewritten);
                    }
                }
            }

            effectiveConjuncts.addAll(equalityInference.generateEqualitiesPartitionedBy(in(variables)).getScopeEqualities());

            return logicalRowExpressions.combineConjuncts(effectiveConjuncts.build());
        }
    }
}
