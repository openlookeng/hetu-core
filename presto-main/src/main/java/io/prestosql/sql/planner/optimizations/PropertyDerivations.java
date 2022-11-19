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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.metadata.TableProperties.TablePartitioning;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConstantProperty;
import io.prestosql.spi.connector.GroupingProperty;
import io.prestosql.spi.connector.LocalProperty;
import io.prestosql.spi.connector.SortingProperty;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.DomainTranslator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.RowExpressionInterpreter;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.ActualProperties.Global;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.CreateIndexNode;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.DynamicFilterSourceNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableUpdateNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.UpdateIndexNode;
import io.prestosql.sql.planner.plan.UpdateNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.SystemSessionProperties.planWithTableNodePartitioning;
import static io.prestosql.spi.predicate.TupleDomain.extractFixedValues;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.optimizations.ActualProperties.Global.arbitraryPartition;
import static io.prestosql.sql.planner.optimizations.ActualProperties.Global.coordinatorSingleStreamPartition;
import static io.prestosql.sql.planner.optimizations.ActualProperties.Global.partitionedOn;
import static io.prestosql.sql.planner.optimizations.ActualProperties.Global.singleStreamPartition;
import static io.prestosql.sql.planner.optimizations.ActualProperties.Global.streamPartitionedOn;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class PropertyDerivations
{
    private PropertyDerivations() {}

    public static ActualProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        List<ActualProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, session, types, typeAnalyzer))
                .collect(toImmutableList());
        return deriveProperties(node, inputProperties, metadata, session, types, typeAnalyzer);
    }

    public static ActualProperties deriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        ActualProperties output = node.accept(new Visitor(metadata, session, types, typeAnalyzer), inputProperties);

        output.getNodePartitioning().ifPresent(partitioning ->
                verify(node.getOutputSymbols().containsAll(partitioning.getColumns()), "Node-level partitioning properties contain columns not present in node's output"));

        verify(node.getOutputSymbols().containsAll(output.getConstants().keySet()), "Node-level constant properties contain columns not present in node's output");

        Set<Symbol> localPropertyColumns = output.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputSymbols().containsAll(localPropertyColumns), "Node-level local properties contain columns not present in node's output");
        return output;
    }

    public static ActualProperties streamBackdoorDeriveProperties(PlanNode node, List<ActualProperties> inputProperties, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        return node.accept(new Visitor(metadata, session, types, typeAnalyzer), inputProperties);
    }

    private static class Visitor
            extends InternalPlanVisitor<ActualProperties, List<ActualProperties>>
    {
        private final Metadata metadata;
        private final Session session;
        private final TypeProvider types;
        private final TypeAnalyzer typeAnalyzer;

        public Visitor(Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
        {
            this.metadata = metadata;
            this.session = session;
            this.types = types;
            this.typeAnalyzer = typeAnalyzer;
        }

        @Override
        public ActualProperties visitPlan(PlanNode node, List<ActualProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitExplainAnalyze(ExplainAnalyzeNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableExecute(PlanNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isCoordinatorOnly()) {
                return ActualProperties.builder()
                        .global(coordinatorSingleStreamPartition())
                        .build();
            }
            return ActualProperties.builder()
                    .global(properties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                    .build();
        }

        @Override
        public ActualProperties visitOutput(OutputNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties)
                    .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column));
        }

        @Override
        public ActualProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitAssignUniqueId(AssignUniqueId node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> newLocalProperties = ImmutableList.builder();
            newLocalProperties.addAll(properties.getLocalProperties());
            newLocalProperties.add(new GroupingProperty<>(ImmutableList.of(node.getIdColumn())));
            node.getSource().getOutputSymbols().stream()
                    .forEach(column -> newLocalProperties.add(new ConstantProperty<>(column)));

            if (properties.getNodePartitioning().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return ActualProperties.builderFrom(properties)
                        .local(newLocalProperties.build())
                        .build();
            }

            return ActualProperties.builderFrom(properties)
                    .global(partitionedOn(ARBITRARY_DISTRIBUTION, ImmutableList.of(node.getIdColumn()), Optional.empty()))
                    .local(newLocalProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitApply(ApplyNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitLateralJoin(LateralJoinNode node, List<ActualProperties> inputProperties)
        {
            throw new IllegalArgumentException("Unexpected node: " + node.getClass().getName());
        }

        @Override
        public ActualProperties visitMarkDistinct(MarkDistinctNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitWindow(WindowNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            // If the input is completely pre-partitioned and sorted, then the original input properties will be respected
            Optional<OrderingScheme> orderingScheme = node.getOrderingScheme();
            if (ImmutableSet.copyOf(node.getPartitionBy()).equals(node.getPrePartitionedInputs())
                    && (!orderingScheme.isPresent() || node.getPreSortedOrderPrefix() == orderingScheme.get().getOrderBy().size())) {
                return properties;
            }

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();

            // If the WindowNode has pre-partitioned inputs, then it will not change the order of those inputs at output,
            // so we should just propagate those underlying local properties that guarantee the pre-partitioning.
            // TODO: come up with a more general form of this operation for other streaming operators
            if (!node.getPrePartitionedInputs().isEmpty()) {
                GroupingProperty<Symbol> prePartitionedProperty = new GroupingProperty<>(node.getPrePartitionedInputs());
                for (LocalProperty<Symbol> localProperty : properties.getLocalProperties()) {
                    if (!prePartitionedProperty.isSimplifiedBy(localProperty)) {
                        break;
                    }
                    localProperties.add(localProperty);
                }
            }

            if (!node.getPartitionBy().isEmpty()) {
                localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            }

            orderingScheme.ifPresent(scheme ->
                    scheme.getOrderBy().stream()
                            .map(column -> new SortingProperty<>(column, scheme.getOrdering(column)))
                            .forEach(localProperties::add));

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.normalizeAndPrune(localProperties.build()))
                    .build();
        }

        @Override
        public ActualProperties visitGroupId(GroupIdNode node, List<ActualProperties> inputProperties)
        {
            Map<Symbol, Symbol> inputToOutputMappings = new HashMap<>();
            for (Map.Entry<Symbol, Symbol> setMapping : node.getGroupingColumns().entrySet()) {
                if (node.getCommonGroupingColumns().contains(setMapping.getKey())) {
                    // TODO: Add support for translating a property on a single column to multiple columns
                    // when GroupIdNode is copying a single input grouping column into multiple output grouping columns (i.e. aliases), this is basically picking one arbitrarily
                    inputToOutputMappings.putIfAbsent(setMapping.getValue(), setMapping.getKey());
                }
            }

            // TODO: Add support for translating a property on a single column to multiple columns
            // this is deliberately placed after the grouping columns, because preserving properties has a bigger perf impact
            for (Symbol argument : node.getAggregationArguments()) {
                inputToOutputMappings.putIfAbsent(argument, argument);
            }

            return Iterables.getOnlyElement(inputProperties).translate(column -> Optional.ofNullable(inputToOutputMappings.get(column)));
        }

        @Override
        public ActualProperties visitAggregation(AggregationNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translated = properties.translate(symbol -> node.getGroupingKeys().contains(symbol) ? Optional.of(symbol) : Optional.empty());

            return ActualProperties.builderFrom(translated)
                    .local(LocalProperties.grouped(node.getGroupingKeys()))
                    .build();
        }

        @Override
        public ActualProperties visitRowNumber(RowNumberNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitTopNRankingNumber(TopNRankingNumberNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ImmutableList.Builder<LocalProperty<Symbol>> localProperties = ImmutableList.builder();
            localProperties.add(new GroupingProperty<>(node.getPartitionBy()));
            for (Symbol column : node.getOrderingScheme().getOrderBy()) {
                localProperties.add(new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)));
            }

            return ActualProperties.builderFrom(properties)
                    .local(localProperties.build())
                    .build();
        }

        @Override
        public ActualProperties visitTopN(TopNNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderingScheme().getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)))
                    .collect(toImmutableList());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties)
                    .build();
        }

        @Override
        public ActualProperties visitSort(SortNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            List<SortingProperty<Symbol>> localProperties = node.getOrderingScheme().getOrderBy().stream()
                    .map(column -> new SortingProperty<>(column, node.getOrderingScheme().getOrdering(column)))
                    .collect(toImmutableList());

            return ActualProperties.builderFrom(properties)
                    .local(localProperties)
                    .build();
        }

        @Override
        public ActualProperties visitLimit(LimitNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitDistinctLimit(DistinctLimitNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.grouped(node.getDistinctSymbols()))
                    .build();
        }

        @Override
        public ActualProperties visitStatisticsWriterNode(StatisticsWriterNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableFinish(TableFinishNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitCubeFinish(CubeFinishNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableDelete(TableDeleteNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableUpdate(TableUpdateNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(coordinatorSingleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitDelete(DeleteNode node, List<ActualProperties> inputProperties)
        {
            // drop all symbols in property because delete doesn't pass on any of the columns
            return Iterables.getOnlyElement(inputProperties).translate(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitUpdate(UpdateNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties).translate(symbol -> Optional.empty());
        }

        @Override
        public ActualProperties visitJoin(JoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            boolean unordered = spillPossible(session, node.getType());

            switch (node.getType()) {
                case INNER:
                    probeProperties = probeProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));
                    buildProperties = buildProperties.translate(column -> filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column));

                    Map<Symbol, NullableValue> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    if (node.isCrossJoin()) {
                        // Cross join preserves only constants from probe and build sides.
                        // Cross join doesn't preserve sorting or grouping local properties on either side.
                        return ActualProperties.builder()
                                .global(probeProperties)
                                .local(ImmutableList.of())
                                .constants(constants)
                                .build();
                    }

                    return ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .unordered(unordered)
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                            .unordered(unordered)
                            .build();
                case RIGHT:
                    buildProperties = buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));

                    return ActualProperties.builderFrom(buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                            .local(ImmutableList.of())
                            .unordered(true)
                            .build();
                case FULL:
                    // We can't say anything about the partitioning scheme because any partition of
                    // a hash-partitioned join can produce nulls in case of a lack of matches
                    return ActualProperties.builder()
                            .global(probeProperties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                            .build();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitSemiJoin(SemiJoinNode node, List<ActualProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public ActualProperties visitSpatialJoin(SpatialJoinNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties buildProperties = inputProperties.get(1);

            switch (node.getType()) {
                case INNER:
                    probeProperties = probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));
                    buildProperties = buildProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column));

                    Map<Symbol, NullableValue> constants = new HashMap<>();
                    constants.putAll(probeProperties.getConstants());
                    constants.putAll(buildProperties.getConstants());

                    return ActualProperties.builderFrom(probeProperties)
                            .constants(constants)
                            .build();
                case LEFT:
                    return ActualProperties.builderFrom(probeProperties.translate(column -> filterIfMissing(node.getOutputSymbols(), column)))
                            .build();
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitIndexJoin(IndexJoinNode node, List<ActualProperties> inputProperties)
        {
            // TODO: include all equivalent columns in partitioning properties
            ActualProperties probeProperties = inputProperties.get(0);
            ActualProperties indexProperties = inputProperties.get(1);

            switch (node.getType()) {
                case INNER:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(ImmutableMap.<Symbol, NullableValue>builder()
                                    .putAll(probeProperties.getConstants())
                                    .putAll(indexProperties.getConstants())
                                    .build())
                            .build();
                case SOURCE_OUTER:
                    return ActualProperties.builderFrom(probeProperties)
                            .constants(probeProperties.getConstants())
                            .build();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public ActualProperties visitDynamicFilterSource(DynamicFilterSourceNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitIndexSource(IndexSourceNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        public static Map<Symbol, Symbol> exchangeInputToOutput(ExchangeNode node, int sourceIndex)
        {
            List<Symbol> inputSymbols = node.getInputs().get(sourceIndex);
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                inputToOutput.put(inputSymbols.get(i), node.getOutputSymbols().get(i));
            }
            return inputToOutput;
        }

        @Override
        public ActualProperties visitExchange(ExchangeNode node, List<ActualProperties> inputProperties)
        {
            checkArgument(node.getScope() != REMOTE || inputProperties.stream().noneMatch(ActualProperties::isNullsAndAnyReplicated), "Null-and-any replicated inputs should not be remotely exchanged");

            Optional<Set<Map.Entry<Symbol, NullableValue>>> entriesOp = Optional.empty();
            for (int sourceIndex = 0; sourceIndex < node.getSources().size(); sourceIndex++) {
                Map<Symbol, Symbol> inputToOutput = exchangeInputToOutput(node, sourceIndex);
                ActualProperties translated = inputProperties.get(sourceIndex).translate(symbol -> Optional.ofNullable(inputToOutput.get(symbol)));
                if (entriesOp.isPresent()) {
                    entriesOp = Optional.of(Sets.intersection(entriesOp.get(), translated.getConstants().entrySet()));
                }
                else {
                    entriesOp = Optional.of(translated.getConstants().entrySet());
                }
            }

            checkState(entriesOp.isPresent());

            Map<Symbol, NullableValue> constants = entriesOp.get().stream()
                    .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));

            ImmutableList.Builder<SortingProperty<Symbol>> localProperties = ImmutableList.builder();
            if (node.getOrderingScheme().isPresent()) {
                node.getOrderingScheme().get().getOrderBy().stream()
                        .map(column -> new SortingProperty<>(column, node.getOrderingScheme().get().getOrdering(column)))
                        .forEach(localProperties::add);
            }

            // Local exchanges are only created in AddLocalExchanges, at the end of optimization, and
            // local exchanges do not produce all global properties as represented by ActualProperties.
            // This is acceptable because AddLocalExchanges does not use global properties and is only
            // interested in the local properties.
            // However, for the purpose of validation, some global properties (single-node vs distributed)
            // are computed for local exchanges.
            // TODO: implement full properties for local exchanges
            if (node.getScope() == LOCAL) {
                ActualProperties.Builder builder = ActualProperties.builder();
                builder.local(localProperties.build());
                builder.constants(constants);

                if (inputProperties.stream().anyMatch(ActualProperties::isCoordinatorOnly)) {
                    builder.global(coordinatorSingleStreamPartition());
                }
                else if (inputProperties.stream().anyMatch(ActualProperties::isSingleNode)) {
                    builder.global(coordinatorSingleStreamPartition());
                }

                return builder.build();
            }

            switch (node.getType()) {
                case GATHER:
                    boolean coordinatorOnly = node.getPartitioningScheme().getPartitioning().getHandle().isCoordinatorOnly();
                    return ActualProperties.builder()
                            .global(coordinatorOnly ? coordinatorSingleStreamPartition() : singleStreamPartition())
                            .local(localProperties.build())
                            .constants(constants)
                            .build();
                case REPARTITION:
                    return ActualProperties.builder()
                            .global(partitionedOn(
                                    node.getPartitioningScheme().getPartitioning(),
                                    Optional.of(node.getPartitioningScheme().getPartitioning()))
                                    .withReplicatedNulls(node.getPartitioningScheme().isReplicateNullsAndAny()))
                            .constants(constants)
                            .build();
                case REPLICATE:
                    // TODO: this should have the same global properties as the stream taking the replicated data
                    return ActualProperties.builder()
                            .global(arbitraryPartition())
                            .constants(constants)
                            .build();
            }

            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        public ActualProperties visitFilter(FilterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            Map<Symbol, NullableValue> constants = new HashMap<>(properties.getConstants());
            if (isExpression(node.getPredicate())) {
                ExpressionDomainTranslator.ExtractionResult decomposedPredicate = ExpressionDomainTranslator.fromPredicate(
                        metadata,
                        session,
                        castToExpression(node.getPredicate()),
                        types);

                constants.putAll(extractFixedValues(decomposedPredicate.getTupleDomain()).orElse(ImmutableMap.of()));
            }
            else {
                RowExpressionDomainTranslator.ExtractionResult decomposedPredicate =
                        (new RowExpressionDomainTranslator(metadata)).fromPredicate(
                                session.toConnectorSession(),
                                node.getPredicate(), DomainTranslator.BASIC_COLUMN_EXTRACTOR);
                TupleDomain<VariableReferenceExpression> tupleDomain = decomposedPredicate.getTupleDomain();
                Map<Symbol, Domain> symDomain = new HashMap<>();
                if (!tupleDomain.isNone()) {
                    tupleDomain.getDomains().get().entrySet().forEach(entry -> {
                        symDomain.put(new Symbol(entry.getKey().getName()), entry.getValue());
                    });
                    constants.putAll(extractFixedValues(TupleDomain.withColumnDomains(symDomain)).orElse(ImmutableMap.of()));
                }
            }
            return ActualProperties.builderFrom(properties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitProject(ProjectNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            ActualProperties translatedProperties = properties.translateRowExpression(node.getAssignments().getMap(), types);

            // Extract additional constants
            Map<Symbol, NullableValue> constants = new HashMap<>();
            for (Map.Entry<Symbol, RowExpression> assignment : node.getAssignments().entrySet()) {
                RowExpression expression = assignment.getValue();
                Symbol output = assignment.getKey();

                if (isExpression(expression)) {
                    Map<NodeRef<Expression>, Type> expressionTypes = typeAnalyzer.getTypes(session, types, castToExpression(expression));
                    Type type = requireNonNull(expressionTypes.get(NodeRef.of(castToExpression(expression))));
                    ExpressionInterpreter optimizer = ExpressionInterpreter.expressionOptimizer(castToExpression(expression), metadata, session, expressionTypes);
                    // TODO:
                    // We want to use a symbol resolver that looks up in the constants from the input subplan
                    // to take advantage of constant-folding for complex expressions
                    // However, that currently causes errors when those expressions operate on arrays or row types
                    // ("ROW comparison not supported for fields with null elements", etc)
                    Object value = optimizer.optimize(NoOpSymbolResolver.INSTANCE);

                    if (value instanceof SymbolReference) {
                        Symbol symbol = SymbolUtils.from((SymbolReference) value);
                        NullableValue existingConstantValue = constants.get(symbol);
                        if (existingConstantValue != null) {
                            constants.put(assignment.getKey(), new NullableValue(type, value));
                        }
                    }
                    else if (!(value instanceof Expression)) {
                        constants.put(assignment.getKey(), new NullableValue(type, value));
                    }
                }
                else {
                    Object value = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED).optimize();

                    if (value instanceof VariableReferenceExpression) {
                        NullableValue existingConstantValue = constants.get(value);
                        if (existingConstantValue != null) {
                            constants.put(output, new NullableValue(expression.getType(), value));
                        }
                    }
                    else if (!(value instanceof RowExpression)) {
                        constants.put(output, new NullableValue(expression.getType(), value));
                    }
                }
            }
            constants.putAll(translatedProperties.getConstants());

            return ActualProperties.builderFrom(translatedProperties)
                    .constants(constants)
                    .build();
        }

        @Override
        public ActualProperties visitVacuumTable(VacuumTableNode node, List<ActualProperties> inputProperties)
        {
            return ActualProperties.builder()
                    .global(partitionedOn(
                            SINGLE_DISTRIBUTION,
                            ImmutableList.of(),
                            Optional.of(ImmutableList.of())))
                    .build();
        }

        @Override
        public ActualProperties visitTableWriter(TableWriterNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);

            if (properties.isCoordinatorOnly()) {
                return ActualProperties.builder()
                        .global(coordinatorSingleStreamPartition())
                        .build();
            }
            return ActualProperties.builder()
                    .global(properties.isSingleNode() ? singleStreamPartition() : arbitraryPartition())
                    .build();
        }

        @Override
        public ActualProperties visitSample(SampleNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUnnest(UnnestNode node, List<ActualProperties> inputProperties)
        {
            Set<Symbol> passThroughInputs = ImmutableSet.copyOf(node.getReplicateSymbols());

            return Iterables.getOnlyElement(inputProperties).translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });
        }

        @Override
        public ActualProperties visitValues(ValuesNode node, List<ActualProperties> context)
        {
            return ActualProperties.builder()
                    .global(singleStreamPartition())
                    .build();
        }

        @Override
        public ActualProperties visitTableScan(TableScanNode node, List<ActualProperties> inputProperties)
        {
            TableProperties layout = metadata.getTableProperties(session, node.getTable());
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            ActualProperties.Builder properties = ActualProperties.builder();

            // Globally constant assignments
            Map<ColumnHandle, NullableValue> globalConstants = new HashMap<>();

            extractFixedValues(metadata.getTableProperties(session, node.getTable()).getPredicate())
                    .orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())
                    .forEach(entry -> globalConstants.put(entry.getKey(), entry.getValue()));

            Map<Symbol, NullableValue> symbolConstants = globalConstants.entrySet().stream()
                    .filter(entry -> assignments.containsKey(entry.getKey()))
                    .collect(toMap(entry -> assignments.get(entry.getKey()), Map.Entry::getValue));
            properties.constants(symbolConstants);

            // Partitioning properties
            properties.global(deriveGlobalProperties(layout, assignments, globalConstants));

            // Append the global constants onto the local properties to maximize their translation potential
            List<LocalProperty<ColumnHandle>> constantAppendedLocalProperties = ImmutableList.<LocalProperty<ColumnHandle>>builder()
                    .addAll(globalConstants.keySet().stream().map(ConstantProperty::new).iterator())
                    .addAll(layout.getLocalProperties())
                    .build();
            properties.local(LocalProperties.translate(constantAppendedLocalProperties, column -> Optional.ofNullable(assignments.get(column))));

            return properties.build();
        }

        @Override
        public ActualProperties visitCreateIndex(CreateIndexNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitUpdateIndex(UpdateIndexNode node, List<ActualProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public ActualProperties visitCTEScan(CTEScanNode node, List<ActualProperties> inputProperties)
        {
            ActualProperties properties = Iterables.getOnlyElement(inputProperties);
            return ActualProperties.builderFrom(properties)
                    .local(LocalProperties.grouped(node.getOutputSymbols()))
                    .build();
        }

        private Global deriveGlobalProperties(TableProperties layout, Map<ColumnHandle, Symbol> assignments, Map<ColumnHandle, NullableValue> constants)
        {
            Optional<List<Symbol>> streamPartitioning = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> translateToNonConstantSymbols(columns, assignments, constants));

            if (planWithTableNodePartitioning(session) && layout.getTablePartitioning().isPresent()) {
                TablePartitioning tablePartitioning = layout.getTablePartitioning().get();
                if (assignments.keySet().containsAll(tablePartitioning.getPartitioningColumns())) {
                    List<Symbol> arguments = tablePartitioning.getPartitioningColumns().stream()
                            .map(assignments::get)
                            .collect(toImmutableList());

                    return partitionedOn(tablePartitioning.getPartitioningHandle(), arguments, streamPartitioning);
                }
            }

            if (streamPartitioning.isPresent()) {
                return streamPartitionedOn(streamPartitioning.get());
            }
            return arbitraryPartition();
        }

        private static Optional<List<Symbol>> translateToNonConstantSymbols(
                Set<ColumnHandle> columnHandles,
                Map<ColumnHandle, Symbol> assignments,
                Map<ColumnHandle, NullableValue> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.containsKey(column))
                    .collect(toImmutableSet());

            ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
            for (ColumnHandle column : constantsStrippedColumns) {
                Symbol translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(ImmutableList.copyOf(builder.build()));
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, Expression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
                if (assignment.getValue() instanceof SymbolReference) {
                    inputToOutput.put(SymbolUtils.from(assignment.getValue()), assignment.getKey());
                }
            }
            return inputToOutput;
        }
    }

    static boolean spillPossible(Session session, JoinNode.Type joinType)
    {
        if (!SystemSessionProperties.isSpillEnabled(session)) {
            return false;
        }
        switch (joinType) {
            case INNER:
            case LEFT:
                // Even though join might not have "spillable" property set yet
                // it might still be set as spillable later on by AddLocalExchanges.
                return true;
            case RIGHT:
            case FULL:
                // Currently there is no spill support for outer on the build side.
                return false;
            default:
                throw new IllegalStateException("Unknown join type: " + joinType);
        }
    }

    public static Optional<Symbol> filterIfMissing(Collection<Symbol> columns, Symbol column)
    {
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        return Optional.empty();
    }

    // Used to filter columns that are not exposed by join node
    // Or, if they are part of the equalities, to translate them
    // to the other symbol if that's exposed, instead.
    public static Optional<Symbol> filterOrRewrite(Collection<Symbol> columns, Collection<JoinNode.EquiJoinClause> equalities, Symbol column)
    {
        // symbol is exposed directly, so no translation needed
        if (columns.contains(column)) {
            return Optional.of(column);
        }

        // if the column is part of the equality conditions and its counterpart
        // is exposed, use that, instead
        for (JoinNode.EquiJoinClause equality : equalities) {
            if (equality.getLeft().equals(column) && columns.contains(equality.getRight())) {
                return Optional.of(equality.getRight());
            }
            else if (equality.getRight().equals(column) && columns.contains(equality.getLeft())) {
                return Optional.of(equality.getLeft());
            }
        }

        return Optional.empty();
    }

    private static Optional<Symbol> rewriteExpression(Map<Symbol, Expression> assignments, Expression expression)
    {
        // Only simple coalesce expressions supported currently
        if (!(expression instanceof CoalesceExpression)) {
            return Optional.empty();
        }

        Set<Expression> arguments = ImmutableSet.copyOf(((CoalesceExpression) expression).getOperands());
        if (!arguments.stream().allMatch(SymbolReference.class::isInstance)) {
            return Optional.empty();
        }

        // We are using the property that the result of coalesce from full outer join keys would not be null despite of the order
        // of the arguments. Thus we extract and compare the symbols of the CoalesceExpression as a set rather than compare the
        // CoalesceExpression directly.
        for (Map.Entry<Symbol, Expression> entry : assignments.entrySet()) {
            if (entry.getValue() instanceof CoalesceExpression) {
                Set<Expression> candidateArguments = ImmutableSet.copyOf(((CoalesceExpression) entry.getValue()).getOperands());
                if (!candidateArguments.stream().allMatch(SymbolReference.class::isInstance)) {
                    return Optional.empty();
                }

                if (candidateArguments.equals(arguments)) {
                    return Optional.of(entry.getKey());
                }
            }
        }
        return Optional.empty();
    }
}
