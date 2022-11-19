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
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.LocalProperty;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.Partitioning.ArgumentBinding;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
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
import io.prestosql.sql.tree.SymbolReference;

import javax.annotation.concurrent.Immutable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.spi.predicate.TupleDomain.extractFixedValues;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static io.prestosql.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.FIXED;
import static io.prestosql.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.MULTIPLE;
import static io.prestosql.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties.StreamDistribution.SINGLE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public final class StreamPropertyDerivations
{
    private StreamPropertyDerivations() {}

    public static StreamProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        List<StreamProperties> inputProperties = node.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, session, types, typeAnalyzer))
                .collect(toImmutableList());
        return StreamPropertyDerivations.deriveProperties(node, inputProperties, metadata, session, types, typeAnalyzer);
    }

    public static StreamProperties deriveProperties(PlanNode node, StreamProperties inputProperties, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        return deriveProperties(node, ImmutableList.of(inputProperties), metadata, session, types, typeAnalyzer);
    }

    public static StreamProperties deriveProperties(PlanNode node, List<StreamProperties> inputProperties, Metadata metadata, Session session, TypeProvider types, TypeAnalyzer typeAnalyzer)
    {
        requireNonNull(node, "node is null");
        requireNonNull(inputProperties, "inputProperties is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(typeAnalyzer, "typeAnalyzer is null");

        // properties.otherActualProperties will never be null here because the only way
        // an external caller should obtain StreamProperties is from this method, and the
        // last line of this method assures otherActualProperties is set.
        ActualProperties otherProperties = PropertyDerivations.streamBackdoorDeriveProperties(
                node,
                inputProperties.stream()
                        .map(properties -> properties.otherActualProperties)
                        .collect(toImmutableList()),
                metadata,
                session,
                types,
                typeAnalyzer);

        StreamProperties result = node.accept(new Visitor(metadata, session), inputProperties)
                .withOtherActualProperties(otherProperties);

        result.getPartitioningColumns().ifPresent(columns ->
                verify(node.getOutputSymbols().containsAll(columns), "Stream-level partitioning properties contain columns not present in node's output"));

        Set<Symbol> localPropertyColumns = result.getLocalProperties().stream()
                .flatMap(property -> property.getColumns().stream())
                .collect(Collectors.toSet());

        verify(node.getOutputSymbols().containsAll(localPropertyColumns), "Stream-level local properties contain columns not present in node's output");

        return result;
    }

    private static class Visitor
            extends InternalPlanVisitor<StreamProperties, List<StreamProperties>>
    {
        private final Metadata metadata;
        private final Session session;

        private Visitor(Metadata metadata, Session session)
        {
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public StreamProperties visitPlan(PlanNode node, List<StreamProperties> inputProperties)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        public StreamProperties visitTableExecute(PlanNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table execute only outputs the row count and fragments
            return properties.withUnspecifiedPartitioning();
        }

        //
        // Joins
        //

        @Override
        public StreamProperties visitJoin(JoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties leftProperties = inputProperties.get(0);
            boolean unordered = spillPossible(session, node);

            switch (node.getType()) {
                case INNER:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterOrRewrite(node.getOutputSymbols(), node.getCriteria(), column))
                            .unordered(unordered);
                case LEFT:
                    return leftProperties
                            .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column))
                            .unordered(unordered);
                case RIGHT:
                    // since this is a right join, none of the matched output rows will contain nulls
                    // in the left partitioning columns, and all of the unmatched rows will have
                    // null for all left columns.  therefore, the output is still partitioned on the
                    // left columns.  the only change is there will be at least two streams so the
                    // output is multiple
                    // There is one exception to this.  If the left is partitioned on empty set, we
                    // we can't say that the output is partitioned on empty set, but we can say that
                    // it is partitioned on the left join symbols
                    // todo do something smarter after https://github.com/prestodb/presto/pull/5877 is merged
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
                case FULL:
                    // the left can contain nulls in any stream so we can't say anything about the
                    // partitioning, and nulls from the right are produced from a extra new stream
                    // so we will always have multiple streams.
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private static boolean spillPossible(Session session, JoinNode node)
        {
            return isSpillEnabled(session) && node.isSpillable().orElseThrow(() -> new IllegalArgumentException("spillable not yet set"));
        }

        @Override
        public StreamProperties visitSpatialJoin(SpatialJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties leftProperties = inputProperties.get(0);

            switch (node.getType()) {
                case INNER:
                case LEFT:
                    return leftProperties.translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column));
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitIndexJoin(IndexJoinNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties probeProperties = inputProperties.get(0);

            switch (node.getType()) {
                case INNER:
                    return probeProperties;
                case SOURCE_OUTER:
                    // the probe can contain nulls in any stream so we can't say anything about the
                    // partitioning but the other properties of the probe will be maintained.
                    return probeProperties.withUnspecifiedPartitioning();
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        @Override
        public StreamProperties visitDynamicFilterSource(DynamicFilterSourceNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        //
        // Source nodes
        //

        @Override
        public StreamProperties visitValues(ValuesNode node, List<StreamProperties> context)
        {
            // values always produces a single stream
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableScan(TableScanNode node, List<StreamProperties> inputProperties)
        {
            TableProperties layout = metadata.getTableProperties(session, node.getTable());
            Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();

            // Globally constant assignments
            Set<ColumnHandle> constants = new HashSet<>();
            extractFixedValues(metadata.getTableProperties(session, node.getTable()).getPredicate())
                    .orElse(ImmutableMap.of())
                    .entrySet().stream()
                    .filter(entry -> !entry.getValue().isNull())  // TODO consider allowing nulls
                    .forEach(entry -> constants.add(entry.getKey()));

            Optional<Set<Symbol>> streamPartitionSymbols = layout.getStreamPartitioningColumns()
                    .flatMap(columns -> getNonConstantSymbols(columns, assignments, constants));

            // if we are partitioned on empty set, we must say multiple of unknown partitioning, because
            // the connector does not guarantee a single split in this case (since it might not understand
            // that the value is a constant).
            if (streamPartitionSymbols.isPresent() && streamPartitionSymbols.get().isEmpty()) {
                return new StreamProperties(MULTIPLE, Optional.empty(), false);
            }
            return new StreamProperties(MULTIPLE, streamPartitionSymbols, false);
        }

        @Override
        public StreamProperties visitUpdateIndex(UpdateIndexNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitCreateIndex(CreateIndexNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // create index only outputs the plan string
            return properties.withUnspecifiedPartitioning();
        }

        private static Optional<Set<Symbol>> getNonConstantSymbols(Set<ColumnHandle> columnHandles, Map<ColumnHandle, Symbol> assignments, Set<ColumnHandle> globalConstants)
        {
            // Strip off the constants from the partitioning columns (since those are not required for translation)
            Set<ColumnHandle> constantsStrippedPartitionColumns = columnHandles.stream()
                    .filter(column -> !globalConstants.contains(column))
                    .collect(toImmutableSet());
            ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();

            for (ColumnHandle column : constantsStrippedPartitionColumns) {
                Symbol translated = assignments.get(column);
                if (translated == null) {
                    return Optional.empty();
                }
                builder.add(translated);
            }

            return Optional.of(builder.build());
        }

        @Override
        public StreamProperties visitExchange(ExchangeNode node, List<StreamProperties> inputProperties)
        {
            if (node.getOrderingScheme().isPresent()) {
                return StreamProperties.ordered();
            }

            if (node.getScope() == REMOTE) {
                // TODO: correctly determine if stream is parallelised
                // based on session properties
                return StreamProperties.fixedStreams();
            }

            switch (node.getType()) {
                case GATHER:
                    return StreamProperties.singleStream();
                case REPARTITION:
                    if (node.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION)) {
                        return new StreamProperties(FIXED, Optional.empty(), false);
                    }
                    return new StreamProperties(
                            FIXED,
                            Optional.of(node.getPartitioningScheme().getPartitioning().getArguments().stream()
                                    .map(ArgumentBinding::getColumn)
                                    .collect(toImmutableList())), false);
                case REPLICATE:
                    return new StreamProperties(MULTIPLE, Optional.empty(), false);
            }

            throw new UnsupportedOperationException("not yet implemented");
        }

        //
        // Nodes that rewrite and/or drop symbols
        //

        @Override
        public StreamProperties visitProject(ProjectNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // We can describe properties in terms of inputs that are projected unmodified (i.e., identity projections)
            Map<Symbol, Symbol> identities = computeIdentityTranslations(node.getAssignments().getMap());
            return properties.translate(column -> Optional.ofNullable(identities.get(column)));
        }

        private static Map<Symbol, Symbol> computeIdentityTranslations(Map<Symbol, RowExpression> assignments)
        {
            Map<Symbol, Symbol> inputToOutput = new HashMap<>();
            for (Map.Entry<Symbol, RowExpression> assignment : assignments.entrySet()) {
                RowExpression expression = assignment.getValue();
                if (isExpression(expression)) {
                    if (castToExpression(expression) instanceof SymbolReference) {
                        inputToOutput.put(SymbolUtils.from(castToExpression(expression)), assignment.getKey());
                    }
                }
                else {
                    if (expression instanceof VariableReferenceExpression) {
                        inputToOutput.put(new Symbol(((VariableReferenceExpression) expression).getName()), assignment.getKey());
                    }
                }
            }
            return inputToOutput;
        }

        @Override
        public StreamProperties visitGroupId(GroupIdNode node, List<StreamProperties> inputProperties)
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
        public StreamProperties visitAggregation(AggregationNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // Only grouped symbols projected symbols are passed through
            return properties.translate(symbol -> node.getGroupingKeys().contains(symbol) ? Optional.of(symbol) : Optional.empty());
        }

        @Override
        public StreamProperties visitStatisticsWriterNode(StatisticsWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // analyze finish only outputs row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitTableFinish(TableFinishNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table finish only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitCubeFinish(CubeFinishNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table finish only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitVacuumTable(VacuumTableNode node, List<StreamProperties> context)
        {
            // vacuum only outputs a single row count
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableDelete(TableDeleteNode node, List<StreamProperties> inputProperties)
        {
            // delete only outputs a single row count
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitTableUpdate(TableUpdateNode node, List<StreamProperties> inputProperties)
        {
            // delete only outputs a single row count
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitDelete(DeleteNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // delete only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitUpdate(UpdateNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitTableWriter(TableWriterNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // table writer only outputs the row count
            return properties.withUnspecifiedPartitioning();
        }

        @Override
        public StreamProperties visitUnnest(UnnestNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);

            // We can describe properties in terms of inputs that are projected unmodified (i.e., not the unnested symbols)
            Set<Symbol> passThroughInputs = ImmutableSet.copyOf(node.getReplicateSymbols());
            return properties.translate(column -> {
                if (passThroughInputs.contains(column)) {
                    return Optional.of(column);
                }
                return Optional.empty();
            });
        }

        @Override
        public StreamProperties visitExplainAnalyze(ExplainAnalyzeNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            // explain only outputs the plan string
            return properties.withUnspecifiedPartitioning();
        }

        //
        // Nodes that gather data into a single stream
        //

        @Override
        public StreamProperties visitIndexSource(IndexSourceNode node, List<StreamProperties> context)
        {
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitUnion(UnionNode node, List<StreamProperties> context)
        {
            // union is implemented using a local gather exchange
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitEnforceSingleRow(EnforceSingleRowNode node, List<StreamProperties> context)
        {
            return StreamProperties.singleStream();
        }

        @Override
        public StreamProperties visitAssignUniqueId(AssignUniqueId node, List<StreamProperties> inputProperties)
        {
            StreamProperties properties = Iterables.getOnlyElement(inputProperties);
            if (properties.getPartitioningColumns().isPresent()) {
                // preserve input (possibly preferred) partitioning
                return properties;
            }

            return new StreamProperties(properties.getDistribution(),
                    Optional.of(ImmutableList.of(node.getIdColumn())),
                    properties.isOrdered());
        }

        //
        // Simple nodes that pass through stream properties
        //

        @Override
        public StreamProperties visitOutput(OutputNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties)
                    .translate(column -> PropertyDerivations.filterIfMissing(node.getOutputSymbols(), column));
        }

        @Override
        public StreamProperties visitMarkDistinct(MarkDistinctNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitWindow(WindowNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitRowNumber(RowNumberNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitTopNRankingNumber(TopNRankingNumberNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitTopN(TopNNode node, List<StreamProperties> inputProperties)
        {
            // Partial TopN doesn't guarantee that stream is ordered
            if (node.getStep().equals(TopNNode.Step.PARTIAL)) {
                return Iterables.getOnlyElement(inputProperties);
            }
            return StreamProperties.ordered();
        }

        @Override
        public StreamProperties visitSort(SortNode node, List<StreamProperties> inputProperties)
        {
            StreamProperties sourceProperties = Iterables.getOnlyElement(inputProperties);
            if (sourceProperties.isSingleStream()) {
                // stream is only sorted if sort operator is executed without parallelism
                return StreamProperties.ordered();
            }

            return sourceProperties;
        }

        @Override
        public StreamProperties visitLimit(LimitNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitDistinctLimit(DistinctLimitNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitSemiJoin(SemiJoinNode node, List<StreamProperties> inputProperties)
        {
            return inputProperties.get(0);
        }

        @Override
        public StreamProperties visitApply(ApplyNode node, List<StreamProperties> inputProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass());
        }

        @Override
        public StreamProperties visitLateralJoin(LateralJoinNode node, List<StreamProperties> inputProperties)
        {
            throw new IllegalStateException("Unexpected node: " + node.getClass());
        }

        @Override
        public StreamProperties visitFilter(FilterNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitSample(SampleNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }

        @Override
        public StreamProperties visitCTEScan(CTEScanNode node, List<StreamProperties> inputProperties)
        {
            return Iterables.getOnlyElement(inputProperties);
        }
    }

    @Immutable
    public static final class StreamProperties
    {
        public enum StreamDistribution
        {
            SINGLE, MULTIPLE, FIXED
        }

        private final StreamDistribution distribution;

        private final Optional<List<Symbol>> partitioningColumns; // if missing => partitioned with some unknown scheme

        private final boolean ordered;

        // We are only interested in the local properties, but PropertyDerivations requires input
        // ActualProperties, so we hold on to the whole object
        private final ActualProperties otherActualProperties;

        // NOTE: Partitioning on zero columns (or effectively zero columns if the columns are constant) indicates that all
        // the rows will be partitioned into a single stream.

        private StreamProperties(StreamDistribution distribution, Optional<? extends Iterable<Symbol>> partitioningColumns, boolean ordered)
        {
            this(distribution, partitioningColumns, ordered, null);
        }

        private StreamProperties(
                StreamDistribution distribution,
                Optional<? extends Iterable<Symbol>> partitioningColumns,
                boolean ordered,
                ActualProperties otherActualProperties)
        {
            this.distribution = requireNonNull(distribution, "distribution is null");

            this.partitioningColumns = requireNonNull(partitioningColumns, "partitioningProperties is null")
                    .map(ImmutableList::copyOf);

            checkArgument(distribution != SINGLE || this.partitioningColumns.equals(Optional.of(ImmutableList.of())),
                    "Single stream must be partitioned on empty set");
            checkArgument(distribution == SINGLE || !this.partitioningColumns.equals(Optional.of(ImmutableList.of())),
                    "Multiple streams must not be partitioned on empty set");

            this.ordered = ordered;
            checkArgument(!ordered || distribution == SINGLE, "Ordered must be a single stream");

            this.otherActualProperties = otherActualProperties;
        }

        public List<LocalProperty<Symbol>> getLocalProperties()
        {
            checkState(otherActualProperties != null, "otherActualProperties not set");
            return otherActualProperties.getLocalProperties();
        }

        private static StreamProperties singleStream()
        {
            return new StreamProperties(SINGLE, Optional.of(ImmutableSet.of()), false);
        }

        private static StreamProperties fixedStreams()
        {
            return new StreamProperties(FIXED, Optional.empty(), false);
        }

        private static StreamProperties ordered()
        {
            return new StreamProperties(SINGLE, Optional.of(ImmutableSet.of()), true);
        }

        private StreamProperties unordered(boolean unordered)
        {
            if (unordered) {
                ActualProperties updatedProperies = null;
                if (otherActualProperties != null) {
                    updatedProperies = ActualProperties.builderFrom(otherActualProperties)
                            .unordered(true)
                            .build();
                }
                return new StreamProperties(
                        distribution,
                        partitioningColumns,
                        false,
                        updatedProperies);
            }
            return this;
        }

        public boolean isSingleStream()
        {
            return distribution == SINGLE;
        }

        public StreamDistribution getDistribution()
        {
            return distribution;
        }

        public boolean isExactlyPartitionedOn(Iterable<Symbol> columns)
        {
            return partitioningColumns.isPresent() && columns.equals(ImmutableList.copyOf(partitioningColumns.get()));
        }

        public boolean isPartitionedOn(Iterable<Symbol> columns)
        {
            if (!partitioningColumns.isPresent()) {
                return false;
            }

            // partitioned on (k_1, k_2, ..., k_n) => partitioned on (k_1, k_2, ..., k_n, k_n+1, ...)
            // can safely ignore all constant columns when comparing partition properties
            return ImmutableSet.copyOf(columns).containsAll(partitioningColumns.get());
        }

        public boolean isOrdered()
        {
            return ordered;
        }

        private StreamProperties withUnspecifiedPartitioning()
        {
            // a single stream has no symbols
            if (isSingleStream()) {
                return this;
            }
            // otherwise we are distributed on some symbols, but since we are trying to remove all symbols,
            // just say we have multiple partitions with an unknown scheme
            return new StreamProperties(distribution, Optional.empty(), ordered);
        }

        private StreamProperties withOtherActualProperties(ActualProperties actualProperties)
        {
            return new StreamProperties(distribution, partitioningColumns, ordered, actualProperties);
        }

        public StreamProperties translate(Function<Symbol, Optional<Symbol>> translator)
        {
            return new StreamProperties(
                    distribution,
                    partitioningColumns.flatMap(partitioning -> {
                        ImmutableList.Builder<Symbol> newPartitioningColumns = ImmutableList.builder();
                        for (Symbol partitioningColumn : partitioning) {
                            Optional<Symbol> translated = translator.apply(partitioningColumn);
                            if (!translated.isPresent()) {
                                return Optional.empty();
                            }
                            newPartitioningColumns.add(translated.get());
                        }
                        return Optional.of(newPartitioningColumns.build());
                    }),
                    ordered, otherActualProperties.translate(translator));
        }

        public Optional<List<Symbol>> getPartitioningColumns()
        {
            return partitioningColumns;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(distribution, partitioningColumns);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            StreamProperties other = (StreamProperties) obj;
            return Objects.equals(this.distribution, other.distribution) &&
                    Objects.equals(this.partitioningColumns, other.partitioningColumns);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("distribution", distribution)
                    .add("partitioningColumns", partitioningColumns)
                    .toString();
        }
    }
}
