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
package io.prestosql.sql.planner.planprinter;

import com.google.common.base.CaseFormat;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Streams;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageStats;
import io.prestosql.execution.TableInfo;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.ExceptNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.IntersectNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.JoinOnAggregationNode.JoinInternalAggregation;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.OrderingScheme;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Marker;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Partitioning;
import io.prestosql.sql.planner.PartitioningScheme;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.SortExpressionContext;
import io.prestosql.sql.planner.SortExpressionExtractor;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.JoinNodeUtils;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.CacheTableWriterNode;
import io.prestosql.sql.planner.plan.CreateIndexNode;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExchangeNode.Scope;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.IndexSourceNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.OffsetNode;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableExecuteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableUpdateNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.UpdateIndexNode;
import io.prestosql.sql.planner.plan.UpdateNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.sql.planner.planprinter.NodeRepresentation.TypedSymbol;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.util.GraphvizPrinter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.execution.StageInfo.getAllStages;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.planprinter.PlanNodeStatsSummarizer.aggregateStageStats;
import static io.prestosql.sql.planner.planprinter.TextRenderer.formatDouble;
import static io.prestosql.sql.planner.planprinter.TextRenderer.formatPositions;
import static io.prestosql.sql.planner.planprinter.TextRenderer.indentString;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.lang.String.format;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

public class PlanPrinter
{
    private final PlanRepresentation representation;
    private final Function<TableScanNode, TableInfo> tableInfoSupplier;
    private final ValuePrinter valuePrinter;
    private final Function<RowExpression, String> formatter;

    // NOTE: do NOT add Metadata or Session to this class.  The plan printer must be usable outside of a transaction.
    private PlanPrinter(
            PlanNode planRoot,
            TypeProvider types,
            Optional<StageExecutionDescriptor> stageExecutionStrategy,
            Function<TableScanNode, TableInfo> tableInfoSupplier,
            ValuePrinter valuePrinter,
            StatsAndCosts estimatedStatsAndCosts,
            Optional<Map<PlanNodeId, PlanNodeStats>> stats,
            Metadata metadata)
    {
        requireNonNull(planRoot, "planRoot is null");
        requireNonNull(types, "types is null");
        requireNonNull(tableInfoSupplier, "tableInfoSupplier is null");
        requireNonNull(valuePrinter, "valuePrinter is null");
        requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
        requireNonNull(stats, "stats is null");

        this.tableInfoSupplier = tableInfoSupplier;
        this.valuePrinter = valuePrinter;

        Optional<Duration> totalCpuTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeScheduledTime().toMillis())
                .sum(), MILLISECONDS));

        Optional<Duration> totalScheduledTime = stats.map(s -> new Duration(s.values().stream()
                .mapToLong(planNode -> planNode.getPlanNodeCpuTime().toMillis())
                .sum(), MILLISECONDS));

        this.representation = new PlanRepresentation(planRoot, types, totalCpuTime, totalScheduledTime);

        RowExpressionFormatter rowExpressionFormatter = new RowExpressionFormatter(metadata);
        this.formatter = rowExpressionFormatter::formatRowExpression;

        Visitor visitor = new Visitor(stageExecutionStrategy, types, estimatedStatsAndCosts, stats, metadata);
        planRoot.accept(visitor, null);
    }

    private String toText(boolean verbose, int level)
    {
        return new TextRenderer(verbose, level).render(representation);
    }

    private String toJson()
    {
        return new JsonRenderer().render(representation);
    }

    public static String jsonFragmentPlan(PlanNode root, Map<Symbol, Type> symbols, Metadata metadata, Session session)
    {
        TypeProvider typeProvider = TypeProvider.copyOf(symbols.entrySet().stream()
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));

        TableInfoSupplier supplier = new TableInfoSupplier(metadata, session);
        ValuePrinter printer = new ValuePrinter(metadata, session);
        return new PlanPrinter(root, typeProvider, Optional.empty(), supplier, printer, StatsAndCosts.empty(), Optional.empty(), metadata).toJson();
    }

    public static String textLogicalPlan(
            PlanNode plan,
            TypeProvider types,
            Metadata metadata,
            StatsAndCosts estimatedStatsAndCosts,
            Session session,
            int level,
            boolean verbose)
    {
        TableInfoSupplier supplier = new TableInfoSupplier(metadata, session);
        ValuePrinter printer = new ValuePrinter(metadata, session);
        return new PlanPrinter(plan, types, Optional.empty(), supplier, printer, estimatedStatsAndCosts, Optional.empty(), metadata).toText(verbose, level);
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, Metadata metadata, Session session, boolean verbose)
    {
        return textDistributedPlan(
                outputStageInfo,
                new ValuePrinter(metadata, session),
                verbose,
                metadata);
    }

    public static String textDistributedPlan(StageInfo outputStageInfo, ValuePrinter valuePrinter, boolean verbose, Metadata metadata)
    {
        Map<PlanNodeId, TableInfo> tableInfos = getAllStages(Optional.of(outputStageInfo)).stream()
                .map(StageInfo::getTables)
                .map(Map::entrySet)
                .flatMap(Collection::stream)
                .collect(toImmutableMap(Entry::getKey, Entry::getValue));

        StringBuilder builder = new StringBuilder();
        List<StageInfo> allStages = getAllStages(Optional.of(outputStageInfo));
        List<PlanFragment> allFragments = allStages.stream()
                .map(StageInfo::getPlan)
                .collect(toImmutableList());
        Map<PlanNodeId, PlanNodeStats> aggregatedStats = aggregateStageStats(allStages);
        for (StageInfo stageInfo : allStages) {
            builder.append(formatFragment(
                    tableScanNode -> tableInfos.get(tableScanNode.getId()),
                    valuePrinter,
                    stageInfo.getPlan(),
                    Optional.of(stageInfo),
                    Optional.of(aggregatedStats),
                    verbose,
                    allFragments,
                    metadata));
        }

        return builder.toString();
    }

    public static String textDistributedPlan(SubPlan plan, Metadata metadata, Session session, boolean verbose)
    {
        TableInfoSupplier supplier = new TableInfoSupplier(metadata, session);
        ValuePrinter printer = new ValuePrinter(metadata, session);
        StringBuilder builder = new StringBuilder();
        for (PlanFragment fragment : plan.getAllFragments()) {
            builder.append(formatFragment(supplier, printer, fragment, Optional.empty(), Optional.empty(), verbose, plan.getAllFragments(), metadata));
        }

        return builder.toString();
    }

    private static String formatFragment(
            Function<TableScanNode, TableInfo> tableInfoSupplier,
            ValuePrinter valuePrinter,
            PlanFragment fragment,
            Optional<StageInfo> stageInfo,
            Optional<Map<PlanNodeId, PlanNodeStats>> planNodeStats,
            boolean verbose,
            List<PlanFragment> allFragments,
            Metadata metadata)
    {
        StringBuilder builder = new StringBuilder();
        builder.append(format("Fragment %s [%s]\n",
                fragment.getId(),
                fragment.getPartitioning()));

        if (stageInfo.isPresent()) {
            StageStats stageStats = stageInfo.get().getStageStats();

            double avgPositionsPerTask = stageInfo.get().getTasks().stream().mapToLong(task -> task.getStats().getProcessedInputPositions()).average().orElse(Double.NaN);
            double squaredDifferences = stageInfo.get().getTasks().stream().mapToDouble(task -> Math.pow(task.getStats().getProcessedInputPositions() - avgPositionsPerTask, 2)).sum();
            double sdAmongTasks = Math.sqrt(squaredDifferences / stageInfo.get().getTasks().size());

            builder.append(indentString(1))
                    .append(format("CPU: %s, Scheduled: %s, Input: %s (%s); per task: avg.: %s std.dev.: %s, Output: %s (%s)\n",
                            stageStats.getTotalCpuTime().convertToMostSuccinctTimeUnit(),
                            stageStats.getTotalScheduledTime().convertToMostSuccinctTimeUnit(),
                            formatPositions(stageStats.getProcessedInputPositions()),
                            stageStats.getProcessedInputDataSize(),
                            formatDouble(avgPositionsPerTask),
                            formatDouble(sdAmongTasks),
                            formatPositions(stageStats.getOutputPositions()),
                            stageStats.getOutputDataSize()));
        }

        PartitioningScheme partitioningScheme = fragment.getPartitioningScheme();
        builder.append(indentString(1))
                .append(format("Output layout: [%s]\n",
                        Joiner.on(", ").join(partitioningScheme.getOutputLayout())));

        boolean replicateNullsAndAny = partitioningScheme.isReplicateNullsAndAny();
        List<String> arguments = partitioningScheme.getPartitioning().getArguments().stream()
                .map(argument -> {
                    if (argument.isConstant()) {
                        NullableValue constant = argument.getConstant();
                        String printableValue = valuePrinter.castToVarchar(constant.getType(), constant.getValue());
                        return constant.getType().getDisplayName() + "(" + printableValue + ")";
                    }
                    return argument.getColumn().toString();
                })
                .collect(toImmutableList());
        builder.append(indentString(1));
        if (replicateNullsAndAny) {
            builder.append(format("Output partitioning: %s (replicate nulls and any) [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        else {
            builder.append(format("Output partitioning: %s [%s]%s\n",
                    partitioningScheme.getPartitioning().getHandle(),
                    Joiner.on(", ").join(arguments),
                    formatHash(partitioningScheme.getHashColumn())));
        }
        builder.append(indentString(1)).append(format("Stage Execution Strategy: %s\n", fragment.getStageExecutionDescriptor().getStageExecutionStrategy()));

        TypeProvider typeProvider = TypeProvider.copyOf(allFragments.stream()
                .flatMap(f -> f.getSymbols().entrySet().stream())
                .distinct()
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue)));
        builder.append(
                new PlanPrinter(
                        fragment.getRoot(),
                        typeProvider,
                        Optional.of(fragment.getStageExecutionDescriptor()),
                        tableInfoSupplier,
                        valuePrinter,
                        fragment.getStatsAndCosts(),
                        planNodeStats,
                        metadata).toText(verbose, 1))
                .append("\n");

        return builder.toString();
    }

    public static String graphvizLogicalPlan(PlanNode plan, TypeProvider types)
    {
        // TODO: This should move to something like GraphvizRenderer
        PlanFragment fragment = new PlanFragment(
                new PlanFragmentId("graphviz_plan"),
                plan,
                types.allTypes(),
                SINGLE_DISTRIBUTION,
                ImmutableList.of(plan.getId()),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getOutputSymbols()),
                ungroupedExecution(),
                StatsAndCosts.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        return GraphvizPrinter.printLogical(ImmutableList.of(fragment));
    }

    public static String graphvizDistributedPlan(SubPlan plan)
    {
        return GraphvizPrinter.printDistributed(plan);
    }

    private class Visitor
            extends InternalPlanVisitor<Void, Void>
    {
        private final Optional<StageExecutionDescriptor> stageExecutionStrategy;
        private final TypeProvider types;
        private final StatsAndCosts estimatedStatsAndCosts;
        private final Optional<Map<PlanNodeId, PlanNodeStats>> stats;
        private final Metadata metadata;
        private final LogicalRowExpressions logicalRowExpressions;

        public Visitor(Optional<StageExecutionDescriptor> stageExecutionStrategy, TypeProvider types, StatsAndCosts estimatedStatsAndCosts, Optional<Map<PlanNodeId, PlanNodeStats>> stats, Metadata metadata)
        {
            this.stageExecutionStrategy = requireNonNull(stageExecutionStrategy, "stageExecutionStrategy is null");
            this.types = requireNonNull(types, "types is null");
            this.estimatedStatsAndCosts = requireNonNull(estimatedStatsAndCosts, "estimatedStatsAndCosts is null");
            this.stats = requireNonNull(stats, "stats is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        }

        @Override
        public Void visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            addNode(node, "ExplainAnalyze");
            return processChildren(node, context);
        }

        @Override
        public Void visitTableExecute(PlanNode node, Void context)
        {
            TableExecuteNode tableExecuteNode = (TableExecuteNode) node;
            NodeRepresentation nodeOutput = addNode(tableExecuteNode, "TableExecute");
            for (int i = 0; i < tableExecuteNode.getColumnNames().size(); i++) {
                String name = tableExecuteNode.getColumnNames().get(i);
                Symbol symbol = tableExecuteNode.getColumns().get(i);
                nodeOutput.appendDetailsLine("%s := %s", name, symbol);
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause).toString());
            }
            node.getFilter().map(formatter::apply).ifPresent(joinExpressions::add);

            NodeRepresentation nodeOutput;
            if (node.isCrossJoin()) {
                checkState(joinExpressions.isEmpty());
                nodeOutput = addNode(node, "CrossJoin");
            }
            else {
                nodeOutput = addNode(node,
                        node.getType().getJoinLabel(),
                        format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol())));
            }

            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            if (!node.getDynamicFilters().isEmpty()) {
                nodeOutput.appendDetails("dynamicFilterAssignments = %s", printDynamicFilterAssignments(node.getDynamicFilters()));
            }

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter().flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata, node.getRightOutputSymbols(), filter));
            sortExpressionContext.ifPresent(sortContext -> nodeOutput.appendDetails("SortExpression[%s]", formatter.apply(sortContext.getSortExpression())));
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitJoinOnAggregation(JoinOnAggregationNode node, Void context)
        {
            List<String> joinExpressions = new ArrayList<>();
            for (JoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(JoinNodeUtils.toExpression(clause).toString());
            }
            node.getFilter().map(formatter::apply).ifPresent(joinExpressions::add);

            NodeRepresentation nodeOutput;
            nodeOutput = addNode(node,
                    "Group" + node.getType().getJoinLabel(),
                    format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getLeftHashSymbol(), node.getRightHashSymbol())));

            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            if (!node.getDynamicFilters().isEmpty()) {
                nodeOutput.appendDetailsLine("dynamicFilterAssignments = %s", printDynamicFilterAssignments(node.getDynamicFilters()));
            }

            Optional<SortExpressionContext> sortExpressionContext = node.getFilter().flatMap(filter -> SortExpressionExtractor.extractSortExpression(metadata, node.getRightOutputSymbols(), filter));
            sortExpressionContext.ifPresent(sortContext -> nodeOutput.appendDetailsLine("SortExpression[%s]", formatter.apply(sortContext.getSortExpression())));

            JoinInternalAggregation aggrOnLeft = node.getAggrOnLeft();
            JoinInternalAggregation aggrOnRight = node.getAggrOnRight();
            nodeOutput.appendDetailsLine("AggrOnAggrLeft = {%s}", getJoinInternalAggregationDetails(aggrOnLeft));
            nodeOutput.appendDetailsLine("AggrOnAggrRight = {%s}", getJoinInternalAggregationDetails(aggrOnRight));

            JoinInternalAggregation leftAggr = node.getLeftAggr();
            JoinInternalAggregation rightAggr = node.getRightAggr();
            nodeOutput.appendDetailsLine("AggrLeft = {%s}", getJoinInternalAggregationDetails(leftAggr));
            nodeOutput.appendDetailsLine("AggrRight = {%s}", getJoinInternalAggregationDetails(rightAggr));

            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);
            return null;
        }

        @Override
        public Void visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    node.getType().getJoinLabel(),
                    format("[%s]", formatter.apply(node.getFilter())));

            nodeOutput.appendDetailsLine("Distribution: %s", node.getDistributionType());
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        public Void visitSemiJoin(SemiJoinNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "SemiJoin",
                    format("[%s = %s]%s",
                            node.getSourceJoinSymbol(),
                            node.getFilteringSourceJoinSymbol(),
                            formatHash(node.getSourceHashSymbol(), node.getFilteringSourceHashSymbol())));
            node.getDistributionType().ifPresent(distributionType -> nodeOutput.appendDetailsLine("Distribution: %s", distributionType));
            node.getDynamicFilterId().ifPresent(dynamicFilterId -> nodeOutput.appendDetailsLine("dynamicFilterId: %s", dynamicFilterId));
            node.getSource().accept(this, context);
            node.getFilteringSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitIndexSource(IndexSourceNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node,
                    "IndexSource",
                    format("[%s, lookup = %s]", node.getIndexHandle(), node.getLookupSymbols()));

            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                if (node.getOutputSymbols().contains(entry.getKey())) {
                    nodeOutput.appendDetailsLine("%s := %s", entry.getKey(), entry.getValue());
                }
            }
            return null;
        }

        @Override
        public Void visitIndexJoin(IndexJoinNode node, Void context)
        {
            List<Expression> joinExpressions = new ArrayList<>();
            for (IndexJoinNode.EquiJoinClause clause : node.getCriteria()) {
                joinExpressions.add(new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                        toSymbolReference(clause.getProbe()),
                        toSymbolReference(clause.getIndex())));
            }

            addNode(node,
                    format("%sIndexJoin", node.getType().getJoinLabel()),
                    format("[%s]%s", Joiner.on(" AND ").join(joinExpressions), formatHash(node.getProbeHashSymbol(), node.getIndexHashSymbol())));
            node.getProbeSource().accept(this, context);
            node.getIndexSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitOffset(OffsetNode node, Void context)
        {
            addNode(node,
                    "Offset",
                    format("[%s]", node.getCount()));
            return processChildren(node, context);
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            addNode(node,
                    format("Limit%s", node.isPartial() ? "Partial" : ""),
                    format("[%s%s]", node.getCount(), node.isWithTies() ? "+ties" : ""));
            return processChildren(node, context);
        }

        @Override
        public Void visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            addNode(node,
                    format("DistinctLimit%s", node.isPartial() ? "Partial" : ""),
                    format("[%s]%s", node.getLimit(), formatHash(node.getHashSymbol())));
            return processChildren(node, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            if (node.isStreamable()) {
                type = format("%s(STREAMING)", type);
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }

            NodeRepresentation nodeOutput;
            if (node.getAggregationType().equals(AggregationNode.AggregationType.SORT_BASED)) {
                nodeOutput = addNode(node,
                        format("SortAggregate%s%s%s", type, key, formatHash(node.getHashSymbol())));
            }
            else {
                nodeOutput = addNode(node,
                        format("HashAggregate%s%s%s", type, key, formatHash(node.getHashSymbol())));
            }

            node.getAggregations().forEach((symbol, aggregation) -> nodeOutput.appendDetailsLine("%s := %s", symbol, formatAggregation(aggregation)));

            return processChildren(node, context);
        }

        @Override
        public Void visitGroupId(GroupIdNode node, Void context)
        {
            // grouping sets are easier to understand in terms of inputs
            List<List<Symbol>> inputGroupingSetSymbols = node.getGroupingSets().stream()
                    .map(set -> set.stream()
                            .map(symbol -> node.getGroupingColumns().get(symbol))
                            .collect(Collectors.toList()))
                    .collect(Collectors.toList());

            NodeRepresentation nodeOutput = addNode(node, "GroupId", format("%s", inputGroupingSetSymbols));

            for (Map.Entry<Symbol, Symbol> mapping : node.getGroupingColumns().entrySet()) {
                nodeOutput.appendDetailsLine("%s := %s", mapping.getKey(), mapping.getValue());
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            addNode(node,
                    "MarkDistinct",
                    format("[distinct=%s marker=%s]%s", formatOutputs(types, node.getDistinctSymbols()), node.getMarkerSymbol(), formatHash(node.getHashSymbol())));

            return processChildren(node, context);
        }

        @Override
        public Void visitWindow(WindowNode node, Void context)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());

            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                List<Symbol> prePartitioned = node.getPartitionBy().stream()
                        .filter(node.getPrePartitionedInputs()::contains)
                        .collect(toImmutableList());

                List<Symbol> notPrePartitioned = node.getPartitionBy().stream()
                        .filter(column -> !node.getPrePartitionedInputs().contains(column))
                        .collect(toImmutableList());

                StringBuilder builder = new StringBuilder();
                if (!prePartitioned.isEmpty()) {
                    builder.append("<")
                            .append(Joiner.on(", ").join(prePartitioned))
                            .append(">");
                    if (!notPrePartitioned.isEmpty()) {
                        builder.append(", ");
                    }
                }
                if (!notPrePartitioned.isEmpty()) {
                    builder.append(Joiner.on(", ").join(notPrePartitioned));
                }
                args.add(format("partition by (%s)", builder));
            }
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                args.add(format("order by (%s)", Stream.concat(
                        orderingScheme.getOrderBy().stream()
                                .limit(node.getPreSortedOrderPrefix())
                                .map(symbol -> "<" + symbol + " " + orderingScheme.getOrdering(symbol) + ">"),
                        orderingScheme.getOrderBy().stream()
                                .skip(node.getPreSortedOrderPrefix())
                                .map(symbol -> symbol + " " + orderingScheme.getOrdering(symbol)))
                        .collect(Collectors.joining(", "))));
            }

            NodeRepresentation nodeOutput = addNode(node, "Window", format("[%s]%s", Joiner.on(", ").join(args), formatHash(node.getHashSymbol())));

            for (Map.Entry<Symbol, WindowNode.Function> entry : node.getWindowFunctions().entrySet()) {
                WindowNode.Function function = entry.getValue();
                String frameInfo = formatFrame(function.getFrame());

                nodeOutput.appendDetailsLine(
                        "%s := %s(%s) %s",
                        entry.getKey(),
                        function.getFunctionCall().getDisplayName(),
                        Joiner.on(", ").join(function.getArguments()),
                        frameInfo);
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTopNRankingNumber(TopNRankingNumberNode node, Void context)
        {
            List<String> partitionBy = node.getPartitionBy().stream()
                    .map(Functions.toStringFunction())
                    .collect(toImmutableList());

            List<String> orderBy = node.getOrderingScheme().getOrderBy().stream()
                    .map(input -> input + " " + node.getOrderingScheme().getOrdering(input))
                    .collect(toImmutableList());

            List<String> args = new ArrayList<>();
            args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            args.add(format("order by (%s)", Joiner.on(", ").join(orderBy)));

            NodeRepresentation nodeOutput = addNode(node,
                    "TopNRankingNumber",
                    format("[%s limit %s]%s", Joiner.on(", ").join(args), node.getMaxRowCountPerPartition(), formatHash(node.getHashSymbol())));

            nodeOutput.appendDetailsLine("%s := %s", node.getRowNumberSymbol(), "row_number()");

            return processChildren(node, context);
        }

        @Override
        public Void visitVacuumTable(VacuumTableNode node, Void context)
        {
            TableHandle table = node.getTable();
            if (stageExecutionStrategy.isPresent()) {
                addNode(node,
                        "VacuumTable",
                        format("[%s, grouped = %s]", table, stageExecutionStrategy.get().isScanGroupedExecution(node.getId())));
            }
            else {
                addNode(node, "VacuumTable", format("[%s]", table));
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitRowNumber(RowNumberNode node, Void context)
        {
            List<String> partitionBy = Lists.transform(node.getPartitionBy(), Functions.toStringFunction());
            List<String> args = new ArrayList<>();
            if (!partitionBy.isEmpty()) {
                args.add(format("partition by (%s)", Joiner.on(", ").join(partitionBy)));
            }

            if (node.getMaxRowCountPerPartition().isPresent()) {
                args.add(format("limit = %s", node.getMaxRowCountPerPartition().get()));
            }

            NodeRepresentation nodeOutput = addNode(node,
                    "RowNumber",
                    format("[%s]%s", Joiner.on(", ").join(args), formatHash(node.getHashSymbol())));
            nodeOutput.appendDetailsLine("%s := %s", node.getRowNumberSymbol(), "row_number()");

            return processChildren(node, context);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Void context)
        {
            TableHandle table = node.getTable();
            NodeRepresentation nodeOutput;
            String operatorName = "";
            String reuseTypeName = "";
            if (node.getStrategy().equals(REUSE_STRATEGY_PRODUCER)) {
                operatorName = "ReuseTableScan";
                reuseTypeName = "(Producer [ID: " + node.getId().toString() + "])";
            }
            else if (node.getStrategy().equals(REUSE_STRATEGY_CONSUMER)) {
                operatorName = "ReuseTableScan";
                reuseTypeName = "(Consumer)";
            }
            else {
                operatorName += "TableScan";
            }

            operatorName = operatorName + reuseTypeName;

            {
                String formatString = "[";
                List<Object> arguments = new LinkedList<>();

                formatString += "table = %s";
                arguments.add(table);
                if (stageExecutionStrategy.isPresent()) {
                    formatString += ", grouped = %s, ";
                    arguments.add(stageExecutionStrategy.get().isScanGroupedExecution(node.getId()));
                }

                String pushdownPredicates = printTablePushDownFilters(node);
                if (pushdownPredicates.length() > 0) {
                    formatString += ", pushdownFilters = %s";
                    arguments.add(pushdownPredicates);
                }

                formatString += "]";
                nodeOutput = addNode(
                        node,
                        operatorName,
                        format(formatString, arguments.toArray()));
            }
            printTableScanInfo(nodeOutput, node);
            return null;
        }

        @Override
        public Void visitCreateIndex(CreateIndexNode node, Void context)
        {
            return processChildren(node, context);
        }

        @Override
        public Void visitUpdateIndex(UpdateIndexNode node, Void context)
        {
            return processChildren(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Values");
            for (List<RowExpression> row : node.getRows()) {
                nodeOutput.appendDetailsLine("(" + row.stream().map(formatter::apply).collect(Collectors.joining(", ")) + ")");
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            return visitScanFilterAndProjectInfo(node, Optional.of(node), Optional.empty(), context);
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            if (node.getSource() instanceof FilterNode) {
                return visitScanFilterAndProjectInfo(node, Optional.of((FilterNode) node.getSource()), Optional.of(node), context);
            }

            return visitScanFilterAndProjectInfo(node, Optional.empty(), Optional.of(node), context);
        }

        private Void visitScanFilterAndProjectInfo(
                PlanNode node,
                Optional<FilterNode> filterNode,
                Optional<ProjectNode> projectNode,
                Void context)
        {
            checkState(projectNode.isPresent() || filterNode.isPresent());

            PlanNode sourceNode;
            if (filterNode.isPresent()) {
                sourceNode = filterNode.get().getSource();
            }
            else {
                sourceNode = projectNode.get().getSource();
            }

            Optional<TableScanNode> scanNode;
            if (sourceNode instanceof TableScanNode) {
                scanNode = Optional.of((TableScanNode) sourceNode);
            }
            else {
                scanNode = Optional.empty();
            }

            String formatString = "[";
            String operatorName = "";
            String reuseTypeName = "";
            List<Object> arguments = new LinkedList<>();

            if (scanNode.isPresent()) {
                if (scanNode.get().getStrategy() == REUSE_STRATEGY_PRODUCER) {
                    operatorName = "ReuseScan";
                    reuseTypeName = "(Producer)";
                }
                else if (scanNode.get().getStrategy() == REUSE_STRATEGY_CONSUMER) {
                    operatorName = "ReuseScan";
                    reuseTypeName = "(Consumer)";
                }
                else {
                    operatorName += "Scan";
                }

                formatString += "table = %s, ";
                TableHandle table = scanNode.get().getTable();
                arguments.add(table);
                if (stageExecutionStrategy.isPresent()) {
                    formatString += "grouped = %s, ";
                    arguments.add(stageExecutionStrategy.get().isScanGroupedExecution(scanNode.get().getId()));
                }

                String pushdownPredicates = printTablePushDownFilters(scanNode.get());
                if (pushdownPredicates.length() > 0) {
                    formatString += "pushdownFilters = %s, ";
                    arguments.add(pushdownPredicates);
                }
            }

            if (filterNode.isPresent()) {
                operatorName += "Filter";
                formatString += "filterPredicate = %s, ";
                RowExpression predicate = filterNode.get().getPredicate();
                DynamicFilters.ExtractResult extractResult = extractDynamicFilters(predicate);
                arguments.add(formatter.apply(logicalRowExpressions.combineConjuncts(extractResult.getStaticConjuncts())));
                if (!extractResult.getDynamicConjuncts().isEmpty()) {
                    formatString += "dynamicFilter = %s, ";
                    arguments.add(printDynamicFilters(extractResult.getDynamicConjuncts()));
                }
            }

            if (formatString.length() > 1) {
                formatString = formatString.substring(0, formatString.length() - 2);
            }
            formatString += "]";

            if (projectNode.isPresent()) {
                operatorName += "Project";
            }

            List<PlanNodeId> allNodes = Stream.of(scanNode, filterNode, projectNode)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(PlanNode::getId)
                    .collect(toList());

            NodeRepresentation nodeOutput = addNode(
                    node,
                    operatorName + reuseTypeName,
                    format(formatString, arguments.toArray()),
                    allNodes,
                    ImmutableList.of(sourceNode),
                    ImmutableList.of());

            if (projectNode.isPresent()) {
                printAssignments(nodeOutput, projectNode.get().getAssignments());
            }

            if (scanNode.isPresent()) {
                printTableScanInfo(nodeOutput, scanNode.get());
                PlanNodeStats nodeStats = stats.map(s -> s.get(node.getId())).orElse(null);
                if (nodeStats != null) {
                    // Add to 'details' rather than 'statistics', since these stats are node-specific
                    nodeOutput.appendDetails("Input: %s (%s)", formatPositions(nodeStats.getPlanNodeInputPositions()), nodeStats.getPlanNodeInputDataSize().toString());
                    double filtered = 100.0d * (nodeStats.getPlanNodeInputPositions() - nodeStats.getPlanNodeOutputPositions()) / nodeStats.getPlanNodeInputPositions();
                    nodeOutput.appendDetailsLine(", Filtered: %s%%", formatDouble(filtered));
                }
                return null;
            }

            sourceNode.accept(this, context);
            return null;
        }

        private String printDynamicFilters(Collection<DynamicFilters.Descriptor> filters)
        {
            return filters.stream()
                    .map(filter -> filter.getId() + " -> " + filter.getInput())
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private String printDynamicFilterAssignments(Map<String, Symbol> filters)
        {
            return filters.entrySet().stream()
                    .map(filter -> filter.getValue() + " -> " + filter.getKey())
                    .collect(Collectors.joining(", ", "{", "}"));
        }

        private String printTablePushDownFilters(TableScanNode node)
        {
            StringBuilder str = new StringBuilder();
            TupleDomain<ColumnHandle> predicate = tableInfoSupplier.apply(node).getPredicate();
            if (!predicate.isNone() && !predicate.isAll()) {
                if (predicate.isAll() && (!node.getEnforcedConstraint().isAll() || !node.getEnforcedConstraint().isNone())) {
                    predicate = node.getEnforcedConstraint();
                }

                if (!predicate.isNone() && !predicate.isAll()) {
                    str.append("[ ");
                    str.append(predicate.getDomains().get()
                            .entrySet().stream()
                            .map(filter -> filter.getKey() + " <- " + formatDomain(filter.getValue().simplify()))
                            .collect(Collectors.joining(" AND ", "{", "}")));
                    str.append(" ]");
                    if (node.getTable().getConnectorHandle().hasDisjunctFiltersPushdown()) {
                        str.append("[ ");
                        str.append(" AND ");
                        str.append(" ]");
                    }
                }
            }

            if (node.getTable().getConnectorHandle().hasDisjunctFiltersPushdown()) {
                str.append(node.getTable().getConnectorHandle()
                        .getDisjunctFilterConditions((domain) -> formatDomain(domain)));
            }

            return str.toString();
        }

        private void printTableScanInfo(NodeRepresentation nodeOutput, TableScanNode node)
        {
            TupleDomain<ColumnHandle> predicate = tableInfoSupplier.apply(node).getPredicate();

            if (predicate.isNone()) {
                nodeOutput.appendDetailsLine(":: NONE");
            }
            else {
                if (predicate.isAll() && (!node.getEnforcedConstraint().isAll() || !node.getEnforcedConstraint().isNone())) {
                    predicate = node.getEnforcedConstraint();
                }

                // first, print output columns and their constraints
                for (Map.Entry<Symbol, ColumnHandle> assignment : node.getAssignments().entrySet()) {
                    ColumnHandle column = assignment.getValue();
                    nodeOutput.appendDetailsLine("%s := %s", assignment.getKey(), column);
                    printConstraint(nodeOutput, column, predicate);
                }

                // then, print constraints for columns that are not in the output
                if (!predicate.isAll()) {
                    Set<ColumnHandle> outputs = ImmutableSet.copyOf(node.getAssignments().values());

                    TupleDomain<ColumnHandle> finalPredicate = predicate;
                    predicate.getDomains().get()
                            .entrySet().stream()
                            .filter(entry -> !outputs.contains(entry.getKey()))
                            .forEach(entry -> {
                                ColumnHandle column = entry.getKey();
                                nodeOutput.appendDetailsLine("%s", column);
                                printConstraint(nodeOutput, column, finalPredicate);
                            });
                }
            }
        }

        @Override
        public Void visitUnnest(UnnestNode node, Void context)
        {
            addNode(node,
                    "Unnest",
                    format("[replicate=%s, unnest=%s]", formatOutputs(types, node.getReplicateSymbols()), formatOutputs(types, node.getUnnestSymbols().keySet())));
            return processChildren(node, context);
        }

        @Override
        public Void visitOutput(OutputNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Output", format("[%s]", Joiner.on(", ").join(node.getColumnNames())));
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getOutputSymbols().get(i);
                if (!name.equals(symbol.toString())) {
                    nodeOutput.appendDetailsLine("%s := %s", name, symbol);
                }
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            addNode(node,
                    format("TopN%s", node.getStep() == TopNNode.Step.PARTIAL ? "Partial" : ""),
                    format("[%s by (%s)]", node.getCount(), Joiner.on(", ").join(keys)));
            return processChildren(node, context);
        }

        @Override
        public Void visitSort(SortNode node, Void context)
        {
            Iterable<String> keys = Iterables.transform(node.getOrderingScheme().getOrderBy(), input -> input + " " + node.getOrderingScheme().getOrdering(input));

            addNode(node,
                    format("%sSort", node.isPartial() ? "Partial" : ""),
                    format("[%s]", Joiner.on(", ").join(keys)));

            return processChildren(node, context);
        }

        @Override
        public Void visitRemoteSource(RemoteSourceNode node, Void context)
        {
            addNode(node,
                    format("Remote%s", node.getOrderingScheme().isPresent() ? "Merge" : "Source"),
                    format("[%s]", Joiner.on(',').join(node.getSourceFragmentIds())),
                    ImmutableList.of(),
                    ImmutableList.of(),
                    node.getSourceFragmentIds());

            return null;
        }

        @Override
        public Void visitUnion(UnionNode node, Void context)
        {
            addNode(node, "Union");

            return processChildren(node, context);
        }

        @Override
        public Void visitIntersect(IntersectNode node, Void context)
        {
            addNode(node, "Intersect");

            return processChildren(node, context);
        }

        @Override
        public Void visitExcept(ExceptNode node, Void context)
        {
            addNode(node, "Except");

            return processChildren(node, context);
        }

        @Override
        public Void visitTableWriter(TableWriterNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableWriter");
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                nodeOutput.appendDetailsLine("%s := %s", name, symbol);
            }

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(nodeOutput, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitCacheTableWriter(CacheTableWriterNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableWriter");
            for (int i = 0; i < node.getColumnNames().size(); i++) {
                String name = node.getColumnNames().get(i);
                Symbol symbol = node.getColumns().get(i);
                nodeOutput.appendDetailsLine("%s := %s", name, symbol);
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            addNode(node, "StatisticsWriter", format("[%s]", node.getTarget()));
            return processChildren(node, context);
        }

        @Override
        public Void visitTableFinish(TableFinishNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "TableCommit", format("[%s]", node.getTarget()));

            if (node.getStatisticsAggregation().isPresent()) {
                verify(node.getStatisticsAggregationDescriptor().isPresent(), "statisticsAggregationDescriptor is not present");
                printStatisticAggregations(nodeOutput, node.getStatisticsAggregation().get(), node.getStatisticsAggregationDescriptor().get());
            }

            return processChildren(node, context);
        }

        @Override
        public Void visitCacheTableFinish(CacheTableFinishNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "CacheTableCommit", format("[%s]", node.getTarget()));
            return processChildren(node, context);
        }

        @Override
        public Void visitCubeFinish(CubeFinishNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "CubeCommit", format("[%s]", "target"));
            nodeOutput.appendDetailsLine("CubeFinishMetadataCreation");
            return processChildren(node, context);
        }

        private void printStatisticAggregations(NodeRepresentation nodeOutput, StatisticAggregations aggregations, StatisticAggregationsDescriptor<Symbol> descriptor)
        {
            nodeOutput.appendDetailsLine("Collected statistics:");
            printStatisticAggregationsInfo(nodeOutput, descriptor.getTableStatistics(), descriptor.getColumnStatistics(), aggregations.getAggregations());
            nodeOutput.appendDetailsLine(indentString(1) + "grouped by => [%s]", getStatisticGroupingSetsInfo(descriptor.getGrouping()));
        }

        private String getStatisticGroupingSetsInfo(Map<String, Symbol> columnMappings)
        {
            return columnMappings.entrySet().stream()
                    .map(entry -> format("%s := %s", entry.getValue(), entry.getKey()))
                    .collect(joining(", "));
        }

        private void printStatisticAggregationsInfo(
                NodeRepresentation nodeOutput,
                Map<TableStatisticType, Symbol> tableStatistics,
                Map<ColumnStatisticMetadata, Symbol> columnStatistics,
                Map<Symbol, AggregationNode.Aggregation> aggregations)
        {
            nodeOutput.appendDetailsLine("aggregations =>");
            for (Map.Entry<TableStatisticType, Symbol> tableStatistic : tableStatistics.entrySet()) {
                nodeOutput.appendDetailsLine(indentString(1) + "%s => [%s := %s]",
                        tableStatistic.getValue(),
                        tableStatistic.getKey(),
                        formatAggregation(aggregations.get(tableStatistic.getValue())));
            }

            for (Map.Entry<ColumnStatisticMetadata, Symbol> columnStatistic : columnStatistics.entrySet()) {
                nodeOutput.appendDetailsLine(
                        indentString(1) + "%s[%s] => [%s := %s]",
                        columnStatistic.getKey().getStatisticType(),
                        columnStatistic.getKey().getColumnName(),
                        columnStatistic.getValue(),
                        formatAggregation(aggregations.get(columnStatistic.getValue())));
            }
        }

        @Override
        public Void visitSample(SampleNode node, Void context)
        {
            addNode(node, "Sample", format("[%s: %s]", node.getSampleType(), node.getSampleRatio()));

            return processChildren(node, context);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            if (node.getOrderingScheme().isPresent()) {
                OrderingScheme orderingScheme = node.getOrderingScheme().get();
                List<String> orderBy = orderingScheme.getOrderBy()
                        .stream()
                        .map(input -> input + " " + orderingScheme.getOrdering(input))
                        .collect(toImmutableList());

                addNode(node,
                        format("%sMerge", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        format("[%s]", Joiner.on(", ").join(orderBy)));
            }
            else if (node.getScope() == Scope.LOCAL) {
                addNode(node,
                        "LocalExchange",
                        format("[%s%s]%s (%s)",
                                node.getPartitioningScheme().getPartitioning().getHandle(),
                                node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                                formatHash(node.getPartitioningScheme().getHashColumn()),
                                Joiner.on(", ").join(node.getPartitioningScheme().getPartitioning().getArguments())));
            }
            else {
                addNode(node,
                        format("%sExchange", UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, node.getScope().toString())),
                        format("[%s%s]%s",
                                node.getType(),
                                node.getPartitioningScheme().isReplicateNullsAndAny() ? " - REPLICATE NULLS AND ANY" : "",
                                formatHash(node.getPartitioningScheme().getHashColumn())));
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitDelete(DeleteNode node, Void context)
        {
            addNode(node, "Delete", format("[%s]", node.getTarget()));

            return processChildren(node, context);
        }

        @Override
        public Void visitUpdate(UpdateNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, format("Update[%s]", node.getTarget()));
            int index = 0;
            for (String columnName : node.getTarget().getUpdatedColumns()) {
                nodeOutput.appendDetailsLine("%s := %s", columnName, node.getColumnValueAndRowIdSymbols().get(index).getName());
                index++;
            }
            return processChildren(node, context);
        }

        @Override
        public Void visitTableDelete(TableDeleteNode node, Void context)
        {
            String formatString = "[%s";
            List<Object> arguments = new ArrayList<>();
            arguments.add(node.getTarget());
            if (node.getFilter().isPresent()) {
                formatString += ", filterPredicate: %s";
                arguments.add(node.getFilter().get());
            }
            formatString += "]";
            addNode(node, "TableDelete", format(formatString, arguments.toArray()));

            return processChildren(node, context);
        }

        @Override
        public Void visitTableUpdate(TableUpdateNode node, Void context)
        {
            String formatString = "[%s";
            List<Object> arguments = new ArrayList<>();
            arguments.add(node.getTarget());
            formatString += "]";
            addNode(node, "TableUpdate", format(formatString, arguments.toArray()));

            return processChildren(node, context);
        }

        @Override
        public Void visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            addNode(node, "EnforceSingleRow");

            return processChildren(node, context);
        }

        @Override
        public Void visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            addNode(node, "AssignUniqueId");

            return processChildren(node, context);
        }

        @Override
        public Void visitGroupReference(GroupReference node, Void context)
        {
            addNode(node, "GroupReference", format("[%s]", node.getGroupId()), ImmutableList.of());

            return null;
        }

        @Override
        public Void visitApply(ApplyNode node, Void context)
        {
            NodeRepresentation nodeOutput = addNode(node, "Apply", format("[%s]", node.getCorrelation()));
            printAssignments(nodeOutput, node.getSubqueryAssignments());

            return processChildren(node, context);
        }

        @Override
        public Void visitLateralJoin(LateralJoinNode node, Void context)
        {
            addNode(node,
                    "Lateral",
                    format("[%s%s]",
                            node.getCorrelation(),
                            node.getFilter().equals(TRUE_LITERAL) ? "" : " " + node.getFilter()));

            return processChildren(node, context);
        }

        @Override
        public Void visitCTEScan(CTEScanNode node, Void context)
        {
            addNode(node, "CTEScan[CTE = " + node.getCteRefName() + "]");
            return processChildren(node, context);
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        private Void processChildren(PlanNode node, Void context)
        {
            for (PlanNode child : node.getSources()) {
                child.accept(this, context);
            }

            return null;
        }

        private void printAssignments(NodeRepresentation nodeOutput, Assignments assignments)
        {
            for (Map.Entry<Symbol, RowExpression> entry : assignments.getMap().entrySet()) {
                if (entry.getValue() instanceof VariableReferenceExpression && ((VariableReferenceExpression) entry.getValue()).getName().equals(entry.getKey().getName())) {
                    // skip identity assignments
                    continue;
                }
                nodeOutput.appendDetailsLine("%s := %s", entry.getKey(), formatter.apply(entry.getValue()));
            }
        }

        private void printConstraint(NodeRepresentation nodeOutput, ColumnHandle column, TupleDomain<ColumnHandle> constraint)
        {
            checkArgument(!constraint.isNone());
            Map<ColumnHandle, Domain> domains = constraint.getDomains().get();
            if (!constraint.isAll() && domains.containsKey(column)) {
                nodeOutput.appendDetailsLine("    :: %s", formatDomain(domains.get(column).simplify()));
            }
        }

        private String formatDomain(Domain domain)
        {
            ImmutableList.Builder<String> parts = ImmutableList.builder();

            if (domain.isNullAllowed()) {
                parts.add("NULL");
            }

            Type type = domain.getType();

            domain.getValues().getValuesProcessor().consume(
                    ranges -> {
                        for (Range range : ranges.getOrderedRanges()) {
                            StringBuilder builder = new StringBuilder();
                            if (range.isSingleValue()) {
                                String value = valuePrinter.castToVarchar(type, range.getSingleValue());
                                builder.append('[').append(value).append(']');
                            }
                            else {
                                builder.append((range.getLow().getBound() == Marker.Bound.EXACTLY) ? '[' : '(');

                                if (range.getLow().isLowerUnbounded()) {
                                    builder.append("<min>");
                                }
                                else {
                                    builder.append(valuePrinter.castToVarchar(type, range.getLow().getValue()));
                                }

                                builder.append(", ");

                                if (range.getHigh().isUpperUnbounded()) {
                                    builder.append("<max>");
                                }
                                else {
                                    builder.append(valuePrinter.castToVarchar(type, range.getHigh().getValue()));
                                }

                                builder.append((range.getHigh().getBound() == Marker.Bound.EXACTLY) ? ']' : ')');
                            }
                            parts.add(builder.toString());
                        }
                    },
                    discreteValues -> discreteValues.getValues().stream()
                            .map(value -> valuePrinter.castToVarchar(type, value))
                            .sorted() // Sort so the values will be printed in predictable order
                            .forEach(parts::add),
                    allOrNone -> {
                        if (allOrNone.isAll()) {
                            parts.add("ALL VALUES");
                        }
                    });

            return "[" + Joiner.on(", ").join(parts.build()) + "]";
        }

        public NodeRepresentation addNode(PlanNode node, String name)
        {
            return addNode(node, name, "");
        }

        public NodeRepresentation addNode(PlanNode node, String name, String identifier)
        {
            return addNode(node, name, identifier, node.getSources());
        }

        public NodeRepresentation addNode(PlanNode node, String name, String identifier, List<PlanNode> children)
        {
            return addNode(node, name, identifier, ImmutableList.of(node.getId()), children, ImmutableList.of());
        }

        public NodeRepresentation addNode(PlanNode rootNode, String name, String identifier, List<PlanNodeId> allNodes, List<PlanNode> children, List<PlanFragmentId> remoteSources)
        {
            List<PlanNodeId> childrenIds = children.stream().map(PlanNode::getId).collect(toImmutableList());
            List<PlanNodeStatsEstimate> estimatedStats = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getStats().getOrDefault(nodeId, PlanNodeStatsEstimate.unknown()))
                    .collect(toList());
            List<PlanCostEstimate> estimatedCosts = allNodes.stream()
                    .map(nodeId -> estimatedStatsAndCosts.getCosts().getOrDefault(nodeId, PlanCostEstimate.unknown()))
                    .collect(toList());

            NodeRepresentation nodeOutput = new NodeRepresentation(
                    rootNode.getId(),
                    name,
                    rootNode.getClass().getSimpleName(),
                    identifier,
                    rootNode.getOutputSymbols().stream()
                            .map(s -> new TypedSymbol(s, types.get(s).getDisplayName()))
                            .collect(toImmutableList()),
                    stats.map(s -> s.get(rootNode.getId())),
                    estimatedStats,
                    estimatedCosts,
                    childrenIds,
                    remoteSources);

            representation.addNode(nodeOutput);
            return nodeOutput;
        }

        private String getJoinInternalAggregationDetails(JoinInternalAggregation node)
        {
            String type = "";
            if (node.getStep() != AggregationNode.Step.SINGLE) {
                type = format("(%s)", node.getStep().toString());
            }
            String key = "";
            if (!node.getGroupingKeys().isEmpty()) {
                key = node.getGroupingKeys().toString();
            }
            String value = "";
            if (node.getAggregationType().equals(AggregationNode.AggregationType.HASH)) {
                value = format("hashAggregate%s%s%s", type, key, formatHash(node.getHashSymbol()));
            }
            StringBuilder builder = new StringBuilder(value);
            if (node.getAggregations().size() > 0) {
                node.getAggregations().forEach((symbol, aggregation) -> builder.append(format("%s := %s", symbol, formatAggregation(aggregation))));
                // to remove the last: ", "
                builder.setLength(builder.length() - 2);
            }
            else {
                builder.setLength(builder.length() > 0 ? builder.length() - 1 : 0);
            }
            String columns = node.getOutputSymbols().stream()
                    .map(symbol -> symbol + ":" + types.get(symbol).getDisplayName())
                    .collect(joining(", "));
            builder.append(" Layout: [").append(columns).append("]");
            return builder.toString();
        }
    }

    private static String formatFrame(WindowNode.Frame frame)
    {
        StringBuilder builder = new StringBuilder(frame.getType().toString());

        frame.getOriginalStartValue().ifPresent(value -> builder.append(" ").append(value));
        builder.append(" ").append(frame.getStartType());

        frame.getOriginalEndValue().ifPresent(value -> builder.append(" ").append(value));
        builder.append(" ").append(frame.getEndType());

        return builder.toString();
    }

    private static String formatHash(Optional<Symbol>... hashes)
    {
        List<Symbol> symbols = stream(hashes)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toList());

        if (symbols.isEmpty()) {
            return "";
        }

        return "[" + Joiner.on(", ").join(symbols) + "]";
    }

    private static String formatOutputs(TypeProvider types, Iterable<Symbol> outputs)
    {
        return Streams.stream(outputs)
                .map(input -> input + ":" + types.get(input).getDisplayName())
                .collect(Collectors.joining(", "));
    }

    public static String formatAggregation(Aggregation aggregation)
    {
        StringBuilder builder = new StringBuilder();

        String arguments = Joiner.on(", ").join(aggregation.getArguments());
        if (aggregation.getArguments().isEmpty() && "count".equalsIgnoreCase(aggregation.getFunctionCall().getDisplayName())) {
            arguments = "*";
        }
        if (aggregation.isDistinct()) {
            arguments = "DISTINCT " + arguments;
        }

        builder.append(aggregation.getFunctionCall().getDisplayName())
                .append('(').append(arguments);

        aggregation.getOrderingScheme().ifPresent(orderingScheme -> builder.append(' ').append(orderingScheme.getOrderBy().stream()
                .map(input -> input + " " + orderingScheme.getOrdering(input))
                .collect(joining(", "))));

        builder.append(')');

        aggregation.getFilter().ifPresent(expression -> builder.append(" FILTER (WHERE ").append(expression).append(")"));

        aggregation.getMask().ifPresent(symbol -> builder.append(" (mask = ").append(symbol).append(")"));
        return builder.toString();
    }
}
