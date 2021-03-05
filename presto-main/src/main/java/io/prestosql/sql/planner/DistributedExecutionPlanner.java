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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.SplitCacheMap;
import io.prestosql.execution.TableInfo;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.snapshot.MarkerSplitSource;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.split.SampledSplitSource;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.CreateIndexNode;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.VacuumTarget;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.Objects.requireNonNull;

public class DistributedExecutionPlanner
{
    private static final Logger log = Logger.get(DistributedExecutionPlanner.class);

    public enum Mode
    {
        NORMAL, // First time to plan the query with snapshot off
        SNAPSHOT, // First time to plan the query with snapshot on
        RESUME // Resuming query execution from a snapshot (null means from start)
    }

    private final SplitManager splitManager;
    private final Metadata metadata;

    @Inject
    public DistributedExecutionPlanner(SplitManager splitManager, Metadata metadata)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public StageExecutionPlan plan(SubPlan root, Session session, Mode mode, Long resumeSnapshotId, long nextSnapshotId)
    {
        ImmutableList.Builder<SplitSource> allSplitSources = ImmutableList.builder();
        try {
            if (mode != Mode.SNAPSHOT) {
                return doPlan(mode, root, session, resumeSnapshotId, nextSnapshotId, allSplitSources, null, null);
            }

            // Capture dependencies among table scan sources. Only need to do this for the initial planning.
            // The leftmost source of each fragment. Key is fragment id; value is SplitSource or ValuesNode or RemoteSourceNode
            Map<PlanFragmentId, Object> leftmostSources = new HashMap<>();
            // Source dependency. Key is SplitSource or ValuesNode or RemoteSourceNode; value is SplitSource or ValuesNode or RemoteSourceNode
            Multimap<Object, Object> sourceDependencies = HashMultimap.create();
            StageExecutionPlan ret = doPlan(mode, root, session, resumeSnapshotId, nextSnapshotId, allSplitSources, leftmostSources, sourceDependencies);

            for (Map.Entry<Object, Object> entry : sourceDependencies.entries()) {
                List<MarkerSplitSource> right = collectSources(leftmostSources, entry.getValue());
                for (MarkerSplitSource source : collectSources(leftmostSources, entry.getKey())) {
                    for (SplitSource dependency : right) {
                        source.addDependency((MarkerSplitSource) dependency);
                    }
                }
            }

            return ret;
        }
        catch (Throwable t) {
            allSplitSources.build().forEach(DistributedExecutionPlanner::closeSplitSource);
            throw t;
        }
    }

    private List<MarkerSplitSource> collectSources(Map<PlanFragmentId, Object> leftmostSources, Object source)
    {
        if (source instanceof ValuesNode) {
            // TODO-cp-I2X9J6: should we worry about dependencies about Values operators, when it's the "left" of a join?
            return ImmutableList.of();
        }

        if (source instanceof RemoteSourceNode) {
            List<PlanFragmentId> fragments = ((RemoteSourceNode) source).getSourceFragmentIds();
            if (fragments.size() == 1) {
                return collectSources(leftmostSources, leftmostSources.get(fragments.get(0)));
            }
            List<MarkerSplitSource> sources = new ArrayList<>();
            for (PlanFragmentId id : fragments) {
                sources.addAll(collectSources(leftmostSources, leftmostSources.get(id)));
            }
            return sources;
        }

        // Must be a split source
        return ImmutableList.of((MarkerSplitSource) source);
    }

    private static void closeSplitSource(SplitSource source)
    {
        try {
            source.close();
        }
        catch (Throwable t) {
            log.warn(t, "Error closing split source");
        }
    }

    private StageExecutionPlan doPlan(
            Mode mode,
            SubPlan root,
            Session session,
            Long resumeSnapshotId,
            long nextSnapshotId,
            ImmutableList.Builder<SplitSource> allSplitSources,
            Map<PlanFragmentId, Object> leftmostSources,
            Multimap<Object, Object> sourceDependencies)
    {
        PlanFragment currentFragment = root.getFragment();

        // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
        Map<PlanNodeId, SplitSource> splitSources;
        switch (mode) {
            case NORMAL:
                splitSources = currentFragment.getRoot().accept(new Visitor(session, currentFragment.getStageExecutionDescriptor(), allSplitSources), null);
                break;
            case SNAPSHOT:
                // Add additional logic to record which sources depend on (are blocked by) which other sources through join operators
                SnapshotAwareVisitor visitor = new SnapshotAwareVisitor(session, currentFragment.getStageExecutionDescriptor(), nextSnapshotId, allSplitSources, sourceDependencies);
                splitSources = currentFragment.getRoot().accept(visitor, null);
                leftmostSources.put(currentFragment.getId(), visitor.leftmostSource);
                break;
            case RESUME:
                splitSources = currentFragment.getRoot().accept(new UpdateValuesVisitor(session, currentFragment.getStageExecutionDescriptor(),
                        resumeSnapshotId, nextSnapshotId, allSplitSources), null);
                break;
            default:
                throw new RuntimeException("Unexpected mode: " + mode);
        }

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            dependencies.add(doPlan(mode, childPlan, session, resumeSnapshotId, nextSnapshotId, allSplitSources, leftmostSources, sourceDependencies));
        }

        // extract TableInfo
        Map<PlanNodeId, TableInfo> tables = searchFrom(root.getFragment().getRoot())
                .where(TableScanNode.class::isInstance)
                .findAll()
                .stream()
                .map(TableScanNode.class::cast)
                .collect(toImmutableMap(PlanNode::getId, node -> getTableInfo(node, session)));

        return new StageExecutionPlan(
                currentFragment,
                splitSources,
                dependencies.build(),
                tables);
    }

    private TableInfo getTableInfo(TableScanNode node, Session session)
    {
        TableMetadata tableMetadata = metadata.getTableMetadata(session, node.getTable());
        TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
        return new TableInfo(tableMetadata.getQualifiedName(), tableProperties.getPredicate());
    }

    private class Visitor
            extends InternalPlanVisitor<Map<PlanNodeId, SplitSource>, Void>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final ImmutableList.Builder<SplitSource> splitSources;

        private Visitor(Session session, StageExecutionDescriptor stageExecutionDescriptor, ImmutableList.Builder<SplitSource> allSplitSources)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
            this.splitSources = allSplitSources;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExplainAnalyze(ExplainAnalyzeNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableScan(TableScanNode node, Void context)
        {
            return visitScanAndFilter(node.getId(), node.getTable(), Optional.empty(), node.getAssignments(), Optional.empty(), Collections.emptyMap(),
                    node.getStrategy() != ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitVacuumTable(VacuumTableNode node, Void context)
        {
            ConnectorVacuumTableHandle connectorVacuumTableHandle = ((VacuumTarget) node.getTarget()).getHandle().getConnectorHandle();
            ImmutableMap<String, Object> queryInfo = ImmutableMap.of(
                    "FULL", node.isFull(),
                    "partition", node.getPartition(),
                    "vacuumHandle", connectorVacuumTableHandle);
            return visitScanAndFilter(node.getId(), node.getTable(), Optional.empty(), ImmutableMap.of(), Optional.of(QueryType.VACUUM),
                    queryInfo, false);
        }

        Map<PlanNodeId, SplitSource> visitScanAndFilter(PlanNodeId nodeId,
                TableHandle tableHandle,
                Optional<FilterNode> filter,
                Map<Symbol, ColumnHandle> assignments,
                Optional<QueryType> queryType,
                Map<String, Object> queryInfo,
                boolean partOfReuse)
        {
            List<DynamicFilters.Descriptor> dynamicFilters = filter
                    .map(FilterNode::getPredicate)
                    .map(DynamicFilters::extractDynamicFilters)
                    .map(DynamicFilters.ExtractResult::getDynamicConjuncts)
                    .orElse(ImmutableList.of());

            Supplier<Set<DynamicFilter>> dynamicFilterSupplier = null;
            if (!dynamicFilters.isEmpty() && !stageExecutionDescriptor.isScanGroupedExecution(nodeId)) {
                dynamicFilterSupplier = DynamicFilterService.getDynamicFilterSupplier(session.getQueryId(), dynamicFilters, assignments);
            }

            //How would this change when we add support to cache  small tables entirely without the need to provide predicates
            Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates = ImmutableSet.of();
            if (PropertyService.getBooleanProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED) && tableHandle.getConnectorHandle().isTableCacheable()) {
                userDefinedCachePredicates = SplitCacheMap.getInstance().getCachePredicateTupleDomains(tableHandle.getFullyQualifiedName());
            }

            // get dataSource for table
            SplitSource splitSource = splitManager.getSplits(
                    session,
                    tableHandle,
                    stageExecutionDescriptor.isScanGroupedExecution(nodeId) ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING,
                    dynamicFilterSupplier,
                    queryType,
                    queryInfo,
                    userDefinedCachePredicates,
                    partOfReuse,
                    nodeId);

            splitSources.add(splitSource);

            return ImmutableMap.of(nodeId, splitSource);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSemiJoin(SemiJoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> sourceSplits = node.getSource().accept(this, context);
            Map<PlanNodeId, SplitSource> filteringSourceSplits = node.getFilteringSource().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(sourceSplits)
                    .putAll(filteringSourceSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            Map<PlanNodeId, SplitSource> leftSplits = node.getLeft().accept(this, context);
            Map<PlanNodeId, SplitSource> rightSplits = node.getRight().accept(this, context);
            return ImmutableMap.<PlanNodeId, SplitSource>builder()
                    .putAll(leftSplits)
                    .putAll(rightSplits)
                    .build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitIndexJoin(IndexJoinNode node, Void context)
        {
            return node.getProbeSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            // remote source node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Void context)
        {
            // values node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitFilter(FilterNode node, Void context)
        {
            if (node.getSource() instanceof TableScanNode) {
                TableScanNode scan = (TableScanNode) node.getSource();
                return visitScanAndFilter(scan.getId(), scan.getTable(), Optional.of(node), scan.getAssignments(), Optional.empty(), Collections.emptyMap(),
                        scan.getStrategy() != ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT);
            }

            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSample(SampleNode node, Void context)
        {
            switch (node.getSampleType()) {
                case BERNOULLI:
                    return node.getSource().accept(this, context);
                case SYSTEM:
                    Map<PlanNodeId, SplitSource> nodeSplits = node.getSource().accept(this, context);
                    // TODO: when this happens we should switch to either BERNOULLI or page sampling
                    if (nodeSplits.size() == 1) {
                        PlanNodeId planNodeId = getOnlyElement(nodeSplits.keySet());
                        SplitSource sampledSplitSource = new SampledSplitSource(nodeSplits.get(planNodeId), node.getSampleRatio());
                        return ImmutableMap.of(planNodeId, sampledSplitSource);
                    }
                    // table sampling on a sub query without splits is meaningless
                    return nodeSplits;

                default:
                    throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
            }
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitGroupId(GroupIdNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitWindow(WindowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRowNumber(RowNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopNRankingNumber(TopNRankingNumberNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnnest(UnnestNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDistinctLimit(DistinctLimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableWriter(TableWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableFinish(TableFinishNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitCubeFinish(CubeFinishNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitStatisticsWriterNode(StatisticsWriterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitDelete(DeleteNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitTableDelete(TableDeleteNode node, Void context)
        {
            // node does not have splits
            return ImmutableMap.of();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitUnion(UnionNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitExchange(ExchangeNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        private Map<PlanNodeId, SplitSource> processSources(List<PlanNode> sources, Void context)
        {
            ImmutableMap.Builder<PlanNodeId, SplitSource> result = ImmutableMap.builder();
            for (PlanNode child : sources) {
                result.putAll(child.accept(this, context));
            }

            return result.build();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitCreateIndex(CreateIndexNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitCTEScan(CTEScanNode node, Void context)
        {
            return processSources(node.getSources(), context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }

    private class UpdateValuesVisitor
            extends Visitor
    {
        private final Long resumeSnapshotId;
        private final long nextSnapshotId;

        private UpdateValuesVisitor(
                Session session,
                StageExecutionDescriptor stageExecutionDescriptor,
                Long resumeSnapshotId,
                long nextSnapshotId,
                ImmutableList.Builder<SplitSource> allSplitSources)
        {
            super(session, stageExecutionDescriptor, allSplitSources);
            this.resumeSnapshotId = resumeSnapshotId;
            this.nextSnapshotId = nextSnapshotId;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Void context)
        {
            node.setupSnapshot(resumeSnapshotId, nextSnapshotId);
            return super.visitValues(node, context);
        }
    }

    private class SnapshotAwareVisitor
            extends Visitor
    {
        private final Multimap<Object, Object> sourceDependencies;
        private final long nextSnapshotId;
        // Which are the corresponding "left" sources from join nodes while visiting the "right" nodes.
        // Value can be SplitSource, ValuesNode, or RemoteSourceNode
        private final Stack<Object> sourceStack;
        private Object leftmostSource;
        private int pendingJoins;

        private SnapshotAwareVisitor(
                Session session,
                StageExecutionDescriptor stageExecutionDescriptor,
                long nextSnapshotId,
                ImmutableList.Builder<SplitSource> allSplitSources,
                Multimap<Object, Object> sourceDependencies)
        {
            super(session, stageExecutionDescriptor, allSplitSources);
            this.sourceDependencies = sourceDependencies;
            this.nextSnapshotId = nextSnapshotId;
            sourceStack = new Stack<>();
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitJoin(JoinNode node, Void context)
        {
            pendingJoins++;
            Map<PlanNodeId, SplitSource> ret = super.visitJoin(node, context);
            sourceStack.pop();
            return ret;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSemiJoin(SemiJoinNode node, Void context)
        {
            pendingJoins++;
            Map<PlanNodeId, SplitSource> ret = super.visitSemiJoin(node, context);
            sourceStack.pop();
            return ret;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            pendingJoins++;
            Map<PlanNodeId, SplitSource> ret = super.visitSpatialJoin(node, context);
            sourceStack.pop();
            return ret;
        }

        @Override
        Map<PlanNodeId, SplitSource> visitScanAndFilter(PlanNodeId nodeId,
                TableHandle tableHandle,
                Optional<FilterNode> filter,
                Map<Symbol, ColumnHandle> assignments,
                Optional<QueryType> queryType,
                Map<String, Object> queryInfo,
                boolean partOfReuse)
        {
            Map<PlanNodeId, SplitSource> ret = super.visitScanAndFilter(nodeId, tableHandle, filter, assignments, queryType, queryInfo, partOfReuse);
            handleLeafNode(ret.values().iterator().next());
            return ret;
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitRemoteSource(RemoteSourceNode node, Void context)
        {
            handleLeafNode(node);
            return super.visitRemoteSource(node, context);
        }

        @Override
        public Map<PlanNodeId, SplitSource> visitValues(ValuesNode node, Void context)
        {
            node.setupSnapshot(null, nextSnapshotId);
            handleLeafNode(node);
            return super.visitValues(node, context);
        }

        private void handleLeafNode(Object source)
        {
            if (leftmostSource == null) {
                leftmostSource = source;
            }

            if (!sourceStack.empty()) {
                // this source is part of the "right" table of a join
                Object left = sourceStack.peek();
                // Current leftmost source depend on remote sources from these fragments
                sourceDependencies.put(left, source);
            }

            if (pendingJoins > 0) {
                // This source serves as the "left" table for all the pending joins.
                // e.g. 2 level join: (A + B) + C, where A is the left for both joins,
                // and only A->B and A->C dependencies need to be recorded
                while (pendingJoins > 0) {
                    sourceStack.push(source);
                    pendingJoins--;
                }
            }
        }
    }
}
