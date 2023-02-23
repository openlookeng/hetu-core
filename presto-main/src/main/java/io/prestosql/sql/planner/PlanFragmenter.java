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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.StatsAndCosts;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.scheduler.BucketNodeMap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableProperties.TablePartitioning;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.CacheTableWriterNode;
import io.prestosql.sql.planner.plan.CubeFinishNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.StatisticsWriterNode;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableUpdateNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TopNRankingNumberNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getQueryMaxStageCount;
import static io.prestosql.SystemSessionProperties.getRetryPolicy;
import static io.prestosql.SystemSessionProperties.isDynamicSchduleForGroupedExecution;
import static io.prestosql.SystemSessionProperties.isForceSingleNodeOutput;
import static io.prestosql.operator.StageExecutionDescriptor.ungroupedExecution;
import static io.prestosql.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.connector.StandardWarningCode.TOO_MANY_STAGES;
import static io.prestosql.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static io.prestosql.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.sql.planner.planprinter.PlanPrinter.jsonFragmentPlan;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private static final String TOO_MANY_STAGES_MESSAGE = "" +
            "If the query contains multiple aggregates with DISTINCT over different columns, please set the 'use_mark_distinct' session property to false. " +
            "If the query contains WITH clauses that are referenced more than once, please create temporary table(s) for the queries in those clauses.";

    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final QueryManagerConfig config;

    @Inject
    public PlanFragmenter(Metadata metadata, NodePartitioningManager nodePartitioningManager, QueryManagerConfig queryManagerConfig)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.config = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, WarningCollector warningCollector)
    {
        Fragmenter fragmenter = new Fragmenter(session, metadata, plan.getTypes(), plan.getStatsAndCosts());

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        subPlan = reassignPartitioningHandleIfNecessary(session, subPlan);
        subPlan = analyzeGroupedExecution(session, subPlan);

        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(subPlan, warningCollector, getQueryMaxStageCount(session), config.getStageCountWarningThreshold());

        return subPlan;
    }

    private void sanityCheckFragmentedPlan(SubPlan subPlan, WarningCollector warningCollector, int maxStageCount, int stageCountSoftLimit)
    {
        subPlan.sanityCheck();
        int fragmentCount = subPlan.getAllFragments().size();
        if (fragmentCount > maxStageCount) {
            throw new PrestoException(QUERY_HAS_TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the allowed maximum (%s). " + TOO_MANY_STAGES_MESSAGE,
                    fragmentCount,
                    maxStageCount));
        }
        if (fragmentCount > stageCountSoftLimit) {
            warningCollector.add(new PrestoWarning(TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the soft limit (%s). " + TOO_MANY_STAGES_MESSAGE,
                    fragmentCount,
                    stageCountSoftLimit)));
        }
    }

    private SubPlan analyzeGroupedExecution(Session session, SubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        GroupedExecutionProperties properties = fragment.getRoot().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager), null);
        if (properties.isSubTreeUseful()) {
            boolean preferDynamic = fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)
                    && isDynamicSchduleForGroupedExecution(session);
            BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, fragment.getPartitioning(), preferDynamic);
            if (bucketNodeMap.isDynamic()) {
                fragment = fragment.withDynamicLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes());
            }
            else {
                fragment = fragment.withFixedLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes());
            }
        }
        ImmutableList.Builder<SubPlan> result = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            result.add(analyzeGroupedExecution(session, child));
        }
        return new SubPlan(fragment, result.build());
    }

    private SubPlan reassignPartitioningHandleIfNecessary(Session session, SubPlan subPlan)
    {
        return reassignPartitioningHandleIfNecessaryHelper(session, subPlan, subPlan.getFragment().getPartitioning());
    }

    private SubPlan reassignPartitioningHandleIfNecessaryHelper(Session session, SubPlan subPlan, PartitioningHandle newOutputPartitioningHandle)
    {
        PlanFragment fragment = subPlan.getFragment();

        PlanNode newRoot = fragment.getRoot();
        // If the fragment's partitioning is SINGLE or COORDINATOR_ONLY, leave the sources as is (this is for single-node execution)
        if (!fragment.getPartitioning().isSingleNode()) {
            PartitioningHandleReassigner partitioningHandleReassigner = new PartitioningHandleReassigner(fragment.getPartitioning(), metadata, session);
            newRoot = SimplePlanRewriter.rewriteWith(partitioningHandleReassigner, newRoot);
        }
        PartitioningScheme outputPartitioningScheme = fragment.getPartitioningScheme();
        Partitioning newOutputPartitioning = outputPartitioningScheme.getPartitioning();
        if (outputPartitioningScheme.getPartitioning().getHandle().getConnectorId().isPresent()) {
            // Do not replace the handle if the source's output handle is a system one, e.g. broadcast.
            newOutputPartitioning = newOutputPartitioning.withAlternativePartitiongingHandle(newOutputPartitioningHandle);
        }
        PlanFragment newFragment = new PlanFragment(
                fragment.getId(),
                newRoot,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionedSources(),
                new PartitioningScheme(
                        newOutputPartitioning,
                        outputPartitioningScheme.getOutputLayout(),
                        outputPartitioningScheme.getHashColumn(),
                        outputPartitioningScheme.isReplicateNullsAndAny(),
                        outputPartitioningScheme.getBucketToPartition()),
                fragment.getStageExecutionDescriptor(),
                fragment.getStatsAndCosts(),
                fragment.getJsonRepresentation(),
                fragment.getFeederCTEId(),
                fragment.getFeederCTEParentId());

        ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            childrenBuilder.add(reassignPartitioningHandleIfNecessaryHelper(session, child, fragment.getPartitioning()));
        }
        return new SubPlan(newFragment, childrenBuilder.build());
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Session session;
        private final Metadata metadata;
        private final TypeProvider types;
        private final StatsAndCosts statsAndCosts;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;
        private final Map<Integer, SubPlan> subPlanMap = new HashMap<>();
        private final Map<Integer, List<CTEScanNode>> cteToParentsMap = new HashMap<>();

        public Fragmenter(Session session, Metadata metadata, TypeProvider types, StatsAndCosts statsAndCosts)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.types = requireNonNull(types, "types is null");
            this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, new PlanFragmentId(String.valueOf(ROOT_FRAGMENT_ID)), false, Optional.empty());
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(String.valueOf(nextFragmentId++));
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId, boolean isFeederCTE, Optional<PlanNodeId> feederCTEParentId)
        {
            Set<Symbol> dependencies = SymbolsExtractor.extractAllSymbols(root, Lookup.noLookup());

            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            boolean equals = properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder));
            checkArgument(equals, "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)", schedulingOrder, properties.getPartitionedSources());

            Map<Symbol, Type> symbols = Maps.filterKeys(types.allTypes(), in(dependencies));

            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    symbols,
                    properties.getPartitioningHandle(),
                    schedulingOrder,
                    properties.getPartitioningScheme(),
                    ungroupedExecution(),
                    statsAndCosts.getForSubplan(root),
                    Optional.of(jsonFragmentPlan(root, symbols, metadata, session)),
                    isFeederCTE ? Optional.of(fragmentId) : Optional.empty(),
                    feederCTEParentId);

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.get().setSingleNodeDistribution();
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitCacheTableFinish(CacheTableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitCubeFinish(CubeFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableDelete(TableDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableUpdate(TableUpdateNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, node.getTable())
                    .getTablePartitioning()
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getPartitioningScheme().isPresent()) {
                context.get().setDistribution(node.getPartitioningScheme().get().getPartitioning().getHandle(), metadata, session);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitCacheTableWriter(CacheTableWriterNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getPartitioningScheme().isPresent()) {
                context.get().setDistribution(node.getPartitioningScheme().get().getPartitioning().getHandle(), metadata, session);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitVacuumTable(VacuumTableNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().addSourceDistribution(node.getId(), SOURCE_DISTRIBUTION, metadata, session);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSingleNodeDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            if (exchange.getScope() != REMOTE) {
                return context.defaultRewrite(exchange, context.get());
            }

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(partitioningScheme.getPartitioning().getHandle(), metadata, session);
            }

            ImmutableList.Builder<FragmentProperties> childrenProperties = ImmutableList.builder();
            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
                childrenProperties.add(childProperties);
                // check if it has CTE.
                Integer planNodeId = checkForCTENode(exchange.getSources().get(sourceIndex), exchange.getId());
                if (planNodeId == null) {
                    builder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context, false, Optional.empty()));
                }
                else {
                    if (subPlanMap.containsKey(planNodeId)) {
                        PlanFragment currFragment = subPlanMap.get(planNodeId).getFragment();
                        PlanFragment fragment = new PlanFragment(nextFragmentId(),
                                currFragment.getRoot(),
                                currFragment.getSymbols(),
                                currFragment.getPartitioning(),
                                currFragment.getPartitionedSources(),
                                currFragment.getPartitioningScheme(),
                                currFragment.getStageExecutionDescriptor(),
                                currFragment.getStatsAndCosts(),
                                currFragment.getJsonRepresentation(),
                                Optional.empty(),   // This is not feeder CTE, so we pass empty
                                currFragment.getFeederCTEParentId());
                        builder.add(new SubPlan(fragment, subPlanMap.get(planNodeId).getChildren()));
                        List<CTEScanNode> cteNodes = cteToParentsMap.get(planNodeId);
                        cteNodes.add(getCTENode(exchange.getSources().get(sourceIndex)));
                        cteToParentsMap.put(planNodeId, cteNodes);
                    }
                    else {
                        SubPlan plan = buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context, true, Optional.of(exchange.getId()));
                        subPlanMap.put(planNodeId, plan);
                        List<CTEScanNode> cteNodes = new ArrayList<>();
                        cteNodes.add(getCTENode(exchange.getSources().get(sourceIndex)));
                        cteToParentsMap.put(planNodeId, cteNodes);
                        builder.add(plan);
                    }
                }
            }

            List<SubPlan> children = builder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols(), exchange.getOrderingScheme(), exchange.getType(), isWorkerCoordinatorBoundary(context.get(), childrenProperties.build()) ? getRetryPolicy(session) : RetryPolicy.NONE);
        }

        private Integer checkForCTENode(PlanNode node, PlanNodeId nodeId)
        {
            PlanNode planNode = node;
            while (planNode instanceof ProjectNode) {
                planNode = ((ProjectNode) planNode).getSource();
            }

            if (planNode instanceof CTEScanNode) {
                int commonCTERefNum = ((CTEScanNode) planNode).getCommonCTERefNum();
                if (cteToParentsMap.containsKey(commonCTERefNum)) {
                    cteToParentsMap.get(commonCTERefNum).forEach(x -> x.updateConsumerPlan(nodeId));
                    ((CTEScanNode) planNode).updateConsumerPlans(cteToParentsMap.get(commonCTERefNum).get(0).getConsumerPlans());
                }
                else {
                    ((CTEScanNode) planNode).updateConsumerPlan(nodeId);
                }

                return commonCTERefNum;
            }

            return null;
        }

        private CTEScanNode getCTENode(PlanNode node)
        {
            PlanNode planNode = node;
            while (planNode instanceof ProjectNode) {
                planNode = ((ProjectNode) planNode).getSource();
            }

            if (planNode instanceof CTEScanNode) {
                return (CTEScanNode) planNode;
            }

            // Should never come here.
            return null;
        }

        private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context, boolean isfeederCTE, Optional<PlanNodeId> feederCTEParentId)
        {
            PlanFragmentId planFragmentId = nextFragmentId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId, isfeederCTE, feederCTEParentId);
        }

        private static boolean isWorkerCoordinatorBoundary(FragmentProperties fragmentProperties, List<FragmentProperties> childFragmentsProperties)
        {
            if (!fragmentProperties.getPartitioningHandle().isCoordinatorOnly()) {
                // receiver stage is not a coordinator stage
                return false;
            }
            if (childFragmentsProperties.stream().allMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly())) {
                // coordinator to coordinator exchange
                return false;
            }
            checkArgument(
                    childFragmentsProperties.stream().noneMatch(properties -> properties.getPartitioningHandle().isCoordinatorOnly()),
                    "Plans are not expected to have a mix of coordinator only fragments and distributed fragments as siblings");
            return true;
        }
    }

    private static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private final PartitioningScheme partitioningScheme;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private final Set<PlanNodeId> partitionedSources = new HashSet<>();

        public FragmentProperties(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
        }

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(!partitioningHandle.isPresent(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(PartitioningHandle distribution, Metadata metadata, Session session)
        {
            if (!partitioningHandle.isPresent()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = this.partitioningHandle.get();

            if (isCompatibleSystemPartitioning(distribution)) {
                return this;
            }

            if (currentPartitioning.equals(SOURCE_DISTRIBUTION)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.isSingleNode()) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    this.partitioningHandle));
        }

        private boolean isCompatibleSystemPartitioning(PartitioningHandle distribution)
        {
            ConnectorPartitioningHandle currentHandle = partitioningHandle.get().getConnectorHandle();
            ConnectorPartitioningHandle distributionHandle = distribution.getConnectorHandle();
            if ((currentHandle instanceof SystemPartitioningHandle) &&
                    (distributionHandle instanceof SystemPartitioningHandle)) {
                return ((SystemPartitioningHandle) currentHandle).getPartitioning() ==
                        ((SystemPartitioningHandle) distributionHandle).getPartitioning();
            }
            return false;
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(!partitioningHandle.isPresent() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution, Metadata metadata, Session session)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);

            if (!partitioningHandle.isPresent()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = partitioningHandle.get();

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.equals(SINGLE_DISTRIBUTION) || currentPartitioning.equals(COORDINATOR_DISTRIBUTION)) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format("Cannot overwrite distribution with %s (currently set to %s)", distribution, currentPartitioning));
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public PartitioningScheme getPartitioningScheme()
        {
            return partitioningScheme;
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle.get();
        }

        public Set<PlanNodeId> getPartitionedSources()
        {
            return partitionedSources;
        }
    }

    private static class GroupedExecutionTagger
            extends InternalPlanVisitor<GroupedExecutionProperties, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final NodePartitioningManager nodePartitioningManager;
        private final boolean groupedExecutionEnabled;

        public GroupedExecutionTagger(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.groupedExecutionEnabled = SystemSessionProperties.isGroupedExecutionEnabled(session);
        }

        @Override
        public GroupedExecutionProperties visitPlan(PlanNode node, Void context)
        {
            if (node.getSources().isEmpty()) {
                return GroupedExecutionProperties.notCapable();
            }
            return processChildren(node);
        }

        @Override
        public GroupedExecutionProperties visitJoin(JoinNode node, Void context)
        {
            GroupedExecutionProperties left = node.getLeft().accept(this, null);
            GroupedExecutionProperties right = node.getRight().accept(this, null);

            if (!node.getDistributionType().isPresent()) {
                // This is possible when the optimizers is invoked with `forceSingleNode` set to true.
                return GroupedExecutionProperties.notCapable();
            }

            if ((node.getType() == JoinNode.Type.RIGHT || node.getType() == JoinNode.Type.FULL) && !right.currentNodeCapable) {
                // For a plan like this, if the fragment participates in grouped execution,
                // the LookupOuterOperator corresponding to the RJoin will not work execute properly.
                //
                // * The operator has to execute as not-grouped because it can only look at the "used" flags in
                //   join build after all probe has finished.
                // * The operator has to execute as grouped the subsequent LJoin expects that incoming
                //   operators are grouped. Otherwise, the LJoin won't be able to throw out the build side
                //   for each group as soon as the group completes.
                //
                //       LJoin
                //       /   \
                //   RJoin   Scan
                //   /   \
                // Scan Remote
                //
                // TODO:
                // The RJoin can still execute as grouped if there is no subsequent operator that depends
                // on the RJoin being executed in a grouped manner. However, this is not currently implemented.
                // Support for this scenario is already implemented in the execution side.
                return GroupedExecutionProperties.notCapable();
            }

            switch (node.getDistributionType().get()) {
                case REPLICATED:
                    // Broadcast join maintains partitioning for the left side.
                    // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                    checkState(!right.currentNodeCapable);
                    return left;
                case PARTITIONED:
                    if (left.currentNodeCapable && right.currentNodeCapable) {
                        return new GroupedExecutionProperties(
                                true,
                                true,
                                ImmutableList.<PlanNodeId>builder()
                                        .addAll(left.capableTableScanNodes)
                                        .addAll(right.capableTableScanNodes)
                                        .build());
                    }
                    // right.subTreeUseful && !left.currentNodeCapable:
                    //   It's not particularly helpful to do grouped execution on the right side
                    //   because the benefit is likely cancelled out due to required buffering for hash build.
                    //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                    //   However, this is not currently implemented. JoinBridgeManager need to support such a lifecycle.
                    // !right.currentNodeCapable:
                    //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                    //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                    //
                    return left;
                default:
                    throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
            }
        }

        @Override
        public GroupedExecutionProperties visitJoinOnAggregation(JoinOnAggregationNode node, Void context)
        {
            GroupedExecutionProperties left = node.getLeft().accept(this, null);
            GroupedExecutionProperties right = node.getRight().accept(this, null);

            if (!node.getDistributionType().isPresent()) {
                // This is possible when the optimizers is invoked with `forceSingleNode` set to true.
                return GroupedExecutionProperties.notCapable();
            }

            if ((node.getType() == JoinNode.Type.RIGHT || node.getType() == JoinNode.Type.FULL) && !right.currentNodeCapable) {
                // For a plan like this, if the fragment participates in grouped execution,
                // the LookupOuterOperator corresponding to the RJoin will not work execute properly.
                //
                // * The operator has to execute as not-grouped because it can only look at the "used" flags in
                //   join build after all probe has finished.
                // * The operator has to execute as grouped the subsequent LJoin expects that incoming
                //   operators are grouped. Otherwise, the LJoin won't be able to throw out the build side
                //   for each group as soon as the group completes.
                //
                //       LJoin
                //       /   \
                //   RJoin   Scan
                //   /   \
                // Scan Remote
                //
                // TODO:
                // The RJoin can still execute as grouped if there is no subsequent operator that depends
                // on the RJoin being executed in a grouped manner. However, this is not currently implemented.
                // Support for this scenario is already implemented in the execution side.
                return GroupedExecutionProperties.notCapable();
            }

            switch (node.getDistributionType().get()) {
                case REPLICATED:
                    // Broadcast join maintains partitioning for the left side.
                    // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                    checkState(!right.currentNodeCapable);
                    return left;
                case PARTITIONED:
                    if (left.currentNodeCapable && right.currentNodeCapable) {
                        return new GroupedExecutionProperties(
                                true,
                                true,
                                ImmutableList.<PlanNodeId>builder()
                                        .addAll(left.capableTableScanNodes)
                                        .addAll(right.capableTableScanNodes)
                                        .build());
                    }
                    // right.subTreeUseful && !left.currentNodeCapable:
                    //   It's not particularly helpful to do grouped execution on the right side
                    //   because the benefit is likely cancelled out due to required buffering for hash build.
                    //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                    //   However, this is not currently implemented. JoinBridgeManager need to support such a lifecycle.
                    // !right.currentNodeCapable:
                    //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                    //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                    //
                    return left;
                default:
                    throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
            }
        }

        @Override
        public GroupedExecutionProperties visitAggregation(AggregationNode node, Void context)
        {
            GroupedExecutionProperties properties = node.getSource().accept(this, null);
            if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
                switch (node.getStep()) {
                    case SINGLE:
                    case FINAL:
                        return new GroupedExecutionProperties(true, true, properties.capableTableScanNodes);
                    case PARTIAL:
                    case INTERMEDIATE:
                        return properties;
                }
            }
            return GroupedExecutionProperties.notCapable();
        }

        @Override
        public GroupedExecutionProperties visitWindow(WindowNode node, Void context)
        {
            return processWindowFunction(node);
        }

        @Override
        public GroupedExecutionProperties visitRowNumber(RowNumberNode node, Void context)
        {
            return processWindowFunction(node);
        }

        @Override
        public GroupedExecutionProperties visitTopNRankingNumber(TopNRankingNumberNode node, Void context)
        {
            return processWindowFunction(node);
        }

        private GroupedExecutionProperties processWindowFunction(PlanNode node)
        {
            GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
            if (groupedExecutionEnabled && properties.isCurrentNodeCapable()) {
                return new GroupedExecutionProperties(true, true, properties.capableTableScanNodes);
            }
            return GroupedExecutionProperties.notCapable();
        }

        @Override
        public GroupedExecutionProperties visitTableScan(TableScanNode node, Void context)
        {
            Optional<TablePartitioning> tablePartitioning = metadata.getTableProperties(session, node.getTable()).getTablePartitioning();
            if (!tablePartitioning.isPresent()) {
                return GroupedExecutionProperties.notCapable();
            }
            List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
            if (ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles)) {
                return new GroupedExecutionProperties(false, false, ImmutableList.of());
            }
            else {
                return new GroupedExecutionProperties(true, false, ImmutableList.of(node.getId()));
            }
        }

        private GroupedExecutionProperties processChildren(PlanNode node)
        {
            // Each fragment has a partitioning handle, which is derived from leaf nodes in the fragment.
            // Leaf nodes with different partitioning handle are not allowed to share a single fragment
            // (except for special cases as detailed in addSourceDistribution).
            // As a result, it is not necessary to check the compatibility between node.getSources because
            // they are guaranteed to be compatible.

            // * If any child is "not capable", return "not capable"
            // * When all children are capable ("capable and useful" or "capable but not useful")
            //   * if any child is "capable and useful", return "capable and useful"
            //   * if no children is "capable and useful", return "capable but not useful"
            boolean anyUseful = false;
            ImmutableList.Builder<PlanNodeId> capableTableScanNodes = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                GroupedExecutionProperties properties = source.accept(this, null);
                if (!properties.isCurrentNodeCapable()) {
                    return GroupedExecutionProperties.notCapable();
                }
                anyUseful |= properties.isSubTreeUseful();
                capableTableScanNodes.addAll(properties.capableTableScanNodes);
            }
            return new GroupedExecutionProperties(true, anyUseful, capableTableScanNodes.build());
        }
    }

    private static class GroupedExecutionProperties
    {
        // currentNodeCapable:
        //   Whether grouped execution is possible with the current node.
        //   For example, a table scan is capable iff it supports addressable split discovery.
        // subTreeUseful:
        //   Whether grouped execution is beneficial in the current node, or any node below it.
        //   For example, a JOIN can benefit from grouped execution because build can be flushed early, reducing peak memory requirement.
        //
        // In the current implementation, subTreeUseful implies currentNodeCapable.
        // In theory, this doesn't have to be the case. Take an example where a GROUP BY feeds into the build side of a JOIN.
        // Even if JOIN cannot take advantage of grouped execution, it could still be beneficial to execute the GROUP BY with grouped execution
        // (e.g. when the underlying aggregation's intermediate group state may be larger than aggregation output).

        private final boolean currentNodeCapable;
        private final boolean subTreeUseful;
        private final List<PlanNodeId> capableTableScanNodes;

        public GroupedExecutionProperties(boolean currentNodeCapable, boolean subTreeUseful, List<PlanNodeId> capableTableScanNodes)
        {
            this.currentNodeCapable = currentNodeCapable;
            this.subTreeUseful = subTreeUseful;
            this.capableTableScanNodes = ImmutableList.copyOf(requireNonNull(capableTableScanNodes, "capableTableScanNodes is null"));
            // Verify that `subTreeUseful` implies `currentNodeCapable`
            checkArgument(!subTreeUseful || currentNodeCapable);
            checkArgument(currentNodeCapable == !capableTableScanNodes.isEmpty());
        }

        public static GroupedExecutionProperties notCapable()
        {
            return new GroupedExecutionProperties(false, false, ImmutableList.of());
        }

        public boolean isCurrentNodeCapable()
        {
            return currentNodeCapable;
        }

        public boolean isSubTreeUseful()
        {
            return subTreeUseful;
        }

        public List<PlanNodeId> getCapableTableScanNodes()
        {
            return capableTableScanNodes;
        }
    }

    private static final class PartitioningHandleReassigner
            extends SimplePlanRewriter<Void>
    {
        private final PartitioningHandle fragmentPartitioningHandle;
        private final Metadata metadata;
        private final Session session;

        public PartitioningHandleReassigner(PartitioningHandle fragmentPartitioningHandle, Metadata metadata, Session session)
        {
            this.fragmentPartitioningHandle = fragmentPartitioningHandle;
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            PartitioningHandle partitioning = metadata.getTableProperties(session, node.getTable())
                    .getTablePartitioning()
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            if (partitioning.equals(fragmentPartitioningHandle)) {
                // do nothing if the current scan node's partitioning matches the fragment's
                return node;
            }

            TableHandle newTable = metadata.makeCompatiblePartitioning(session, node.getTable(), fragmentPartitioningHandle);
            return new TableScanNode(
                    node.getId(),
                    newTable,
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    node.getEnforcedConstraint(),
                    node.getPredicate(), node.getStrategy(),
                    node.getReuseTableScanMappingId(), 0, node.isForDelete());
        }
    }
}
