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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.operator.StageExecutionDescriptor;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.LimitNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.TopNNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.split.SampledSplitSource;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.DeleteNode;
import io.prestosql.sql.planner.plan.DistinctLimitNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.ExplainAnalyzeNode;
import io.prestosql.sql.planner.plan.IndexJoinNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
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
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.planner.plan.UpdateNode;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static java.util.Objects.requireNonNull;

public class SplitSourceFactory
{
    private static final Logger log = Logger.get(SplitSourceFactory.class);

    private final SplitManager splitManager;

    //TODO(SURYA) commenting plannerContext for now as we are not using this now. Will uncomment once this is required.
//    private final PlannerContext plannerContext;
    private final DynamicFilterService dynamicFilterService;
    private final TypeAnalyzer typeAnalyzer;

    @Inject
//    public SplitSourceFactory(SplitManager splitManager, PlannerContext plannerContext, DynamicFilterService dynamicFilterService, TypeAnalyzer typeAnalyzer)
    public SplitSourceFactory(SplitManager splitManager, DynamicFilterService dynamicFilterService, TypeAnalyzer typeAnalyzer)
    {
        this.splitManager = requireNonNull(splitManager, "splitManager is null");
//        this.plannerContext = requireNonNull(plannerContext, "metadata is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
    }

    public Map<PlanNodeId, SplitSource> createSplitSources(Session session, PlanFragment fragment)
    {
        ImmutableList.Builder<SplitSource> allSplitSources = ImmutableList.builder();
        try {
            // get splits for this fragment, this is lazy so split assignments aren't actually calculated here
            return fragment.getRoot().accept(
                    new Visitor(session, fragment.getStageExecutionDescriptor(), TypeProvider.copyOf(fragment.getSymbols()), allSplitSources),
                    null);
        }
        catch (Throwable t) {
            allSplitSources.build().forEach(SplitSourceFactory::closeSplitSource);
            throw t;
        }
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

    private final class Visitor
            extends InternalPlanVisitor<Map<PlanNodeId, SplitSource>, Void>
    {
        private final Session session;
        private final StageExecutionDescriptor stageExecutionDescriptor;
        private final TypeProvider typeProvider;
        private final ImmutableList.Builder<SplitSource> splitSources;

        private Visitor(
                Session session,
                StageExecutionDescriptor stageExecutionDescriptor,
                TypeProvider typeProvider,
                ImmutableList.Builder<SplitSource> allSplitSources)
        {
            this.session = session;
            this.stageExecutionDescriptor = stageExecutionDescriptor;
            this.typeProvider = typeProvider;
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
            return visitScanAndFilter(node, Optional.empty());
        }

        private Map<PlanNodeId, SplitSource> visitScanAndFilter(TableScanNode node, Optional<FilterNode> filter)
        {
            Optional<RowExpression> filterPredicate = filter.map(FilterNode::getPredicate);

            List<DynamicFilters.Descriptor> dynamicFilters = filterPredicate
                    .map(DynamicFilters::extractDynamicFilters)
                    .map(DynamicFilters.ExtractResult::getDynamicConjuncts)
                    .orElse(ImmutableList.of());

            // get dataSource for table
            //TODO(SURYA): not passing dynamic filter supplier instead using overloaded constructor which passes default values.
            SplitSource splitSource = splitManager.getSplits(
                    session,
                    node.getTable(),
                    stageExecutionDescriptor.isScanGroupedExecution(node.getId()) ? GROUPED_SCHEDULING : UNGROUPED_SCHEDULING);

            splitSources.add(splitSource);

            return ImmutableMap.of(node.getId(), splitSource);
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
                return visitScanAndFilter(scan, Optional.of(node));
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
            }
            throw new UnsupportedOperationException("Sampling is not supported for type " + node.getSampleType());
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
        public Map<PlanNodeId, SplitSource> visitUpdate(UpdateNode node, Void context)
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
        public Map<PlanNodeId, SplitSource> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}
