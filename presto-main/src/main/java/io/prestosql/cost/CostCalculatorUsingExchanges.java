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

package io.prestosql.cost;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
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
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.CacheTableFinishNode;
import io.prestosql.sql.planner.plan.CacheTableWriterNode;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.RowNumberNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.adjustReplicatedJoinLocalExchangeCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateJoinInputCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateLocalRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteGatherCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteRepartitionCost;
import static io.prestosql.cost.CostCalculatorWithEstimatedExchanges.calculateRemoteReplicateCost;
import static io.prestosql.cost.LocalCostEstimate.addPartialComponents;
import static io.prestosql.spi.plan.AggregationNode.Step.FINAL;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.AggregationNode.Step.SINGLE;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Simple implementation of CostCalculator. It assumes that ExchangeNodes are already in the plan.
 */
@ThreadSafe
public class CostCalculatorUsingExchanges
        implements CostCalculator
{
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorUsingExchanges(TaskCountEstimator taskCountEstimator)
    {
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session, TypeProvider types)
    {
        CostEstimator costEstimator = new CostEstimator(stats, sourcesCosts, types, taskCountEstimator);
        return node.accept(costEstimator, null);
    }

    private static class CostEstimator
            extends InternalPlanVisitor<PlanCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final CostProvider sourcesCosts;
        private final TypeProvider types;
        private final TaskCountEstimator taskCountEstimator;

        CostEstimator(StatsProvider stats, CostProvider sourcesCosts, TypeProvider types, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.sourcesCosts = requireNonNull(sourcesCosts, "sourcesCosts is null");
            this.types = requireNonNull(types, "types is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        public PlanCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement cost estimates for all plan nodes
            return PlanCostEstimate.unknown();
        }

        @Override
        public PlanCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanCostEstimate visitAssignUniqueId(AssignUniqueId node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(ImmutableList.of(node.getIdColumn()), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitRowNumber(RowNumberNode node, Void context)
        {
            List<Symbol> symbols = node.getOutputSymbols();
            // when maxRowCountPerPartition is set, the RowNumberOperator
            // copies values for all the columns into a page builder
            if (!node.getMaxRowCountPerPartition().isPresent()) {
                symbols = ImmutableList.<Symbol>builder()
                        .addAll(node.getPartitionBy())
                        .add(node.getRowNumberSymbol())
                        .build();
            }
            PlanNodeStatsEstimate tmpStats = getStats(node);
            double cpuCost = tmpStats.getOutputSizeInBytes(symbols, types);
            double memoryCost = node.getPartitionBy().isEmpty() ? 0 : tmpStats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitGroupId(GroupIdNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitTopN(TopNNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitMarkDistinct(MarkDistinctNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSort(SortNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitWindow(WindowNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitOutput(OutputNode node, Void context)
        {
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitTableScan(TableScanNode node, Void context)
        {
            // TODO: add network cost, based on input size in bytes? Or let connector provide this cost?
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForSource(node, localCost);
        }

        @Override
        public PlanCostEstimate visitFilter(FilterNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node.getSource()).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitProject(ProjectNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            if (node.getStep() != FINAL && node.getStep() != PARTIAL && node.getStep() != SINGLE) {
                return PlanCostEstimate.unknown();
            }
            PlanNodeStatsEstimate aggregationStats = getStats(node);
            PlanNodeStatsEstimate sourceStats = getStats(node.getSource());
            double cpuCost = sourceStats.getOutputSizeInBytes(node.getSource().getOutputSymbols(), types);
            double hashTableCost = 0;
            double maxDistinctValuesCount = 0;
            if (!node.getAggregationType().equals(AggregationNode.AggregationType.SORT_BASED)) {
                double values;
                for (Symbol groupingKeys : node.getGroupingKeys()) {
                    values = aggregationStats.getSymbolStatistics(groupingKeys).getDistinctValuesCount();
                    if (Double.isNaN(values)) {
                        continue;
                    }
                    maxDistinctValuesCount = max(maxDistinctValuesCount, values);
                }
                /*the hash table type is long and it will store distinct values so distinct values * hash size(long) */
                hashTableCost = maxDistinctValuesCount * Long.BYTES;
            }
            double memoryCost = aggregationStats.getOutputSizeInBytes(node.getOutputSymbols(), types) + hashTableCost;
            LocalCostEstimate localCost = LocalCostEstimate.of(cpuCost, memoryCost, 0);
            return costForAccumulation(node, localCost);
        }

        @Override
        public PlanCostEstimate visitJoin(JoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitJoinOnAggregation(JoinOnAggregationNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)));
            return costForLookupJoin(node, localCost);
        }

        private LocalCostEstimate calculateJoinCost(PlanNode join, PlanNode probe, PlanNode build, boolean replicated)
        {
            int estimatedSourceDistributedTaskCount = taskCountEstimator.estimateSourceDistributedTaskCount();
            LocalCostEstimate joinInputCost = calculateJoinInputCost(
                    probe,
                    build,
                    stats,
                    types,
                    replicated,
                    estimatedSourceDistributedTaskCount);
            // TODO: Use traits (https://gitee.com/openlookeng/hetu-core/issues/I68CXF) instead, to correctly estimate
            // local exchange cost for replicated join in CostCalculatorUsingExchanges#visitExchange
            LocalCostEstimate adjustedLocalExchangeCost = adjustReplicatedJoinLocalExchangeCost(
                    build,
                    stats,
                    types,
                    replicated,
                    estimatedSourceDistributedTaskCount);
            LocalCostEstimate joinOutputCost = calculateJoinOutputCost(join);
            return addPartialComponents(joinInputCost, adjustedLocalExchangeCost, joinOutputCost);
        }

        private LocalCostEstimate calculateJoinOutputCost(PlanNode join)
        {
            PlanNodeStatsEstimate outputStats = getStats(join);
            double joinOutputSize = outputStats.getOutputSizeInBytes(join.getOutputSymbols(), types);
            return LocalCostEstimate.ofCpu(joinOutputSize);
        }

        @Override
        public PlanCostEstimate visitExchange(ExchangeNode node, Void context)
        {
            return costForStreaming(node, calculateExchangeCost(node));
        }

        private LocalCostEstimate calculateExchangeCost(ExchangeNode node)
        {
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types);
            switch (node.getScope()) {
                case LOCAL:
                    switch (node.getType()) {
                        case GATHER:
                            return LocalCostEstimate.zero();
                        case REPARTITION:
                            return calculateLocalRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            return LocalCostEstimate.zero();
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                case REMOTE:
                    switch (node.getType()) {
                        case GATHER:
                            return calculateRemoteGatherCost(inputSizeInBytes);
                        case REPARTITION:
                            return calculateRemoteRepartitionCost(inputSizeInBytes);
                        case REPLICATE:
                            // assuming that destination is always source distributed
                            // it is true as now replicated exchange is used for joins only
                            // for replicated join probe side is usually source distributed
                            return calculateRemoteReplicateCost(inputSizeInBytes, taskCountEstimator.estimateSourceDistributedTaskCount());
                        default:
                            throw new IllegalArgumentException("Unexpected type: " + node.getType());
                    }
                default:
                    throw new IllegalArgumentException("Unexpected scope: " + node.getScope());
            }
        }

        @Override
        public PlanCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getSource(),
                    node.getFilteringSource(),
                    node.getDistributionType().orElse(SemiJoinNode.DistributionType.PARTITIONED).equals(SemiJoinNode.DistributionType.REPLICATED));
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            LocalCostEstimate localCost = calculateJoinCost(
                    node,
                    node.getLeft(),
                    node.getRight(),
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED);
            return costForLookupJoin(node, localCost);
        }

        @Override
        public PlanCostEstimate visitValues(ValuesNode node, Void context)
        {
            return costForSource(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitEnforceSingleRow(EnforceSingleRowNode node, Void context)
        {
            return costForAccumulation(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitLimit(LimitNode node, Void context)
        {
            // This is just a wild guess. First of all, LimitNode is rather rare except as a top node of a query plan,
            // so proper cost estimation is not that important. Second, since LimitNode can lead to incomplete evaluation
            // of the source, true cost estimation should be implemented as a "constraint" enforced on a sub-tree and
            // evaluated in context of actual source node type (and their sources).
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitUnion(UnionNode node, Void context)
        {
            // Cost will be accounted either in CostCalculatorUsingExchanges#CostEstimator#visitExchange
            // or in CostCalculatorWithEstimatedExchanges#CostEstimator#visitUnion
            // This stub is needed just to avoid the cumulative cost being set to unknown
            return costForStreaming(node, LocalCostEstimate.zero());
        }

        @Override
        public PlanCostEstimate visitCTEScan(CTEScanNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitCacheTableWriter(CacheTableWriterNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        @Override
        public PlanCostEstimate visitCacheTableFinish(CacheTableFinishNode node, Void context)
        {
            LocalCostEstimate localCost = LocalCostEstimate.ofCpu(getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types));
            return costForStreaming(node, localCost);
        }

        private PlanCostEstimate costForSource(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().isEmpty(), "Unexpected sources for %s: %s", node, node.getSources());
            return new PlanCostEstimate(localCost.getCpuCost(), localCost.getMaxMemory(), localCost.getMaxMemory(), localCost.getNetworkCost(), localCost);
        }

        private PlanCostEstimate costForAccumulation(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Accumulating operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    localCost.getMaxMemory(), // Source freed its memory allocations when finished its output
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanCostEstimate costForStreaming(PlanNode node, LocalCostEstimate localCost)
        {
            PlanCostEstimate sourcesCost = getSourcesEstimations(node)
                    .reduce(PlanCostEstimate.zero(), CostCalculatorUsingExchanges::addParallelSiblingsCost);
            return new PlanCostEstimate(
                    sourcesCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            sourcesCost.getMaxMemory(), // Streaming operator allocates insignificant amount of memory (usually none) before first input page is received
                            sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    sourcesCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(),
                    sourcesCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanCostEstimate costForLookupJoin(PlanNode node, LocalCostEstimate localCost)
        {
            verify(node.getSources().size() == 2, "Unexpected number of sources for %s: %s", node, node.getSources());
            List<PlanCostEstimate> tmpSourcesCosts = getSourcesEstimations(node).collect(toImmutableList());
            verify(tmpSourcesCosts.size() == 2);
            PlanCostEstimate probeCost = tmpSourcesCosts.get(0);
            PlanCostEstimate buildCost = tmpSourcesCosts.get(1);

            return new PlanCostEstimate(
                    probeCost.getCpuCost() + buildCost.getCpuCost() + localCost.getCpuCost(),
                    max(
                            probeCost.getMaxMemory() + buildCost.getMaxMemory(), // Probe and build execute independently, so their max memory allocations can be realized at the same time
                            probeCost.getMaxMemory() + buildCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory()),
                    probeCost.getMaxMemoryWhenOutputting() + localCost.getMaxMemory(), // Build side finished and freed its memory allocations
                    probeCost.getNetworkCost() + buildCost.getNetworkCost() + localCost.getNetworkCost(),
                    localCost);
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }

        private Stream<PlanCostEstimate> getSourcesEstimations(PlanNode node)
        {
            return node.getSources().stream()
                    .map(sourcesCosts::getCost);
        }
    }

    private static PlanCostEstimate addParallelSiblingsCost(PlanCostEstimate a, PlanCostEstimate b)
    {
        return new PlanCostEstimate(
                a.getCpuCost() + b.getCpuCost(),
                a.getMaxMemory() + b.getMaxMemory(),
                a.getMaxMemoryWhenOutputting() + b.getMaxMemoryWhenOutputting(),
                a.getNetworkCost() + b.getNetworkCost());
    }
}
