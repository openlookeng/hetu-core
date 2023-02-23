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

import io.prestosql.Session;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.rule.DetermineJoinDistributionType;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Objects;
import java.util.Optional;

import static io.prestosql.cost.LocalCostEstimate.addPartialComponents;
import static java.util.Objects.requireNonNull;

/**
 * A wrapper around CostCalculator that estimates ExchangeNodes cost.
 * <p>
 * Certain rules (e.g. {@link io.prestosql.sql.planner.iterative.rule.ReorderJoins} and {@link DetermineJoinDistributionType}) are run before exchanges
 * are added to a plan. This cost calculator adds the implied costs for the exchanges that will be added later.
 * It is needed to account for the differences in exchange costs for different types of joins.
 */
@ThreadSafe
public class CostCalculatorWithEstimatedExchanges
        implements CostCalculator
{
    private final CostCalculator costCalculator;
    private final TaskCountEstimator taskCountEstimator;

    @Inject
    public CostCalculatorWithEstimatedExchanges(CostCalculator costCalculator, TaskCountEstimator taskCountEstimator)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
    }

    @Override
    public PlanCostEstimate calculateCost(PlanNode node, StatsProvider stats, CostProvider sourcesCosts, Session session, TypeProvider types)
    {
        ExchangeCostEstimator exchangeCostEstimator = new ExchangeCostEstimator(stats, types, taskCountEstimator);
        PlanCostEstimate costEstimate = costCalculator.calculateCost(node, stats, sourcesCosts, session, types);
        LocalCostEstimate estimatedExchangeCost = node.accept(exchangeCostEstimator, null);
        return addExchangeCost(costEstimate, estimatedExchangeCost);
    }

    private static PlanCostEstimate addExchangeCost(PlanCostEstimate costEstimate, LocalCostEstimate estimatedExchangeCost)
    {
        // Exchange memory estimates are imprecise, because they don't take into account whether current node is streaming, accumulating or a join.
        // This is OK based on the assumption that exchange memory estimate is small anyway.
        return new PlanCostEstimate(
                costEstimate.getCpuCost() + estimatedExchangeCost.getCpuCost(),
                // "Estimated" (emulated) exchanges are below current node, not above, so we cannot assume exchange is not allocated concurrently
                // with the subplan realizing it's max memory allocation. Conservatively we assume this can happen at the same time and so
                // we increase max memory estimate.
                costEstimate.getMaxMemory() + estimatedExchangeCost.getMaxMemory(),
                // "Estimated" (emulated) exchanges are below current node, not above. If the current node is accumulating (e.g. final aggregation),
                // exchange memory allocation will actually be freed before node is outputting. Conservatively we assume the exchanges can still
                // hold the memory when the node is outputting.
                costEstimate.getMaxMemoryWhenOutputting() + estimatedExchangeCost.getMaxMemory(),
                costEstimate.getNetworkCost() + estimatedExchangeCost.getNetworkCost(),
                addPartialComponents(costEstimate.getRootNodeLocalCostEstimate(), estimatedExchangeCost));
    }

    private static class ExchangeCostEstimator
            extends InternalPlanVisitor<LocalCostEstimate, Void>
    {
        private final StatsProvider stats;
        private final TypeProvider types;
        private final TaskCountEstimator taskCountEstimator;

        ExchangeCostEstimator(StatsProvider stats, TypeProvider types, TaskCountEstimator taskCountEstimator)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.types = requireNonNull(types, "types is null");
            this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        }

        @Override
        public LocalCostEstimate visitPlan(PlanNode node, Void context)
        {
            // TODO implement logic for other node types and return LocalCostEstimate.unknown() here (or throw)
            return LocalCostEstimate.zero();
        }

        @Override
        public LocalCostEstimate visitGroupReference(GroupReference node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalCostEstimate visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource();
            double inputSizeInBytes = getStats(source).getOutputSizeInBytes(source.getOutputSymbols(), types);

            LocalCostEstimate remoteRepartitionCost = calculateRemoteRepartitionCost(inputSizeInBytes);
            LocalCostEstimate localRepartitionCost = calculateLocalRepartitionCost(inputSizeInBytes);

            // TODO consider cost of aggregation itself, not only exchanges, based on aggregation's properties
            return addPartialComponents(remoteRepartitionCost, localRepartitionCost);
        }

        @Override
        public LocalCostEstimate visitJoin(JoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getLeft(),
                    node.getRight(),
                    stats,
                    types,
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)),
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public LocalCostEstimate visitJoinOnAggregation(JoinOnAggregationNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getLeft(),
                    node.getRight(),
                    stats,
                    types,
                    Objects.equals(node.getDistributionType(), Optional.of(JoinNode.DistributionType.REPLICATED)),
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public LocalCostEstimate visitSemiJoin(SemiJoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getSource(),
                    node.getFilteringSource(),
                    stats,
                    types,
                    Objects.equals(node.getDistributionType(), Optional.of(SemiJoinNode.DistributionType.REPLICATED)),
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public LocalCostEstimate visitSpatialJoin(SpatialJoinNode node, Void context)
        {
            return calculateJoinExchangeCost(
                    node.getLeft(),
                    node.getRight(),
                    stats,
                    types,
                    node.getDistributionType() == SpatialJoinNode.DistributionType.REPLICATED,
                    taskCountEstimator.estimateSourceDistributedTaskCount());
        }

        @Override
        public LocalCostEstimate visitUnion(UnionNode node, Void context)
        {
            // this assumes that all union inputs will be gathered over the network
            // that is not aways true
            // but this estimate is better that returning UNKNOWN, as it sets
            // cumulative cost to unknown
            double inputSizeInBytes = getStats(node).getOutputSizeInBytes(node.getOutputSymbols(), types);
            return calculateRemoteGatherCost(inputSizeInBytes);
        }

        private PlanNodeStatsEstimate getStats(PlanNode node)
        {
            return stats.getStats(node);
        }
    }

    public static LocalCostEstimate calculateRemoteGatherCost(double inputSizeInBytes)
    {
        return LocalCostEstimate.ofNetwork(inputSizeInBytes);
    }

    public static LocalCostEstimate calculateRemoteRepartitionCost(double inputSizeInBytes)
    {
        return LocalCostEstimate.of(inputSizeInBytes, 0, inputSizeInBytes);
    }

    public static LocalCostEstimate calculateLocalRepartitionCost(double inputSizeInBytes)
    {
        return LocalCostEstimate.ofCpu(inputSizeInBytes);
    }

    public static LocalCostEstimate calculateRemoteReplicateCost(double inputSizeInBytes, int destinationTaskCount)
    {
        return LocalCostEstimate.ofNetwork(inputSizeInBytes * destinationTaskCount);
    }

    public static LocalCostEstimate calculateJoinCostWithoutOutput(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        LocalCostEstimate exchangesCost = calculateJoinExchangeCost(
                probe,
                build,
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);

        // TODO: Remove once traits (https://gitee.com/openlookeng/hetu-core/issues/I68CXF) are used to correctly estimate
        // local exchange cost for replicated join in CostCalculatorUsingExchanges#visitExchange
        LocalCostEstimate adjustedLocalExchangeCost = adjustReplicatedJoinLocalExchangeCost(
                build,
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);

        LocalCostEstimate inputCost = calculateJoinInputCost(
                probe,
                build,
                stats,
                types,
                replicated,
                estimatedSourceDistributedTaskCount);
        return addPartialComponents(exchangesCost, adjustedLocalExchangeCost, inputCost);
    }

    public static LocalCostEstimate adjustReplicatedJoinLocalExchangeCost(
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        if (!replicated) {
            return LocalCostEstimate.zero();
        }

        /*
         * HACK!
         *
         * Stats model doesn't multiply the number of rows by the number of tasks for replicated
         * exchange to avoid misestimation of the JOIN output.
         *
         * Thus the cost estimation for the operations that come after a replicated exchange is
         * underestimated. And the cost of operations over the replicated copies must be explicitly
         * added here.
         */

        // Add the cost of a local repartitioning of build side copies.
        // Cost of the repartitioning of a single data copy has been already added in
        // CostCalculatorWithEstimatedExchanges#calculateJoinExchangeCost or in CostCalculatorUsingExchanges#visitExchange
        PlanNodeStatsEstimate buildStats = stats.getStats(build);
        double buildSideSize = buildStats.getOutputSizeInBytes(build.getOutputSymbols(), types);
        double cpuCost = buildSideSize * (estimatedSourceDistributedTaskCount - 1);
        return LocalCostEstimate.of(cpuCost, 0, 0);
    }

    private static LocalCostEstimate calculateJoinExchangeCost(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        double probeSizeInBytes = stats.getStats(probe).getOutputSizeInBytes(probe.getOutputSymbols(), types);
        double buildSizeInBytes = stats.getStats(build).getOutputSizeInBytes(build.getOutputSymbols(), types);
        if (replicated) {
            // assuming the probe side of a replicated join is always source distributed
            LocalCostEstimate replicateCost = calculateRemoteReplicateCost(buildSizeInBytes, estimatedSourceDistributedTaskCount);
            // cost of the copies repartitioning is added in CostCalculatorUsingExchanges#calculateJoinCost
            LocalCostEstimate localRepartitionCost = calculateLocalRepartitionCost(buildSizeInBytes);
            return addPartialComponents(replicateCost, localRepartitionCost);
        }
        else {
            LocalCostEstimate probeCost = calculateRemoteRepartitionCost(probeSizeInBytes);
            LocalCostEstimate buildRemoteRepartitionCost = calculateRemoteRepartitionCost(buildSizeInBytes);
            LocalCostEstimate buildLocalRepartitionCost = calculateLocalRepartitionCost(buildSizeInBytes);
            return addPartialComponents(probeCost, buildRemoteRepartitionCost, buildLocalRepartitionCost);
        }
    }

    public static LocalCostEstimate calculateJoinInputCost(
            PlanNode probe,
            PlanNode build,
            StatsProvider stats,
            TypeProvider types,
            boolean replicated,
            int estimatedSourceDistributedTaskCount)
    {
        int buildSizeMultiplier = replicated ? estimatedSourceDistributedTaskCount : 1;

        PlanNodeStatsEstimate probeStats = stats.getStats(probe);
        PlanNodeStatsEstimate buildStats = stats.getStats(build);

        double buildSideSize = buildStats.getOutputSizeInBytes(build.getOutputSymbols(), types);
        double probeSideSize = probeStats.getOutputSizeInBytes(probe.getOutputSymbols(), types);

        double cpuCost = probeSideSize + buildSideSize * buildSizeMultiplier;

        double memoryCost = buildSideSize * buildSizeMultiplier;

        return LocalCostEstimate.of(cpuCost, memoryCost, 0);
    }
}
