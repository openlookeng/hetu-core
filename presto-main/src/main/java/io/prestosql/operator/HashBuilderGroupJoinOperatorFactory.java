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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class HashBuilderGroupJoinOperatorFactory
        implements OperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
    private final List<Integer> outputChannels;
    private final List<Integer> hashChannels;
    private final OptionalInt preComputedHashChannel;
    private final Optional<JoinFilterFunctionFactory> filterFunctionFactory;
    private final Optional<Integer> sortChannel;
    private final Optional<Integer> countChannel;
    private final List<JoinFilterFunctionFactory> searchFunctionFactories;
    private final PagesIndex.Factory pagesIndexFactory;
    private final int expectedPositions;
    private final boolean spillEnabled;
    private final Map<Lifespan, Integer> partitionIndexManager = new HashMap<>();
    private boolean closed;
    private boolean spillToHdfsEnabled;
    private final GroupJoinAggregator aggrOnAggrfactory;
    private final GroupJoinAggregator aggrfactory;
    private final List<Symbol> buildFinalOutputSymbols;
    private final List<Integer> buildFinalOutputChannels;

    private final ListeningExecutorService executor;

    public static Builder builder()
    {
        return new Builder();
    }

    public HashBuilderGroupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
            List<Integer> outputChannels,
            List<Integer> hashChannels,
            OptionalInt preComputedHashChannel,
            Optional<JoinFilterFunctionFactory> filterFunctionFactory,
            Optional<Integer> sortChannel,
            Optional<Integer> countChannel,
            List<JoinFilterFunctionFactory> searchFunctionFactories,
            int expectedPositions,
            PagesIndex.Factory pagesIndexFactory,
            boolean spillEnabled,
            boolean spillToHdfsEnabled,
            GroupJoinAggregator aggrfactory,
            GroupJoinAggregator aggrOnAggrfactory,
            List<Symbol> buildFinalOutputSymbols,
            List<Integer> buildFinalOutputChannels,
            ListeningExecutorService executor)
    {
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        requireNonNull(sortChannel, "sortChannel can not be null");
        requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
        checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
        this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

        this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
        this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
        this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
        this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
        this.sortChannel = sortChannel;
        this.countChannel = countChannel;
        this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
        this.spillEnabled = spillEnabled;
        this.spillToHdfsEnabled = spillToHdfsEnabled;
        this.expectedPositions = expectedPositions;

        this.aggrfactory = aggrfactory;
        this.aggrOnAggrfactory = aggrOnAggrfactory;

        this.buildFinalOutputSymbols = buildFinalOutputSymbols;
        this.buildFinalOutputChannels = buildFinalOutputChannels;
        this.executor = executor;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashBuilderGroupJoinOperator.class.getSimpleName());

        PartitionedLookupSourceFactory partitionedLookupSourceFactory = this.lookupSourceFactoryManager.getJoinBridge(driverContext.getLifespan());
        int incrementPartitionIndex = getAndIncrementPartitionIndex(driverContext.getLifespan());
        // Snapshot: make driver ID and source/partition index the same, to ensure consistency before and after resuming.
        // LocalExchangeSourceOperator also uses the same mechanism to ensure consistency.
        if (addOperatorContext.isSnapshotEnabled()) {
            incrementPartitionIndex = driverContext.getDriverId();
        }
        verify(incrementPartitionIndex < partitionedLookupSourceFactory.partitions());
        return new HashBuilderGroupJoinOperator(
                addOperatorContext,
                partitionedLookupSourceFactory,
                incrementPartitionIndex,
                outputChannels,
                hashChannels,
                preComputedHashChannel,
                filterFunctionFactory,
                sortChannel,
                countChannel,
                searchFunctionFactories,
                expectedPositions,
                pagesIndexFactory,
                spillEnabled,
                spillToHdfsEnabled,
                aggrfactory,
                aggrOnAggrfactory,
                buildFinalOutputSymbols,
                buildFinalOutputChannels,
                executor);
    }

    @Override
    public void noMoreOperators()
    {
        closed = true;
    }

    @Override
    public OperatorFactory duplicate()
    {
        throw new UnsupportedOperationException("Parallel hash build can not be duplicated");
    }

    private int getAndIncrementPartitionIndex(Lifespan lifespan)
    {
        return partitionIndexManager.compute(lifespan, (k, v) -> v == null ? 1 : v + 1) - 1;
    }

    public static class Builder
    {
        private GroupJoinAggregator aggrOnAggrfactory;
        private GroupJoinAggregator aggrfactory;
        private List<Symbol> buildFinalOutputSymbols;
        private List<Integer> buildFinalOutputChannels;

        private int operatorId;
        private PlanNodeId planNodeId;
        private JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private List<Integer> outputChannels;
        private List<Integer> hashChannels;
        private OptionalInt preComputedHashChannel;
        private Optional<JoinFilterFunctionFactory> filterFunctionFactory;
        private Optional<Integer> sortChannel;
        private Optional<Integer> countChannel;
        private List<JoinFilterFunctionFactory> searchFunctionFactories;
        private int expectedPositions;
        private PagesIndex.Factory pagesIndexFactory;
        private boolean spillEnabled;
        private boolean spillToHdfsEnabled;

        private ListeningExecutorService executor;

        public Builder()
        {
        }

        public Builder withExecutor(ListeningExecutorService executor)
        {
            this.executor = executor;
            return this;
        }

        public Builder withJoinInfo(int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager,
                List<Integer> outputChannels,
                List<Integer> hashChannels,
                OptionalInt preComputedHashChannel,
                Optional<JoinFilterFunctionFactory> filterFunctionFactory,
                Optional<Integer> sortChannel,
                Optional<Integer> countChannel,
                List<JoinFilterFunctionFactory> searchFunctionFactories,
                int expectedPositions,
                PagesIndex.Factory pagesIndexFactory,
                boolean spillEnabled,
                boolean spillToHdfsEnabled)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            requireNonNull(sortChannel, "sortChannel can not be null");
            requireNonNull(searchFunctionFactories, "searchFunctionFactories is null");
            checkArgument(sortChannel.isPresent() != searchFunctionFactories.isEmpty(), "both or none sortChannel and searchFunctionFactories must be set");
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");

            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.hashChannels = ImmutableList.copyOf(requireNonNull(hashChannels, "hashChannels is null"));
            this.preComputedHashChannel = requireNonNull(preComputedHashChannel, "preComputedHashChannel is null");
            this.filterFunctionFactory = requireNonNull(filterFunctionFactory, "filterFunctionFactory is null");
            this.sortChannel = sortChannel;
            this.countChannel = countChannel;
            this.searchFunctionFactories = ImmutableList.copyOf(searchFunctionFactories);
            this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
            this.spillEnabled = spillEnabled;
            this.spillToHdfsEnabled = spillToHdfsEnabled;
            this.expectedPositions = expectedPositions;
            return this;
        }

        public Builder withBuildOutputInfo(List<Symbol> buildFinalOutputSymbols,
                List<Integer> buildFinalOutputChannels)
        {
            this.buildFinalOutputChannels = ImmutableList.copyOf(buildFinalOutputChannels);
            this.buildFinalOutputSymbols = ImmutableList.copyOf(buildFinalOutputSymbols);
            return this;
        }

        public Builder withAggrOnAggrFactory(List<Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                AggregationNode.Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory,
                Optional<PartialAggregationController> partialAggregationController,
                boolean produceDefaultOutput)
        {
            aggrOnAggrfactory = new GroupJoinAggregator(hashChannel,
                    groupIdChannel,
                    accumulatorFactories,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    expectedGroups,
                    maxPartialMemory,
                    joinCompiler,
                    useSystemMemory,
                    partialAggregationController,
                    produceDefaultOutput);
            return this;
        }

        public Builder withAggrFactory(List<Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                AggregationNode.Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory,
                Optional<PartialAggregationController> partialAggregationController,
                boolean produceDefaultOutput)
        {
            aggrfactory = new GroupJoinAggregator(hashChannel,
                    groupIdChannel,
                    accumulatorFactories,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    expectedGroups,
                    maxPartialMemory,
                    joinCompiler,
                    useSystemMemory,
                    partialAggregationController,
                    produceDefaultOutput);
            return this;
        }

        public HashBuilderGroupJoinOperatorFactory build()
        {
            requireNonNull(aggrfactory, "aggrfactory is null");
            requireNonNull(aggrOnAggrfactory, "aggrOnAggrfactory is null");
            requireNonNull(executor, "executor is null");
            requireNonNull(buildFinalOutputChannels, "buildFinalOutputChannels is null");
            requireNonNull(hashChannels, "hashChannels is null");
            return new HashBuilderGroupJoinOperatorFactory(
                    operatorId,
                    planNodeId,
                    lookupSourceFactoryManager,
                    outputChannels,
                    hashChannels,
                    preComputedHashChannel,
                    filterFunctionFactory,
                    sortChannel,
                    countChannel,
                    searchFunctionFactories,
                    expectedPositions,
                    pagesIndexFactory,
                    spillEnabled,
                    spillToHdfsEnabled,
                    aggrfactory,
                    aggrOnAggrfactory,
                    buildFinalOutputSymbols,
                    buildFinalOutputChannels,
                    executor);
        }
    }
}
