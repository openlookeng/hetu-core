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
import io.airlift.units.DataSize;
import io.prestosql.execution.Lifespan;
import io.prestosql.operator.GroupJoinProbe.GroupJoinProbeFactory;
import io.prestosql.operator.LookupJoinOperators.JoinType;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.SystemSessionProperties.isSpillToHdfsEnabled;
import static io.prestosql.operator.LookupJoinOperators.JoinType.INNER;
import static java.util.Objects.requireNonNull;

public class LookupGroupJoinOperatorFactory
        implements JoinOperatorFactory
{
    private final int operatorId;
    private final PlanNodeId planNodeId;
    private final List<Type> probeTypes;
    private final List<Type> outputTypes;
    private final List<Type> buildTypes;
    private final LookupJoinOperators.JoinType joinType;
    private final GroupJoinProbeFactory joinProbeFactory;
    private final Optional<OuterOperatorFactoryResult> outerOperatorFactoryResult;
    private final JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;
    private final OptionalInt totalOperatorsCount;
    private final HashGenerator probeHashGenerator;
    private final boolean forked;
    private final PartitioningSpillerFactory partitioningSpillerFactory;
    private final List<Symbol> probeFinalOutputSymbols;
    private final List<Integer> probeFinalOutputChannels;
    private final List<Integer> buildFinalOutputChannels;
    private boolean closed;
    private final GroupJoinAggregator aggrOnAggrfactory;
    private final GroupJoinAggregator aggrfactory;

    public static Builder builder()
    {
        return new Builder();
    }

    public LookupGroupJoinOperatorFactory(
            int operatorId,
            PlanNodeId planNodeId,
            JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
            List<Type> probeTypes,
            List<Type> outputTypes,
            List<Type> buildTypes,
            JoinType joinType,
            GroupJoinProbeFactory joinProbeFactory,
            OptionalInt totalOperatorsCount,
            List<Integer> probeJoinChannels,
            OptionalInt probeHashChannel,
            PartitioningSpillerFactory partitioningSpillerFactory,
            boolean forked,
            GroupJoinAggregator aggrfactory,
            GroupJoinAggregator aggrOnAggrfactory,
            List<Symbol> probeFinalOutputSymbols,
            List<Integer> probeFinalOutputChannels,
            List<Integer> buildFinalOutputChannels)
    {
        this.forked = forked;
        this.operatorId = operatorId;
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
        this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
        this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
        this.joinType = requireNonNull(joinType, "joinType is null");
        this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");

        this.joinBridgeManager = lookupSourceFactoryManager;
        joinBridgeManager.incrementProbeFactoryCount();
        checkArgument(joinType == INNER, "joinType is not INNER");
        this.outerOperatorFactoryResult = Optional.empty();
        this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");

        requireNonNull(probeHashChannel, "probeHashChannel is null");
        if (probeHashChannel.isPresent()) {
            this.probeHashGenerator = new PrecomputedHashGenerator(probeHashChannel.getAsInt());
        }
        else {
            requireNonNull(probeJoinChannels, "probeJoinChannels is null");
            List<Type> hashTypes = probeJoinChannels.stream()
                    .map(probeTypes::get)
                    .collect(toImmutableList());
            this.probeHashGenerator = new InterpretedHashGenerator(hashTypes, probeJoinChannels);
        }
        this.partitioningSpillerFactory = partitioningSpillerFactory;

        this.aggrfactory = aggrfactory;
        this.aggrOnAggrfactory = aggrOnAggrfactory;

        this.probeFinalOutputSymbols = probeFinalOutputSymbols;
        this.probeFinalOutputChannels = probeFinalOutputChannels;
        this.buildFinalOutputChannels = buildFinalOutputChannels;
    }

    private LookupGroupJoinOperatorFactory(LookupGroupJoinOperatorFactory other)
    {
        requireNonNull(other, "other is null");
        checkArgument(!other.closed, "cannot duplicated closed OperatorFactory");

        this.forked = true;
        this.operatorId = other.operatorId;
        this.planNodeId = other.planNodeId;
        this.probeTypes = other.probeTypes;
        this.outputTypes = other.outputTypes;
        this.buildTypes = other.buildTypes;
        this.joinType = other.joinType;
        this.joinProbeFactory = other.joinProbeFactory;
        this.joinBridgeManager = other.joinBridgeManager;
        this.outerOperatorFactoryResult = other.outerOperatorFactoryResult;
        this.totalOperatorsCount = other.totalOperatorsCount;
        this.probeHashGenerator = other.probeHashGenerator;
        this.partitioningSpillerFactory = other.partitioningSpillerFactory;

        this.aggrfactory = other.aggrfactory;
        this.aggrOnAggrfactory = other.aggrOnAggrfactory;

        this.probeFinalOutputChannels = other.probeFinalOutputChannels;
        this.probeFinalOutputSymbols = other.probeFinalOutputSymbols;
        this.buildFinalOutputChannels = other.buildFinalOutputChannels;

        this.closed = false;
        this.joinBridgeManager.incrementProbeFactoryCount();
    }

    public int getOperatorId()
    {
        return operatorId;
    }

    @Override
    public Operator createOperator(DriverContext driverContext)
    {
        checkState(!closed, "Factory is already closed");
        LookupSourceFactory lookupSourceFactory = joinBridgeManager.getJoinBridge(driverContext.getLifespan());

        OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, LookupGroupJoinOperator.class.getSimpleName());
        lookupSourceFactory.setTaskContext(driverContext.getPipelineContext().getTaskContext());

        joinBridgeManager.probeOperatorCreated(driverContext.getLifespan());
        return new LookupGroupJoinOperator(
                operatorContext,
                forked,
                probeTypes,
                outputTypes,
                buildTypes,
                joinType,
                lookupSourceFactory,
                joinProbeFactory,
                () -> joinBridgeManager.probeOperatorClosed(driverContext.getLifespan()),
                totalOperatorsCount,
                probeHashGenerator,
                partitioningSpillerFactory,
                () -> joinBridgeManager.probeOperatorFinished(driverContext.getLifespan()),
                isSpillToHdfsEnabled(driverContext.getPipelineContext().getTaskContext().getSession()),
                aggrfactory,
                aggrOnAggrfactory,
                probeFinalOutputSymbols,
                probeFinalOutputChannels,
                buildFinalOutputChannels);
    }

    @Override
    public void noMoreOperators()
    {
        checkState(!closed);
        closed = true;
        joinBridgeManager.probeOperatorFactoryClosedForAllLifespans();
    }

    @Override
    public void noMoreOperators(Lifespan lifespan)
    {
        joinBridgeManager.probeOperatorFactoryClosed(lifespan);
    }

    @Override
    public OperatorFactory duplicate()
    {
        return new LookupGroupJoinOperatorFactory(this);
    }

    @Override
    public Optional<OuterOperatorFactoryResult> createOuterOperatorFactory()
    {
        return outerOperatorFactoryResult;
    }

    public LookupSourceFactory getLookupSourceFactory(Lifespan lifespan)
    {
        return joinBridgeManager.getJoinBridge(lifespan);
    }

    public static class Builder
    {
        private GroupJoinAggregator aggrOnAggrfactory;
        private GroupJoinAggregator aggrfactory;
        private List<Symbol> probeFinalOutputSymbols;
        private List<Integer> probeFinalOutputChannels;
        private List<Integer> buildFinalOutputChannels;

        private int operatorId;
        private PlanNodeId planNodeId;
        private List<Type> probeTypes;
        private List<Type> outputTypes;
        private List<Type> buildTypes;
        private LookupJoinOperators.JoinType joinType;
        private GroupJoinProbeFactory joinProbeFactory;
        private JoinBridgeManager<? extends LookupSourceFactory> joinBridgeManager;
        private OptionalInt totalOperatorsCount;
        private boolean forked;
        private PartitioningSpillerFactory partitioningSpillerFactory;
        private OptionalInt probeHashChannel;
        private List<Integer> probeJoinChannels;

        public Builder()
        {
        }

        public Builder withJoinInfo(int operatorId,
                PlanNodeId planNodeId,
                JoinBridgeManager<? extends LookupSourceFactory> lookupSourceFactoryManager,
                List<Type> probeTypes,
                List<Type> outputTypes,
                List<Type> buildTypes,
                JoinType joinType,
                GroupJoinProbeFactory joinProbeFactory,
                OptionalInt totalOperatorsCount,
                List<Integer> probeJoinChannels,
                OptionalInt probeHashChannel,
                PartitioningSpillerFactory partitioningSpillerFactory,
                boolean forked)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.forked = forked;
            this.probeTypes = ImmutableList.copyOf(requireNonNull(probeTypes, "probeTypes is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            this.buildTypes = ImmutableList.copyOf(requireNonNull(buildTypes, "buildTypes is null"));
            this.joinType = requireNonNull(joinType, "joinType is null");
            this.joinProbeFactory = requireNonNull(joinProbeFactory, "joinProbeFactory is null");
            this.joinBridgeManager = lookupSourceFactoryManager;
            checkArgument(joinType == INNER, "joinType is not INNER");
            this.totalOperatorsCount = requireNonNull(totalOperatorsCount, "totalOperatorsCount is null");
            requireNonNull(probeHashChannel, "probeHashChannel is null");
            this.probeHashChannel = probeHashChannel;
            this.probeJoinChannels = probeJoinChannels;
            this.partitioningSpillerFactory = partitioningSpillerFactory;
            return this;
        }

        public Builder withProbeOutputInfo(List<Symbol> probeFinalOutputSymbols,
                List<Integer> probeFinalOutputChannels)
        {
            this.probeFinalOutputChannels = ImmutableList.copyOf(probeFinalOutputChannels);
            this.probeFinalOutputSymbols = ImmutableList.copyOf(probeFinalOutputSymbols);
            return this;
        }

        public Builder withBuildOutputInfo(List<Integer> buildFinalOutputChannels)
        {
            this.buildFinalOutputChannels = ImmutableList.copyOf(buildFinalOutputChannels);
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

        public LookupGroupJoinOperatorFactory build()
        {
            requireNonNull(aggrfactory, "aggrfactory is null");
            requireNonNull(aggrOnAggrfactory, "aggrOnAggrfactory is null");
            requireNonNull(buildFinalOutputChannels, "buildFinalOutputChannels is null");
            requireNonNull(probeFinalOutputChannels, "buildFinalOutputChannels is null");
            requireNonNull(joinProbeFactory, "joinProbeFactory is null");
            return new LookupGroupJoinOperatorFactory(
                    operatorId,
                    planNodeId,
                    joinBridgeManager,
                    probeTypes,
                    outputTypes,
                    buildTypes,
                    joinType,
                    joinProbeFactory,
                    totalOperatorsCount,
                    probeJoinChannels,
                    probeHashChannel,
                    partitioningSpillerFactory,
                    forked,
                    aggrfactory,
                    aggrOnAggrfactory,
                    probeFinalOutputSymbols,
                    probeFinalOutputChannels,
                    buildFinalOutputChannels);
        }
    }
}
