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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.aggregation.Accumulator;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.operator.aggregation.partial.PartialAggregationController;
import io.prestosql.operator.scalar.CombineHashFunction;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static io.prestosql.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"snapshotState", "groupByTypes", "groupByChannels", "globalAggregationGroupIds",
        "accumulatorFactories", "hashChannel", "groupIdChannel", "maxPartialMemory", "memoryLimitForMerge",
        "memoryLimitForMergeWithMemory", "spillerFactory", "joinCompiler", "types", "outputPages", "unfinishedWork", "hashCollisionsCounter"})
public class GroupAggregationOperator
        implements Operator
{
    protected static final double MERGE_WITH_MEMORY_RATIO = 0.9;

    public static class GroupAggregationOperatorFactory
            implements OperatorFactory
    {
        protected final int operatorId;
        protected final PlanNodeId planNodeId;
        protected final List<Type> groupByTypes;
        protected final List<Integer> groupByChannels;
        protected final List<Integer> globalAggregationGroupIds;
        protected final AggregationNode.Step step;
        protected final boolean produceDefaultOutput;
        protected final List<AccumulatorFactory> accumulatorFactories;
        protected final Optional<Integer> hashChannel;
        protected final Optional<Integer> groupIdChannel;

        protected final int expectedGroups;
        protected final Optional<DataSize> maxPartialMemory;
        protected final boolean spillEnabled;
        protected final DataSize memoryLimitForMerge;
        protected final DataSize memoryLimitForMergeWithMemory;
        protected final SpillerFactory spillerFactory;
        protected final JoinCompiler joinCompiler;
        protected final boolean useSystemMemory;
        protected final Optional<PartialAggregationController> partialAggregationController;

        protected boolean closed;

        public GroupAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                AggregationNode.Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize unspillMemoryLimit,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory,
                Optional<PartialAggregationController> partialAggregationController)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    produceDefaultOutput,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    spillEnabled,
                    unspillMemoryLimit,
                    DataSize.succinctBytes((long) (unspillMemoryLimit.toBytes() * MERGE_WITH_MEMORY_RATIO)),
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory,
                    partialAggregationController);
        }

        public GroupAggregationOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                AggregationNode.Step step,
                boolean produceDefaultOutput,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                boolean spillEnabled,
                DataSize memoryLimitForMerge,
                DataSize memoryLimitForMergeWithMemory,
                SpillerFactory spillerFactory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory,
                Optional<PartialAggregationController> partialAggregationController)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
            this.groupByTypes = ImmutableList.copyOf(groupByTypes);
            this.groupByChannels = ImmutableList.copyOf(groupByChannels);
            this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
            this.step = step;
            this.produceDefaultOutput = produceDefaultOutput;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.expectedGroups = expectedGroups;
            this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
            this.spillEnabled = spillEnabled;
            this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.useSystemMemory = useSystemMemory;
            this.partialAggregationController = requireNonNull(partialAggregationController, "partialAggregationController is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException();
        }
    }

    protected static final int pageFinalizeLocation = 2;
    protected final OperatorContext operatorContext;
    protected final Optional<PartialAggregationController> partialAggregationController;
    protected final SingleInputSnapshotState snapshotState;
    protected final List<Type> groupByTypes;
    protected final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    protected final AggregationNode.Step step;
    protected final boolean produceDefaultOutput;
    protected final List<AccumulatorFactory> accumulatorFactories;
    protected final Optional<Integer> hashChannel;
    protected final Optional<Integer> groupIdChannel;
    protected final int expectedGroups;
    protected final Optional<DataSize> maxPartialMemory;
    protected final boolean spillEnabled;
    protected final DataSize memoryLimitForMerge;
    protected final DataSize memoryLimitForMergeWithMemory;
    protected final SpillerFactory spillerFactory;
    protected final JoinCompiler joinCompiler;
    protected final boolean useSystemMemory;
    protected final List<Type> types;

    protected HashCollisionsCounter hashCollisionsCounter;
    protected AggregationBuilder aggregationBuilder;
    protected LocalMemoryContext memoryContext;
    protected WorkProcessor<Page> outputPages;
    protected boolean inputProcessed;
    protected boolean finishing;
    protected boolean finished;

    // for yield when memory is not available
    protected Work<?> unfinishedWork;
    protected long numberOfInputRowsProcessed;
    protected long numberOfUniqueRowsProduced;

    public GroupAggregationOperator(
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            AggregationNode.Step step,
            boolean produceDefaultOutput,
            List<AccumulatorFactory> accumulatorFactories,
            Optional<Integer> hashChannel,
            Optional<Integer> groupIdChannel,
            int expectedGroups,
            Optional<DataSize> maxPartialMemory,
            boolean spillEnabled,
            DataSize memoryLimitForMerge,
            DataSize memoryLimitForMergeWithMemory,
            SpillerFactory spillerFactory,
            JoinCompiler joinCompiler,
            boolean useSystemMemory,
            Optional<PartialAggregationController> partialAggregationController)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.partialAggregationController = requireNonNull(partialAggregationController, "partialAggregationControl is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");
        checkArgument(!partialAggregationController.isPresent() || step.isOutputPartial(), "partialAggregationController should be present only for partial aggregation");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;

        this.groupByTypes = ImmutableList.copyOf(groupByTypes);
        this.groupByChannels = ImmutableList.copyOf(groupByChannels);
        this.globalAggregationGroupIds = ImmutableList.copyOf(globalAggregationGroupIds);
        this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
        this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
        this.groupIdChannel = requireNonNull(groupIdChannel, "groupIdChannel is null");
        this.step = step;
        this.produceDefaultOutput = produceDefaultOutput;
        this.expectedGroups = expectedGroups;
        this.maxPartialMemory = requireNonNull(maxPartialMemory, "maxPartialMemory is null");
        this.types = toTypes(groupByTypes, step, accumulatorFactories, hashChannel);
        this.spillEnabled = spillEnabled;
        this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
        this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
        this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
        this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        this.useSystemMemory = useSystemMemory;

        //Sort Based Aggregation uses both Sort (for finalized values) and Hash (for un-finalized Values),So when hash is used this is required
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);

        this.memoryContext = operatorContext.localUserMemoryContext();
        if (useSystemMemory) {
            this.memoryContext = operatorContext.localSystemMemoryContext();
        }
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return finished;
    }

    @Override
    public boolean needsInput()
    {
        if (finishing || outputPages != null) {
            return false;
        }
        else if (aggregationBuilder != null && aggregationBuilder.isFull()) {
            return false;
        }
        else {
            return unfinishedWork == null;
        }
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException();
    }

    public void createAggregationBuilder()
    {
        throw new UnsupportedOperationException();
    }

    protected boolean hasOrderBy()
    {
        return accumulatorFactories.stream().anyMatch(AccumulatorFactory::hasOrderBy);
    }

    protected boolean hasDistinct()
    {
        return accumulatorFactories.stream().anyMatch(AccumulatorFactory::hasDistinct);
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            return aggregationBuilder.startMemoryRevoke();
        }
        return NOT_BLOCKED;
    }

    @Override
    public void finishMemoryRevoke()
    {
        if (aggregationBuilder != null) {
            aggregationBuilder.finishMemoryRevoke();
        }
    }

    @Override
    public Page getOutput()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException();
    }

    @VisibleForTesting
    public AggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    protected Page getGlobalAggregationOutput()
    {
        List<Accumulator> accumulators = accumulatorFactories.stream()
                .map(AccumulatorFactory::createAccumulator)
                .collect(Collectors.toList());

        // global aggregation output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder output = new PageBuilder(globalAggregationGroupIds.size(), types);

        for (int groupId : globalAggregationGroupIds) {
            output.declarePosition();
            int channel = 0;

            for (; channel < groupByTypes.size(); channel++) {
                if (channel == groupIdChannel.get()) {
                    output.getBlockBuilder(channel).writeLong(groupId);
                }
                else {
                    output.getBlockBuilder(channel).appendNull();
                }
            }

            if (hashChannel.isPresent()) {
                long hashValue = calculateDefaultOutputHash(groupByTypes, groupIdChannel.get(), groupId);
                output.getBlockBuilder(channel++).writeLong(hashValue);
            }

            for (int j = 0; j < accumulators.size(); channel++, j++) {
                if (step.isOutputPartial()) {
                    accumulators.get(j).evaluateIntermediate(output.getBlockBuilder(channel));
                }
                else {
                    accumulators.get(j).evaluateFinal(output.getBlockBuilder(channel));
                }
            }
        }

        if (output.isEmpty()) {
            return null;
        }
        return output.build();
    }

    private static long calculateDefaultOutputHash(List<Type> groupByChannels, int groupIdChannel, int groupId)
    {
        // Default output has NULLs on all columns except of groupIdChannel
        long result = INITIAL_HASH_VALUE;
        for (int channel = 0; channel < groupByChannels.size(); channel++) {
            if (channel != groupIdChannel) {
                result = CombineHashFunction.getHash(result, NULL_HASH_CODE);
            }
            else {
                result = CombineHashFunction.getHash(result, BigintType.hash(groupId));
            }
        }
        return result;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        GroupAggregationOperatorState myState = new GroupAggregationOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        if (aggregationBuilder != null) {
            myState.aggregationBuilder = aggregationBuilder.capture(serdeProvider);
        }
        myState.memoryContext = memoryContext.getBytes();
        myState.inputProcessed = inputProcessed;
        myState.finishing = finishing;
        myState.finished = finished;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        GroupAggregationOperatorState myState = (GroupAggregationOperatorState) state;
        operatorContext.restore(myState.operatorContext, serdeProvider);
        if (myState.aggregationBuilder != null) {
            if (this.aggregationBuilder == null) {
                createAggregationBuilder();
            }
            aggregationBuilder.restore(myState.aggregationBuilder, serdeProvider);
        }
        else {
            aggregationBuilder = null;
        }
        this.memoryContext.setBytes(myState.memoryContext);
        inputProcessed = myState.inputProcessed;
        finishing = myState.finishing;
        finished = myState.finished;
    }

    @Override
    public boolean supportsConsolidatedWrites()
    {
        return false;
    }

    public static class GroupAggregationOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private Object aggregationBuilder;
        private long memoryContext;
        private boolean inputProcessed;
        private boolean finishing;
        private boolean finished;
    }
}
