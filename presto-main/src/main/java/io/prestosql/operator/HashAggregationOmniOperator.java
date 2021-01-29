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
import io.prestosql.operator.aggregation.builder.HashAggregationBuilder;
import io.prestosql.operator.scalar.CombineHashFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.AggregationNode.Step;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OmniRuntime;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.omnicache.vector.VecType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder.toTypes;
import static io.prestosql.sql.planner.optimizations.HashGenerationOptimizer.INITIAL_HASH_VALUE;
import static io.prestosql.type.TypeUtils.NULL_HASH_CODE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOmniOperator
        implements Operator
{
    private static final double MERGE_WITH_MEMORY_RATIO = 0.9;
    private final OperatorContext operatorContext;
    private final List<Type> groupByTypes;
    private final List<Integer> groupByChannels;
    private final List<Integer> globalAggregationGroupIds;
    private final Step step;
    private final boolean produceDefaultOutput;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Optional<Integer> hashChannel;
    private final Optional<Integer> groupIdChannel;
    private final int expectedGroups;
    private final Optional<DataSize> maxPartialMemory;
    private final boolean spillEnabled;
    private final DataSize memoryLimitForMerge;
    private final DataSize memoryLimitForMergeWithMemory;
    private final SpillerFactory spillerFactory;
    private final JoinCompiler joinCompiler;
    private final boolean useSystemMemory;
    private final List<Type> types;
    private final HashCollisionsCounter hashCollisionsCounter;
    //omni
    String compileID;
    OmniRuntime omniRuntime;
    private HashAggregationBuilder aggregationBuilder;
    private LocalMemoryContext memoryContext;
    private WorkProcessor<Page> outputPages;
    private boolean inputProcessed;
    private boolean finishing;
    private boolean finished;
    // for yield when memory is not available
    private Work<?> unfinishedWork;

    private OmniPageContainer omniPageContainer;

    public HashAggregationOmniOperator(
            OmniRuntime omniRuntime,
            String compileID,
            OperatorContext operatorContext,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            List<Integer> globalAggregationGroupIds,
            Step step,
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
            boolean useSystemMemory)
    {
        this.omniRuntime = omniRuntime;
        this.compileID = compileID;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        requireNonNull(step, "step is null");
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        requireNonNull(operatorContext, "operatorContext is null");

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
        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
        this.useSystemMemory = useSystemMemory;

        this.memoryContext = operatorContext.localUserMemoryContext();
        if (useSystemMemory) {
            this.memoryContext = operatorContext.localSystemMemoryContext();
        }
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

    private static class OmniPageContainer
    {
        private final int capacity = 128 * 1024 * 1024;
        private Vec[] buffers;
        private int totalRowCount = 0;

        public OmniPageContainer(int length)
        {
            buffers = new Vec[length];
            for (int idx = 0; idx < length; idx++) {
                buffers[idx] = new LongVec(capacity);
            }
        }

        public void appendPage(Page page)
        {
            for (int ridx = 0; ridx < page.getPositionCount(); ridx++) {
                for (int cidx = 0; cidx < page.getChannelCount(); cidx++) {
                    LongVec vec = (LongVec) page.getBlock(cidx).getValuesVec();
                    long value = vec.get(ridx);
                    buffers[cidx].set(totalRowCount, value);
                }
                totalRowCount++;
            }
        }

        public Vec<?>[] getBuffers()
        {
            return buffers;
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
        finishing = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        if (omniPageContainer == null) {
            omniPageContainer = new OmniPageContainer(page.getChannelCount());
        }
        omniPageContainer.appendPage(page);
    }

    private boolean hasOrderBy()
    {
        return accumulatorFactories.stream().anyMatch(AccumulatorFactory::hasOrderBy);
    }

    private boolean hasDistinct()
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

    public Page toResult(Vec<?>[] omniExecutionResult)
    {
        List<Type> builderType = new ArrayList<>();
        builderType.add(BigintType.BIGINT);
        builderType.add(BigintType.BIGINT);
        final PageBuilder pageBuilder = new PageBuilder(Integer.MAX_VALUE, builderType);

        long start1 = System.currentTimeMillis();

        Vec<?>[] vecs = omniExecutionResult;
        int size = vecs[0].size();
        BlockBuilder key = pageBuilder.getBlockBuilder(0);
        BlockBuilder value = pageBuilder.getBlockBuilder(1);

        boolean[] valueIsNull = new boolean[size];
        for (int i = 0; i < size; i++) {
            valueIsNull[i] = false;
        }
        LongArrayBlock[] longArrayBlocks = new LongArrayBlock[2];
        longArrayBlocks[0] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[0]);
        longArrayBlocks[1] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[1]);
//            longArrayBlocks[2] = new LongArrayBlock(size, Optional.of(valueIsNull), (LongVec) vecs[1]);
        Page page = new Page(longArrayBlocks);

        long end1 = System.currentTimeMillis();
        System.out.println("get omni result: " + (end1 - start1));
        return page;
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (!finishing) {
            finished = true;
            if(omniPageContainer != null) {
                VecType[] outTypes = {VecType.LONG, VecType.LONG};
                Vec<?>[] executeResult = omniRuntime.execute(compileID, omniPageContainer.getBuffers(), omniPageContainer.totalRowCount, outTypes);
                return toResult(executeResult);
            } else {
                return null;
            }
        }
        return null;
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    @VisibleForTesting
    public HashAggregationBuilder getAggregationBuilder()
    {
        return aggregationBuilder;
    }

    private void closeAggregationBuilder()
    {
        outputPages = null;
        if (aggregationBuilder != null) {
            aggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        memoryContext.setBytes(0);
    }

    private Page getGlobalAggregationOutput()
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

    public static class HashAggregationOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final List<Integer> globalAggregationGroupIds;
        private final Step step;
        private final boolean produceDefaultOutput;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final Optional<Integer> hashChannel;
        private final Optional<Integer> groupIdChannel;
        private final int expectedGroups;
        private final Optional<DataSize> maxPartialMemory;
        private final boolean spillEnabled;
        private final DataSize memoryLimitForMerge;
        private final DataSize memoryLimitForMergeWithMemory;
        private final SpillerFactory spillerFactory;
        private final JoinCompiler joinCompiler;
        private final boolean useSystemMemory;
        private OmniRuntime omniRuntime;
        private String compileID;
        private boolean closed;

        @VisibleForTesting
        public HashAggregationOmniOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
                List<AccumulatorFactory> accumulatorFactories,
                Optional<Integer> hashChannel,
                Optional<Integer> groupIdChannel,
                int expectedGroups,
                Optional<DataSize> maxPartialMemory,
                JoinCompiler joinCompiler,
                boolean useSystemMemory)
        {
            this(operatorId,
                    planNodeId,
                    groupByTypes,
                    groupByChannels,
                    globalAggregationGroupIds,
                    step,
                    false,
                    accumulatorFactories,
                    hashChannel,
                    groupIdChannel,
                    expectedGroups,
                    maxPartialMemory,
                    false,
                    new DataSize(0, MEGABYTE),
                    new DataSize(0, MEGABYTE),
                    (types, spillContext, memoryContext) -> {
                        throw new UnsupportedOperationException();
                    },
                    joinCompiler,
                    useSystemMemory);
        }

        public HashAggregationOmniOperatorFactory(
//                OmniRuntime omniRuntime,
//                String compileID,
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
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
                boolean useSystemMemory)
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
                    useSystemMemory);
        }

        @VisibleForTesting
        HashAggregationOmniOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
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
                boolean useSystemMemory)
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
        }

        public <E> HashAggregationOmniOperatorFactory(
                OmniRuntime omniRuntime,
                String compileID,
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> groupByTypes,
                List<Integer> groupByChannels,
                List<Integer> globalAggregationGroupIds,
                Step step,
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
                boolean useSystemMemory)
        {
            this.omniRuntime = omniRuntime;
            this.compileID = compileID;
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
            DataSize memoryLimitForMerge = unspillMemoryLimit;
            DataSize memoryLimitForMergeWithMemory = DataSize.succinctBytes((long) (unspillMemoryLimit.toBytes() * MERGE_WITH_MEMORY_RATIO));
            this.memoryLimitForMerge = requireNonNull(memoryLimitForMerge, "memoryLimitForMerge is null");
            this.memoryLimitForMergeWithMemory = requireNonNull(memoryLimitForMergeWithMemory, "memoryLimitForMergeWithMemory is null");
            this.spillerFactory = requireNonNull(spillerFactory, "spillerFactory is null");
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
            this.useSystemMemory = useSystemMemory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOmniOperator.class.getSimpleName());
            HashAggregationOmniOperator hashAggregationOperator = new HashAggregationOmniOperator(
                    omniRuntime,
                    compileID,
                    operatorContext,
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
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);

            return hashAggregationOperator;
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new HashAggregationOmniOperatorFactory(
                    operatorId,
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
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler,
                    useSystemMemory);
        }
    }
}
