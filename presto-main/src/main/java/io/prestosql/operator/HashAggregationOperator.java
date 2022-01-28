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
import io.airlift.units.DataSize;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.operator.aggregation.builder.SpillableHashAggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;

public class HashAggregationOperator
        extends GroupAggregationOperator
{
    public static class HashAggregationOperatorFactory
            extends GroupAggregationOperatorFactory
    {
        @VisibleForTesting
        public HashAggregationOperatorFactory(
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

        public HashAggregationOperatorFactory(
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
        HashAggregationOperatorFactory(
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
            super(
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

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, HashAggregationOperator.class.getSimpleName());
            HashAggregationOperator hashAggregationOperator = new HashAggregationOperator(
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
            return new HashAggregationOperatorFactory(
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

    public HashAggregationOperator(
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
        super(operatorContext,
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

        this.hashCollisionsCounter = new HashCollisionsCounter(operatorContext);
        operatorContext.setInfoSupplier(hashCollisionsCounter);
    }

    @Override
    public void finish()
    {
        finishing = true;
    }

    @Override
    public void addInput(Page page)
    {
        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        checkState(unfinishedWork == null, "Operator has unfinished work");
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        inputProcessed = true;

        if (aggregationBuilder == null) {
            createAggregationBuilder();
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        unfinishedWork = aggregationBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        aggregationBuilder.updateMemory();
    }

    @Override
    public void createAggregationBuilder()
    {
        // TODO: We ignore spillEnabled here if any aggregate has ORDER BY clause or DISTINCT because they are not yet implemented for spilling.
        if (step.isOutputPartial() || !spillEnabled || hasOrderBy() || hasDistinct()) {
            //TODO-cp-I39B76 snapshot support
            aggregationBuilder = new InMemoryHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext,
                    maxPartialMemory,
                    joinCompiler,
                    () -> {
                        memoryContext.setBytes(((InMemoryHashAggregationBuilder) aggregationBuilder).getSizeInMemory());
                        if (step.isOutputPartial() && maxPartialMemory.isPresent()) {
                            // do not yield on memory for partial aggregations
                            return true;
                        }
                        return operatorContext.isWaitingForMemory().isDone();
                    });
        }
        else {
            verify(!useSystemMemory, "using system memory in spillable aggregations is not supported");
            aggregationBuilder = new SpillableHashAggregationBuilder(
                    accumulatorFactories,
                    step,
                    expectedGroups,
                    groupByTypes,
                    groupByChannels,
                    hashChannel,
                    operatorContext,
                    memoryLimitForMerge,
                    memoryLimitForMergeWithMemory,
                    spillerFactory,
                    joinCompiler);
        }

        // assume initial aggregationBuilder is not full
    }

    @Override
    public Page getOutput()
    {
        if (snapshotState != null) {
            Page marker = snapshotState.nextMarker();
            if (marker != null) {
                return marker;
            }
        }

        if (finished) {
            return null;
        }

        // process unfinished work if one exists
        if (unfinishedWork != null) {
            boolean workDone = unfinishedWork.process();
            aggregationBuilder.updateMemory();
            if (!workDone) {
                return null;
            }
            unfinishedWork = null;
        }

        if (outputPages == null) {
            if (finishing) {
                if (!inputProcessed && produceDefaultOutput) {
                    // global aggregations always generate an output row with the default aggregation output (e.g. 0 for COUNT, NULL for SUM)
                    finished = true;
                    return getGlobalAggregationOutput();
                }

                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            // only flush if we are finishing or the aggregation builder is full
            if (!finishing && (aggregationBuilder == null || !aggregationBuilder.isFull())) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult();
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            return null;
        }

        return outputPages.getResult();
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
        if (snapshotState != null) {
            snapshotState.close();
        }
    }

    protected void closeAggregationBuilder()
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
}
