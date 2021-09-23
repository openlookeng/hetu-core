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

import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.builder.AggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import io.prestosql.operator.aggregation.builder.InMemorySortAggregationBuilder;
import io.prestosql.operator.aggregation.builder.SpillableHashAggregationBuilder;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.SpillerFactory;
import io.prestosql.sql.gen.JoinCompiler;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"sortedPageFinished", "aggregationBuilderType"})
public class SortAggregationOperator
        extends GroupAggregationOperator
{
    private boolean sortedPageFinished;
    private AggregationNode.AggregationType aggregationBuilderType;

    public static class SortAggregationOperatorFactory
            extends GroupAggregationOperatorFactory
    {
        private boolean isFinalizedValuePresent;

        public SortAggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, List<? extends Type> groupByTypes,
                                              List<Integer> groupByChannels, List<Integer> globalAggregationGroupIds,
                                              AggregationNode.Step step, boolean produceDefaultOutput,
                                              List<AccumulatorFactory> accumulatorFactories, Optional<Integer> hashChannel,
                                              Optional<Integer> groupIdChannel, int expectedGroups,
                                              Optional<DataSize> maxPartialMemory, boolean spillEnabled,
                                              DataSize unspillMemoryLimit, SpillerFactory spillerFactory,
                                              JoinCompiler joinCompiler, boolean useSystemMemory, boolean isFinalizedValuePresent)
        {
            super(operatorId, planNodeId, groupByTypes, groupByChannels, globalAggregationGroupIds, step, produceDefaultOutput,
                    accumulatorFactories, hashChannel, groupIdChannel, expectedGroups, maxPartialMemory, spillEnabled,
                    unspillMemoryLimit, spillerFactory, joinCompiler, useSystemMemory);
            this.isFinalizedValuePresent = isFinalizedValuePresent;
        }

        SortAggregationOperatorFactory(
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
                boolean isFinalizedValuePresent)
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
            this.isFinalizedValuePresent = isFinalizedValuePresent;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, SortAggregationOperator.class.getSimpleName());
            SortAggregationOperator sortAggregationOperator = new SortAggregationOperator(
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
                    useSystemMemory,
                    isFinalizedValuePresent);
            return sortAggregationOperator;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new SortAggregationOperatorFactory(
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
                    useSystemMemory,
                    isFinalizedValuePresent);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    private static final Logger LOG = Logger.get(SortAggregationOperator.class);
    private boolean isFinalizedValuePresent;

    public SortAggregationOperator(OperatorContext operatorContext, List<Type> groupByTypes,
                                   List<Integer> groupByChannels, List<Integer> globalAggregationGroupIds,
                                   AggregationNode.Step step, boolean produceDefaultOutput,
                                   List<AccumulatorFactory> accumulatorFactories, Optional<Integer> hashChannel,
                                   Optional<Integer> groupIdChannel, int expectedGroups,
                                   Optional<DataSize> maxPartialMemory, boolean spillEnabled,
                                   DataSize memoryLimitForMerge, DataSize memoryLimitForMergeWithMemory,
                                   SpillerFactory spillerFactory, JoinCompiler joinCompiler, boolean useSystemMemory,
                                   boolean isFinalizedValuePresent)
    {
        super(operatorContext, groupByTypes, groupByChannels, globalAggregationGroupIds, step, produceDefaultOutput,
                accumulatorFactories, hashChannel, groupIdChannel, expectedGroups, maxPartialMemory, spillEnabled,
                memoryLimitForMerge, memoryLimitForMergeWithMemory, spillerFactory, joinCompiler, useSystemMemory);
        this.isFinalizedValuePresent = isFinalizedValuePresent;
    }

    public AggregationBuilder createAggregationBuilder(Page page, boolean spillEnabled)
    {
        AggregationNode.AggregationType aggregationType;
        if (step.equals(AggregationNode.Step.FINAL)) {
            if (BOOLEAN.getBoolean(page.getBlock(page.getChannelCount() - pageFinalizeLocation), 0)) {
                // page contains finalized values show go to sort aggregation
                aggregationType = AggregationNode.AggregationType.SORT_BASED;
            }
            else {
                //corner values we should use hash aggregation
                aggregationType = AggregationNode.AggregationType.HASH;
            }
        }
        else {
            // For partial Aggregation only sort based one is chosen
            aggregationType = AggregationNode.AggregationType.SORT_BASED;
        }

        if (aggregationType.equals(AggregationNode.AggregationType.SORT_BASED)) {
            //sort aggregation is done for finalized values
            this.aggregationBuilderType = AggregationNode.AggregationType.SORT_BASED;
            return new InMemorySortAggregationBuilder(
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
                        memoryContext.setBytes(((InMemorySortAggregationBuilder) aggregationBuilder).getSizeInMemory());
                        if (step.isOutputPartial() && maxPartialMemory.isPresent()) {
                            // do not yield on memory for partial aggregations
                            return true;
                        }
                        return operatorContext.isWaitingForMemory().isDone();
                    });
        }
        else if (!spillEnabled) {
            //hash aggregation is done for not finalized values
            this.aggregationBuilderType = AggregationNode.AggregationType.HASH;
            return new InMemoryHashAggregationBuilder(
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
            this.aggregationBuilderType = AggregationNode.AggregationType.HASH;
            return new SpillableHashAggregationBuilder(
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
    }

    @Override
    public void addInput(Page page)
    {
        checkState(unfinishedWork == null, "Operator has unfinished work");
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        inputProcessed = true;

        if (aggregationBuilder == null) {
            // TODO: We ignore spillEnabled here if any aggregate has ORDER BY clause or DISTINCT because they are not yet implemented for spilling.
            if (step.isOutputPartial() || !spillEnabled || hasOrderBy() || hasDistinct()) {
                aggregationBuilder = createAggregationBuilder(page, false);
            }
            else {
                aggregationBuilder = createAggregationBuilder(page, true);
            }
        }
        else {
            checkState(!aggregationBuilder.isFull(), "Aggregation buffer is full");
        }

        // process the current page; save the unfinished work if we are waiting for memory
        unfinishedWork = aggregationBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        if (step.equals(AggregationNode.Step.FINAL)) {
            if (BOOLEAN.getBoolean(page.getBlock(page.getChannelCount() - pageFinalizeLocation), 0)) {
                sortedPageFinished = true;
            }
        }

        aggregationBuilder.updateMemory();
    }

    @Override
    public Page getOutput()
    {
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
                if (aggregationBuilder == null) {
                    finished = true;
                    return null;
                }
            }

            if (aggregationBuilder == null) {
                return null;
            }

            /* When step is FINAL all finalized values will be handled by InMemorySortAggregationBuilder so need to wait till
            memory is filled or all pages are received, it will yield output immediately.
            But values that are not finalized received to InMemoryHashAggregationBuilder yield only after memory full or when all pages received */
            if (step.equals(AggregationNode.Step.FINAL) && (!(aggregationBuilderType.equals(AggregationNode.AggregationType.SORT_BASED)) &&
                            !finishing && !aggregationBuilder.isFull())) {
                return null;
            }

            // only flush if we are finishing or the aggregation builder is full
            if (step.equals(AggregationNode.Step.PARTIAL) && !finishing && !aggregationBuilder.isFull()) {
                return null;
            }

            outputPages = aggregationBuilder.buildResult(step, isFinalizedValuePresent);
        }

        if (!outputPages.process()) {
            return null;
        }

        if (outputPages.isFinished()) {
            closeAggregationBuilder();
            return null;
        }

        Page page = outputPages.getResult();

        if (step.equals(AggregationNode.Step.FINAL) && sortedPageFinished == true && finishing == false) {
            closeAggregationBuilder();
        }

        return page;
    }

    @Override
    public void close()
    {
        closeAggregationBuilder();
    }

    protected void closeAggregationBuilder()
    {
        outputPages = null;
        if (aggregationBuilder != null) {
            if (!aggregationBuilderType.equals(AggregationNode.AggregationType.SORT_BASED)) {
                aggregationBuilder.recordHashCollisions(hashCollisionsCounter);
            }

            aggregationBuilder.close();
            // aggregationBuilder.close() will release all memory reserved in memory accounting.
            // The reference must be set to null afterwards to avoid unaccounted memory.
            aggregationBuilder = null;
        }
        memoryContext.setBytes(0);
    }

    @Override
    public void finish()
    {
        finishing = true;
        sortedPageFinished = true;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        SortAggregationOperatorState myState = new SortAggregationOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        if (aggregationBuilder != null) {
            myState.aggregationBuilder = aggregationBuilder.capture(serdeProvider);
        }
        myState.memoryContext = memoryContext.getBytes();
        myState.inputProcessed = inputProcessed;
        myState.finishing = finishing;
        myState.finished = finished;
        myState.isFinalizedValuePresent = isFinalizedValuePresent;
        myState.baseState = super.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        SortAggregationOperatorState myState = (SortAggregationOperatorState) state;
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
        isFinalizedValuePresent = myState.isFinalizedValuePresent;
        super.restore(myState.baseState, serdeProvider);
    }

    private static class SortAggregationOperatorState
            extends GroupAggregationOperatorState
    {
        private Object baseState;
        private Object operatorContext;
        private Object aggregationBuilder;
        private long memoryContext;
        private boolean inputProcessed;
        private boolean finishing;
        private boolean finished;
        private boolean isFinalizedValuePresent;
    }
}
