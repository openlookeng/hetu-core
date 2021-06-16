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
package io.prestosql.operator.aggregation.builder;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.operator.GroupBy;
import io.prestosql.operator.GroupByIdBlock;
import io.prestosql.operator.HashCollisionsCounter;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.TransformWork;
import io.prestosql.operator.UpdateMemory;
import io.prestosql.operator.Work;
import io.prestosql.operator.WorkProcessor;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.operator.aggregation.GroupedAccumulator;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;
import static io.prestosql.operator.GroupBySort.createGroupBySort;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"updateMemory"})
public abstract class InMemoryAggregationBuilder
        implements AggregationBuilder, Restorable
{
    protected final GroupBy groupBy;
    protected final List<Aggregator> aggregators;
    protected final boolean partial;
    protected final OptionalLong maxPartialMemory;
    protected final UpdateMemory updateMemory;
    protected boolean full;

    public InMemoryAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory,
            AggregationNode.AggregationType aggregationType)
    {
        this(accumulatorFactories,
                step,
                expectedGroups,
                groupByTypes,
                groupByChannels,
                hashChannel,
                operatorContext,
                maxPartialMemory,
                Optional.empty(),
                joinCompiler,
                updateMemory,
                aggregationType);
    }

    public InMemoryAggregationBuilder(
            List<AccumulatorFactory> accumulatorFactories,
            AggregationNode.Step step,
            int expectedGroups,
            List<Type> groupByTypes,
            List<Integer> groupByChannels,
            Optional<Integer> hashChannel,
            OperatorContext operatorContext,
            Optional<DataSize> maxPartialMemory,
            Optional<Integer> overwriteIntermediateChannelOffset,
            JoinCompiler joinCompiler,
            UpdateMemory updateMemory,
            AggregationNode.AggregationType aggregationType)
    {
        if (aggregationType.equals(AggregationNode.AggregationType.SORT_BASED)) {
            this.groupBy = createGroupBySort(
                    groupByTypes,
                    Ints.toArray(groupByChannels),
                    hashChannel,
                    expectedGroups,
                    isDictionaryAggregationEnabled(operatorContext.getSession()),
                    joinCompiler,
                    updateMemory,
                    step);
        }
        else {
            this.groupBy = createGroupByHash(
                    groupByTypes,
                    Ints.toArray(groupByChannels),
                    hashChannel,
                    expectedGroups,
                    isDictionaryAggregationEnabled(operatorContext.getSession()),
                    joinCompiler,
                    updateMemory);
        }
        this.partial = step.isOutputPartial();
        this.maxPartialMemory = maxPartialMemory.map(dataSize -> OptionalLong.of(dataSize.toBytes())).orElseGet(OptionalLong::empty);
        this.updateMemory = requireNonNull(updateMemory, "updateMemory is null");

        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        for (int i = 0; i < accumulatorFactories.size(); i++) {
            AccumulatorFactory accumulatorFactory = accumulatorFactories.get(i);
            Optional<Integer> overwriteIntermediateChannel = Optional.empty();
            if (overwriteIntermediateChannelOffset.isPresent()) {
                overwriteIntermediateChannel = Optional.of(overwriteIntermediateChannelOffset.get() + i);
            }
            builder.add(new Aggregator(accumulatorFactory, step, overwriteIntermediateChannel));
        }
        aggregators = builder.build();
    }

    @Override
    public void close() {}

    @Override
    public Work<?> processPage(Page page)
    {
        if (aggregators.isEmpty()) {
            return groupBy.addPage(page);
        }
        else {
            return new TransformWork<>(
                    groupBy.getGroupIds(page),
                    groupByIdBlock -> {
                        for (Aggregator aggregator : aggregators) {
                            aggregator.processPage(groupByIdBlock, page);
                        }
                        // we do not need any output from TransformWork for this case
                        return null;
                    });
        }
    }

    @Override
    public void updateMemory()
    {
        updateMemory.update();
    }

    @Override
    public boolean isFull()
    {
        return full;
    }

    @Override
    public void recordHashCollisions(HashCollisionsCounter hashCollisionsCounter)
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for InMemoryAggregationBuilder");
    }

    @Override
    public ListenableFuture<?> startMemoryRevoke()
    {
        throw new UnsupportedOperationException("startMemoryRevoke not supported for InMemoryAggregationBuilder");
    }

    @Override
    public void finishMemoryRevoke()
    {
        throw new UnsupportedOperationException("finishMemoryRevoke not supported for InMemoryAggregationBuilder");
    }

    public long getSizeInMemory()
    {
        long sizeInMemory = groupBy.getEstimatedSize();
        for (Aggregator aggregator : aggregators) {
            sizeInMemory += aggregator.getEstimatedSize();
        }

        updateIsFull(sizeInMemory);
        return sizeInMemory;
    }

    private void updateIsFull(long sizeInMemory)
    {
        if (!partial || !maxPartialMemory.isPresent()) {
            return;
        }

        full = sizeInMemory > maxPartialMemory.getAsLong();
    }

    /**
     * building hash sorted results requires memory for sorting group IDs.
     * This method returns size of that memory requirement.
     */
    public long getGroupIdsSortingSize()
    {
        return getGroupCount() * Integer.BYTES;
    }

    public void setOutputPartial()
    {
        for (Aggregator aggregator : aggregators) {
            aggregator.setOutputPartial();
        }
    }

    public int getKeyChannels()
    {
        return groupBy.getTypes().size();
    }

    public long getGroupCount()
    {
        return groupBy.getGroupCount();
    }

    public List<Type> buildIntermediateTypes()
    {
        ArrayList<Type> types = new ArrayList<>(groupBy.getTypes());
        for (InMemoryHashAggregationBuilder.Aggregator aggregator : aggregators) {
            types.add(aggregator.getIntermediateType());
        }
        return types;
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupBy.getCapacity();
    }

    protected IntIterator consecutiveGroupIds()
    {
        return IntIterators.fromTo(0, groupBy.getGroupCount());
    }

    protected static class Aggregator
            implements Restorable
    {
        private final GroupedAccumulator aggregation;
        private AggregationNode.Step step;
        private final int intermediateChannel;

        protected Aggregator(AccumulatorFactory accumulatorFactory, AggregationNode.Step step, Optional<Integer> overwriteIntermediateChannel)
        {
            if (step.isInputRaw()) {
                this.intermediateChannel = -1;
                this.aggregation = accumulatorFactory.createGroupedAccumulator();
            }
            else if (overwriteIntermediateChannel.isPresent()) {
                this.intermediateChannel = overwriteIntermediateChannel.get();
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            else {
                checkArgument(accumulatorFactory.getInputChannels().size() == 1, "expected 1 input channel for intermediate aggregation");
                this.intermediateChannel = accumulatorFactory.getInputChannels().get(0);
                this.aggregation = accumulatorFactory.createGroupedIntermediateAccumulator();
            }
            this.step = step;
        }

        public long getEstimatedSize()
        {
            return aggregation.getEstimatedSize();
        }

        public Type getType()
        {
            if (step.isOutputPartial()) {
                return aggregation.getIntermediateType();
            }
            else {
                return aggregation.getFinalType();
            }
        }

        public void processPage(GroupByIdBlock groupIds, Page page)
        {
            if (step.isInputRaw()) {
                aggregation.addInput(groupIds, page);
            }
            else {
                aggregation.addIntermediate(groupIds, page.getBlock(intermediateChannel));
            }
        }

        public void prepareFinal()
        {
            aggregation.prepareFinal();
        }

        public void evaluate(int groupId, BlockBuilder output)
        {
            if (step.isOutputPartial()) {
                aggregation.evaluateIntermediate(groupId, output);
            }
            else {
                aggregation.evaluateFinal(groupId, output);
            }
        }

        public void setOutputPartial()
        {
            step = AggregationNode.Step.partialOutput(step);
        }

        public Type getIntermediateType()
        {
            return aggregation.getIntermediateType();
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            AggregatorState myState = new AggregatorState();
            myState.step = step.toString();
            myState.aggregation = aggregation.capture(serdeProvider);
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            AggregatorState myState = (AggregatorState) state;
            step = AggregationNode.Step.valueOf(myState.step);
            aggregation.restore(myState.aggregation, serdeProvider);
        }

        private static class AggregatorState
                implements Serializable
        {
            private String step;
            private Object aggregation;
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        InMemoryAggregationBuilderState myState = new InMemoryAggregationBuilderState();
        myState.groupBy = groupBy.capture(serdeProvider);
        List<Object> aggregators = new ArrayList<>();
        for (Aggregator aggregator : this.aggregators) {
            aggregators.add(aggregator.capture(serdeProvider));
        }
        myState.aggregators = aggregators;
        myState.full = full;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        InMemoryAggregationBuilderState myState = (InMemoryAggregationBuilderState) state;
        this.groupBy.restore(myState.groupBy, serdeProvider);
        for (int i = 0; i < this.aggregators.size(); i++) {
            this.aggregators.get(i).restore(myState.aggregators.get(i), serdeProvider);
        }
        this.full = myState.full;
    }

    @Override
    public WorkProcessor<Page> buildResult()
    {
        throw new UnsupportedOperationException();
    }

    private static class InMemoryAggregationBuilderState
            implements Serializable
    {
        private Object groupBy;
        private List<Object> aggregators;
        private boolean full;
    }
}
