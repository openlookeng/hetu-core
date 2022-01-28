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
import com.google.common.primitives.Ints;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.aggregation.AccumulatorFactory;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.plan.AggregationNode.Step;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.io.Serializable;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"groupByTypes", "groupByChannels", "accumulatorFactories", "pagesHashStrategy", "outputPages", "snapshotState"})
public class StreamingAggregationOperator
        implements Operator
{
    public static class StreamingAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Type> groupByTypes;
        private final List<Integer> groupByChannels;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final JoinCompiler joinCompiler;
        private boolean closed;

        public StreamingAggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, List<Type> sourceTypes, List<Type> groupByTypes, List<Integer> groupByChannels, Step step, List<AccumulatorFactory> accumulatorFactories, JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
            this.groupByChannels = ImmutableList.copyOf(requireNonNull(groupByChannels, "groupByChannels is null"));
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(requireNonNull(accumulatorFactories, "accumulatorFactories is null"));
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, StreamingAggregationOperator.class.getSimpleName());
            return new StreamingAggregationOperator(addOperatorContext, sourceTypes, groupByTypes, groupByChannels, step, accumulatorFactories, joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new StreamingAggregationOperatorFactory(operatorId, planNodeId, sourceTypes, groupByTypes, groupByChannels, step, accumulatorFactories, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext systemMemoryContext;
    private final LocalMemoryContext userMemoryContext;
    private final List<Type> groupByTypes;
    private final int[] groupByChannels;
    private final List<AccumulatorFactory> accumulatorFactories;
    private final Step step;
    private final PagesHashStrategy pagesHashStrategy;

    private List<Aggregator> aggregates;
    private final PageBuilder pageBuilder;
    private final Deque<Page> outputPages = new LinkedList<>();
    private Page currentGroup;
    private boolean finishing;

    private final SingleInputSnapshotState snapshotState;

    public StreamingAggregationOperator(OperatorContext operatorContext, List<Type> sourceTypes, List<Type> groupByTypes, List<Integer> groupByChannels, Step step, List<AccumulatorFactory> accumulatorFactories, JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(StreamingAggregationOperator.class.getSimpleName());
        this.userMemoryContext = operatorContext.localUserMemoryContext();
        this.groupByTypes = ImmutableList.copyOf(requireNonNull(groupByTypes, "groupByTypes is null"));
        this.groupByChannels = Ints.toArray(requireNonNull(groupByChannels, "groupByChannels is null"));
        this.accumulatorFactories = requireNonNull(accumulatorFactories, "accumulatorFactories is null");
        this.step = requireNonNull(step, "step is null");

        this.aggregates = setupAggregates(step, accumulatorFactories);
        this.pageBuilder = new PageBuilder(toTypes(groupByTypes, aggregates));
        requireNonNull(joinCompiler, "joinCompiler is null");

        requireNonNull(sourceTypes, "sourceTypes is null");
        pagesHashStrategy = joinCompiler.compilePagesHashStrategyFactory(sourceTypes, groupByChannels, Optional.empty())
                .createPagesHashStrategy(
                        sourceTypes.stream()
                                .map(type -> ImmutableList.<Block>of())
                                .collect(toImmutableList()), OptionalInt.empty());
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
    }

    private List<Aggregator> setupAggregates(Step step, List<AccumulatorFactory> accumulatorFactories)
    {
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AccumulatorFactory factory : accumulatorFactories) {
            builder.add(new Aggregator(factory, step));
        }
        return builder.build();
    }

    private static List<Type> toTypes(List<Type> groupByTypes, List<Aggregator> aggregates)
    {
        ImmutableList.Builder<Type> builder = ImmutableList.builder();
        builder.addAll(groupByTypes);
        aggregates.stream()
                .map(Aggregator::getType)
                .forEach(builder::add);
        return builder.build();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finishing && outputPages.isEmpty();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        processInput(page);
        updateMemoryUsage();
    }

    private void updateMemoryUsage()
    {
        long memorySize = pageBuilder.getRetainedSizeInBytes();
        for (Page output : outputPages) {
            memorySize += output.getRetainedSizeInBytes();
        }
        for (Aggregator aggregator : aggregates) {
            memorySize += aggregator.getEstimatedSize();
        }

        if (currentGroup != null) {
            memorySize += currentGroup.getRetainedSizeInBytes();
        }

        if (step.isOutputPartial()) {
            systemMemoryContext.setBytes(memorySize);
        }
        else {
            userMemoryContext.setBytes(memorySize);
        }
    }

    private void processInput(Page page)
    {
        requireNonNull(page, "page is null");

        Page groupByPage = extractColumns(page, groupByChannels);
        if (currentGroup != null) {
            if (!pagesHashStrategy.rowEqualsRow(0, extractColumns(currentGroup, groupByChannels), 0, groupByPage)) {
                // page starts with new group, so flush it
                evaluateAndFlushGroup(currentGroup, 0);
            }
            currentGroup = null;
        }

        int startPosition = 0;
        while (true) {
            // may be equal to page.getPositionCount() if the end is not found in this page
            int nextGroupStart = findNextGroupStart(startPosition, groupByPage);
            addRowsToAggregates(page, startPosition, nextGroupStart - 1);

            if (nextGroupStart < page.getPositionCount()) {
                // current group stops somewhere in the middle of the page, so flush it
                evaluateAndFlushGroup(page, startPosition);
                startPosition = nextGroupStart;
            }
            else {
                currentGroup = page.getRegion(page.getPositionCount() - 1, 1);
                return;
            }
        }
    }

    private static Page extractColumns(Page page, int[] channels)
    {
        Block[] newBlocks = new Block[channels.length];
        for (int i = 0; i < channels.length; i++) {
            newBlocks[i] = page.getBlock(channels[i]);
        }
        return new Page(page.getPositionCount(), newBlocks);
    }

    private void addRowsToAggregates(Page page, int startPosition, int endPosition)
    {
        for (Aggregator aggregator : aggregates) {
            aggregator.processPage(page.getRegion(startPosition, endPosition - startPosition + 1));
        }
    }

    private void evaluateAndFlushGroup(Page page, int position)
    {
        pageBuilder.declarePosition();
        for (int i = 0; i < groupByTypes.size(); i++) {
            Block block = page.getBlock(groupByChannels[i]);
            Type type = groupByTypes.get(i);
            type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
        }
        int offset = groupByTypes.size();
        for (int i = 0; i < aggregates.size(); i++) {
            aggregates.get(i).evaluate(pageBuilder.getBlockBuilder(offset + i));
        }

        if (pageBuilder.isFull()) {
            outputPages.add(pageBuilder.build());
            pageBuilder.reset();
        }

        aggregates = setupAggregates(step, accumulatorFactories);
    }

    private int findNextGroupStart(int startPosition, Page page)
    {
        for (int i = startPosition + 1; i < page.getPositionCount(); i++) {
            if (!pagesHashStrategy.rowEqualsRow(startPosition, page, i, page)) {
                return i;
            }
        }

        return page.getPositionCount();
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

        if (!outputPages.isEmpty()) {
            return outputPages.removeFirst();
        }

        return null;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void finish()
    {
        finishing = true;

        if (currentGroup != null) {
            evaluateAndFlushGroup(currentGroup, 0);
            currentGroup = null;
        }

        if (!pageBuilder.isEmpty()) {
            outputPages.add(pageBuilder.build());
            pageBuilder.reset();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return finishing && outputPages.isEmpty() && currentGroup == null && pageBuilder.isEmpty();
    }

    @Override
    public void close()
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        StreamingAggregationOperatorState myState = new StreamingAggregationOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.systemMemoryContext = systemMemoryContext.getBytes();
        myState.userMemoryContext = userMemoryContext.getBytes();
        myState.aggregates = new Object[aggregates.size()];
        for (int i = 0; i < aggregates.size(); i++) {
            myState.aggregates[i] = aggregates.get(i).capture(serdeProvider);
        }
        myState.pageBuilder = pageBuilder.capture(serdeProvider);
        if (currentGroup != null) {
            SerializedPage serializedPage = ((PagesSerde) serdeProvider).serialize(currentGroup);
            myState.currentGroup = serializedPage.capture(serdeProvider);
        }
        myState.finishing = finishing;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        StreamingAggregationOperatorState myState = (StreamingAggregationOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.systemMemoryContext.setBytes(myState.systemMemoryContext);
        this.userMemoryContext.setBytes(myState.userMemoryContext);
        for (int i = 0; i < aggregates.size(); i++) {
            aggregates.get(i).restore(myState.aggregates[i], serdeProvider);
        }
        this.pageBuilder.restore(myState.pageBuilder, serdeProvider);
        this.currentGroup = null;
        if (myState.currentGroup != null) {
            SerializedPage serializedPage = SerializedPage.restoreSerializedPage(myState.currentGroup);
            this.currentGroup = ((PagesSerde) serdeProvider).deserialize(serializedPage);
        }
        this.finishing = myState.finishing;
    }

    private static class StreamingAggregationOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long systemMemoryContext;
        private long userMemoryContext;
        private Object[] aggregates;
        private Object pageBuilder;
        private Object currentGroup;
        private boolean finishing;
    }
}
