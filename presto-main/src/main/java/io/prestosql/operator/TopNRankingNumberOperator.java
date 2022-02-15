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
import com.google.common.primitives.Ints;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.window.RankingFunction;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"outputChannels", "unfinishedWork", "outputIterator", "rankingFunction", "snapshotState"})
public class TopNRankingNumberOperator
        implements Operator
{
    public static class TopNRankingNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;

        private final List<Type> sourceTypes;

        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final int maxRowCountPerPartition;
        private final boolean partial;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;

        private final boolean generateRankingNumber;
        private boolean closed;
        private final JoinCompiler joinCompiler;
        private final Optional<RankingFunction> rankingFunction;

        public TopNRankingNumberOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder,
                int maxRowCountPerPartition,
                boolean partial,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler,
                Optional<RankingFunction> rankingFunction)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder));
            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            this.partial = partial;
            checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");
            this.maxRowCountPerPartition = maxRowCountPerPartition;
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            this.generateRankingNumber = !partial;
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");

            this.rankingFunction = requireNonNull(rankingFunction, "rankingFunction is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TopNRankingNumberOperator.class.getSimpleName());
            return new TopNRankingNumberOperator(
                    addOperatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    sortChannels,
                    sortOrder,
                    maxRowCountPerPartition,
                    generateRankingNumber,
                    hashChannel,
                    expectedPositions,
                    joinCompiler,
                    rankingFunction);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public TopNRankingNumberOperatorFactory duplicate()
        {
            return new TopNRankingNumberOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, partitionChannels, partitionTypes, sortChannels, sortOrder, maxRowCountPerPartition, partial, hashChannel, expectedPositions, joinCompiler, rankingFunction);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;

    private final List<Integer> outputChannels;

    private final GroupByHash groupByHash;
    private final GroupedTopNBuilder groupedTopNBuilder;

    private boolean finishing;
    private Work<?> unfinishedWork;
    private Iterator<Page> outputIterator;
    private Optional<RankingFunction> rankingFunction;

    private final SingleInputSnapshotState snapshotState;

    public TopNRankingNumberOperator(
            OperatorContext operatorContext,
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            List<Integer> sortChannels,
            List<SortOrder> sortOrders,
            int maxRowCountPerPartition,
            boolean generateRankingNumber,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler,
            Optional<RankingFunction> rankingFunction)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();

        ImmutableList.Builder<Integer> outputChannelsBuilder = ImmutableList.builder();
        for (int channel : requireNonNull(outputChannels, "outputChannels is null")) {
            outputChannelsBuilder.add(channel);
        }
        if (generateRankingNumber) {
            outputChannelsBuilder.add(outputChannels.size());
        }
        this.outputChannels = outputChannelsBuilder.build();

        checkArgument(maxRowCountPerPartition > 0, "maxRowCountPerPartition must be > 0");

        if (!partitionChannels.isEmpty()) {
            checkArgument(expectedPositions > 0, "expectedPositions must be > 0");
            groupByHash = createGroupByHash(
                    partitionTypes,
                    Ints.toArray(partitionChannels),
                    hashChannel,
                    expectedPositions,
                    isDictionaryAggregationEnabled(operatorContext.getSession()),
                    joinCompiler,
                    this::updateMemoryReservation);
        }
        else {
            groupByHash = new NoChannelGroupByHash();
        }

        List<Type> types = toTypes(sourceTypes, outputChannels, generateRankingNumber);
        this.groupedTopNBuilder = new GroupedTopNBuilder(
                ImmutableList.copyOf(sourceTypes),
                new SimplePageWithPositionComparator(types, sortChannels, sortOrders),
                maxRowCountPerPartition,
                generateRankingNumber,
                rankingFunction,
                groupByHash);

        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
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
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        // has no more input, has finished flushing, and has no unfinished work
        return finishing && outputIterator != null && !outputIterator.hasNext() && unfinishedWork == null;
    }

    @Override
    public boolean needsInput()
    {
        // still has more input, has not started flushing yet, and has no unfinished work
        return !finishing && outputIterator == null && unfinishedWork == null;
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        checkState(unfinishedWork == null, "Cannot add input with the operator when unfinished work is not empty");
        checkState(outputIterator == null, "Cannot add input with the operator when flushing");
        requireNonNull(page, "page is null");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        unfinishedWork = groupedTopNBuilder.processPage(page);
        if (unfinishedWork.process()) {
            unfinishedWork = null;
        }
        updateMemoryReservation();
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

        if (unfinishedWork != null) {
            boolean finished = unfinishedWork.process();
            updateMemoryReservation();
            if (!finished) {
                return null;
            }
            unfinishedWork = null;
        }

        if (!finishing) {
            return null;
        }

        if (outputIterator == null) {
            // start flushing
            outputIterator = groupedTopNBuilder.buildResult();
        }

        Page output = null;
        if (outputIterator.hasNext()) {
            Page page = outputIterator.next();
            // rewrite to expected column ordering
            Block[] blocks = new Block[page.getChannelCount()];
            for (int i = 0; i < outputChannels.size(); i++) {
                blocks[i] = page.getBlock(outputChannels.get(i));
            }
            output = new Page(blocks);
        }
        updateMemoryReservation();
        return output;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        checkState(groupByHash != null);
        return groupByHash.getCapacity();
    }

    private boolean updateMemoryReservation()
    {
        // TODO: may need to use trySetMemoryReservation with a compaction to free memory (but that may cause GC pressure)
        localUserMemoryContext.setBytes(groupedTopNBuilder.getEstimatedSizeInBytes());
        return operatorContext.isWaitingForMemory().isDone();
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels, boolean generateRowNumber)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int channel : outputChannels) {
            types.add(sourceTypes.get(channel));
        }
        if (generateRowNumber) {
            types.add(BIGINT);
        }
        return types.build();
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
        TopNRankingNumberOperatorState myState = new TopNRankingNumberOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.localUserMemoryContext = localUserMemoryContext.getBytes();
        myState.groupByHash = groupByHash.capture(serdeProvider);
        myState.groupedTopNBuilder = groupedTopNBuilder.capture(serdeProvider);
        myState.finishing = finishing;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        TopNRankingNumberOperatorState myState = (TopNRankingNumberOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.localUserMemoryContext.setBytes(myState.localUserMemoryContext);
        this.groupByHash.restore(myState.groupByHash, serdeProvider);
        this.groupedTopNBuilder.restore(myState.groupedTopNBuilder, serdeProvider);
        this.finishing = myState.finishing;
    }

    private static class TopNRankingNumberOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long localUserMemoryContext;
        private Object groupByHash;
        private Object groupedTopNBuilder;
        private boolean finishing;
    }
}
