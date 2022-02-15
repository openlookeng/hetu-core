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
import io.prestosql.array.LongBigArray;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.SystemSessionProperties.isDictionaryAggregationEnabled;
import static io.prestosql.operator.GroupByHash.createGroupByHash;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"outputChannels", "types", "groupByHash", "inputPage", "maxRowsPerPartition",
        "selectedRowPageBuilder", "unfinishedWork", "snapshotState"})
public class RowNumberOperator
        implements Operator
{
    public static class RowNumberOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Optional<Integer> maxRowsPerPartition;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<Integer> partitionChannels;
        private final List<Type> partitionTypes;
        private final Optional<Integer> hashChannel;
        private final int expectedPositions;
        private boolean closed;
        private final JoinCompiler joinCompiler;

        public RowNumberOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> partitionChannels,
                List<? extends Type> partitionTypes,
                Optional<Integer> maxRowsPerPartition,
                Optional<Integer> hashChannel,
                int expectedPositions,
                JoinCompiler joinCompiler)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(sourceTypes);
            this.outputChannels = ImmutableList.copyOf(requireNonNull(outputChannels, "outputChannels is null"));
            this.partitionChannels = ImmutableList.copyOf(requireNonNull(partitionChannels, "partitionChannels is null"));
            this.partitionTypes = ImmutableList.copyOf(requireNonNull(partitionTypes, "partitionTypes is null"));
            this.maxRowsPerPartition = requireNonNull(maxRowsPerPartition, "maxRowsPerPartition is null");

            this.hashChannel = requireNonNull(hashChannel, "hashChannel is null");
            checkArgument(expectedPositions > 0, "expectedPositions < 0");
            this.expectedPositions = expectedPositions;
            this.joinCompiler = requireNonNull(joinCompiler, "joinCompiler is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");

            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, RowNumberOperator.class.getSimpleName());
            return new RowNumberOperator(
                    addOperatorContext,
                    sourceTypes,
                    outputChannels,
                    partitionChannels,
                    partitionTypes,
                    maxRowsPerPartition,
                    hashChannel,
                    expectedPositions,
                    joinCompiler);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new RowNumberOperatorFactory(operatorId, planNodeId, sourceTypes, outputChannels, partitionChannels, partitionTypes, maxRowsPerPartition, hashChannel, expectedPositions, joinCompiler);
        }
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext localUserMemoryContext;
    private boolean finishing;

    private final int[] outputChannels;
    private final List<Type> types;

    private GroupByIdBlock partitionIds;
    private final Optional<GroupByHash> groupByHash;

    private Page inputPage;
    private final LongBigArray partitionRowCount;

    private final Optional<Integer> maxRowsPerPartition;
    // Only present if maxRowsPerPartition is present
    private final Optional<PageBuilder> selectedRowPageBuilder;

    // for yield when memory is not available
    private Work<GroupByIdBlock> unfinishedWork;

    private final SingleInputSnapshotState snapshotState;

    public RowNumberOperator(
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            List<Integer> outputChannels,
            List<Integer> partitionChannels,
            List<Type> partitionTypes,
            Optional<Integer> maxRowsPerPartition,
            Optional<Integer> hashChannel,
            int expectedPositions,
            JoinCompiler joinCompiler)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.outputChannels = Ints.toArray(outputChannels);
        this.types = toTypes(sourceTypes, outputChannels);

        this.maxRowsPerPartition = maxRowsPerPartition;
        if (maxRowsPerPartition.isPresent()) {
            selectedRowPageBuilder = Optional.of(new PageBuilder(types));
        }
        else {
            selectedRowPageBuilder = Optional.empty();
        }

        this.partitionRowCount = new LongBigArray(0);
        if (partitionChannels.isEmpty()) {
            this.groupByHash = Optional.empty();
        }
        else {
            int[] channels = Ints.toArray(partitionChannels);
            this.groupByHash = Optional.of(createGroupByHash(partitionTypes, channels, hashChannel, expectedPositions, isDictionaryAggregationEnabled(operatorContext.getSession()), joinCompiler, this::updateMemoryReservation));
        }

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

        if (isSinglePartition() && maxRowsPerPartition.isPresent()) {
            if (finishing && !hasUnfinishedInput()) {
                return true;
            }
            return partitionRowCount.get(0) == maxRowsPerPartition.get();
        }

        return finishing && !hasUnfinishedInput();
    }

    @Override
    public boolean needsInput()
    {
        if (isSinglePartition() && maxRowsPerPartition.isPresent()) {
            // Check if single partition is done
            return partitionRowCount.get(0) < maxRowsPerPartition.get() && !finishing && !hasUnfinishedInput();
        }
        return !finishing && !hasUnfinishedInput();
    }

    @Override
    public void addInput(Page page)
    {
        checkState(!finishing, "Operator is already finishing");
        requireNonNull(page, "page is null");
        checkState(!hasUnfinishedInput());

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        inputPage = page;
        if (groupByHash.isPresent()) {
            unfinishedWork = groupByHash.get().getGroupIds(inputPage);
            processUnfinishedWork();
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

        if (unfinishedWork != null && !processUnfinishedWork()) {
            return null;
        }

        if (inputPage == null) {
            return null;
        }

        Page outputPage;
        if (maxRowsPerPartition.isPresent()) {
            outputPage = getSelectedRows();
        }
        else {
            outputPage = getRowsWithRowNumber();
        }

        inputPage = null;
        updateMemoryReservation();
        return outputPage;
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    private boolean hasUnfinishedInput()
    {
        return inputPage != null || unfinishedWork != null;
    }

    /**
     * Update memory usage.
     *
     * @return true if the reservation is within the limit
     */
    // TODO: update in the interface now that the new memory tracking framework is landed
    // Essentially we would love to have clean interfaces to support both pushing and pulling memory usage
    // The following implementation is a hybrid model, where the push model is going to call the pull model causing reentrancy
    private boolean updateMemoryReservation()
    {
        // Operator/driver will be blocked on memory after we call localUserMemoryContext.setBytes().
        // If memory is not available, once we return, this operator will be blocked until memory is available.
        long memorySizeInBytes = groupByHash.map(GroupByHash::getEstimatedSize).orElse(0L) + partitionRowCount.sizeOf();
        localUserMemoryContext.setBytes(memorySizeInBytes);
        // If memory is not available, inform the caller that we cannot proceed for allocation.
        return operatorContext.isWaitingForMemory().isDone();
    }

    private boolean processUnfinishedWork()
    {
        verify(unfinishedWork != null);
        if (!unfinishedWork.process()) {
            return false;
        }
        partitionIds = unfinishedWork.getResult();
        partitionRowCount.ensureCapacity(partitionIds.getGroupCount());
        unfinishedWork = null;
        return true;
    }

    private boolean isSinglePartition()
    {
        return !groupByHash.isPresent();
    }

    private Page getRowsWithRowNumber()
    {
        Block rowNumberBlock = createRowNumberBlock();
        Block[] sourceBlocks = new Block[inputPage.getChannelCount()];
        for (int i = 0; i < outputChannels.length; i++) {
            sourceBlocks[i] = inputPage.getBlock(outputChannels[i]);
        }

        Block[] outputBlocks = Arrays.copyOf(sourceBlocks, sourceBlocks.length + 1); // +1 for the row number column
        outputBlocks[sourceBlocks.length] = rowNumberBlock;

        return new Page(inputPage.getPositionCount(), outputBlocks);
    }

    private Block createRowNumberBlock()
    {
        BlockBuilder rowNumberBlock = BIGINT.createFixedSizeBlockBuilder(inputPage.getPositionCount());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            long partitionId = getPartitionId(currentPosition);
            long nextRowCount = partitionRowCount.get(partitionId) + 1;
            BIGINT.writeLong(rowNumberBlock, nextRowCount);
            partitionRowCount.set(partitionId, nextRowCount);
        }
        return rowNumberBlock.build();
    }

    private Page getSelectedRows()
    {
        verify(selectedRowPageBuilder.isPresent());

        int rowNumberChannel = types.size() - 1;
        PageBuilder pageBuilder = selectedRowPageBuilder.get();
        verify(pageBuilder.isEmpty());
        for (int currentPosition = 0; currentPosition < inputPage.getPositionCount(); currentPosition++) {
            long partitionId = getPartitionId(currentPosition);
            long rowCount = partitionRowCount.get(partitionId);
            if (rowCount == maxRowsPerPartition.get()) {
                continue;
            }
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int channel = outputChannels[i];
                Type type = types.get(i);
                type.appendTo(inputPage.getBlock(channel), currentPosition, pageBuilder.getBlockBuilder(i));
            }
            BIGINT.writeLong(pageBuilder.getBlockBuilder(rowNumberChannel), rowCount + 1);
            partitionRowCount.set(partitionId, rowCount + 1);
        }
        if (pageBuilder.isEmpty()) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private long getPartitionId(int position)
    {
        return isSinglePartition() ? 0 : partitionIds.getGroupId(position);
    }

    private static List<Type> toTypes(List<? extends Type> sourceTypes, List<Integer> outputChannels)
    {
        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();
        for (int channel : outputChannels) {
            typeBuilder.add(sourceTypes.get(channel));
        }
        typeBuilder.add(BIGINT);
        return typeBuilder.build();
    }

    @VisibleForTesting
    public int getCapacity()
    {
        return groupByHash.map(GroupByHash::getCapacity).orElse(0);
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
        RowNumberOperatorState myState = new RowNumberOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.localUserMemoryContext = localUserMemoryContext.getBytes();
        myState.finishing = finishing;
        if (partitionIds != null) {
            myState.partitionIds = partitionIds.capture(serdeProvider);
        }
        myState.partitionRowCount = partitionRowCount.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        RowNumberOperatorState myState = (RowNumberOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.localUserMemoryContext.setBytes(myState.localUserMemoryContext);
        this.finishing = myState.finishing;
        if (myState.partitionIds != null) {
            this.partitionIds.restore(myState.partitionIds, serdeProvider);
        }
        else {
            this.partitionIds = null;
        }
        this.partitionRowCount.restore(myState.partitionRowCount, serdeProvider);
    }

    private static class RowNumberOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long localUserMemoryContext;
        private boolean finishing;
        private Object partitionIds;
        private Object partitionRowCount;
    }
}
