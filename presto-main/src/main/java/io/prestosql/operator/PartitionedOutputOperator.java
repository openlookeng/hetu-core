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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.util.Mergeable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"pagePreprocessor", "snapshotState"})
public class PartitionedOutputOperator
        implements SinkOperator
{
    public static class PartitionedOutputFactory
            implements OutputFactory
    {
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final OutputBuffer outputBuffer;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final DataSize maxMemory;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
        }

        @Override
        public OperatorFactory createOutputOperator(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> types,
                Function<Page, Page> pagePreprocessor,
                TaskContext taskContext)
        {
            outputBuffer.setTaskContext(taskContext);
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    types,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    maxMemory);
        }
    }

    public static class PartitionedOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final Function<Page, Page> pagePreprocessor;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<NullableValue>> partitionConstants;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel;
        private final OutputBuffer outputBuffer;
        private final DataSize maxMemory;
        // Snapshot: When a factory is duplicated, factory instances share the same OutputBuffer.
        // All these factory instances now share this duplicateCount, so only the last factory that receives "noMoreOperators"
        // (the one that decrements the count to 0) should inform OutputBuffer about "setNoMoreInputChannels".
        private final AtomicInteger duplicateCount;

        public PartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory)
        {
            this(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    maxMemory,
                    new AtomicInteger(1));
        }

        private PartitionedOutputOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<Type> sourceTypes,
                Function<Page, Page> pagePreprocessor,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory,
                AtomicInteger duplicateCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.duplicateCount = requireNonNull(duplicateCount, "duplicateCount is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, PartitionedOutputOperator.class.getSimpleName());
            String id = addOperatorContext.getUniqueId();
            outputBuffer.addInputChannel(id);
            return new PartitionedOutputOperator(
                    id,
                    addOperatorContext,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    maxMemory);
        }

        @Override
        public void noMoreOperators()
        {
            if (duplicateCount.decrementAndGet() == 0) {
                outputBuffer.setNoMoreInputChannels();
            }
        }

        @Override
        public OperatorFactory duplicate()
        {
            checkState(duplicateCount.get() > 0);
            duplicateCount.incrementAndGet();
            return new PartitionedOutputOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    pagePreprocessor,
                    partitionFunction,
                    partitionChannels,
                    partitionConstants,
                    replicatesAnyRow,
                    nullChannel,
                    outputBuffer,
                    maxMemory,
                    duplicateCount);
        }
    }

    private final OperatorContext operatorContext;
    private final Function<Page, Page> pagePreprocessor;
    private final PagePartitioner partitionFunction;
    private final LocalMemoryContext systemMemoryContext;
    private final long partitionsInitialRetainedSize;
    private boolean finished;
    private final SingleInputSnapshotState snapshotState;
    private final boolean isStage0;

    public PartitionedOutputOperator(
            String id,
            OperatorContext operatorContext,
            List<Type> sourceTypes,
            Function<Page, Page> pagePreprocessor,
            PartitionFunction partitionFunction,
            List<Integer> partitionChannels,
            List<Optional<NullableValue>> partitionConstants,
            boolean replicatesAnyRow,
            OptionalInt nullChannel,
            OutputBuffer outputBuffer,
            DataSize maxMemory)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.partitionFunction = new PagePartitioner(
                id,
                partitionFunction,
                partitionChannels,
                partitionConstants,
                replicatesAnyRow,
                nullChannel,
                outputBuffer,
                operatorContext.getDriverContext().getSerde(),
                sourceTypes,
                maxMemory);

        operatorContext.setInfoSupplier(this::getInfo);
        this.systemMemoryContext = operatorContext.newLocalSystemMemoryContext(PartitionedOutputOperator.class.getSimpleName());
        this.partitionsInitialRetainedSize = this.partitionFunction.getRetainedSizeInBytes();
        this.systemMemoryContext.setBytes(partitionsInitialRetainedSize);
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.isStage0 = operatorContext.getDriverContext().getPipelineContext().getTaskContext().getTaskId().getStageId().getId() == 0;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public PartitionedOutputInfo getInfo()
    {
        return partitionFunction.getInfo();
    }

    @Override
    public void finish()
    {
        finished = true;
        partitionFunction.flush(true);
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = partitionFunction.isFull();
        return blocked.isDone() ? NOT_BLOCKED : blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");

        if (page.getPositionCount() == 0) {
            return;
        }

        Page inputPage = page;
        if (inputPage instanceof MarkerPage) {
            // Send out all pending pages, then broadcast the marker. This must be done BEFORE snapshotState.processPage(),
            // otherwise what's in PageBuilders are captured, and their content will be sent AGAIN after resume.
            partitionFunction.flush(true);

            snapshotState.processPage(inputPage);
            MarkerPage marker = snapshotState.nextMarker();

            if (isStage0) {
                // Do not add marker to final output.
                return;
            }

            // Snapshot: driver/thread 1 reaches here and adds marker 1 to the output buffer.
            // It's the first time marker 1 is received, so marker 1 will be broadcasted to all client buffers.
            // Then driver/thread 2 adds markers 1 and 2 to the output buffer.
            // Marker 1 was already seen, so it's not sent to client buffers, but marker 2 is seen the first time, and is sent to client buffers.
            // Without the following synchronization, it's possible for the 2 threads to interact with client buffers at the same time.
            // That is, marker 1 is added to client buffer #1, then thread 2 takes over, and adds marker 2 to client buffer #1 and #2.
            // The result is that for buffer #2, it receives marker 2 before marker 1.
            synchronized (partitionFunction.outputBuffer) {
                partitionFunction.outputBuffer.enqueue(0, Collections.singletonList(SerializedPage.forMarker(marker)), partitionFunction.id);
            }
        }
        else {
            inputPage = pagePreprocessor.apply(inputPage);
            partitionFunction.partitionPage(inputPage);
        }

        operatorContext.recordOutput(inputPage.getSizeInBytes(), inputPage.getPositionCount());

        // We use getSizeInBytes() here instead of getRetainedSizeInBytes() for an approximation of
        // the amount of memory used by the pageBuilders, because calculating the retained
        // size can be expensive especially for complex types.
        long partitionsSizeInBytes = partitionFunction.getSizeInBytes();

        // We also add partitionsInitialRetainedSize as an approximation of the object overhead of the partitions.
        systemMemoryContext.setBytes(partitionsSizeInBytes + partitionsInitialRetainedSize);
    }

    @RestorableConfig(stateClassName = "PagePartitionerState", uncapturedFields = {"outputBuffer", "sourceTypes", "partitionFunction", "partitionChannels",
            "partitionConstants", "serde", "pageBuilders"})
    private static class PagePartitioner
            implements Restorable
    {
        private final String id;
        //shared field
        private final OutputBuffer outputBuffer;
        private final List<Type> sourceTypes;
        private final PartitionFunction partitionFunction;
        private final List<Integer> partitionChannels;
        private final List<Optional<Block>> partitionConstants;
        private final PagesSerde serde;
        private final PageBuilder[] pageBuilders;
        private final boolean replicatesAnyRow;
        private final OptionalInt nullChannel; // when present, send the position to every partition if this channel is null.
        private final AtomicLong rowsAdded = new AtomicLong();
        private final AtomicLong pagesAdded = new AtomicLong();
        private boolean hasAnyRowBeenReplicated;

        public PagePartitioner(
                String id,
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                PagesSerde serde,
                List<Type> sourceTypes,
                DataSize maxMemory)
        {
            this.id = id;
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null").stream()
                    .map(constant -> constant.map(NullableValue::asBlock))
                    .collect(toImmutableList());
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.sourceTypes = requireNonNull(sourceTypes, "sourceTypes is null");
            this.serde = requireNonNull(serde, "serde is null");

            int partitionCount = partitionFunction.getPartitionCount();
            int pageSize = min(DEFAULT_MAX_PAGE_SIZE_IN_BYTES, ((int) maxMemory.toBytes()) / partitionCount);
            pageSize = max(1, pageSize);

            this.pageBuilders = new PageBuilder[partitionCount];
            for (int i = 0; i < partitionCount; i++) {
                pageBuilders[i] = PageBuilder.withMaxPageSize(pageSize, sourceTypes);
            }
        }

        public ListenableFuture<?> isFull()
        {
            return outputBuffer.isFull();
        }

        public long getSizeInBytes()
        {
            // We use a foreach loop instead of streams
            // as it has much better performance.
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getSizeInBytes();
            }
            return sizeInBytes;
        }

        /**
         * This method can be expensive for complex types.
         */
        public long getRetainedSizeInBytes()
        {
            long sizeInBytes = 0;
            for (PageBuilder pageBuilder : pageBuilders) {
                sizeInBytes += pageBuilder.getRetainedSizeInBytes();
            }
            return sizeInBytes;
        }

        public PartitionedOutputInfo getInfo()
        {
            return new PartitionedOutputInfo(rowsAdded.get(), pagesAdded.get(), outputBuffer.getPeakMemoryUsage());
        }

        public void partitionPage(Page page)
        {
            requireNonNull(page, "page is null");

            Page partitionFunctionArgs = getPartitionFunctionArguments(page);
            for (int position = 0; position < page.getPositionCount(); position++) {
                boolean shouldReplicate = (replicatesAnyRow && !hasAnyRowBeenReplicated) ||
                        nullChannel.isPresent() && page.getBlock(nullChannel.getAsInt()).isNull(position);
                if (shouldReplicate) {
                    for (PageBuilder pageBuilder : pageBuilders) {
                        appendRow(pageBuilder, page, position);
                    }
                    hasAnyRowBeenReplicated = true;
                }
                else {
                    int partition = partitionFunction.getPartition(partitionFunctionArgs, position);
                    appendRow(pageBuilders[partition], page, position);
                }
            }
            flush(false);
        }

        private Page getPartitionFunctionArguments(Page page)
        {
            Block[] blocks = new Block[partitionChannels.size()];
            for (int i = 0; i < blocks.length; i++) {
                Optional<Block> partitionConstant = partitionConstants.get(i);
                if (partitionConstant.isPresent()) {
                    blocks[i] = new RunLengthEncodedBlock(partitionConstant.get(), page.getPositionCount());
                }
                else {
                    blocks[i] = page.getBlock(partitionChannels.get(i));
                }
            }
            return new Page(page.getPositionCount(), blocks);
        }

        private void appendRow(PageBuilder pageBuilder, Page page, int position)
        {
            pageBuilder.declarePosition();

            for (int channel = 0; channel < sourceTypes.size(); channel++) {
                Type type = sourceTypes.get(channel);
                type.appendTo(page.getBlock(channel), position, pageBuilder.getBlockBuilder(channel));
            }
        }

        public void flush(boolean force)
        {
            // add all full pages to output buffer
            for (int partition = 0; partition < pageBuilders.length; partition++) {
                PageBuilder partitionPageBuilder = pageBuilders[partition];
                if (!partitionPageBuilder.isEmpty() && (force || partitionPageBuilder.isFull())) {
                    Page pagePartition = partitionPageBuilder.build();
                    partitionPageBuilder.reset();

                    List<SerializedPage> serializedPages = splitPage(pagePartition, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                            .map(page -> serde.serialize(page))
                            .collect(toImmutableList());

                    outputBuffer.enqueue(partition, serializedPages, id);
                    pagesAdded.incrementAndGet();
                    rowsAdded.addAndGet(pagePartition.getPositionCount());
                }
            }
        }

        @Override
        public Object capture(BlockEncodingSerdeProvider serdeProvider)
        {
            PagePartitionerState myState = new PagePartitionerState();
            // This was just flushed, so page builders must be empty
            for (int i = 0; i < pageBuilders.length; i++) {
                checkState(pageBuilders[i].isEmpty());
            }
            myState.rowsAdded = rowsAdded.get();
            myState.pagesAdded = pagesAdded.get();
            myState.hasAnyRowBeenReplicated = hasAnyRowBeenReplicated;
            return myState;
        }

        @Override
        public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
        {
            PagePartitionerState myState = (PagePartitionerState) state;
            this.rowsAdded.set(myState.rowsAdded);
            this.pagesAdded.set(myState.pagesAdded);
            this.hasAnyRowBeenReplicated = myState.hasAnyRowBeenReplicated;
        }

        private static class PagePartitionerState
                implements Serializable
        {
            private long rowsAdded;
            private long pagesAdded;
            private boolean hasAnyRowBeenReplicated;
        }
    }

    public static class PartitionedOutputInfo
            implements Mergeable<PartitionedOutputInfo>, OperatorInfo
    {
        private final long rowsAdded;
        private final long pagesAdded;
        private final long outputBufferPeakMemoryUsage;

        @JsonCreator
        public PartitionedOutputInfo(
                @JsonProperty("rowsAdded") long rowsAdded,
                @JsonProperty("pagesAdded") long pagesAdded,
                @JsonProperty("outputBufferPeakMemoryUsage") long outputBufferPeakMemoryUsage)
        {
            this.rowsAdded = rowsAdded;
            this.pagesAdded = pagesAdded;
            this.outputBufferPeakMemoryUsage = outputBufferPeakMemoryUsage;
        }

        @JsonProperty
        public long getRowsAdded()
        {
            return rowsAdded;
        }

        @JsonProperty
        public long getPagesAdded()
        {
            return pagesAdded;
        }

        @JsonProperty
        public long getOutputBufferPeakMemoryUsage()
        {
            return outputBufferPeakMemoryUsage;
        }

        @Override
        public PartitionedOutputInfo mergeWith(PartitionedOutputInfo other)
        {
            return new PartitionedOutputInfo(
                    rowsAdded + other.rowsAdded,
                    pagesAdded + other.pagesAdded,
                    Math.max(outputBufferPeakMemoryUsage, other.outputBufferPeakMemoryUsage));
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("rowsAdded", rowsAdded)
                    .add("pagesAdded", pagesAdded)
                    .add("outputBufferPeakMemoryUsage", outputBufferPeakMemoryUsage)
                    .toString();
        }
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
        PartitionedOutputOperatorState myState = new PartitionedOutputOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.partitionFunction = partitionFunction.capture(serdeProvider);
        myState.systemMemoryContext = systemMemoryContext.getBytes();
        myState.finished = finished;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        PartitionedOutputOperatorState myState = (PartitionedOutputOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.partitionFunction.restore(myState.partitionFunction, serdeProvider);
        this.systemMemoryContext.setBytes(myState.systemMemoryContext);
        this.finished = myState.finished;
    }

    private static class PartitionedOutputOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private Object partitionFunction;
        private long systemMemoryContext;
        private boolean finished;
    }
}
