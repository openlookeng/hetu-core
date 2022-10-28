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
package io.prestosql.operator.output;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.operator.OperatorInfo;
import io.prestosql.operator.OutputFactory;
import io.prestosql.operator.PartitionFunction;
import io.prestosql.operator.SinkOperator;
import io.prestosql.operator.TaskContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.util.Mergeable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
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
        private final PositionsAppenderFactory positionsAppenderFactory;

        public PartitionedOutputFactory(
                PartitionFunction partitionFunction,
                List<Integer> partitionChannels,
                List<Optional<NullableValue>> partitionConstants,
                boolean replicatesAnyRow,
                OptionalInt nullChannel,
                OutputBuffer outputBuffer,
                DataSize maxMemory,
                PositionsAppenderFactory positionsAppenderFactory)
        {
            this.partitionFunction = requireNonNull(partitionFunction, "partitionFunction is null");
            this.partitionChannels = requireNonNull(partitionChannels, "partitionChannels is null");
            this.partitionConstants = requireNonNull(partitionConstants, "partitionConstants is null");
            this.replicatesAnyRow = replicatesAnyRow;
            this.nullChannel = requireNonNull(nullChannel, "nullChannel is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.maxMemory = requireNonNull(maxMemory, "maxMemory is null");
            this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
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
                    maxMemory,
                    positionsAppenderFactory);
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
        private final PositionsAppenderFactory positionsAppenderFactory;
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
                DataSize maxMemory,
                PositionsAppenderFactory positionsAppenderFactory)
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
                    new AtomicInteger(1),
                    positionsAppenderFactory);
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
                AtomicInteger duplicateCount,
                PositionsAppenderFactory positionsAppenderFactory)
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
            this.positionsAppenderFactory = requireNonNull(positionsAppenderFactory, "positionsAppenderFactory is null");
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
                    maxMemory,
                    positionsAppenderFactory);
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
                    duplicateCount,
                    positionsAppenderFactory);
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
            DataSize maxMemory,
            PositionsAppenderFactory positionsAppenderFactory)
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
                operatorContext,
                sourceTypes,
                maxMemory,
                positionsAppenderFactory);

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
