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

import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.MarkerPage;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

@RestorableConfig(uncapturedFields = {"outputBuffer", "pagePreprocessor", "serde", "snapshotState"})
public class TaskOutputOperator
        implements SinkOperator
{
    public static class TaskOutputFactory
            implements OutputFactory
    {
        private final OutputBuffer outputBuffer;

        public TaskOutputFactory(OutputBuffer outputBuffer)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
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
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pagePreprocessor);
        }
    }

    public static class TaskOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final OutputBuffer outputBuffer;
        private final Function<Page, Page> pagePreprocessor;
        // Snapshot: When a factory is duplicated, factory instances share the same OutputBuffer.
        // All these factory instances now share this duplicateCount, so only the last factory that receives "noMoreOperators"
        // (the one that decrements the count to 0) should inform OutputBuffer about "setNoMoreInputChannels".
        private final AtomicInteger duplicateCount;

        public TaskOutputOperatorFactory(int operatorId, PlanNodeId planNodeId, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor)
        {
            this(operatorId, planNodeId, outputBuffer, pagePreprocessor, new AtomicInteger(1));
        }

        private TaskOutputOperatorFactory(int operatorId, PlanNodeId planNodeId, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor, AtomicInteger duplicateCount)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.duplicateCount = requireNonNull(duplicateCount, "duplicateCount is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext addOperatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TaskOutputOperator.class.getSimpleName());
            String uniqueId = addOperatorContext.getUniqueId();
            outputBuffer.addInputChannel(uniqueId);
            return new TaskOutputOperator(uniqueId, addOperatorContext, outputBuffer, pagePreprocessor);
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
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pagePreprocessor, duplicateCount);
        }
    }

    private final String id;
    private final OperatorContext operatorContext;
    private final OutputBuffer outputBuffer;
    private final Function<Page, Page> pagePreprocessor;
    private final SingleInputSnapshotState snapshotState;
    private final boolean isStage0;
    private final PagesSerde serde;
    private boolean finished;

    public TaskOutputOperator(String id, OperatorContext operatorContext, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor)
    {
        this.id = id;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.serde = requireNonNull(operatorContext.getDriverContext().getSerde(), "serde is null");
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.isStage0 = operatorContext.getDriverContext().getPipelineContext().getTaskContext().getTaskId().getStageId().getId() == 0;
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = outputBuffer.isFull();
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

        Page inputPage = page;
        if (snapshotState != null) {
            if (snapshotState.processPage(inputPage)) {
                inputPage = snapshotState.nextMarker();
            }
        }

        if (inputPage.getPositionCount() == 0) {
            return;
        }

        if (!(inputPage instanceof MarkerPage)) {
            inputPage = pagePreprocessor.apply(inputPage);
        }
        else if (isStage0) {
            // Do not add marker to final output.
            return;
        }

        List<SerializedPage> serializedPages = splitPage(inputPage, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                .map(p -> serde.serialize(p))
                .collect(toImmutableList());

        if (inputPage instanceof MarkerPage) {
            // Snapshot: driver/thread 1 reaches here and adds marker 1 to the output buffer.
            // It's the first time marker 1 is received, so marker 1 will be broadcasted to all client buffers.
            // Then driver/thread 2 adds markers 1 and 2 to the output buffer.
            // Marker 1 was already seen, so it's not sent to client buffers, but marker 2 is seen the first time, and is sent to client buffers.
            // Without the following synchronization, it's possible for the 2 threads to interact with client buffers at the same time.
            // That is, marker 1 is added to client buffer #1, then thread 2 takes over, and adds marker 2 to client buffer #1 and #2.
            // The result is that for buffer #2, it receives marker 2 before marker 1.
            synchronized (outputBuffer) {
                outputBuffer.enqueue(serializedPages, id);
            }
        }
        else {
            outputBuffer.enqueue(serializedPages, id);
        }
        operatorContext.recordOutput(inputPage.getSizeInBytes(), inputPage.getPositionCount());
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
        TaskOutputOperatorState myState = new TaskOutputOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.finished = finished;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        TaskOutputOperatorState myState = (TaskOutputOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.finished = myState.finished;
    }

    private static class TaskOutputOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private boolean finished;
    }
}
