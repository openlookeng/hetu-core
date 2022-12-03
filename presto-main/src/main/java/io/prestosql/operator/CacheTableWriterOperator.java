/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.execution.DriverPipelineTaskId;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.split.PageSinkManager;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.util.AutoCloseableCloser;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static java.util.Objects.requireNonNull;

public class CacheTableWriterOperator
        implements Operator
{
    public static class CacheTableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final TableWriterNode.WriterTarget target;
        private final List<Integer> columnChannels;
        private final Session session;
        private final Optional<TaskId> taskId;
        private boolean closed;
        private long thresholdSize;

        public CacheTableWriterOperatorFactory(int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                TableWriterNode.WriterTarget writerTarget,
                List<Integer> columnChannels,
                Session session,
                Optional<TaskId> taskId,
                long thresholdSize)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(writerTarget instanceof TableWriterNode.CreateTarget
                            || writerTarget instanceof TableWriterNode.InsertTarget
                            || writerTarget instanceof TableWriterNode.UpdateAsInsertTarget
                            || writerTarget instanceof TableWriterNode.DeleteAsInsertTarget
                            || writerTarget instanceof TableWriterNode.TableExecuteTarget,
                    "writerTarget must be CreateTarget or InsertTarget or UpdateAsInsertTarget or DeleteAsInsertTarget");
            this.target = requireNonNull(writerTarget, "writerTarget is null");
            this.session = session;
            this.taskId = taskId;
            this.thresholdSize = thresholdSize;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, CacheTableWriterOperator.class.getSimpleName());
            boolean cpuTimerEnabled = isStatisticsCpuTimerEnabled(session);
            return new CacheTableWriterOperator(context, createPageSink(driverContext), columnChannels, cpuTimerEnabled, thresholdSize);
        }

        private ConnectorPageSink createPageSink(DriverContext driverContext)
        {
            Optional<DriverPipelineTaskId> driverTaskId = Optional.of(new DriverPipelineTaskId(taskId, driverContext.getPipelineContext().getPipelineId(), driverContext.getDriverId()));
            if (target instanceof TableWriterNode.CreateTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((TableWriterNode.CreateTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.InsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((TableWriterNode.InsertTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.UpdateAsInsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((TableWriterNode.UpdateAsInsertTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.DeleteAsInsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((TableWriterNode.DeleteAsInsertTarget) target).getHandle());
            }
            if (target instanceof TableWriterNode.TableExecuteTarget) {
                return pageSinkManager.createPageSink(session, ((TableWriterNode.TableExecuteTarget) target).getExecuteHandle());
            }
            throw new UnsupportedOperationException("Unhandled target type: " + target.getClass().getName());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CacheTableWriterOperator.CacheTableWriterOperatorFactory(operatorId,
                    planNodeId,
                    pageSinkManager,
                    target,
                    columnChannels,
                    session,
                    taskId,
                    thresholdSize);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext pageSinkMemoryContext;
    private final ConnectorPageSink pageSink;
    private final List<Integer> columnChannels;
    private final AtomicLong pageSinkPeakMemoryUsage = new AtomicLong();

    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private CompletableFuture<Collection<Slice>> finishFuture;
    private CacheTableWriterOperator.State state = State.RUNNING;
    private long rowCount;
    private boolean committed;
    private boolean closed;
    private long writtenBytes;
    private long thresholdSize;
    private long currentSize;
    private boolean isPageSinkAborted;

    private final OperationTimer.OperationTiming statisticsTiming = new OperationTimer.OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final SingleInputSnapshotState snapshotState;
    private Queue<Page> outputQueue = new LinkedList<>();

    public CacheTableWriterOperator(OperatorContext operatorContext, ConnectorPageSink pageSink, List<Integer> columnChannels, boolean statisticsCpuTimerEnabled, long thresholdSize)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageSinkMemoryContext = operatorContext.newLocalSystemMemoryContext(CacheTableWriterOperator.class.getSimpleName());
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.thresholdSize = thresholdSize;
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        return state == State.FINISHED && blocked.isDone();
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING && blocked.isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator does not need input");

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        Block[] blocks = new Block[columnChannels.size()];
        for (int outputChannel = 0; outputChannel < columnChannels.size(); outputChannel++) {
            blocks[outputChannel] = page.getBlock(columnChannels.get(outputChannel));
        }

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        timer.end(statisticsTiming);

        CompletableFuture<?> future = CompletableFuture.completedFuture(null);
        Page inputPage = new Page(blocks);
        currentSize += inputPage.getSizeInBytes();
        if (currentSize > thresholdSize) {
            if (!isPageSinkAborted) {
                pageSink.abort();
            }
            isPageSinkAborted = true;
        }
        else {
            future = pageSink.appendPage(inputPage);
        }
        outputQueue.add(inputPage);

        updateMemoryUsage();
        blocked = toListenableFuture(future);
        rowCount += page.getPositionCount();
        updateWrittenBytes();
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

        if (!blocked.isDone()) {
            return null;
        }

        if (outputQueue.isEmpty()) {
            if (state == State.FINISHING) {
                state = State.FINISHED;
            }
            return null;
        }

        return outputQueue.poll();
    }

    @Override
    public void close()
            throws Exception
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void finish()
    {
        ListenableFuture<?> currentlyBlocked = blocked;

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        timer.end(statisticsTiming);

        ListenableFuture<?> blockedOnFinish = NOT_BLOCKED;
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = pageSink.finish();
            blockedOnFinish = toListenableFuture(finishFuture);
            updateWrittenBytes();
        }
        this.blocked = allAsList(currentlyBlocked, blockedOnFinish);
    }

    private void closeImpl(AutoCloseable closeAction)
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!closed) {
            closed = true;
            if (!committed) {
                closer.register(closeAction);
            }
        }
        closer.register(() -> pageSinkMemoryContext.close());
        closer.close();
    }

    private void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getSystemMemoryUsage();
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    private void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }
}
