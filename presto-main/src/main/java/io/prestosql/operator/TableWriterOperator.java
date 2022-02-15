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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.DriverPipelineTaskId;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.OperationTimer.OperationTiming;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.type.Type;
import io.prestosql.split.PageSinkManager;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.TableWriterNode.WriterTarget;
import io.prestosql.util.AutoCloseableCloser;
import io.prestosql.util.Mergeable;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.sql.planner.plan.TableWriterNode.CreateTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.DeleteAsInsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.InsertTarget;
import static io.prestosql.sql.planner.plan.TableWriterNode.UpdateAsInsertTarget;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@RestorableConfig(stateClassName = "TableWriterOperatorState", uncapturedFields = {"columnChannels", "types", "blocked", "finishFuture", "snapshotState"})
public class TableWriterOperator
        implements Operator
{
    public static final int ROW_COUNT_CHANNEL = 0;
    public static final int FRAGMENT_CHANNEL = 1;
    public static final int STATS_START_CHANNEL = 2;

    public static class TableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final WriterTarget target;
        private final List<Integer> columnChannels;
        private final Session session;
        private final Optional<TaskId> taskId;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final List<Type> types;
        private boolean closed;

        public TableWriterOperatorFactory(int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                WriterTarget writerTarget,
                List<Integer> columnChannels,
                Session session,
                OperatorFactory statisticsAggregationOperatorFactory,
                List<Type> types,
                Optional<TaskId> taskId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(writerTarget instanceof CreateTarget || writerTarget instanceof InsertTarget || writerTarget instanceof UpdateAsInsertTarget
                    || writerTarget instanceof TableWriterNode.DeleteAsInsertTarget, "writerTarget must be CreateTarget or InsertTarget or UpdateAsInsertTarget or DeleteAsInsertTarget");
            this.target = requireNonNull(writerTarget, "writerTarget is null");
            this.session = session;
            this.taskId = taskId;
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean cpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableWriterOperator(context, createPageSink(driverContext), columnChannels, statisticsAggregationOperator, types, cpuTimerEnabled);
        }

        private ConnectorPageSink createPageSink(DriverContext driverContext)
        {
            Optional<DriverPipelineTaskId> driverTaskId = Optional.of(new DriverPipelineTaskId(taskId, driverContext.getPipelineContext().getPipelineId(), driverContext.getDriverId()));
            if (target instanceof CreateTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((CreateTarget) target).getHandle());
            }
            if (target instanceof InsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((InsertTarget) target).getHandle());
            }
            if (target instanceof UpdateAsInsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((UpdateAsInsertTarget) target).getHandle());
            }
            if (target instanceof DeleteAsInsertTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((DeleteAsInsertTarget) target).getHandle());
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
            return new TableWriterOperatorFactory(operatorId,
                    planNodeId,
                    pageSinkManager,
                    target,
                    columnChannels,
                    session,
                    statisticsAggregationOperatorFactory,
                    types,
                    taskId);
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
    private final Operator statisticAggregationOperator;
    private final List<Type> types;

    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private CompletableFuture<Collection<Slice>> finishFuture;
    private State state = State.RUNNING;
    private long rowCount;
    private boolean committed;
    private boolean closed;
    private long writtenBytes;

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final SingleInputSnapshotState snapshotState;

    public TableWriterOperator(
            OperatorContext operatorContext,
            ConnectorPageSink pageSink,
            List<Integer> columnChannels,
            Operator statisticAggregationOperator,
            List<Type> types,
            boolean statisticsCpuTimerEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageSinkMemoryContext = operatorContext.newLocalSystemMemoryContext(TableWriterOperator.class.getSimpleName());
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
        this.operatorContext.setInfoSupplier(this::getInfo);
        this.statisticAggregationOperator = requireNonNull(statisticAggregationOperator, "statisticAggregationOperator is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
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
        ListenableFuture<?> currentlyBlocked = blocked;

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticAggregationOperator.finish();
        timer.end(statisticsTiming);

        ListenableFuture<?> blockedOnAggregation = statisticAggregationOperator.isBlocked();
        ListenableFuture<?> blockedOnFinish = NOT_BLOCKED;
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = pageSink.finish();
            blockedOnFinish = toListenableFuture(finishFuture);
            updateWrittenBytes();
        }
        this.blocked = allAsList(currentlyBlocked, blockedOnAggregation, blockedOnFinish);
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
    public ListenableFuture<?> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING || !blocked.isDone()) {
            return false;
        }
        return statisticAggregationOperator.needsInput();
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
        statisticAggregationOperator.addInput(page);
        timer.end(statisticsTiming);

        ListenableFuture<?> blockedOnAggregation = statisticAggregationOperator.isBlocked();
        CompletableFuture<?> future = pageSink.appendPage(new Page(blocks));
        updateMemoryUsage();
        ListenableFuture<?> blockedOnWrite = toListenableFuture(future);
        blocked = allAsList(blockedOnAggregation, blockedOnWrite);
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

        if (!statisticAggregationOperator.isFinished()) {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page aggregationOutput = statisticAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (aggregationOutput == null) {
                return null;
            }
            return createStatisticsPage(aggregationOutput);
        }

        if (state != State.FINISHING) {
            return null;
        }

        Page fragmentsPage = createFragmentsPage();
        int positionCount = fragmentsPage.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                outputBlocks[channel] = fragmentsPage.getBlock(channel);
            }
            else {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
        }

        state = State.FINISHED;
        return new Page(positionCount, outputBlocks);
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    private Page createStatisticsPage(Page aggregationOutput)
    {
        int positionCount = aggregationOutput.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
            else {
                outputBlocks[channel] = aggregationOutput.getBlock(channel - 2);
            }
        }
        return new Page(positionCount, outputBlocks);
    }

    private Page createFragmentsPage()
    {
        Collection<Slice> fragments = getFutureValue(finishFuture);
        committed = true;
        updateWrittenBytes();

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(fragments.size() + 1, ImmutableList.of(types.get(ROW_COUNT_CHANNEL), types.get(FRAGMENT_CHANNEL)));
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        BlockBuilder fragmentBuilder = page.getBlockBuilder(1);

        // write row count
        page.declarePosition();
        BIGINT.writeLong(rowsBuilder, rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            page.declarePosition();
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        return page.build();
    }

    @Override
    public void close()
            throws Exception
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
        closeImpl(pageSink::abort);
    }

    @Override
    public void cancelToResume()
            throws Exception
    {
        closeImpl(pageSink::cancelToResume);
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
        closer.register(statisticAggregationOperator);
        closer.register(() -> pageSinkMemoryContext.close());
        closer.close();
    }

    private void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }

    private void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getSystemMemoryUsage();
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @VisibleForTesting
    Operator getStatisticAggregationOperator()
    {
        return statisticAggregationOperator;
    }

    @VisibleForTesting
    TableWriterInfo getInfo()
    {
        return new TableWriterInfo(
                pageSinkPeakMemoryUsage.get(),
                new Duration(statisticsTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(statisticsTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(pageSink.getValidationCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit());
    }

    public static class TableWriterInfo
            implements Mergeable<TableWriterInfo>, OperatorInfo
    {
        private final long pageSinkPeakMemoryUsage;
        private final Duration statisticsWallTime;
        private final Duration statisticsCpuTime;
        private final Duration validationCpuTime;

        @JsonCreator
        public TableWriterInfo(
                @JsonProperty("pageSinkPeakMemoryUsage") long pageSinkPeakMemoryUsage,
                @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
                @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime,
                @JsonProperty("validationCpuTime") Duration validationCpuTime)
        {
            this.pageSinkPeakMemoryUsage = pageSinkPeakMemoryUsage;
            this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
            this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
            this.validationCpuTime = requireNonNull(validationCpuTime, "validationCpuTime is null");
        }

        @JsonProperty
        public long getPageSinkPeakMemoryUsage()
        {
            return pageSinkPeakMemoryUsage;
        }

        @JsonProperty
        public Duration getStatisticsWallTime()
        {
            return statisticsWallTime;
        }

        @JsonProperty
        public Duration getStatisticsCpuTime()
        {
            return statisticsCpuTime;
        }

        @JsonProperty
        public Duration getValidationCpuTime()
        {
            return validationCpuTime;
        }

        @Override
        public TableWriterInfo mergeWith(TableWriterInfo other)
        {
            return new TableWriterInfo(
                    Math.max(pageSinkPeakMemoryUsage, other.pageSinkPeakMemoryUsage),
                    new Duration(statisticsWallTime.getValue(NANOSECONDS) + other.statisticsWallTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(statisticsCpuTime.getValue(NANOSECONDS) + other.statisticsCpuTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                    new Duration(validationCpuTime.getValue(NANOSECONDS) + other.validationCpuTime.getValue(NANOSECONDS), NANOSECONDS).convertToMostSuccinctTimeUnit());
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
                    .add("pageSinkPeakMemoryUsage", pageSinkPeakMemoryUsage)
                    .add("statisticsWallTime", statisticsWallTime)
                    .add("statisticsCpuTime", statisticsCpuTime)
                    .add("validationCpuTime", validationCpuTime)
                    .toString();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        checkState(blocked.isDone(), "Received capture request when there are pending writes");

        TableWriterOperatorState myState = new TableWriterOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.pageSinkMemoryContext = pageSinkMemoryContext.getBytes();
        myState.pageSinkPeakMemoryUsage = pageSinkPeakMemoryUsage.get();
        myState.pageSink = pageSink.capture(serdeProvider);
        myState.statisticAggregationOperator = statisticAggregationOperator.capture(serdeProvider);
        myState.state = state.toString();
        myState.rowCount = rowCount;
        myState.committed = committed;
        myState.closed = closed;
        myState.writtenBytes = writtenBytes;
        myState.statisticsTiming = statisticsTiming.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        checkState(blocked.isDone(), "Received restore request when there are pending writes");

        TableWriterOperatorState myState = (TableWriterOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.pageSinkMemoryContext.setBytes(myState.pageSinkMemoryContext);
        this.pageSinkPeakMemoryUsage.set(myState.pageSinkPeakMemoryUsage);
        long resumeCount = operatorContext.getDriverContext().getPipelineContext().getTaskContext().getSnapshotManager().getResumeCount();
        this.pageSink.restore(myState.pageSink, serdeProvider, resumeCount);
        this.statisticAggregationOperator.restore(myState.statisticAggregationOperator, serdeProvider);
        this.state = State.valueOf(myState.state);
        this.rowCount = myState.rowCount;
        this.committed = myState.committed;
        this.closed = myState.closed;
        this.writtenBytes = myState.writtenBytes;
        this.statisticsTiming.restore(myState.statisticsTiming, serdeProvider);
        this.blocked = NOT_BLOCKED;
    }

    private static class TableWriterOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private long pageSinkMemoryContext;
        private long pageSinkPeakMemoryUsage;
        private Object pageSink;
        private Object statisticAggregationOperator;
        private String state;
        private long rowCount;
        private boolean committed;
        private boolean closed;
        private long writtenBytes;
        private Object statisticsTiming;
    }
}
