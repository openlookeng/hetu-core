/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.DriverTaskId;
import io.prestosql.execution.TaskId;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.metadata.Split;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSink.VacuumResult;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.UpdatablePageSource;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.type.Type;
import io.prestosql.split.EmptySplit;
import io.prestosql.split.PageSinkManager;
import io.prestosql.split.PageSourceProvider;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.util.AutoCloseableCloser;
import io.prestosql.util.Mergeable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.prestosql.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class VacuumTableOperator
        implements SourceOperator
{
    public static final int ROW_COUNT_CHANNEL = 0;
    public static final int FRAGMENT_CHANNEL = 1;
    public static final int STATS_START_CHANNEL = 2;

    public static class VacuumTableOperatorFactory
            implements SourceOperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSourceProvider pageSourceProvider;
        private final PageSinkManager pageSinkManager;
        private final TableHandle table;
        private final TableWriterNode.WriterTarget writerTarget;
        private final Session session;
        private final Optional<TaskId> taskId;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final List<Type> types;
        private boolean closed;

        public VacuumTableOperatorFactory(int operatorId,
                                          PlanNodeId planNodeId,
                                          PageSourceProvider pageSourceProvider,
                                          PageSinkManager pageSinkManager,
                                          TableWriterNode.WriterTarget writerTarget,
                                          TableHandle table,
                                          Session session,
                                          OperatorFactory statisticsAggregationOperatorFactory,
                                          List<Type> types,
                                          Optional<TaskId> taskId)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageSourceProvider = requireNonNull(pageSourceProvider, "pageSourceProvider is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(writerTarget instanceof TableWriterNode.VacuumTarget,
                    "writerTarget must be VacuumTarget");
            this.writerTarget = requireNonNull(writerTarget, "writerTarget is null");
            this.table = table;
            this.session = session;
            this.taskId = taskId;
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        public int getOperatorId()
        {
            return operatorId;
        }

        @Override
        public PlanNodeId getSourceId()
        {
            return planNodeId;
        }

        public String getOperatorType()
        {
            return VacuumTableOperator.class.getSimpleName();
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, getOperatorType());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            ConnectorPageSourceProvider connectorPageSourceProvider = this.pageSourceProvider.getPageSourceProvider(table.getCatalogName());
            return new VacuumTableOperator(
                    context,
                    planNodeId,
                    createPageSink(driverContext),
                    connectorPageSourceProvider,
                    table,
                    statisticsAggregationOperator,
                    statisticsCpuTimerEnabled,
                    types);
        }

        private ConnectorPageSink createPageSink(DriverContext driverContext)
        {
            Optional<DriverTaskId> driverTaskId = Optional.of(new DriverTaskId(taskId, driverContext.getDriverId()));
            if (writerTarget instanceof TableWriterNode.VacuumTarget) {
                return pageSinkManager.createPageSink(session, driverTaskId, ((TableWriterNode.VacuumTarget) writerTarget).getHandle());
            }
            throw new UnsupportedOperationException("Unhandled target type: " + writerTarget.getClass().getName());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }
    }

    public VacuumTableOperator(OperatorContext context,
                               PlanNodeId planNodeId,
                               ConnectorPageSink pageSink,
                               ConnectorPageSourceProvider connectorPageSourceProvider,
                               TableHandle table,
                               Operator statisticsAggregationOperator,
                               boolean statisticsCpuTimerEnabled, List<Type> types)
    {
        this.operatorContext = requireNonNull(context, "operatorContext is null");
        this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
        this.vacuumMemoryContext = operatorContext.newLocalSystemMemoryContext(VacuumTableOperator.class.getSimpleName());
        this.connectorPageSourceProvider = requireNonNull(connectorPageSourceProvider, "pageSink is null");
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.table = table;
        this.statisticAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticAggregationOperator is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final PlanNodeId planNodeId;
    private final LocalMemoryContext vacuumMemoryContext;
    private final TableHandle table;
    private final ConnectorPageSink pageSink;
    private final AtomicLong vacuumPeakMemoryUsage = new AtomicLong();
    private final Operator statisticAggregationOperator;
    private final ConnectorPageSourceProvider connectorPageSourceProvider;
    private final List<Type> types;

    private List<ConnectorSplit> splits = new ArrayList<>();
    private boolean finished;

    private final SettableFuture<?> splitBlocked = SettableFuture.create();

    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private CompletableFuture<Collection<Slice>> finishFuture;
    private State state = State.RUNNING;
    private boolean committed;
    private boolean closed;
    private long writtenBytes;

    private final OperationTimer.OperationTiming statisticsTiming = new OperationTimer.OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    @Override
    public PlanNodeId getSourceId()
    {
        return planNodeId;
    }

    @Override
    public Supplier<Optional<UpdatablePageSource>> addSplit(Split split)
    {
        requireNonNull(split, "split is null");
        if (finished) {
            return Optional::empty;
        }
        checkArgument(split.getCatalogName().equals(table.getCatalogName()), "mismatched split and table");

        Object splitInfo = split.getInfo();
        if (splitInfo != null) {
            operatorContext.setInfoSupplier(() -> new SplitOperatorInfo(splitInfo));
        }
        if (!(split.getConnectorSplit() instanceof EmptySplit)) {
            //Skip empty splits
            this.splits.add(split.getConnectorSplit());
        }
        splitBlocked.set(null);
        return Optional::empty;
    }

    @Override
    public void noMoreSplits()
    {
        if (splits.isEmpty()) {
            finished = true;
        }
        splitBlocked.set(null);
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
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (!blocked.isDone()) {
            return null;
        }

        VacuumResult vacuumResult = null;
        if (!splits.isEmpty()) {
            vacuumResult = pageSink.vacuum(connectorPageSourceProvider,
                    table.getTransaction(),
                    table.getConnectorHandle(),
                    splits);
            if (vacuumResult.getPage() != null) {
                // For statistical purpose.
                statisticAggregationOperator.addInput(vacuumResult.getPage());
            }
        }

        updateMemoryUsage();

        if (vacuumResult == null || vacuumResult.isFinished()) {
            //Since there is no source operator for this, calling self finish.
            finish();
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
        BIGINT.writeLong(rowsBuilder, pageSink.getRowsWritten());
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
    public ListenableFuture<?> startMemoryRevoke()
    {
        return null;
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

    private void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }

    private void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getSystemMemoryUsage();
        vacuumMemoryContext.setBytes(pageSinkMemoryUsage);
        vacuumPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED && blocked.isDone();
    }

    @Override
    public void close() throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!closed) {
            closed = true;
            if (!committed) {
                closer.register(pageSink::abort);
            }
        }
        closer.register(statisticAggregationOperator);
        closer.register(() -> vacuumMemoryContext.close());
        closer.close();
    }

    @VisibleForTesting
    VacuumInfo getInfo()
    {
        return new VacuumInfo(
                vacuumPeakMemoryUsage.get(),
                new Duration(statisticsTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(statisticsTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(pageSink.getValidationCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit());
    }

    public static class VacuumInfo
            implements Mergeable<VacuumInfo>, OperatorInfo
    {
        private final long vacuumPeakMemoryUsage;
        private final Duration statisticsWallTime;
        private final Duration statisticsCpuTime;
        private final Duration validationCpuTime;

        @JsonCreator
        public VacuumInfo(
                @JsonProperty("vacuumPeakMemoryUsage") long vacuumPeakMemoryUsage,
                @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
                @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime,
                @JsonProperty("validationCpuTime") Duration validationCpuTime)
        {
            this.vacuumPeakMemoryUsage = vacuumPeakMemoryUsage;
            this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
            this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
            this.validationCpuTime = requireNonNull(validationCpuTime, "validationCpuTime is null");
        }

        @JsonProperty
        public long getVacuumPeakMemoryUsage()
        {
            return vacuumPeakMemoryUsage;
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
        public VacuumInfo mergeWith(VacuumInfo other)
        {
            return new VacuumInfo(
                    Math.max(vacuumPeakMemoryUsage, other.vacuumPeakMemoryUsage),
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
                    .add("vacuumPeakMemoryUsage", vacuumPeakMemoryUsage)
                    .add("statisticsWallTime", statisticsWallTime)
                    .add("statisticsCpuTime", statisticsCpuTime)
                    .add("validationCpuTime", validationCpuTime)
                    .toString();
        }
    }
}
