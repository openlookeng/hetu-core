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
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.operator.OperationTimer.OperationTiming;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.RestorableConfig;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.prestosql.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static io.prestosql.operator.TableWriterOperator.FRAGMENT_CHANNEL;
import static io.prestosql.operator.TableWriterOperator.ROW_COUNT_CHANNEL;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@RestorableConfig(uncapturedFields = {"tableFinisher", "descriptor", "outputMetadata", "snapshotState"})
public class TableFinishOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);

    public static class TableFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final TableFinisher tableFinisher;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private final Session session;
        private boolean closed;

        public TableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinisher tableFinisher,
                OperatorFactory statisticsAggregationOperatorFactory,
                StatisticAggregationsDescriptor<Integer> descriptor,
                Session session)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableFinisher is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            Operator aggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean cpuTimerEnabled = !(aggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableFinishOperator(context, tableFinisher, aggregationOperator, descriptor, cpuTimerEnabled);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFinishOperatorFactory(operatorId, planNodeId, tableFinisher, statisticsAggregationOperatorFactory, descriptor, session);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableFinisher tableFinisher;
    private final Operator statisticsAggregationOperator;
    private final StatisticAggregationsDescriptor<Integer> descriptor;

    private State state = State.RUNNING;
    private long rowCount;
    private Optional<ConnectorOutputMetadata> outputMetadata = Optional.empty();
    private final List<Slice> fragment = new ArrayList<>();
    private final List<ComputedStatistics> computedStatistics = new ArrayList<>();

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final SingleInputSnapshotState snapshotState;

    public TableFinishOperator(
            OperatorContext operatorContext,
            TableFinisher tableFinisher,
            Operator statisticsAggregationOperator,
            StatisticAggregationsDescriptor<Integer> descriptor,
            boolean statisticsCpuTimerEnabled)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.statisticsAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticsAggregationOperator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        operatorContext.setInfoSupplier(this::getInfo);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticsAggregationOperator.finish();
        timer.end(statisticsTiming);

        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        if (snapshotState != null && snapshotState.hasMarker()) {
            // Snapshot: there are pending markers. Need to send them out before finishing this operator.
            return false;
        }

        if (state == State.FINISHED) {
            verify(statisticsAggregationOperator.isFinished());
            return true;
        }
        return false;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return statisticsAggregationOperator.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING) {
            return false;
        }
        return statisticsAggregationOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        if (snapshotState != null) {
            if (snapshotState.processPage(page)) {
                return;
            }
        }

        Block rowCountBlock = page.getBlock(ROW_COUNT_CHANNEL);
        Block fragmentBlock = page.getBlock(FRAGMENT_CHANNEL);
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!rowCountBlock.isNull(position)) {
                rowCount += BIGINT.getLong(rowCountBlock, position);
            }
            if (!fragmentBlock.isNull(position)) {
                fragment.add(VARBINARY.getSlice(fragmentBlock, position));
            }
        }

        extractStatisticsRows(page).ifPresent(statisticsPage -> {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            statisticsAggregationOperator.addInput(statisticsPage);
            timer.end(statisticsTiming);
        });
    }

    private static Optional<Page> extractStatisticsRows(Page page)
    {
        int statisticsPositionCount = 0;
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                statisticsPositionCount++;
            }
        }

        if (statisticsPositionCount == 0) {
            return Optional.empty();
        }

        if (statisticsPositionCount == page.getPositionCount()) {
            return Optional.of(page);
        }

        int selectedPositionsIndex = 0;
        int[] selectedPositions = new int[statisticsPositionCount];
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (isStatisticsPosition(page, position)) {
                selectedPositions[selectedPositionsIndex] = position;
                selectedPositionsIndex++;
            }
        }

        Block[] blocks = new Block[page.getChannelCount()];
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            blocks[channel] = page.getBlock(channel).getPositions(selectedPositions, 0, statisticsPositionCount);
        }
        return Optional.of(new Page(statisticsPositionCount, blocks));
    }

    /**
     * Both the statistics and the row_count + fragments are transferred over the same communication
     * link between the TableWriterOperator and the TableFinishOperator. Thus the multiplexing is needed.
     * <p>
     * The transferred page layout looks like:
     * <p>
     * [[row_count_channel], [fragment_channel], [statistic_channel_1] ... [statistic_channel_N]]
     * <p>
     * [row_count_channel] - contains number of rows processed by a TableWriterOperator instance
     * [fragment_channel] - contains arbitrary binary data provided by the ConnectorPageSink#finish for
     * the further post processing on the coordinator
     * <p>
     * [statistic_channel_1] ... [statistic_channel_N] - contain pre-aggregated statistics computed by the
     * statistics aggregation operator within the
     * TableWriterOperator
     * <p>
     * Since the final aggregation operator in the TableFinishOperator doesn't know what to do with the
     * first two channels, those must be pruned. For the convenience we never set both, the
     * [row_count_channel] + [fragment_channel] and the [statistic_channel_1] ... [statistic_channel_N].
     * <p>
     * If this is a row that holds statistics - the [row_count_channel] + [fragment_channel] will be NULL.
     * <p>
     * It this is a row that holds the row count or the fragment - all the statistics channels will be set
     * to NULL.
     * <p>
     * Since neither [row_count_channel] or [fragment_channel] cannot hold the NULL value naturally, by
     * checking isNull on these two channels we can determine if this is a row that contains statistics.
     */
    private static boolean isStatisticsPosition(Page page, int position)
    {
        return page.getBlock(ROW_COUNT_CHANNEL).isNull(position) && page.getBlock(FRAGMENT_CHANNEL).isNull(position);
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

        if (!isBlocked().isDone()) {
            return null;
        }

        if (!statisticsAggregationOperator.isFinished()) {
            verify(statisticsAggregationOperator.isBlocked().isDone(), "aggregation operator should not be blocked");

            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page page = statisticsAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (page == null) {
                return null;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                computedStatistics.add(getComputedStatistics(page, position));
            }
            return null;
        }

        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        outputMetadata = tableFinisher.finishTable(ImmutableList.copyOf(fragment), ImmutableList.copyOf(computedStatistics));

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        page.declarePosition();
        BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
        return page.build();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    private ComputedStatistics getComputedStatistics(Page page, int position)
    {
        ImmutableList.Builder<String> groupingColumns = ImmutableList.builder();
        ImmutableList.Builder<Block> groupingValues = ImmutableList.builder();
        descriptor.getGrouping().forEach((column, channel) -> {
            groupingColumns.add(column);
            groupingValues.add(page.getBlock(channel).getSingleValueBlock(position));
        });

        ComputedStatistics.Builder statistics = ComputedStatistics.builder(groupingColumns.build(), groupingValues.build());

        descriptor.getTableStatistics().forEach((type, channel) ->
                statistics.addTableStatistic(type, page.getBlock(channel).getSingleValueBlock(position)));

        descriptor.getColumnStatistics().forEach((metadata, channel) -> statistics.addColumnStatistic(metadata, page.getBlock(channel).getSingleValueBlock(position)));

        return statistics.build();
    }

    @VisibleForTesting
    TableFinishInfo getInfo()
    {
        return new TableFinishInfo(
                outputMetadata,
                new Duration(statisticsTiming.getWallNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(statisticsTiming.getCpuNanos(), NANOSECONDS).convertToMostSuccinctTimeUnit());
    }

    @Override
    public void close()
            throws Exception
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
        statisticsAggregationOperator.close();
    }

    public interface TableFinisher
    {
        Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        TableFinishOperatorState myState = new TableFinishOperatorState();
        myState.operatorContext = operatorContext.capture(serdeProvider);
        myState.statisticsAggregationOperator = statisticsAggregationOperator.capture(serdeProvider);
        myState.state = state.toString();
        myState.rowCount = rowCount;
        myState.fragment = new byte[fragment.size()][];
        for (int i = 0; i < fragment.size(); i++) {
            myState.fragment[i] = fragment.get(i).getBytes();
        }
        myState.computedStatistics = new Object[computedStatistics.size()];
        for (int i = 0; i < computedStatistics.size(); i++) {
            myState.computedStatistics[i] = computedStatistics.get(i).capture(serdeProvider);
        }
        myState.statisticsTiming = statisticsTiming.capture(serdeProvider);
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        TableFinishOperatorState myState = (TableFinishOperatorState) state;
        this.operatorContext.restore(myState.operatorContext, serdeProvider);
        this.statisticsAggregationOperator.restore(myState.statisticsAggregationOperator, serdeProvider);
        this.state = State.valueOf(myState.state);
        this.rowCount = myState.rowCount;
        this.fragment.clear();
        for (int i = 0; i < myState.fragment.length; i++) {
            fragment.add(Slices.wrappedBuffer(myState.fragment[i]));
        }
        this.computedStatistics.clear();
        for (int i = 0; i < myState.computedStatistics.length; i++) {
            computedStatistics.add(ComputedStatistics.restoreComputedStatistics(myState.computedStatistics[i], serdeProvider));
        }
        this.statisticsTiming.restore(myState.statisticsTiming, serdeProvider);
    }

    private static class TableFinishOperatorState
            implements Serializable
    {
        private Object operatorContext;
        private Object statisticsAggregationOperator;
        private String state;
        private long rowCount;
        private byte[][] fragment;
        private Object[] computedStatistics;
        private Object statisticsTiming;
    }
}
