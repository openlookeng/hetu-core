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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.execution.TableExecuteContext;
import io.prestosql.execution.TableExecuteContextManager;
import io.prestosql.snapshot.SingleInputSnapshotState;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static java.util.Objects.requireNonNull;

public class CacheTableFinishOperator
        implements Operator
{
    private static final Logger LOG = Logger.get(CacheTableFinishOperator.class);

    public static class CacheTableFinishOperatorFactory
            implements OperatorFactory
    {
        private final TableExecuteContextManager tableExecuteContextManager;
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final TableFinishOperator.TableFinisher tableFinisher;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private final Session session;
        private boolean closed;
        private final long thresholdSize;
        private final BiFunction<Long, Long, Void> commit;
        private final Callable<Void> abort;

        public CacheTableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinishOperator.TableFinisher tableFinisher,
                StatisticAggregationsDescriptor<Integer> descriptor,
                TableExecuteContextManager tableExecuteContextManager,
                Session session,
                long thresholdSize,
                BiFunction<Long, Long, Void> commit,
                Callable<Void> abort)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableFinisher is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.tableExecuteContextManager = requireNonNull(tableExecuteContextManager, "tableExecuteContextManager is null");
            this.session = requireNonNull(session, "session is null");
            this.thresholdSize = thresholdSize;
            this.commit = requireNonNull(commit, "commit is null");
            this.abort = requireNonNull(abort, "abort is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            boolean cpuTimerEnabled = isStatisticsCpuTimerEnabled(session);
            QueryId queryId = driverContext.getPipelineContext().getTaskContext().getQueryContext().getQueryId();
            TableExecuteContext tableExecuteContextForQuery = tableExecuteContextManager.getTableExecuteContextForQuery(queryId);
            return new CacheTableFinishOperator(context, tableFinisher, descriptor, tableExecuteContextForQuery, cpuTimerEnabled, thresholdSize, commit, abort);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CacheTableFinishOperator.CacheTableFinishOperatorFactory(operatorId, planNodeId, tableFinisher, descriptor, tableExecuteContextManager, session, thresholdSize, commit, abort);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableFinishOperator.TableFinisher tableFinisher;
    private final StatisticAggregationsDescriptor<Integer> descriptor;

    private State state = State.RUNNING;
    private Optional<ConnectorOutputMetadata> outputMetadata = Optional.empty();
    private final List<Slice> fragment = new ArrayList<>();
    private final List<ComputedStatistics> computedStatistics = new ArrayList<>();

    private final OperationTimer.OperationTiming statisticsTiming = new OperationTimer.OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final TableExecuteContext tableExecuteContext;

    private final SingleInputSnapshotState snapshotState;

    private Queue<Page> outputPages = new LinkedList<>();

    private final long thresholdSize;
    private long currentSize;
    private final BiFunction<Long, Long, Void> commit;
    private final Callable<Void> abort;

    public CacheTableFinishOperator(
            OperatorContext operatorContext,
            TableFinishOperator.TableFinisher tableFinisher,
            StatisticAggregationsDescriptor<Integer> descriptor,
            TableExecuteContext tableExecuteContext,
            boolean statisticsCpuTimerEnabled,
            long thresholdSize,
            BiFunction<Long, Long, Void> commit,
            Callable<Void> abort)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.snapshotState = operatorContext.isSnapshotEnabled() ? SingleInputSnapshotState.forOperator(this, operatorContext) : null;
        this.tableExecuteContext = requireNonNull(tableExecuteContext, "tableExecuteContext is null");
        this.thresholdSize = thresholdSize;
        this.commit = requireNonNull(commit, "commit is null");
        this.abort = requireNonNull(abort, "abort is null");
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
            return true;
        }
        return false;
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING) {
            return false;
        }
        return true;
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

        currentSize += page.getSizeInBytes();

        outputPages.add(page);
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

        if (outputPages.isEmpty()) {
            if (state == State.FINISHING) {
                if (currentSize <= thresholdSize) {
                    outputMetadata = tableFinisher.finishTable(ImmutableList.copyOf(fragment), ImmutableList.copyOf(computedStatistics), tableExecuteContext);
                    commit.apply(System.currentTimeMillis(), currentSize);
                }
                else {
                    try {
                        abort.call();
                    }
                    catch (Exception e) {
                        LOG.debug("abort call to cache failed with exception: " + e.getMessage());
                    }
                }
                state = State.FINISHED;
            }
        }

        return outputPages.poll();
    }

    @Override
    public Page pollMarker()
    {
        return snapshotState.nextMarker();
    }

    @Override
    public void close()
            throws Exception
    {
        if (snapshotState != null) {
            snapshotState.close();
        }
    }
}
