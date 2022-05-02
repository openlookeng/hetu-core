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

package io.prestosql.execution;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.stats.Distribution;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.operator.BlockedReason;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.PipelineStats;
import io.prestosql.operator.TaskStats;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.util.Failures;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctDuration;
import static io.prestosql.SystemSessionProperties.isReuseTableScanEnabled;
import static io.prestosql.execution.StageState.ABORTED;
import static io.prestosql.execution.StageState.CANCELED;
import static io.prestosql.execution.StageState.FAILED;
import static io.prestosql.execution.StageState.FINISHED;
import static io.prestosql.execution.StageState.PLANNED;
import static io.prestosql.execution.StageState.RESCHEDULING;
import static io.prestosql.execution.StageState.RESUMABLE_FAILURE;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULED;
import static io.prestosql.execution.StageState.SCHEDULING;
import static io.prestosql.execution.StageState.SCHEDULING_SPLITS;
import static io.prestosql.execution.StageState.TERMINAL_STAGE_STATES;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_CONSUMER;
import static io.prestosql.spi.operator.ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_PRODUCER;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class StageStateMachine
{
    private static final Logger LOG = Logger.get(StageStateMachine.class);

    private final StageId stageId;
    private final URI location;
    private final PlanFragment fragment;
    private final Session session;
    private final Map<PlanNodeId, TableInfo> tables;
    private final SplitSchedulerStats scheduledStats;

    private final StateMachine<StageState> stageState;
    private final StateMachine<Optional<StageInfo>> finalStageInfo;
    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<DateTime> schedulingComplete = new AtomicReference<>();
    private final Distribution getSplitDistribution = new Distribution();

    private final AtomicLong peakUserMemory = new AtomicLong();
    private final AtomicLong peakRevocableMemory = new AtomicLong();
    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong currentRevocableMemory = new AtomicLong();
    private final AtomicLong currentTotalMemory = new AtomicLong();
    private final TableScanNode consumerScanNode;
    private final TableScanNode producerScanNode;

    public StageStateMachine(
            StageId stageId,
            URI location,
            Session session,
            PlanFragment fragment,
            Map<PlanNodeId, TableInfo> tables,
            ExecutorService executor,
            SplitSchedulerStats schedulerStats)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.location = requireNonNull(location, "location is null");
        this.session = requireNonNull(session, "session is null");
        this.fragment = requireNonNull(fragment, "fragment is null");
        this.tables = ImmutableMap.copyOf(requireNonNull(tables, "tables is null"));
        this.scheduledStats = requireNonNull(schedulerStats, "schedulerStats is null");

        stageState = new StateMachine<>("stage " + stageId, executor, PLANNED, TERMINAL_STAGE_STATES);
        stageState.addStateChangeListener(state -> LOG.debug("Stage %s is %s", stageId, state));

        finalStageInfo = new StateMachine<>("final stage " + stageId, executor, Optional.empty());

        if (isReuseTableScanEnabled(session)) {
            consumerScanNode = fragment.getPartitionedSourceNodes().stream()
                    .filter(node -> node instanceof TableScanNode && ((TableScanNode) node).getStrategy()
                            .equals(REUSE_STRATEGY_CONSUMER)).findAny().map(TableScanNode.class::cast).orElse(null);
            producerScanNode = fragment.getPartitionedSourceNodes().stream()
                    .filter(node -> node instanceof TableScanNode && ((TableScanNode) node).getStrategy()
                            .equals(REUSE_STRATEGY_PRODUCER)).findAny().map(TableScanNode.class::cast).orElse(null);
        }
        else {
            consumerScanNode = null;
            producerScanNode = null;
        }
    }

    public StageId getStageId()
    {
        return stageId;
    }

    public URI getLocation()
    {
        return location;
    }

    public Session getSession()
    {
        return session;
    }

    public StageState getState()
    {
        return stageState.get();
    }

    public PlanFragment getFragment()
    {
        return fragment;
    }

    public TableScanNode getConsumerScanNode()
    {
        return consumerScanNode;
    }

    public TableScanNode getProducerScanNode()
    {
        return producerScanNode;
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<StageState> stateChangeListener)
    {
        stageState.addStateChangeListener(stateChangeListener);
    }

    public synchronized boolean transitionToScheduling()
    {
        return stageState.compareAndSet(PLANNED, SCHEDULING);
    }

    public synchronized boolean transitionToSchedulingSplits()
    {
        return stageState.setIf(SCHEDULING_SPLITS, currentState -> currentState == PLANNED || currentState == SCHEDULING);
    }

    public synchronized boolean transitionToScheduled()
    {
        schedulingComplete.compareAndSet(null, DateTime.now());
        return stageState.setIf(SCHEDULED, currentState -> currentState == PLANNED || currentState == SCHEDULING || currentState == SCHEDULING_SPLITS);
    }

    public boolean transitionToRunning()
    {
        return stageState.setIf(RUNNING, currentState -> currentState != RUNNING && !currentState.isDone());
    }

    public boolean transitionToFinished()
    {
        SqlStageExecution.setReuseTableScanMappingIdStatus(this);
        return stageState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToCanceled()
    {
        SqlStageExecution.setReuseTableScanMappingIdStatus(this);
        return stageState.setIf(CANCELED, currentState -> !currentState.isDone());
    }

    public boolean transitionToAborted()
    {
        SqlStageExecution.setReuseTableScanMappingIdStatus(this);
        return stageState.setIf(ABORTED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        SqlStageExecution.setReuseTableScanMappingIdStatus(this);
        failureCause.compareAndSet(null, Failures.toFailure(throwable));
        boolean failed = stageState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            LOG.error(throwable, "Stage %s failed", stageId);
        }
        else {
            LOG.debug(throwable, "Failure after stage %s finished", stageId);
        }
        return failed;
    }

    public boolean transitionToResumableFailure()
    {
        LOG.debug("Moving stage %s to Resumable Failure state", stageId);
        return stageState.setIf(RESUMABLE_FAILURE, currentState -> !currentState.isDone());
    }

    public boolean transitionToRescheduling()
    {
        LOG.debug("Moving stage %s to Rescheduling state", stageId);
        // Force it, even when the stage is in FINISHED state, which was before the resume occurred
        return stageState.forceSet(RESCHEDULING) == RESCHEDULING;
    }

    /**
     * Add a listener for the final stage info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addFinalStageInfoListener(StateChangeListener<StageInfo> finalStatusListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<StageInfo>> fireOnceStateChangeListener = finalStageInfo -> {
            if (finalStageInfo.isPresent() && done.compareAndSet(false, true)) {
                finalStatusListener.stateChanged(finalStageInfo.get());
            }
        };
        finalStageInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public void setAllTasksFinal(Iterable<TaskInfo> finalTaskInfos, boolean restoreInProgress, long captureSnapshotId)
    {
        requireNonNull(finalTaskInfos, "finalTaskInfos is null");
        checkState(stageState.get().isDone());
        StageInfo stageInfo = getStageInfo(() -> finalTaskInfos, restoreInProgress, captureSnapshotId);
        checkArgument(stageInfo.isCompleteInfo(), "finalTaskInfos are not all done");
        finalStageInfo.compareAndSet(Optional.empty(), Optional.of(stageInfo));
    }

    public long getUserMemoryReservation()
    {
        return currentUserMemory.get();
    }

    public long getTotalMemoryReservation()
    {
        return currentTotalMemory.get();
    }

    public void updateMemoryUsage(long deltaUserMemoryInBytes, long deltaRevocableMemoryInBytes, long deltaTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentRevocableMemory.addAndGet(deltaRevocableMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> max(currentUserMemory.get(), currentPeakValue));
        peakRevocableMemory.updateAndGet(currentPeakValue -> max(currentRevocableMemory.get(), currentPeakValue));
    }

    public BasicStageStats getBasicStageStats(Supplier<Iterable<TaskInfo>> taskInfosSupplier)
    {
        Optional<StageInfo> localFinalStageInfo = this.finalStageInfo.get();
        if (localFinalStageInfo.isPresent()) {
            return localFinalStageInfo.get()
                    .getStageStats()
                    .toBasicStageStats(localFinalStageInfo.get().getState());
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();
        // Snapshot: RESCHEDULING, although a done state for stage, should not be deemed as "scheduled".
        boolean isScheduled = (state == RUNNING) || state.isDone() && state != RESCHEDULING;

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int completedDrivers = 0;

        long cumulativeUserMemory = 0;
        long userMemoryReservation = 0;
        long totalMemoryReservation = 0;

        long totalScheduledTime = 0;
        long totalCpuTime = 0;

        long physicalInputDataSize = 0;
        long physicalInputPositions = 0;

        long internalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            TaskStats taskStats = taskInfo.getStats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();

            long taskUserMemory = taskStats.getUserMemoryReservation().toBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservation().toBytes();
            long taskRevocableMemory = taskStats.getRevocableMemoryReservation().toBytes();
            userMemoryReservation += taskUserMemory;
            totalMemoryReservation += taskUserMemory + taskSystemMemory + taskRevocableMemory;

            totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            physicalInputDataSize += taskStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += taskStats.getPhysicalInputPositions();

            internalNetworkInputDataSize += taskStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += taskStats.getInternalNetworkInputPositions();

            if (fragment.getPartitionedSourceNodes().stream().anyMatch(TableScanNode.class::isInstance)) {
                rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
                rawInputPositions += taskStats.getRawInputPositions();
            }
        }

        OptionalDouble progressPercentage = OptionalDouble.empty();
        if (isScheduled && totalDrivers != 0) {
            progressPercentage = OptionalDouble.of(min(100, (completedDrivers * 100.0) / totalDrivers));
        }

        return new BasicStageStats(
                isScheduled,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                completedDrivers,

                succinctBytes(physicalInputDataSize),
                physicalInputPositions,

                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,

                succinctBytes(rawInputDataSize),
                rawInputPositions,

                cumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(totalMemoryReservation),

                new Duration(totalCpuTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalScheduledTime, NANOSECONDS).convertToMostSuccinctTimeUnit(),

                fullyBlocked,
                blockedReasons,

                progressPercentage);
    }

    public StageInfo getStageInfo(Supplier<Iterable<TaskInfo>> taskInfosSupplier, boolean restoreInProgress, long snapshotId)
    {
        Optional<StageInfo> localFinalStageInfo = this.finalStageInfo.get();
        if (localFinalStageInfo.isPresent()) {
            return localFinalStageInfo.get();
        }

        // stage state must be captured first in order to provide a
        // consistent view of the stage. For example, building this
        // information, the stage could finish, and the task states would
        // never be visible.
        StageState state = stageState.get();

        List<TaskInfo> taskInfos = ImmutableList.copyOf(taskInfosSupplier.get());

        int totalTasks = taskInfos.size();
        int runningTasks = 0;
        int completedTasks = 0;

        int totalDrivers = 0;
        int queuedDrivers = 0;
        int runningDrivers = 0;
        int blockedDrivers = 0;
        int completedDrivers = 0;

        long cumulativeUserMemory = 0;
        long userMemoryReservation = 0;
        long revocableMemoryReservation = 0;
        long totalMemoryReservation = 0;
        long peakUserMemoryReservation = peakUserMemory.get();
        long peakRevocableMemoryReservation = peakRevocableMemory.get();

        long totalScheduledTime = 0;
        long totalCpuTime = 0;
        long totalBlockedTime = 0;

        long physicalInputDataSize = 0;
        long physicalInputPositions = 0;

        long internalNetworkInputDataSize = 0;
        long internalNetworkInputPositions = 0;

        long rawInputDataSize = 0;
        long rawInputPositions = 0;

        long processedInputDataSize = 0;
        long processedInputPositions = 0;

        long bufferedDataSize = 0;
        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        int fullGcCount = 0;
        int fullGcTaskCount = 0;
        int minFullGcSec = 0;
        int maxFullGcSec = 0;
        int totalFullGcSec = 0;

        boolean fullyBlocked = true;
        Set<BlockedReason> blockedReasons = new HashSet<>();

        Map<String, OperatorStats> operatorToStats = new HashMap<>();
        for (TaskInfo taskInfo : taskInfos) {
            TaskState taskState = taskInfo.getTaskStatus().getState();
            if (taskState.isDone()) {
                completedTasks++;
            }
            else {
                runningTasks++;
            }

            TaskStats taskStats = taskInfo.getStats();

            totalDrivers += taskStats.getTotalDrivers();
            queuedDrivers += taskStats.getQueuedDrivers();
            runningDrivers += taskStats.getRunningDrivers();
            blockedDrivers += taskStats.getBlockedDrivers();
            completedDrivers += taskStats.getCompletedDrivers();

            cumulativeUserMemory += taskStats.getCumulativeUserMemory();

            long taskUserMemory = taskStats.getUserMemoryReservation().toBytes();
            long taskSystemMemory = taskStats.getSystemMemoryReservation().toBytes();
            long taskRevocableMemory = taskStats.getRevocableMemoryReservation().toBytes();
            userMemoryReservation += taskUserMemory;
            revocableMemoryReservation += taskRevocableMemory;
            totalMemoryReservation += taskUserMemory + taskSystemMemory + taskRevocableMemory;

            totalScheduledTime += taskStats.getTotalScheduledTime().roundTo(NANOSECONDS);
            totalCpuTime += taskStats.getTotalCpuTime().roundTo(NANOSECONDS);
            totalBlockedTime += taskStats.getTotalBlockedTime().roundTo(NANOSECONDS);
            if (!taskState.isDone()) {
                fullyBlocked &= taskStats.isFullyBlocked();
                blockedReasons.addAll(taskStats.getBlockedReasons());
            }

            physicalInputDataSize += taskStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += taskStats.getPhysicalInputPositions();

            internalNetworkInputDataSize += taskStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += taskStats.getInternalNetworkInputPositions();

            rawInputDataSize += taskStats.getRawInputDataSize().toBytes();
            rawInputPositions += taskStats.getRawInputPositions();

            processedInputDataSize += taskStats.getProcessedInputDataSize().toBytes();
            processedInputPositions += taskStats.getProcessedInputPositions();

            bufferedDataSize += taskInfo.getOutputBuffers().getTotalBufferedBytes();
            outputDataSize += taskStats.getOutputDataSize().toBytes();
            outputPositions += taskStats.getOutputPositions();

            physicalWrittenDataSize += taskStats.getPhysicalWrittenDataSize().toBytes();

            fullGcCount += taskStats.getFullGcCount();
            fullGcTaskCount += taskStats.getFullGcCount() > 0 ? 1 : 0;

            int gcSec = toIntExact(taskStats.getFullGcTime().roundTo(SECONDS));
            totalFullGcSec += gcSec;
            minFullGcSec = min(minFullGcSec, gcSec);
            maxFullGcSec = max(maxFullGcSec, gcSec);

            for (PipelineStats pipeline : taskStats.getPipelines()) {
                for (OperatorStats operatorStats : pipeline.getOperatorSummaries()) {
                    String id = pipeline.getPipelineId() + "." + operatorStats.getOperatorId();
                    operatorToStats.compute(id, (k, v) -> v == null ? operatorStats : v.add(operatorStats));
                }
            }
        }

        StageStats stageStats = new StageStats(
                schedulingComplete.get(),
                getSplitDistribution.snapshot(),

                totalTasks,
                runningTasks,
                completedTasks,

                totalDrivers,
                queuedDrivers,
                runningDrivers,
                blockedDrivers,
                completedDrivers,

                cumulativeUserMemory,
                succinctBytes(userMemoryReservation),
                succinctBytes(revocableMemoryReservation),
                succinctBytes(totalMemoryReservation),
                succinctBytes(peakUserMemoryReservation),
                succinctBytes(peakRevocableMemoryReservation),
                succinctDuration(totalScheduledTime, NANOSECONDS),
                succinctDuration(totalCpuTime, NANOSECONDS),
                succinctDuration(totalBlockedTime, NANOSECONDS),
                fullyBlocked && runningTasks > 0,
                blockedReasons,

                succinctBytes(physicalInputDataSize),
                physicalInputPositions,

                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,

                succinctBytes(rawInputDataSize),
                rawInputPositions,

                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(bufferedDataSize),
                succinctBytes(outputDataSize),
                outputPositions,
                succinctBytes(physicalWrittenDataSize),

                new StageGcStatistics(
                        stageId.getId(),
                        totalTasks,
                        fullGcTaskCount,
                        minFullGcSec,
                        maxFullGcSec,
                        totalFullGcSec,
                        (int) (1.0 * totalFullGcSec / fullGcCount)),

                ImmutableList.copyOf(operatorToStats.values()));

        ExecutionFailureInfo failureInfo = null;
        if (state == FAILED) {
            failureInfo = failureCause.get();
        }
        return new StageInfo(stageId,
                state,
                restoreInProgress,
                snapshotId,
                location,
                fragment,
                fragment.getTypes(),
                stageStats,
                taskInfos,
                ImmutableList.of(),
                tables,
                failureInfo);
    }

    public void recordGetSplitTime(long startNanos)
    {
        long elapsedNanos = System.nanoTime() - startNanos;
        getSplitDistribution.add(elapsedNanos);
        scheduledStats.getGetSplitTime().add(elapsedNanos, NANOSECONDS);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("stageId", stageId)
                .add("stageState", stageState)
                .toString();
    }
}
