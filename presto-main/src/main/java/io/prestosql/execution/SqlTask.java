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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.LazyOutputBuffer;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.memory.QueryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.operator.PipelineContext;
import io.prestosql.operator.PipelineStatus;
import io.prestosql.operator.TaskContext;
import io.prestosql.operator.TaskStats;
import io.prestosql.snapshot.RestoreResult;
import io.prestosql.snapshot.SnapshotInfo;
import io.prestosql.snapshot.TaskSnapshotManager;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.sql.planner.PlanFragment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.connector.DataCenterUtility.loadDCCatalogForUpdateTask;
import static io.prestosql.execution.TaskState.ABORTED;
import static io.prestosql.execution.TaskState.CANCELED_TO_RESUME;
import static io.prestosql.execution.TaskState.FAILED;
import static io.prestosql.util.Failures.toFailures;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class SqlTask
{
    private static final Logger log = Logger.get(SqlTask.class);

    private final TaskId taskId;
    private final String taskInstanceId;
    // confirmationInstanceId is generated from the task, and is used confirm in ContinuousTaskStatusFetcher that
    // we are continuing to talk to the same task
    private final String confirmationInstanceId;
    private final URI location;
    private final String nodeId;
    private final TaskStateMachine taskStateMachine;
    private final OutputBuffer outputBuffer;
    private final QueryContext queryContext;

    private final SqlTaskExecutionFactory sqlTaskExecutionFactory;

    private final AtomicReference<DateTime> lastHeartbeat = new AtomicReference<>(DateTime.now());
    private final AtomicLong nextTaskInfoVersion = new AtomicLong(TaskStatus.STARTING_VERSION);

    private final AtomicReference<TaskHolder> taskHolderReference = new AtomicReference<>(new TaskHolder());
    private final AtomicBoolean needsPlan = new AtomicBoolean(true);
    private final Metadata metadata;
    private boolean isSnapshotEnabled;

    public static SqlTask createSqlTask(
            TaskId taskId,
            String instanceId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            Function<SqlTask, ?> onDone,
            DataSize maxBufferSize,
            CounterStat failedTasks,
            Metadata metadata)
    {
        SqlTask sqlTask = new SqlTask(taskId, instanceId, location, nodeId, queryContext, sqlTaskExecutionFactory, taskNotificationExecutor, maxBufferSize, metadata);
        sqlTask.initialize(onDone, failedTasks);
        return sqlTask;
    }

    private SqlTask(
            TaskId taskId,
            String instanceId,
            URI location,
            String nodeId,
            QueryContext queryContext,
            SqlTaskExecutionFactory sqlTaskExecutionFactory,
            ExecutorService taskNotificationExecutor,
            DataSize maxBufferSize,
            Metadata metadata)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.taskInstanceId = requireNonNull(instanceId, "instanceId is null");
        this.confirmationInstanceId = UUID.randomUUID().toString();
        this.location = requireNonNull(location, "location is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.queryContext = requireNonNull(queryContext, "queryContext is null");
        this.sqlTaskExecutionFactory = requireNonNull(sqlTaskExecutionFactory, "sqlTaskExecutionFactory is null");
        this.metadata = requireNonNull(metadata, "requireNonNull is null");
        requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        requireNonNull(maxBufferSize, "maxBufferSize is null");

        outputBuffer = new LazyOutputBuffer(
                taskId,
                taskNotificationExecutor,
                maxBufferSize,
                // Pass a memory context supplier instead of a memory context to the output buffer,
                // because we haven't created the task context that holds the the memory context yet.
                () -> queryContext.getTaskContext(taskInstanceId).localSystemMemoryContext());
        taskStateMachine = new TaskStateMachine(taskId, taskNotificationExecutor);

        log.debug("Created new SqlTask object for task %s, with instanceId %s", taskId, taskInstanceId);
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize(Function<SqlTask, ?> onDone, CounterStat failedTasks)
    {
        requireNonNull(onDone, "onDone is null");
        requireNonNull(failedTasks, "failedTasks is null");
        taskStateMachine.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                if (!newState.isDone()) {
                    return;
                }

                // Update failed tasks counter
                if (newState == FAILED) {
                    failedTasks.update(1);
                }

                // store final task info
                while (true) {
                    TaskHolder taskHolder = taskHolderReference.get();
                    if (taskHolder.isFinished()) {
                        // another concurrent worker already set the final state
                        return;
                    }

                    if (taskHolderReference.compareAndSet(taskHolder, new TaskHolder(createTaskInfo(taskHolder), taskHolder.getIoStats()))) {
                        break;
                    }
                }

                // make sure buffers are cleaned up
                if (newState == FAILED || newState == ABORTED || newState == CANCELED_TO_RESUME) {
                    // don't close buffers for a failed query
                    // closed buffers signal to upstream tasks that everything finished cleanly
                    outputBuffer.fail();
                }
                else {
                    outputBuffer.destroy();
                }

                try {
                    onDone.apply(SqlTask.this);
                }
                catch (Exception e) {
                    log.warn(e, "Error running task cleanup callback %s", SqlTask.this.taskId);
                }
            }
        });
    }

    public boolean isOutputBufferOverutilized()
    {
        return outputBuffer.isOverutilized();
    }

    public SqlTaskIoStats getIoStats()
    {
        return taskHolderReference.get().getIoStats();
    }

    public TaskId getTaskId()
    {
        return taskStateMachine.getTaskId();
    }

    public String getTaskInstanceId()
    {
        return taskInstanceId;
    }

    public void recordHeartbeat()
    {
        lastHeartbeat.set(DateTime.now());
    }

    public TaskInfo getTaskInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskInfo(taskHolderReference.get());
        }
    }

    public TaskStatus getTaskStatus()
    {
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskId)) {
            return createTaskStatus(taskHolderReference.get());
        }
    }

    private TaskStatus createTaskStatus(TaskHolder taskHolder)
    {
        // Always return a new TaskInfo with a larger version number;
        // otherwise a client will not accept the update
        long versionNumber = nextTaskInfoVersion.getAndIncrement();

        TaskState state = taskStateMachine.getState();
        List<ExecutionFailureInfo> failures = ImmutableList.of();
        if (state == FAILED) {
            failures = toFailures(taskStateMachine.getFailureCauses());
        }

        int queuedPartitionedDrivers = 0;
        int runningPartitionedDrivers = 0;
        DataSize physicalWrittenDataSize = new DataSize(0, BYTE);
        DataSize userMemoryReservation = new DataSize(0, BYTE);
        DataSize systemMemoryReservation = new DataSize(0, BYTE);
        DataSize revocableMemoryReservation = new DataSize(0, BYTE);
        // TODO: add a mechanism to avoid sending the whole completedDriverGroups set over the wire for every task status reply
        Set<Lifespan> completedDriverGroups = ImmutableSet.of();
        long fullGcCount = 0;
        Duration fullGcTime = new Duration(0, MILLISECONDS);
        Map<Long, SnapshotInfo> snapshotCaptureResult = ImmutableMap.of();
        Optional<RestoreResult> snapshotRestoreResult = Optional.empty();
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            TaskStats taskStats = finalTaskInfo.getStats();
            queuedPartitionedDrivers = taskStats.getQueuedPartitionedDrivers();
            runningPartitionedDrivers = taskStats.getRunningPartitionedDrivers();
            physicalWrittenDataSize = taskStats.getPhysicalWrittenDataSize();
            userMemoryReservation = taskStats.getUserMemoryReservation();
            systemMemoryReservation = taskStats.getSystemMemoryReservation();
            revocableMemoryReservation = taskStats.getRevocableMemoryReservation();
            fullGcCount = taskStats.getFullGcCount();
            fullGcTime = taskStats.getFullGcTime();

            if (isSnapshotEnabled) {
                // Add snapshot result
                snapshotCaptureResult = finalTaskInfo.getTaskStatus().getSnapshotCaptureResult();
                snapshotRestoreResult = finalTaskInfo.getTaskStatus().getSnapshotRestoreResult();
            }
        }
        else if (taskHolder.getTaskExecution() != null) {
            long physicalWrittenBytes = 0;
            TaskContext taskContext = taskHolder.getTaskExecution().getTaskContext();
            for (PipelineContext pipelineContext : taskContext.getPipelineContexts()) {
                PipelineStatus pipelineStatus = pipelineContext.getPipelineStatus();
                queuedPartitionedDrivers += pipelineStatus.getQueuedPartitionedDrivers();
                runningPartitionedDrivers += pipelineStatus.getRunningPartitionedDrivers();
                physicalWrittenBytes += pipelineContext.getPhysicalWrittenDataSize();
            }
            physicalWrittenDataSize = succinctBytes(physicalWrittenBytes);
            userMemoryReservation = taskContext.getMemoryReservation();
            systemMemoryReservation = taskContext.getSystemMemoryReservation();
            revocableMemoryReservation = taskContext.getRevocableMemoryReservation();
            completedDriverGroups = taskContext.getCompletedDriverGroups();
            fullGcCount = taskContext.getFullGcCount();
            fullGcTime = taskContext.getFullGcTime();

            if (isSnapshotEnabled) {
                // Add snapshot result
                TaskSnapshotManager snapshotManager = taskHolder.taskExecution.getTaskContext().getSnapshotManager();
                snapshotCaptureResult = snapshotManager.getSnapshotCaptureResult();
                snapshotRestoreResult = Optional.ofNullable(snapshotManager.getSnapshotRestoreResult());
            }
        }

        return new TaskStatus(taskStateMachine.getTaskId(),
                confirmationInstanceId,
                versionNumber,
                state,
                location,
                nodeId,
                completedDriverGroups,
                failures,
                queuedPartitionedDrivers,
                runningPartitionedDrivers,
                isOutputBufferOverutilized(),
                physicalWrittenDataSize,
                userMemoryReservation,
                systemMemoryReservation,
                revocableMemoryReservation,
                fullGcCount,
                fullGcTime,
                snapshotCaptureResult,
                snapshotRestoreResult);
    }

    private TaskStats getTaskStats(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getStats();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getTaskContext().getTaskStats();
        }
        // if the task completed without creation, set end time
        DateTime endTime = taskStateMachine.getState().isDone() ? DateTime.now() : null;
        return new TaskStats(taskStateMachine.getCreatedTime(), endTime);
    }

    private static Set<PlanNodeId> getNoMoreSplits(TaskHolder taskHolder)
    {
        TaskInfo finalTaskInfo = taskHolder.getFinalTaskInfo();
        if (finalTaskInfo != null) {
            return finalTaskInfo.getNoMoreSplits();
        }
        SqlTaskExecution taskExecution = taskHolder.getTaskExecution();
        if (taskExecution != null) {
            return taskExecution.getNoMoreSplits();
        }
        return ImmutableSet.of();
    }

    private TaskInfo createTaskInfo(TaskHolder taskHolder)
    {
        TaskStats taskStats = getTaskStats(taskHolder);
        Set<PlanNodeId> noMoreSplits = getNoMoreSplits(taskHolder);

        TaskStatus taskStatus = createTaskStatus(taskHolder);

        return new TaskInfo(
                taskStatus,
                lastHeartbeat.get(),
                outputBuffer.getInfo(),
                noMoreSplits,
                taskStats,
                needsPlan.get());
    }

    public ListenableFuture<TaskStatus> getTaskStatus(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskStatus());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskStatus(), directExecutor());
    }

    public ListenableFuture<TaskInfo> getTaskInfo(TaskState callersCurrentState)
    {
        requireNonNull(callersCurrentState, "callersCurrentState is null");

        // If the caller's current state is already done, just return the current
        // state of this task as it will either be done or possibly still running
        // (due to a bug in the caller), since we can not transition from a done
        // state.
        if (callersCurrentState.isDone()) {
            return immediateFuture(getTaskInfo());
        }

        ListenableFuture<TaskState> futureTaskState = taskStateMachine.getStateChange(callersCurrentState);
        return Futures.transform(futureTaskState, input -> getTaskInfo(), directExecutor());
    }

    public TaskInfo updateTask(Session session, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions, Optional<PlanNodeId> consumer,
            Map<String, CommonTableExecutionContext> cteCtx)
    {
        try {
            // The LazyOutput buffer does not support write methods, so the actual
            // output buffer must be established before drivers are created (e.g.
            // a VALUES query).
            outputBuffer.setOutputBuffers(outputBuffers);

            // assure the task execution is only created once
            SqlTaskExecution taskExecution;
            synchronized (this) {
                // is task already complete?
                TaskHolder taskHolder = taskHolderReference.get();
                if (taskHolder.isFinished()) {
                    return taskHolder.getFinalTaskInfo();
                }
                taskExecution = taskHolder.getTaskExecution();
                if (taskExecution == null) {
                    checkState(fragment.isPresent(), "fragment must be present");
                    loadDCCatalogForUpdateTask(metadata, sources);
                    taskExecution = sqlTaskExecutionFactory.create(taskInstanceId, session, queryContext, taskStateMachine, outputBuffer, fragment.get(), sources, totalPartitions, consumer, cteCtx);
                    taskHolderReference.compareAndSet(taskHolder, new TaskHolder(taskExecution));
                    needsPlan.set(false);
                    isSnapshotEnabled = SystemSessionProperties.isSnapshotEnabled(session);
                }
            }

            if (taskExecution != null) {
                taskExecution.addSources(sources);
            }
        }
        catch (Error e) {
            failed(e);
            throw e;
        }
        catch (RuntimeException e) {
            failed(e);
        }

        return getTaskInfo();
    }

    public ListenableFuture<BufferResult> getTaskResults(OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return outputBuffer.get(bufferId, startingSequenceId, maxSize);
    }

    public void acknowledgeTaskResults(OutputBufferId bufferId, long sequenceId)
    {
        requireNonNull(bufferId, "bufferId is null");

        outputBuffer.acknowledge(bufferId, sequenceId);
    }

    public TaskInfo abortTaskResults(OutputBufferId bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        log.debug("Aborting task %s output %s", taskId, bufferId);
        outputBuffer.abort(bufferId);

        return getTaskInfo();
    }

    public void failed(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        taskStateMachine.failed(cause);
    }

    public TaskInfo cancel(TaskState targetState)
    {
        taskStateMachine.cancel(targetState);
        return getTaskInfo();
    }

    @Override
    public String toString()
    {
        return taskId.toString();
    }

    private static final class TaskHolder
    {
        private final SqlTaskExecution taskExecution;
        private final TaskInfo finalTaskInfo;
        private final SqlTaskIoStats finalIoStats;

        private TaskHolder()
        {
            this.taskExecution = null;
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(SqlTaskExecution taskExecution)
        {
            this.taskExecution = requireNonNull(taskExecution, "taskExecution is null");
            this.finalTaskInfo = null;
            this.finalIoStats = null;
        }

        private TaskHolder(TaskInfo finalTaskInfo, SqlTaskIoStats finalIoStats)
        {
            this.taskExecution = null;
            this.finalTaskInfo = requireNonNull(finalTaskInfo, "finalTaskInfo is null");
            this.finalIoStats = requireNonNull(finalIoStats, "finalIoStats is null");
        }

        public boolean isFinished()
        {
            return finalTaskInfo != null;
        }

        @Nullable
        public SqlTaskExecution getTaskExecution()
        {
            return taskExecution;
        }

        @Nullable
        public TaskInfo getFinalTaskInfo()
        {
            return finalTaskInfo;
        }

        public SqlTaskIoStats getIoStats()
        {
            // if we are finished, return the final IoStats
            if (finalIoStats != null) {
                return finalIoStats;
            }
            // if we haven't started yet, return an empty IoStats
            if (taskExecution == null) {
                return new SqlTaskIoStats();
            }
            // get IoStats from the current task execution
            TaskContext taskContext = taskExecution.getTaskContext();
            return new SqlTaskIoStats(taskContext.getProcessedInputDataSize(), taskContext.getInputPositions(), taskContext.getOutputDataSize(), taskContext.getOutputPositions());
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskStateMachine.addStateChangeListener(stateChangeListener);
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public boolean isSnapshotEnabled()
    {
        return isSnapshotEnabled;
    }
}
