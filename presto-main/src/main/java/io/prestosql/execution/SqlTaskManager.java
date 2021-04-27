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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.stats.CounterStat;
import io.airlift.stats.GcMonitor;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.event.SplitMonitor;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.LocalMemoryManager;
import io.prestosql.memory.MemoryPool;
import io.prestosql.memory.MemoryPoolAssignment;
import io.prestosql.memory.MemoryPoolAssignmentsRequest;
import io.prestosql.memory.NodeMemoryConfig;
import io.prestosql.memory.QueryContext;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.CommonTableExecutionContext;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spiller.LocalSpillManager;
import io.prestosql.spiller.NodeSpillConfig;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.PlanFragment;
import org.joda.time.DateTime;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.notNull;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.SystemSessionProperties.resourceOvercommit;
import static io.prestosql.execution.SqlTask.createSqlTask;
import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static io.prestosql.memory.LocalMemoryManager.RESERVED_POOL;
import static io.prestosql.spi.StandardErrorCode.ABANDONED_TASK;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class SqlTaskManager
        implements TaskManager, Closeable
{
    private static final Logger log = Logger.get(SqlTaskManager.class);

    private final ExecutorService taskNotificationExecutor;
    private final ThreadPoolExecutorMBean taskNotificationExecutorMBean;

    private final ScheduledExecutorService taskManagementExecutor;
    private final ScheduledExecutorService driverYieldExecutor;

    private final Duration infoCacheTime;
    private final Duration clientTimeout;

    private final LocalMemoryManager localMemoryManager;
    private final LoadingCache<QueryId, QueryContext> queryContexts;
    private final LoadingCache<TaskId, SqlTask> tasks;
    // Snapshot: Keep track of which task instances are available.
    // Only requests with an available instance id are answered.
    // Can't rely on task-id only, because tasks with the same id may be scheduled multiple times.
    private final Map<String, SqlTask> currentTaskInstanceIds = new ConcurrentHashMap<>();

    private final SqlTaskIoStats cachedStats = new SqlTaskIoStats();
    private final SqlTaskIoStats finishedTaskStats = new SqlTaskIoStats();

    private final SnapshotUtils snapshotUtils;

    @GuardedBy("this")
    private long currentMemoryPoolAssignmentVersion;
    @GuardedBy("this")
    private String coordinatorId;

    private final CounterStat failedTasks = new CounterStat();

    private static final Map<String, CommonTableExecutionContext> cteCtx = new ConcurrentHashMap<>();

    @Inject
    public SqlTaskManager(
            LocalExecutionPlanner planner,
            LocationFactory locationFactory,
            TaskExecutor taskExecutor,
            SplitMonitor splitMonitor,
            NodeInfo nodeInfo,
            LocalMemoryManager localMemoryManager,
            TaskManagementExecutor taskManagementExecutor,
            TaskManagerConfig config,
            NodeMemoryConfig nodeMemoryConfig,
            LocalSpillManager localSpillManager,
            NodeSpillConfig nodeSpillConfig,
            GcMonitor gcMonitor,
            Metadata metadata,
            SnapshotUtils snapshotUtils)
    {
        requireNonNull(nodeInfo, "nodeInfo is null");
        requireNonNull(config, "config is null");
        infoCacheTime = config.getInfoMaxAge();
        clientTimeout = config.getClientTimeout();

        DataSize maxBufferSize = config.getSinkMaxBufferSize();

        taskNotificationExecutor = newFixedThreadPool(config.getTaskNotificationThreads(), threadsNamed("task-notification-%s"));
        taskNotificationExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) taskNotificationExecutor);

        this.taskManagementExecutor = requireNonNull(taskManagementExecutor, "taskManagementExecutor cannot be null").getExecutor();
        this.driverYieldExecutor = newScheduledThreadPool(config.getTaskYieldThreads(), threadsNamed("task-yield-%s"));

        SqlTaskExecutionFactory sqlTaskExecutionFactory = new SqlTaskExecutionFactory(taskNotificationExecutor, taskExecutor, planner, splitMonitor, config, metadata);

        this.localMemoryManager = requireNonNull(localMemoryManager, "localMemoryManager is null");
        DataSize maxQueryUserMemoryPerNode = nodeMemoryConfig.getMaxQueryMemoryPerNode();
        DataSize maxQueryTotalMemoryPerNode = nodeMemoryConfig.getMaxQueryTotalMemoryPerNode();
        DataSize maxQuerySpillPerNode = nodeSpillConfig.getQueryMaxSpillPerNode();

        this.snapshotUtils = requireNonNull(snapshotUtils, "snapshotUtils cannot be null");
        queryContexts = CacheBuilder.newBuilder().weakValues().build(CacheLoader.from(
                queryId -> createQueryContext(queryId, localMemoryManager, nodeMemoryConfig, localSpillManager, gcMonitor, maxQueryUserMemoryPerNode, maxQueryTotalMemoryPerNode, maxQuerySpillPerNode, snapshotUtils)));

        tasks = CacheBuilder.newBuilder().build(CacheLoader.from(
                taskId -> {
                    SqlTask task = createSqlTask(
                            taskId,
                            locationFactory.createLocalTaskLocation(taskId),
                            nodeInfo.getNodeId(),
                            queryContexts.getUnchecked(taskId.getQueryId()),
                            sqlTaskExecutionFactory,
                            taskNotificationExecutor,
                            sqlTask -> {
                                finishedTaskStats.merge(sqlTask.getIoStats());
                                return null;
                            },
                            maxBufferSize,
                            failedTasks,
                            metadata);
                    checkState(currentTaskInstanceIds.put(task.getTaskInstanceId(), task) == null);
                    return task;
                }));
    }

    private QueryContext createQueryContext(
            QueryId queryId,
            LocalMemoryManager localMemoryManager,
            NodeMemoryConfig nodeMemoryConfig,
            LocalSpillManager localSpillManager,
            GcMonitor gcMonitor,
            DataSize maxQueryUserMemoryPerNode,
            DataSize maxQueryTotalMemoryPerNode,
            DataSize maxQuerySpillPerNode,
            SnapshotUtils snapshotUtils)
    {
        return new QueryContext(
                queryId,
                maxQueryUserMemoryPerNode,
                maxQueryTotalMemoryPerNode,
                localMemoryManager.getGeneralPool(),
                gcMonitor,
                taskNotificationExecutor,
                driverYieldExecutor,
                maxQuerySpillPerNode,
                localSpillManager.getSpillSpaceTracker(),
                snapshotUtils);
    }

    @Override
    public synchronized void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments)
    {
        if (coordinatorId != null && coordinatorId.equals(assignments.getCoordinatorId()) && assignments.getVersion() <= currentMemoryPoolAssignmentVersion) {
            return;
        }
        currentMemoryPoolAssignmentVersion = assignments.getVersion();
        if (coordinatorId != null && !coordinatorId.equals(assignments.getCoordinatorId())) {
            // Disable the warning for multi-coordinator feature
        }
        coordinatorId = assignments.getCoordinatorId();

        for (MemoryPoolAssignment assignment : assignments.getAssignments()) {
            if (assignment.getPoolId().equals(GENERAL_POOL)) {
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(localMemoryManager.getGeneralPool());
            }
            else if (assignment.getPoolId().equals(RESERVED_POOL)) {
                MemoryPool reservedPool = localMemoryManager.getReservedPool()
                        .orElseThrow(() -> new IllegalArgumentException(format("Cannot move %s to the reserved pool as the reserved pool is not enabled", assignment.getQueryId())));
                queryContexts.getUnchecked(assignment.getQueryId()).setMemoryPool(reservedPool);
            }
            else {
                throw new IllegalArgumentException(format("Cannot move %s to %s as the target memory pool id is invalid", assignment.getQueryId(), assignment.getPoolId()));
            }
        }
    }

    @PostConstruct
    public void start()
    {
        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                removeOldTasks();
            }
            catch (Throwable e) {
                log.warn(e, "Error removing old tasks");
            }
            try {
                failAbandonedTasks();
            }
            catch (Throwable e) {
                log.warn(e, "Error canceling abandoned tasks");
            }
        }, 200, 200, TimeUnit.MILLISECONDS);

        taskManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateStats();
            }
            catch (Throwable e) {
                log.warn(e, "Error updating stats");
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @Override
    @PreDestroy
    public void close()
    {
        boolean taskCanceled = false;
        for (SqlTask task : tasks.asMap().values()) {
            if (task.getTaskStatus().getState().isDone()) {
                continue;
            }
            task.failed(new PrestoException(SERVER_SHUTTING_DOWN, format("Server is shutting down. Task %s has been canceled", task.getTaskId())));
            taskCanceled = true;
        }
        if (taskCanceled) {
            try {
                TimeUnit.SECONDS.sleep(5);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        taskNotificationExecutor.shutdownNow();
    }

    @Managed
    @Flatten
    public SqlTaskIoStats getIoStats()
    {
        return cachedStats;
    }

    @Managed(description = "Task notification executor")
    @Nested
    public ThreadPoolExecutorMBean getTaskNotificationExecutor()
    {
        return taskNotificationExecutorMBean;
    }

    @Managed(description = "Failed tasks counter")
    @Nested
    public CounterStat getFailedTasks()
    {
        return failedTasks;
    }

    public List<SqlTask> getAllTasks()
    {
        return ImmutableList.copyOf(tasks.asMap().values());
    }

    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        return ImmutableList.copyOf(transform(tasks.asMap().values(), SqlTask::getTaskInfo));
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo();
    }

    @Override
    public TaskStatus getTaskStatus(TaskId taskId, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInfo(currentState);
    }

    @Override
    public String getTaskInstanceId(TaskId taskId)
    {
        SqlTask sqlTask = tasks.getUnchecked(taskId);
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskInstanceId();
    }

    @Override
    public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(currentState, "currentState is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        sqlTask.recordHeartbeat();
        return sqlTask.getTaskStatus(currentState);
    }

    @Override
    public TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, OptionalInt totalPartitions, Optional<PlanNodeId> consumer, String expectedTaskInstanceId)
    {
        requireNonNull(session, "session is null");
        requireNonNull(taskId, "taskId is null");
        requireNonNull(fragment, "fragment is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(outputBuffers, "outputBuffers is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        if (resourceOvercommit(session)) {
            // TODO: This should have been done when the QueryContext was created. However, the session isn't available at that point.
            queryContexts.getUnchecked(taskId.getQueryId()).setResourceOvercommit();
        }
        sqlTask.recordHeartbeat();
        return sqlTask.updateTask(session, fragment, sources, outputBuffers, totalPartitions, consumer, cteCtx);
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBufferId bufferId, long startingSequenceId, DataSize maxSize, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(startingSequenceId >= 0, "startingSequenceId is negative");
        requireNonNull(maxSize, "maxSize is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        return sqlTask.getTaskResults(bufferId, startingSequenceId, maxSize);
    }

    @Override
    public void acknowledgeTaskResults(TaskId taskId, OutputBufferId bufferId, long sequenceId, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(sequenceId >= 0, "sequenceId is negative");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return;
            }
        }
        sqlTask.acknowledgeTaskResults(bufferId, sequenceId);
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, OutputBufferId bufferId, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        SqlTask sqlTask;
        if (!isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }
        else {
            sqlTask = tasks.getUnchecked(taskId);
        }
        return sqlTask.abortTaskResults(bufferId);
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId, TaskState targetState, String expectedTaskInstanceId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(targetState, "targetState is null");

        SqlTask sqlTask;
        if (isNullOrEmpty(expectedTaskInstanceId)) {
            sqlTask = tasks.getUnchecked(taskId);
        }
        else {
            sqlTask = currentTaskInstanceIds.get(expectedTaskInstanceId);
            if (sqlTask == null) {
                return null;
            }
        }

        TaskState oldState = sqlTask.getTaskStatus().getState();
        TaskInfo result = sqlTask.cancel(targetState);

        log.debug("Cancelling task %s. Old state: %s; new state: %s", taskId, oldState, targetState);
        if (targetState == TaskState.CANCELED_TO_RESUME) {
            cleanupTaskToResume(taskId, sqlTask.getTaskInstanceId());
        }
        return result;
    }

    private void cleanupTaskToResume(TaskId taskId, String taskInstanceId)
    {
        try {
            // Remove old task context, so that memory-context is not reused by newly scheduled tasks
            queryContexts.get(taskId.getQueryId()).removeTaskContext(taskInstanceId);
            // So new SqlTask object is created with new everything
            SqlTask task = tasks.asMap().remove(taskId);
            if (task != null) {
                currentTaskInstanceIds.remove(task.getTaskInstanceId());
            }
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public void removeOldTasks()
    {
        DateTime oldestAllowedTask = DateTime.now().minus(infoCacheTime.toMillis());
        for (TaskInfo taskInfo : filter(transform(tasks.asMap().values(), SqlTask::getTaskInfo), notNull())) {
            TaskId taskId = taskInfo.getTaskStatus().getTaskId();
            try {
                DateTime endTime = taskInfo.getStats().getEndTime();
                if (endTime != null && endTime.isBefore(oldestAllowedTask)) {
                    SqlTask task = tasks.asMap().remove(taskId);
                    if (task != null) {
                        currentTaskInstanceIds.remove(task.getTaskInstanceId());
                    }
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of complete task %s", taskId);
            }
        }
    }

    public void failAbandonedTasks()
    {
        DateTime now = DateTime.now();
        DateTime oldestAllowedHeartbeat = now.minus(clientTimeout.toMillis());
        for (SqlTask sqlTask : tasks.asMap().values()) {
            try {
                TaskInfo taskInfo = sqlTask.getTaskInfo();
                TaskStatus taskStatus = taskInfo.getTaskStatus();
                if (taskStatus.getState().isDone()) {
                    continue;
                }
                DateTime lastHeartbeat = taskInfo.getLastHeartbeat();
                if (lastHeartbeat != null && lastHeartbeat.isBefore(oldestAllowedHeartbeat)) {
                    log.info("Failing abandoned task %s", taskStatus.getTaskId());
                    if (sqlTask.isSnapshotEnabled()) {
                        // When a task is abandoned, to be safe, we cancel it and allow recovery.
                        sqlTask.cancel(TaskState.CANCELED_TO_RESUME);
                    }
                    else {
                        sqlTask.failed(new PrestoException(ABANDONED_TASK, format("Task %s has not been accessed since %s: currentTime %s", taskStatus.getTaskId(), lastHeartbeat, now)));
                    }
                }
            }
            catch (RuntimeException e) {
                log.warn(e, "Error while inspecting age of task %s", sqlTask.getTaskId());
            }
        }
    }

    //
    // Jmxutils only calls nested getters once, so we are forced to maintain a single
    // instance and periodically recalculate the stats.
    //
    private void updateStats()
    {
        SqlTaskIoStats tempIoStats = new SqlTaskIoStats();
        tempIoStats.merge(finishedTaskStats);

        // there is a race here between task completion, which merges stats into
        // finishedTaskStats, and getting the stats from the task.  Since we have
        // already merged the final stats, we could miss the stats from this task
        // which would result in an under-count, but we will not get an over-count.
        tasks.asMap().values().stream()
                .filter(task -> !task.getTaskStatus().getState().isDone())
                .forEach(task -> tempIoStats.merge(task.getIoStats()));

        cachedStats.resetTo(tempIoStats);
    }

    @Override
    public void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener)
    {
        requireNonNull(taskId, "taskId is null");
        tasks.getUnchecked(taskId).addStateChangeListener(stateChangeListener);
    }

    @VisibleForTesting
    public QueryContext getQueryContext(QueryId queryId)

    {
        return queryContexts.getUnchecked(queryId);
    }

    public static void cleanupContext(String queryId)
    {
        cteCtx.keySet().removeIf(x -> x.contains(queryId));
    }
}
