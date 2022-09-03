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
package io.prestosql.execution.scheduler;

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.graph.Traverser;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.exchange.Exchange;
import io.prestosql.exchange.ExchangeContext;
import io.prestosql.exchange.ExchangeId;
import io.prestosql.exchange.ExchangeManager;
import io.prestosql.exchange.ExchangeManagerRegistry;
import io.prestosql.exchange.RetryPolicy;
import io.prestosql.execution.BasicStageStats;
import io.prestosql.execution.ExecutionFailureInfo;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.RemoteTaskFactory;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.SqlTaskManager;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.TableInfo;
import io.prestosql.execution.TaskFailureListener;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableMetadata;
import io.prestosql.metadata.TableProperties;
import io.prestosql.operator.TaskLocation;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.snapshot.QueryRecoveryManager;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.NodePartitionMap;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;

import javax.annotation.concurrent.GuardedBy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Ticker.systemTicker;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getFirst;
import static com.google.common.collect.Iterables.getLast;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Lists.reverse;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.prestosql.SystemSessionProperties.getConcurrentLifespansPerNode;
import static io.prestosql.SystemSessionProperties.getFaultTolerantExecutionPartitionCount;
import static io.prestosql.SystemSessionProperties.getMaxTasksWaitingForNodePerStage;
import static io.prestosql.SystemSessionProperties.getRetryDelayScaleFactor;
import static io.prestosql.SystemSessionProperties.getRetryInitialDelay;
import static io.prestosql.SystemSessionProperties.getRetryMaxDelay;
import static io.prestosql.SystemSessionProperties.getRetryPolicy;
import static io.prestosql.SystemSessionProperties.getTaskRetryAttemptsOverall;
import static io.prestosql.SystemSessionProperties.getTaskRetryAttemptsPerTask;
import static io.prestosql.SystemSessionProperties.getWriterMinSize;
import static io.prestosql.SystemSessionProperties.isReuseTableScanEnabled;
import static io.prestosql.exchange.RetryPolicy.TASK;
import static io.prestosql.execution.BasicStageStats.aggregateBasicStageStats;
import static io.prestosql.execution.QueryState.FINISHING;
import static io.prestosql.execution.QueryState.RECOVERING;
import static io.prestosql.execution.SqlStageExecution.createSqlStageExecution;
import static io.prestosql.execution.StageState.ABORTED;
import static io.prestosql.execution.StageState.CANCELED;
import static io.prestosql.execution.StageState.FAILED;
import static io.prestosql.execution.StageState.FINISHED;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULED;
import static io.prestosql.execution.scheduler.PipelinedStageExecution.createPipelinedStageExecution;
import static io.prestosql.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.prestosql.execution.scheduler.StageExecution.State.FLUSHING;
import static io.prestosql.snapshot.RecoveryConfig.calculateTaskCount;
import static io.prestosql.spi.ErrorType.EXTERNAL;
import static io.prestosql.spi.ErrorType.INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.StandardErrorCode.REMOTE_TASK_FAILED;
import static io.prestosql.spi.connector.CatalogName.isInternalSystemConnector;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.util.Failures.checkCondition;
import static io.prestosql.util.Failures.toFailure;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class SqlQueryScheduler
{
    private static final Logger log = Logger.get(SqlQueryScheduler.class);
    private static final int[] THROTTLE_SLEEP_TIMER = {5, 10, 15}; //seconds
    private static final long MIN_RESUME_INTERVAL = 5000; // milliseconds
    private static final int[] SPLIT_GROUP_GRADATION = {1, 2, 4, 8, 16, 32, 64, 128};
    private static final int[] SPLIT_GROUP_BUCKETS = {5, 10, 20, 40, 80, 100, 120};

    private final QueryStateMachine queryStateMachine;
    private final ExecutionPolicy executionPolicy;
    private final Map<StageId, SqlStageExecution> stages;
    private final ExecutorService executor;
    private final StageId rootStageId;
    private final Map<StageId, StageScheduler> stageSchedulers;
    private final Map<StageId, StageLinkage> stageLinkages;
    private final SplitSchedulerStats schedulerStats;
    private final boolean summarizeTaskInfo;
    private final AtomicBoolean started = new AtomicBoolean();
    private final DynamicFilterService dynamicFilterService;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private final Session session;
    // Snapshot: determine when scheduler finishes scheduling.
    // Only start to reschedule after the previous scheduler finishes
    private final SettableFuture<?> schedulingFuture = SettableFuture.create();
    // Snapshot: when the scheduled is created. This along with the MIN_RESUME_INTERVAL const determines
    // the earliest time resume can be triggered by this scheduler.
    // If resumes are too close to the start of the scheduler, old and new tasks may have unexpected interactions.
    private final long createdAt = System.currentTimeMillis();
    // Make sure we only try to resume at most once for each scheduling attempt
    private boolean resumed;

    private final Set<PlanFragmentId> visitedPlanFrags = new HashSet<>();
    private int currentTimerLevel;

    private final QuerySnapshotManager snapshotManager;
    private final QueryRecoveryManager queryRecoveryManager;
    private final Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes = new ConcurrentHashMap<>();
    private final ExchangeManagerRegistry exchangeManagerRegistry;
    private final LocationFactory locationFactory;
    private final FailureDetector failureDetector;
    private final StageManager stageManager;
    private final CoordinatorStagesScheduler coordinatorStagesScheduler;

    private final RetryPolicy retryPolicy;
    private final int maxTaskRetryAttemptsOverall;
    private final int maxTaskRetryAttemptsPerTask;
    private final int maxTasksWaitingForNodePerStage;
    private final AtomicInteger currentAttempt = new AtomicInteger();
    private final Duration retryInitialDelay;
    private final Duration retryMaxDelay;
    private final double retryDelayScaleFactor;
    private final TaskSourceFactory taskSourceFactory;
    private final TaskDescriptorStorage taskDescriptorStorage;
    private final NodePartitioningManager nodePartitioningManager;
    private final ScheduledExecutorService schedulerExecutor;
    private final NodeAllocatorService nodeAllocatorService;
    private final PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory;
    private final TaskExecutionStats taskExecutionStats;

    @GuardedBy("this")
    private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler = new AtomicReference<>();

    @GuardedBy("this")
    private Future<Void> distributedStagesSchedulingTask;

    public static SqlQueryScheduler createSqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            DynamicFilterService dynamicFilterService,
            HeuristicIndexerManager heuristicIndexerManager,
            QuerySnapshotManager snapshotManager,
            QueryRecoveryManager queryRecoveryManager,
            Map<StageId, Integer> stageTaskCounts,
            boolean isResume,
            ExchangeManagerRegistry exchangeManagerRegistry,
            Metadata metadata,
            SqlTaskManager coordinatorTaskManager,
            TaskSourceFactory taskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
            TaskExecutionStats taskExecutionStats)
    {
        SqlQueryScheduler sqlQueryScheduler = new SqlQueryScheduler(
                queryStateMachine,
                locationFactory,
                plan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                splitBatchSize,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                rootOutputBuffers,
                nodeTaskMap,
                executionPolicy,
                schedulerStats,
                dynamicFilterService,
                heuristicIndexerManager,
                snapshotManager,
                queryRecoveryManager,
                stageTaskCounts,
                isResume,
                exchangeManagerRegistry,
                metadata,
                coordinatorTaskManager,
                taskSourceFactory,
                taskDescriptorStorage,
                nodeAllocatorService,
                partitionMemoryEstimatorFactory,
                taskExecutionStats);
        if (!(getRetryPolicy(queryStateMachine.getSession()) == TASK)) {
            sqlQueryScheduler.initialize();
        }
        return sqlQueryScheduler;
    }

    private SqlQueryScheduler(
            QueryStateMachine queryStateMachine,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            boolean summarizeTaskInfo,
            int splitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            OutputBuffers rootOutputBuffers,
            NodeTaskMap nodeTaskMap,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            DynamicFilterService dynamicFilterService,
            HeuristicIndexerManager heuristicIndexerManager,
            QuerySnapshotManager snapshotManager,
            QueryRecoveryManager queryRecoveryManager,
            Map<StageId, Integer> stageTaskCounts,
            boolean isResumeScheduler,
            ExchangeManagerRegistry exchangeManagerRegistry,
            Metadata metadata,
            SqlTaskManager coordinatorTaskManager,
            TaskSourceFactory taskSourceFactory,
            TaskDescriptorStorage taskDescriptorStorage,
            NodeAllocatorService nodeAllocatorService,
            PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
            TaskExecutionStats taskExecutionStats)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        this.snapshotManager = snapshotManager;
        this.queryRecoveryManager = queryRecoveryManager;
        if (SystemSessionProperties.isRecoveryEnabled(session)) {
            queryRecoveryManager.setCancelToResumeCb(this::cancelToResume);
        }
        this.exchangeManagerRegistry = requireNonNull(exchangeManagerRegistry, "exchangeManagerRegistry is null");
        this.retryPolicy = getRetryPolicy(queryStateMachine.getSession());
        this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
        this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulerBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkageBuilder = ImmutableMap.builder();

        // Only fetch a distribution once per query to assure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();

        OutputBufferId rootBufferId = Iterables.getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        visitedPlanFrags.add(plan.getFragment().getId());
        final boolean isRecoveryEnabled = SystemSessionProperties.isRecoveryEnabled(session);
        if (isResumeScheduler) {
            nodeScheduler.refreshNodeStates();
        }

        this.executor = queryExecutor;
        this.session = session;
        this.currentTimerLevel = 0;
        this.taskSourceFactory = requireNonNull(taskSourceFactory, "taskSourceFactory is null");
        this.taskDescriptorStorage = requireNonNull(taskDescriptorStorage, "taskDescriptorStorage is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
        this.nodeAllocatorService = requireNonNull(nodeAllocatorService, "nodeAllocatorService is null");
        this.partitionMemoryEstimatorFactory = requireNonNull(partitionMemoryEstimatorFactory, "partitionMemoryEstimatorFactory is null");
        this.taskExecutionStats = requireNonNull(taskExecutionStats, "taskExecutionStats is null");

        if (!(retryPolicy == TASK)) {
            stageManager = null;
            coordinatorStagesScheduler = null;
            List<SqlStageExecution> stageExecutions = createStages(
                    (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
                    new AtomicInteger(),
                    locationFactory,
                    plan.withBucketToPartition(Optional.of(new int[1])),
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    (partitioningHandle, nodeCount) -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle, isRecoveryEnabled, nodeCount)),
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulerBuilder,
                    stageLinkageBuilder,
                    isRecoveryEnabled,
                    snapshotManager,
                    queryRecoveryManager,
                    stageTaskCounts,
                    isResumeScheduler);

            SqlStageExecution rootStage = stageExecutions.get(0);
            rootStage.setOutputBuffers(rootOutputBuffers);
            this.rootStageId = rootStage.getStageId();

            this.stages = stageExecutions.stream()
                    .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));

            if (SystemSessionProperties.isSnapshotEnabled(session)) {
                // Recovery: add minimum number of tasks to task list in query snapshot manager, so that we don't complete prematurely,
                // e.g. 1 task is schedule and finishes right away, before other tasks are scheduled, then snapshot manager may think
                // snapshot is complete, because all known tasks are covered.
                for (SqlStageExecution stage : stageExecutions) {
                    snapshotManager.addNewTask(new TaskId(stage.getStageId(), 0, 0));
                    snapshotManager.setStageCompleteListener(stage.getStageId(), stage::OnSnapshotXCompleted);
                }
            }
        }
        else {
            stageManager = StageManager.create(
                    queryStateMachine,
                    queryStateMachine.getSession(),
                    metadata,
                    remoteTaskFactory,
                    nodeTaskMap,
                    queryExecutor,
                    schedulerStats,
                    plan,
                    summarizeTaskInfo,
                    locationFactory,
                    dynamicFilterService,
                    snapshotManager,
                    exchangeManagerRegistry,
                    queryRecoveryManager,
                    failureDetector);

            coordinatorStagesScheduler = CoordinatorStagesScheduler.create(
                    queryStateMachine,
                    nodeScheduler,
                    stageManager,
                    failureDetector,
                    schedulerExecutor,
                    distributedStagesScheduler,
                    coordinatorTaskManager);
            List<SqlStageExecution> stageExecutionList = new ArrayList<>();
            stageExecutionList.addAll(stageManager.getCoordinatorStagesInTopologicalOrder());
            stageExecutionList.addAll(stageManager.getDistributedStagesInTopologicalOrder());

            this.stages = stageExecutionList.stream()
                    .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));
            this.rootStageId = stageExecutionList.get(0).getStageId();
        }

        this.stageSchedulers = stageSchedulerBuilder.build();
        this.stageLinkages = stageLinkageBuilder.build();

        maxTaskRetryAttemptsOverall = getTaskRetryAttemptsOverall(queryStateMachine.getSession());
        maxTaskRetryAttemptsPerTask = getTaskRetryAttemptsPerTask(queryStateMachine.getSession());
        maxTasksWaitingForNodePerStage = getMaxTasksWaitingForNodePerStage(queryStateMachine.getSession());
        retryInitialDelay = getRetryInitialDelay(queryStateMachine.getSession());
        retryMaxDelay = getRetryMaxDelay(queryStateMachine.getSession());
        retryDelayScaleFactor = getRetryDelayScaleFactor(queryStateMachine.getSession());
    }

    // Snapshot: return number of tasks for each stage from old scheduler, so new scheduler can match it.
    public Map<StageId, Integer> getStageTaskCounts()
    {
        Map<StageId, Integer> ret = new HashMap<>();
        for (SqlStageExecution stage : stages.values()) {
            ret.put(stage.getStageId(), stage.getAllTasks().size());
        }
        return ret;
    }

    // Snapshot: called when we need to cancel the current query execution to prepare for a resume,
    // either as a result of a task failure, or a failed attempt to resume the query
    public synchronized void cancelToResume()
    {
        if (!resumed) {
            snapshotManager.setRestoreStartTime(System.currentTimeMillis());
            // Resume at most once for each scheduler
            resumed = true;
            new Thread(() -> {
                // Avoid too frequent resumes. Wait at least MIN_RESUME_INTERVAL from scheduler start.
                long wait = createdAt + MIN_RESUME_INTERVAL - System.currentTimeMillis();
                if (wait > 0) {
                    try {
                        Thread.sleep(wait);
                    }
                    catch (InterruptedException e) {
                        // could be ignored
                    }
                }
                for (SqlStageExecution stageExecution : stages.values()) {
                    stageExecution.cancelToResume();
                }
            }).start();
        }
    }

    // this is a separate method to ensure that the `this` reference is not leaked during construction
    private void initialize()
    {
        SqlStageExecution rootStage = stages.get(rootStageId);
        rootStage.addStateChangeListener(state -> {
            if (state == FINISHED) {
                if (SystemSessionProperties.isRecoveryEnabled(session)) {
                    if (queryRecoveryManager.hasPendingRecovery()) {
                        // Snapshot: query finished but restore wasn't successful. Final result is likely wrong.
                        // Ideally we should rollback all changes and retry, but it's not easy to revert already committed changes.
                        // Fail the query instead.
                        queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Unsuccessful query retry"));
                        return;
                    }
                }
                queryStateMachine.transitionToFinishing();
                cleanupReuseExchangeMappingIdStatus(rootStage);
            }
            else if (state == CANCELED) {
                // output stage was canceled
                queryStateMachine.transitionToCanceled();
                cleanupReuseExchangeMappingIdStatus(rootStage);
            }
        });

        for (SqlStageExecution stage : stages.values()) {
            stage.addStateChangeListener(state -> {
                if (queryStateMachine.isDone()) {
                    return;
                }
                if (state == FAILED) {
                    cleanupReuseExchangeMappingIdStatus(rootStage);
                    queryStateMachine.transitionToFailed(stage.getStageInfo().getFailureCause().toException());
                }
                else if (state == ABORTED) {
                    cleanupReuseExchangeMappingIdStatus(rootStage);
                    // this should never happen, since abort can only be triggered in query clean up after the query is finished
                    queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                }
                else if (queryStateMachine.getQueryState() == QueryState.STARTING) {
                    // if the stage has at least one task, we are running
                    if (stage.hasTasks()) {
                        queryStateMachine.transitionToRunning();
                    }
                }
            });
        }

        // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
        queryStateMachine.addStateChangeListener(newState -> {
            if (newState.isDone()) {
                queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()), queryRecoveryManager);
            }
        });
        for (SqlStageExecution stage : stages.values()) {
            stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()), queryRecoveryManager));
        }
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Map<TaskId, TaskLocation> bufferLocations = tasks.stream()
                .collect(Collectors.toMap(task -> task.getTaskId(), task -> {
                    URI uri = task.getTaskStatus().getSelf();
                    String instanceId = task.getInstanceId();
                    URI newUri = uriBuilderFrom(uri).appendPath("results").appendPath(rootBufferId.toString()).build();
                    return new TaskLocation(newUri, instanceId);
                }));
        queryStateMachine.updateOutputLocations(bufferLocations, noMoreExchangeLocations);
    }

    private List<SqlStageExecution> createStages(
            ExchangeLocationsConsumer parent,
            AtomicInteger nextStageId,
            LocationFactory locationFactory,
            StageExecutionPlan plan,
            NodeScheduler nodeScheduler,
            RemoteTaskFactory remoteTaskFactory,
            Session session,
            int splitBatchSize,
            BiFunction<PartitioningHandle, Integer, NodePartitionMap> partitioningCache,
            NodePartitioningManager nodePartitioningManager,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            ImmutableMap.Builder<StageId, StageScheduler> stageSchedulers,
            ImmutableMap.Builder<StageId, StageLinkage> stageLinkages,
            boolean isRecoveryEnabled,
            QuerySnapshotManager snapshotManager,
            QueryRecoveryManager queryRecoveryManager,
            Map<StageId, Integer> stageTaskCounts,
            boolean isResumeScheduler)
    {
        ImmutableList.Builder<SqlStageExecution> localStages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), nextStageId.getAndIncrement());
        Optional<Exchange> exchange = Optional.empty();
        if (retryPolicy.equals(TASK)) {
            ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
            exchange = createSqlStageExchange(exchangeManager, stageId);
        }
        SqlStageExecution stageExecution = createSqlStageExecution(
                stageId,
                locationFactory.createStageLocation(stageId),
                plan.getFragment(),
                plan.getTables(),
                remoteTaskFactory,
                session,
                summarizeTaskInfo,
                nodeTaskMap,
                queryExecutor,
                failureDetector,
                schedulerStats,
                dynamicFilterService,
                snapshotManager,
                queryRecoveryManager,
                exchange);

        localStages.add(stageExecution);

        Optional<int[]> bucketToPartition;
        PartitioningHandle partitioningHandle = plan.getFragment().getPartitioning();
        boolean keepConsumerOnFeederNodes = !plan.getFragment().getFeederCTEId().isPresent() && plan.getFragment().getFeederCTEParentId().isPresent();
        if (partitioningHandle.equals(SOURCE_DISTRIBUTION)) {
            // nodes are selected dynamically based on the constraints of the splits and the system load
            Entry<PlanNodeId, SplitSource> entry = Iterables.getOnlyElement(plan.getSplitSources().entrySet());
            PlanNodeId planNodeId = entry.getKey();
            SplitSource splitSource = entry.getValue();
            CatalogName catalogName = splitSource.getCatalogName();
            if (isInternalSystemConnector(catalogName)) {
                catalogName = null;
            }
            NodeSelector nodeSelector = nodeScheduler.createNodeSelector(catalogName, keepConsumerOnFeederNodes, feederScheduledNodes);
            if (isRecoveryEnabled) {
                // When snapshot is enabled, then no task can be added after the query started running,
                // otherwise assumptions about how many "input channels" may be broken.
                nodeSelector.lockDownNodes();
            }
            SplitPlacementPolicy placementPolicy = new DynamicSplitPlacementPolicy(nodeSelector, stageExecution::getAllTasks);

            checkArgument(!plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution());

            stageSchedulers.put(stageId, newSourcePartitionedSchedulerAsStageScheduler(stageExecution, planNodeId, splitSource,
                    placementPolicy, splitBatchSize, session, heuristicIndexerManager));

            bucketToPartition = Optional.of(new int[1]);
        }
        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            bucketToPartition = Optional.of(new int[1]);
        }
        else {
            Map<PlanNodeId, SplitSource> splitSources = plan.getSplitSources();
            if (!splitSources.isEmpty()) {
                // contains local source
                List<PlanNodeId> schedulingOrder = plan.getFragment().getPartitionedSources();
                CatalogName catalogName = partitioningHandle.getConnectorId().orElseThrow(IllegalStateException::new);
                List<ConnectorPartitionHandle> connectorPartitionHandles;
                boolean groupedExecutionForStage = plan.getFragment().getStageExecutionDescriptor().isStageGroupedExecution();
                if (groupedExecutionForStage) {
                    connectorPartitionHandles = nodePartitioningManager.listPartitionHandles(session, partitioningHandle);
                    checkState(!ImmutableList.of(NOT_PARTITIONED).equals(connectorPartitionHandles));
                }
                else {
                    connectorPartitionHandles = ImmutableList.of(NOT_PARTITIONED);
                }

                BucketNodeMap bucketNodeMap;
                List<InternalNode> stageNodeList;
                if (plan.getFragment().getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)) {
                    // no remote source
                    boolean dynamicLifespanSchedule = plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule();
                    if (isRecoveryEnabled) {
                        NodeSelector nodeSelector = nodeScheduler.createNodeSelector(catalogName, keepConsumerOnFeederNodes, feederScheduledNodes);
                        int nodeCount;
                        if (stageTaskCounts != null) {
                            // Resuming: need to create same number of tasks as old stage.
                            nodeCount = stageTaskCounts.get(stageId);
                        }
                        else {
                            // Scheduling: reserve some nodes for resuming
                            nodeCount = calculateTaskCount(nodeSelector.selectableNodeCount());
                        }
                        stageNodeList = new ArrayList<>(nodeSelector.selectRandomNodes(nodeCount));
                        checkCondition(stageNodeList.size() == nodeCount, NO_NODES_AVAILABLE, "Snapshot: not enough worker nodes to resume expected number of tasks: " + nodeCount);
                        // Make sure bucketNodeMap uses the same node list
                        bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule, stageNodeList);
                    }
                    else {
                        bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, dynamicLifespanSchedule);
                        stageNodeList = new ArrayList<>(nodeScheduler.createNodeSelector(catalogName, keepConsumerOnFeederNodes, feederScheduledNodes).allNodes());
                    }

                    // verify execution is consistent with planner's decision on dynamic lifespan schedule
                    verify(bucketNodeMap.isDynamic() == dynamicLifespanSchedule);

                    Collections.shuffle(stageNodeList);
                    bucketToPartition = Optional.empty();
                }
                else {
                    // cannot use dynamic lifespan schedule
                    verify(!plan.getFragment().getStageExecutionDescriptor().isDynamicLifespanSchedule());

                    // remote source requires nodePartitionMap
                    NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning(),
                            stageTaskCounts == null ? null : stageTaskCounts.get(stageId));
                    if (groupedExecutionForStage) {
                        checkState(connectorPartitionHandles.size() == nodePartitionMap.getBucketToPartition().length);
                    }
                    stageNodeList = nodePartitionMap.getPartitionToNode();
                    bucketNodeMap = nodePartitionMap.asBucketNodeMap();
                    bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
                }

                stageSchedulers.put(stageId, new FixedSourcePartitionedScheduler(
                        stageExecution,
                        splitSources,
                        plan.getFragment().getStageExecutionDescriptor(),
                        schedulingOrder,
                        stageNodeList,
                        bucketNodeMap,
                        splitBatchSize,
                        getConcurrentLifespansPerNode(session),
                        nodeScheduler.createNodeSelector(catalogName, keepConsumerOnFeederNodes, feederScheduledNodes),
                        connectorPartitionHandles,
                        session,
                        heuristicIndexerManager));
            }
            else {
                // all sources are remote
                NodePartitionMap nodePartitionMap = partitioningCache.apply(plan.getFragment().getPartitioning(),
                        stageTaskCounts == null ? null : stageTaskCounts.get(stageId));
                List<InternalNode> partitionToNode = nodePartitionMap.getPartitionToNode();
                // todo this should asynchronously wait a standard timeout period before failing
                checkCondition(!partitionToNode.isEmpty(), NO_NODES_AVAILABLE, "No worker nodes available");
                stageSchedulers.put(stageId, new FixedCountScheduler(stageExecution, partitionToNode));
                bucketToPartition = Optional.of(nodePartitionMap.getBucketToPartition());
            }
        }

        ImmutableSet.Builder<SqlStageExecution> childStagesBuilder = ImmutableSet.builder();
        for (StageExecutionPlan subStagePlan : plan.getSubStages()) {
            if (visitedPlanFrags.contains(subStagePlan.getFragment().getId())) {
                continue;
            }

            visitedPlanFrags.add(subStagePlan.getFragment().getId());
            List<SqlStageExecution> subTree = createStages(
                    stageExecution::addExchangeLocations,
                    nextStageId,
                    locationFactory,
                    subStagePlan.withBucketToPartition(bucketToPartition),
                    nodeScheduler,
                    remoteTaskFactory,
                    session,
                    splitBatchSize,
                    partitioningCache,
                    nodePartitioningManager,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    stageSchedulers,
                    stageLinkages,
                    isRecoveryEnabled,
                    snapshotManager,
                    queryRecoveryManager,
                    stageTaskCounts,
                    isResumeScheduler);
            localStages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            childStagesBuilder.add(childStage);
            Optional<RemoteSourceNode> parentNode = plan.getFragment().getRemoteSourceNodes().stream().filter(x -> x.getSourceFragmentIds().contains(childStage.getFragment().getId())).findAny();

            checkArgument(parentNode.isPresent(), "Couldn't find parent of a CTE node");
            childStage.setParentId(parentNode.get().getId());
        }
        Set<SqlStageExecution> childStages = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone() && newState != StageState.RECOVERING) {
                // Snapshot: For "rescheduling", tasks are already cancelled (for resume)
                childStages.forEach(SqlStageExecution::cancel);
            }
        });

        stageLinkages.put(stageId, new StageLinkage(plan.getFragment().getId(), parent, childStages));

        if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
            Supplier<Collection<TaskStatus>> sourceTasksProvider = () -> childStages.stream()
                    .map(SqlStageExecution::getAllTasks)
                    .flatMap(Collection::stream)
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            Supplier<Collection<TaskStatus>> writerTasksProvider = () -> stageExecution.getAllTasks().stream()
                    .map(RemoteTask::getTaskStatus)
                    .collect(toList());

            ScaledWriterScheduler scheduler = new ScaledWriterScheduler(
                    stageExecution,
                    sourceTasksProvider,
                    writerTasksProvider,
                    nodeScheduler.createNodeSelector(null, keepConsumerOnFeederNodes, feederScheduledNodes),
                    schedulerExecutor,
                    getWriterMinSize(session),
                    isRecoveryEnabled,
                    stageTaskCounts != null ? stageTaskCounts.get(stageId) : null);
            whenAllStages(childStages, StageState::isDone)
                    .addListener(scheduler::finish, directExecutor());
            stageSchedulers.put(stageId, scheduler);
        }

        return localStages.build();
    }

    public BasicStageStats getBasicStageStats()
    {
        List<BasicStageStats> stageStats = stages.values().stream()
                .map(SqlStageExecution::getBasicStageStats)
                .collect(toImmutableList());

        return aggregateBasicStageStats(stageStats);
    }

    public StageInfo getStageInfo()
    {
        Map<StageId, StageInfo> stageInfos = stages.values().stream()
                .map(SqlStageExecution::getStageInfo)
                .collect(toImmutableMap(StageInfo::getStageId, identity()));

        if (retryPolicy != TASK) {
            return buildStageInfo(rootStageId, stageInfos);
        }
        else {
            return stageManager.buildStageInfo(rootStageId, stageInfos);
        }
    }

    private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = new ArrayList<>();
        if (!stageLinkages.isEmpty()) {
            childStages = stageLinkages.get(stageId).getChildStageIds().stream()
                    .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                    .collect(toImmutableList());
        }
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
                parent.isRestoring(),
                parent.getSnapshotId(),
                parent.getSelf(),
                parent.getPlan(),
                parent.getTypes(),
                parent.getStageStats(),
                parent.getTasks(),
                childStages,
                parent.getTables(),
                parent.getFailureCause());
    }

    public long getUserMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getUserMemoryReservation)
                .sum();
    }

    public long getTotalMemoryReservation()
    {
        return stages.values().stream()
                .mapToLong(SqlStageExecution::getTotalMemoryReservation)
                .sum();
    }

    public Duration getTotalCpuTime()
    {
        long millis = stages.values().stream()
                .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                .sum();
        return new Duration(millis, MILLISECONDS);
    }

    public void start()
    {
        if (retryPolicy == TASK) {
            if (started.get()) {
                return;
            }
            started.set(true);

            if (queryStateMachine.isDone()) {
                return;
            }

            // when query is done or any time a stage completes, attempt to transition query to "final query info ready"
            queryStateMachine.addStateChangeListener(state -> {
                if (!state.isDone()) {
                    return;
                }

                DistributedStagesScheduler stagesScheduler;
                // synchronize to wait on distributed scheduler creation if it is currently in process
                synchronized (this) {
                    stagesScheduler = this.distributedStagesScheduler.get();
                }

                if (state == QueryState.FINISHED) {
                    coordinatorStagesScheduler.cancel();
                    if (stagesScheduler != null) {
                        stagesScheduler.cancel();
                    }
                    stageManager.finish();
                }
                else if (state == QueryState.FAILED) {
                    coordinatorStagesScheduler.abort();
                    if (stagesScheduler != null) {
                        stagesScheduler.abort();
                    }
                    stageManager.abort();
                }

                queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()), queryRecoveryManager);
            });

            Optional<DistributedStagesScheduler> stagesScheduler = createDistributedStagesScheduler(currentAttempt.get());

            coordinatorStagesScheduler.schedule();
            stagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
        }
        else {
            if (started.compareAndSet(false, true)) {
                executor.submit(this::schedule);
            }
        }
    }

    private boolean canScheduleMoreSplits()
    {
        long cachedMemoryUsage = queryStateMachine.getResourceGroupManager().getCachedMemoryUsage(queryStateMachine.getResourceGroup());
        long softReservedMemory = queryStateMachine.getResourceGroupManager().getSoftReservedMemory(queryStateMachine.getResourceGroup());
        if (cachedMemoryUsage < softReservedMemory) {
            return true;
        }

        log.debug("Splits scheduling throttled....!!! Used memory " + cachedMemoryUsage + " configured " + softReservedMemory);
        return false;
    }

    private int getOptimalSmallSplitGroupSize()
    {
        ResourceGroupInfo resourceGroupInfo = queryStateMachine.getResourceGroupManager().getResourceGroupInfo(queryStateMachine.getResourceGroup());
        long queries = resourceGroupInfo.getNumRunningQueries();
        int result = 0;
        for (int i = 0; i < SPLIT_GROUP_BUCKETS.length; i++, result++) {
            if (queries <= SPLIT_GROUP_BUCKETS[i]) {
                break;
            }
        }
        return SPLIT_GROUP_GRADATION[result];
    }

    private void schedule()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            Set<StageId> completedStages = new HashSet<>();
            ExecutionSchedule executionSchedule = executionPolicy.createExecutionSchedule(stages.values());
            while (!executionSchedule.isFinished()) {
                List<ListenableFuture<?>> blockedStages = new ArrayList<>();
                for (SqlStageExecution stage : executionSchedule.getStagesToSchedule()) {
                    if (isReuseTableScanEnabled(session) && !SqlStageExecution.getReuseTableScanMappingIdStatus(stage.getStateMachine())) {
                        continue;
                    }

                    stage.beginScheduling();

                    // Resource group: Check if memory usage within the current resource grooup has exceeded
                    // configured limit. If yes throttle further split scheduling.
                    // Throttle Logic: Wait for x seconds (Wait time will increase till max as per THROTTLE_SLEEP_TIMER)
                    // and then let it schedule 10% of splits.
                    if (queryStateMachine.isThrottlingEnabled() && !canScheduleMoreSplits()) {
                        try {
                            SECONDS.sleep(THROTTLE_SLEEP_TIMER[currentTimerLevel]);
                        }
                        catch (InterruptedException e) {
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, "interrupted while sleeping");
                        }
                        currentTimerLevel = Math.min(currentTimerLevel + 1, THROTTLE_SLEEP_TIMER.length - 1);
                        stage.setThrottledSchedule(true);
                    }
                    else {
                        stage.setThrottledSchedule(false);
                        currentTimerLevel = 0;
                    }

                    // perform some scheduling work
                    /* Get groupSize specification from the ResourceGroupManager */
                    int maxSplitGroupSize = getOptimalSmallSplitGroupSize();
                    ScheduleResult result = stageSchedulers.get(stage.getStageId())
                            .schedule(maxSplitGroupSize);

                    // modify parent and children based on the results of the scheduling
                    if (result.isFinished()) {
                        stage.schedulingComplete();
                    }
                    else if (!result.getBlocked().isDone()) {
                        blockedStages.add(result.getBlocked());
                    }
                    stageLinkages.get(stage.getStageId())
                            .processScheduleResults(stage.getState(), result.getNewTasks());
                    schedulerStats.getSplitsScheduledPerIteration().add(result.getSplitsScheduled());
                    if (result.getBlockedReason().isPresent()) {
                        switch (result.getBlockedReason().get()) {
                            case WRITER_SCALING:
                                // no-op
                                break;
                            case WAITING_FOR_SOURCE:
                                schedulerStats.getWaitingForSource().update(1);
                                break;
                            case SPLIT_QUEUES_FULL:
                                schedulerStats.getSplitQueuesFull().update(1);
                                break;
                            case MIXED_SPLIT_QUEUES_FULL_AND_WAITING_FOR_SOURCE:
                            case NO_ACTIVE_DRIVER_GROUP:
                                break;
                            default:
                                throw new UnsupportedOperationException("Unknown blocked reason: " + result.getBlockedReason().get());
                        }
                    }
                }

                // make sure to update stage linkage at least once per loop to catch async state changes (e.g., partial cancel)
                for (SqlStageExecution stage : stages.values()) {
                    if (!completedStages.contains(stage.getStageId()) && stage.getState().isDone()) {
                        stageLinkages.get(stage.getStageId())
                                .processScheduleResults(stage.getState(), ImmutableSet.of());
                        completedStages.add(stage.getStageId());
                    }
                }

                // wait for a state change and then schedule again
                if (!blockedStages.isEmpty()) {
                    try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                        tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                    }
                    for (ListenableFuture<?> blockedStage : blockedStages) {
                        blockedStage.cancel(true);
                    }
                }
            }

            for (SqlStageExecution stage : stages.values()) {
                StageState state = stage.getState();
                // if state is recovery, then state of stage and query will change soon again. Don't treat as an error.
                if (state != SCHEDULED && state != RUNNING && !state.isDone() && queryRecoveryManager.isRecovering()) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Scheduling is complete, but stage %s is in state %s", stage.getStageId(), state));
                }
            }
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
            throw t;
        }
        finally {
            RuntimeException closeError = new RuntimeException();
            for (StageScheduler scheduler : stageSchedulers.values()) {
                try {
                    if (queryStateMachine.getQueryState() != RECOVERING && !queryRecoveryManager.isRecovering()) {
                        scheduler.close();
                    }
                }
                catch (Throwable t) {
                    queryStateMachine.transitionToFailed(t);
                    // Self-suppression not permitted
                    if (closeError != t) {
                        closeError.addSuppressed(t);
                    }
                }
            }

            // Snpashot: if resuming, notify the new scheduler so it can start scheduling new stages
            schedulingFuture.set(null);
            if (closeError.getSuppressed().length > 0) {
                throw closeError;
            }
        }
    }

    public void cancelStage(StageId stageId)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            SqlStageExecution sqlStageExecution = stages.get(stageId);
            SqlStageExecution stage = requireNonNull(sqlStageExecution, () -> format("Stage %s does not exist", stageId));
            stage.cancel();
        }
    }

    public ListenableFuture<?> doneScheduling()
    {
        return schedulingFuture;
    }

    public void abort()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stages.values().forEach(SqlStageExecution::abort);
        }
    }

    public synchronized void suspend()
    {
        log.info("Suspending query tasks: %s, usingSnapshot: %b", queryStateMachine.getQueryId(), false);
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stages.values().forEach(SqlStageExecution::suspend);
        }
    }

    public synchronized void resume()
    {
        log.info("Resuming suspended query tasks: %s", queryStateMachine.getQueryId());
        try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
            stages.values().forEach(SqlStageExecution::resume);
        }
    }

    private static ListenableFuture<?> whenAllStages(Collection<SqlStageExecution> stages, Predicate<StageState> predicate)
    {
        checkArgument(!stages.isEmpty(), "stages is empty");
        Set<StageId> stageIds = newConcurrentHashSet(stages.stream()
                .map(SqlStageExecution::getStageId)
                .collect(toSet()));
        SettableFuture<?> future = SettableFuture.create();

        for (SqlStageExecution stage : stages) {
            stage.addStateChangeListener(state -> {
                if (predicate.test(state) && stageIds.remove(stage.getStageId()) && stageIds.isEmpty()) {
                    future.set(null);
                }
            });
        }

        return future;
    }

    public void setResuming(long restoringSnapshotId)
    {
        for (SqlStageExecution stageExecution : stages.values()) {
            stageExecution.setResuming(restoringSnapshotId);
        }
        snapshotManager.setRestoringSnapshotId(restoringSnapshotId);
    }

    private interface ExchangeLocationsConsumer
    {
        void addExchangeLocations(PlanFragmentId fragmentId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations);
    }

    private static class StageLinkage
    {
        private final PlanFragmentId currentStageFragmentId;
        private final ExchangeLocationsConsumer parent;
        private final Set<OutputBufferManager> childOutputBufferManagers;
        private final Set<StageId> childStageIds;

        public StageLinkage(PlanFragmentId fragmentId, ExchangeLocationsConsumer parent, Set<SqlStageExecution> children)
        {
            this.currentStageFragmentId = fragmentId;
            this.parent = parent;
            this.childOutputBufferManagers = children.stream()
                    .map(childStage -> {
                        PartitioningHandle partitioningHandle = childStage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
                        if (partitioningHandle.equals(FIXED_BROADCAST_DISTRIBUTION)) {
                            return new BroadcastOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else if (partitioningHandle.equals(SCALED_WRITER_DISTRIBUTION)) {
                            return new ScaledOutputBufferManager(childStage::setOutputBuffers);
                        }
                        else {
                            int partitionCount = Ints.max(childStage.getFragment().getPartitioningScheme().getBucketToPartition().get()) + 1;
                            return new PartitionedOutputBufferManager(partitioningHandle, partitionCount, childStage::setOutputBuffers);
                        }
                    })
                    .collect(toImmutableSet());

            this.childStageIds = children.stream()
                    .map(SqlStageExecution::getStageId)
                    .collect(toImmutableSet());
        }

        public Set<StageId> getChildStageIds()
        {
            return childStageIds;
        }

        public void processScheduleResults(StageState newState, Set<RemoteTask> newTasks)
        {
            boolean noMoreTasks = false;
            switch (newState) {
                case PLANNED:
                    // $FALL-THROUGH$
                case SCHEDULING:
                    // workers are still being added to the query
                    break;
                case SCHEDULING_SPLITS:
                    // $FALL-THROUGH$
                case SCHEDULED:
                    // $FALL-THROUGH$
                case RUNNING:
                    // $FALL-THROUGH$
                case FINISHED:
                    // $FALL-THROUGH$
                case CANCELED:
                    // no more workers will be added to the query
                    noMoreTasks = true;
                    // $FALL-THROUGH$
                case ABORTED:
                    // $FALL-THROUGH$
                case FAILED:
                    // DO NOT complete a FAILED or ABORTED stage.  This will cause the
                    // stage above to finish normally, which will result in a query
                    // completing successfully when it should fail..
                    break;
                default:
                    break;
            }

            // Add an exchange location to the parent stage for each new task
            parent.addExchangeLocations(currentStageFragmentId, newTasks, noMoreTasks);

            if (!childOutputBufferManagers.isEmpty()) {
                // Add an output buffer to the child stages for each new task
                List<OutputBufferId> newOutputBuffers = newTasks.stream()
                        .map(task -> new OutputBufferId(task.getTaskId().getId()))
                        .collect(toImmutableList());
                for (OutputBufferManager child : childOutputBufferManagers) {
                    child.addOutputBuffers(newOutputBuffers, noMoreTasks);
                    if (noMoreTasks) {
                        child.noMoreBuffers();
                    }
                }
            }
        }
    }

    private void cleanupReuseExchangeMappingIdStatus(SqlStageExecution sqlStageExecution)
    {
        List<UUID> uuidList = SqlStageExecution.removeReuseTableScanMappingIdStatus(sqlStageExecution.getStateMachine());
        if (null != uuidList) {
            TableSplitAssignmentInfo tableSplitAssignmentInfo = TableSplitAssignmentInfo.getInstance();
            if (null != tableSplitAssignmentInfo) {
                tableSplitAssignmentInfo.removeFromTableSplitAssignmentInfo(uuidList);
            }
        }
    }

    private synchronized Optional<DistributedStagesScheduler> createDistributedStagesScheduler(int attempt)
    {
        if (queryStateMachine.isDone()) {
            return Optional.empty();
        }

        DistributedStagesScheduler stagesScheduler;
        switch (retryPolicy) {
            case TASK:
                ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                stagesScheduler = FaultTolerantDistributedStagesScheduler.create(
                        queryStateMachine,
                        stageManager,
                        failureDetector,
                        taskSourceFactory,
                        taskDescriptorStorage,
                        exchangeManager,
                        nodePartitioningManager,
                        coordinatorStagesScheduler.getTaskLifecycleListener(),
                        maxTaskRetryAttemptsOverall,
                        maxTaskRetryAttemptsPerTask,
                        maxTasksWaitingForNodePerStage,
                        schedulerExecutor,
                        schedulerStats,
                        nodeAllocatorService,
                        partitionMemoryEstimatorFactory,
                        taskExecutionStats);
                break;
            case NONE:
            default:
                throw new IllegalArgumentException("Unexpected retry policy: " + retryPolicy);
        }

        this.distributedStagesScheduler.set(stagesScheduler);
        stagesScheduler.addStateChangeListener(state -> {
            if (queryStateMachine.getQueryState() == QueryState.STARTING && (state == DistributedStagesSchedulerState.RUNNING || state.isDone())) {
                queryStateMachine.transitionToRunning();
            }

            if (state.isDone() && !state.isFailure()) {
                stageManager.getDistributedStagesInTopologicalOrder().forEach(stage -> stageManager.get(stage.getStageId()).finish());
            }

            if (stageManager.getCoordinatorStagesInTopologicalOrder().isEmpty()) {
                // if there are no coordinator stages (e.g., simple select query) and the distributed stages are finished, do the query transitioning
                // otherwise defer query transitioning to the coordinator stages
                if (state == DistributedStagesSchedulerState.FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == DistributedStagesSchedulerState.CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }

            if (state == DistributedStagesSchedulerState.FAILED) {
                StageFailureInfo stageFailureInfo = stagesScheduler.getFailureCause()
                        .orElseGet(() -> new StageFailureInfo(toFailure(new VerifyException("stagesScheduler failed but failure cause is not present")), Optional.empty()));
                ErrorCode errorCode = stageFailureInfo.getFailureInfo().getErrorCode();

                stageManager.getDistributedStagesInTopologicalOrder().forEach(stage -> {
                    if (stageFailureInfo.getFailedStageId().isPresent() && stageFailureInfo.getFailedStageId().get().equals(stage.getStageId())) {
                        stage.fail(stageFailureInfo.getFailureInfo().toException());
                    }
                    else {
                        stage.abort();
                    }
                });
                queryStateMachine.transitionToFailed(stageFailureInfo.getFailureInfo().toException());
            }
        });
        return Optional.of(stagesScheduler);
    }

    public static Optional<Exchange> createSqlStageExchange(ExchangeManager exchangeManager, StageId stageId)
    {
        Optional<Exchange> exchange;
        if (stageId.getId() == 0) {
            //root stage no need exchange
            exchange = Optional.empty();
        }
        else {
            // create external exchange
            int partitionCount = 50;  //TODO(Alex): get the partition count from config files
            ExchangeContext context = new ExchangeContext(stageId.getQueryId(), new ExchangeId("external-exchange-" + stageId.getId()));
            exchange = Optional.of(exchangeManager.createExchange(context, partitionCount));
        }
        return exchange;
    }

    private static boolean isRetryableErrorCode(ErrorCode errorCode)
    {
        return errorCode == null
                || errorCode.getType() == INTERNAL_ERROR
                || errorCode.getType() == EXTERNAL
                || errorCode.getCode() == CLUSTER_OUT_OF_MEMORY.toErrorCode().getCode();
    }

    private void scheduleRetryWithDelay(long delayInMillis)
    {
        try {
            schedulerExecutor.schedule(this::scheduleRetry, delayInMillis, MILLISECONDS);
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    private synchronized void scheduleRetry()
    {
        try {
            checkState(distributedStagesSchedulingTask != null, "schedulingTask is expected to be set");

            // give current scheduler some time to terminate, usually it is expected to be done right away
            distributedStagesSchedulingTask.get(5, MINUTES);

            Optional<DistributedStagesScheduler> stagesScheduler = createDistributedStagesScheduler(currentAttempt.get());
            stagesScheduler.ifPresent(scheduler -> distributedStagesSchedulingTask = executor.submit(scheduler::schedule, null));
        }
        catch (Throwable t) {
            queryStateMachine.transitionToFailed(t);
        }
    }

    private static class StageManager
    {
        private final QueryStateMachine queryStateMachine;
        private final Map<StageId, SqlStageExecution> stages;
        private final List<SqlStageExecution> coordinatorStagesInTopologicalOrder;
        private final List<SqlStageExecution> distributedStagesInTopologicalOrder;
        private final StageId rootStageId;
        private final Map<StageId, Set<StageId>> children;
        private final Map<StageId, StageId> parents;
        private final QueryRecoveryManager queryRecoveryManager;

        private static StageManager create(
                QueryStateMachine queryStateMachine,
                Session session,
                Metadata metadata,
                RemoteTaskFactory taskFactory,
                NodeTaskMap nodeTaskMap,
                ExecutorService executor,
                SplitSchedulerStats schedulerStats,
                StageExecutionPlan planTree,
                boolean summarizeTaskInfo,
                LocationFactory locationFactory,
                DynamicFilterService dynamicFilterService,
                QuerySnapshotManager snapshotManager,
                ExchangeManagerRegistry exchangeManagerRegistry,
                QueryRecoveryManager queryRecoveryManager,
                FailureDetector failureDetector)
        {
            ImmutableMap.Builder<StageId, SqlStageExecution> sqlStages = ImmutableMap.builder();
            ImmutableList.Builder<SqlStageExecution> cnStagesInTopologicalOrder = ImmutableList.builder();
            ImmutableList.Builder<SqlStageExecution> workerStagesInTopologicalOrder = ImmutableList.builder();
            StageId rootSqlStageId = null;
            ImmutableMap.Builder<StageId, Set<StageId>> childrenStage = ImmutableMap.builder();
            ImmutableMap.Builder<StageId, StageId> parentsStage = ImmutableMap.builder();
            for (StageExecutionPlan planNode : Traverser.forTree(StageExecutionPlan::getSubStages).breadthFirst(planTree)) {
                PlanFragment fragment = planNode.getFragment();
                StageId stageId = getStageId(session.getQueryId(), fragment.getId());
                ExchangeManager exchangeManager = exchangeManagerRegistry.getExchangeManager();
                Optional<Exchange> exchange = createSqlStageExchange(exchangeManager, stageId);
                SqlStageExecution stage = createSqlStageExecution(
                        stageId,
                        locationFactory.createStageLocation(stageId),
                        fragment,
                        extractTableInfo(session, metadata, fragment),
                        taskFactory,
                        session,
                        summarizeTaskInfo,
                        nodeTaskMap,
                        executor,
                        failureDetector,
                        schedulerStats,
                        dynamicFilterService,
                        snapshotManager,
                        queryRecoveryManager,
                        exchange);
                sqlStages.put(stageId, stage);
                if (fragment.getPartitioning().isCoordinatorOnly()) {
                    cnStagesInTopologicalOrder.add(stage);
                }
                else {
                    workerStagesInTopologicalOrder.add(stage);
                }
                if (rootSqlStageId == null) {
                    rootSqlStageId = stageId;
                }
                Set<StageId> childStageIds = planNode.getSubStages().stream()
                        .map(childStage -> getStageId(session.getQueryId(), childStage.getFragment().getId()))
                        .collect(toImmutableSet());
                childrenStage.put(stageId, childStageIds);
                childStageIds.forEach(child -> parentsStage.put(child, stageId));
            }
            StageManager sqlStageManager = new StageManager(
                    queryStateMachine,
                    sqlStages.build(),
                    cnStagesInTopologicalOrder.build(),
                    workerStagesInTopologicalOrder.build(),
                    rootSqlStageId,
                    childrenStage.build(),
                    parentsStage.build(),
                    queryRecoveryManager);
            sqlStageManager.initialize();
            return sqlStageManager;
        }

        private static Map<PlanNodeId, TableInfo> extractTableInfo(Session session, Metadata metadata, PlanFragment fragment)
        {
            return searchFrom(fragment.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findAll()
                    .stream()
                    .map(TableScanNode.class::cast)
                    .collect(toImmutableMap(PlanNode::getId, node -> getTableInfo(session, metadata, node)));
        }

        private static TableInfo getTableInfo(Session session, Metadata metadata, TableScanNode node)
        {
            TableMetadata tableSchema = metadata.getTableMetadata(session, node.getTable());
            TableProperties tableProperties = metadata.getTableProperties(session, node.getTable());
            return new TableInfo(tableSchema.getQualifiedName(), tableProperties.getPredicate());
        }

        private static StageId getStageId(QueryId queryId, PlanFragmentId fragmentId)
        {
            // TODO: refactor fragment id to be based on an integer
            return new StageId(queryId, parseInt(fragmentId.toString()));
        }

        private StageManager(
                QueryStateMachine queryStateMachine,
                Map<StageId, SqlStageExecution> stages,
                List<SqlStageExecution> coordinatorStagesInTopologicalOrder,
                List<SqlStageExecution> distributedStagesInTopologicalOrder,
                StageId rootStageId,
                Map<StageId, Set<StageId>> children,
                Map<StageId, StageId> parents,
                QueryRecoveryManager queryRecoveryManager)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.stages = ImmutableMap.copyOf(requireNonNull(stages, "stages is null"));
            this.coordinatorStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(coordinatorStagesInTopologicalOrder, "coordinatorStagesInTopologicalOrder is null"));
            this.distributedStagesInTopologicalOrder = ImmutableList.copyOf(requireNonNull(distributedStagesInTopologicalOrder, "distributedStagesInTopologicalOrder is null"));
            this.rootStageId = requireNonNull(rootStageId, "rootStageId is null");
            this.children = ImmutableMap.copyOf(requireNonNull(children, "children is null"));
            this.parents = ImmutableMap.copyOf(requireNonNull(parents, "parents is null"));
            this.queryRecoveryManager = requireNonNull(queryRecoveryManager, "queryRecoverManager is null");
        }

        private void initialize()
        {
            for (SqlStageExecution stage : stages.values()) {
                stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()), queryRecoveryManager));
            }
        }

        public void finish()
        {
            stages.values().forEach(SqlStageExecution::finish);
        }

        public void abort()
        {
            stages.values().forEach(SqlStageExecution::abort);
        }

        public List<SqlStageExecution> getCoordinatorStagesInTopologicalOrder()
        {
            return coordinatorStagesInTopologicalOrder;
        }

        public List<SqlStageExecution> getDistributedStagesInTopologicalOrder()
        {
            return distributedStagesInTopologicalOrder;
        }

        public SqlStageExecution getOutputStage()
        {
            return stages.get(rootStageId);
        }

        public SqlStageExecution get(PlanFragmentId fragmentId)
        {
            return get(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public SqlStageExecution get(StageId stageId)
        {
            return requireNonNull(stages.get(stageId), () -> "stage not found: " + stageId);
        }

        public Set<SqlStageExecution> getChildren(PlanFragmentId fragmentId)
        {
            return getChildren(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public Set<SqlStageExecution> getChildren(StageId stageId)
        {
            return children.get(stageId).stream()
                    .map(this::get)
                    .collect(toImmutableSet());
        }

        public Optional<SqlStageExecution> getParent(PlanFragmentId fragmentId)
        {
            return getParent(getStageId(queryStateMachine.getQueryId(), fragmentId));
        }

        public Optional<SqlStageExecution> getParent(StageId stageId)
        {
            return Optional.ofNullable(parents.get(stageId)).map(stages::get);
        }

        public BasicStageStats getBasicStageStats()
        {
            List<BasicStageStats> stageStats = stages.values().stream()
                    .map(SqlStageExecution::getBasicStageStats)
                    .collect(toImmutableList());

            return aggregateBasicStageStats(stageStats);
        }

        public StageInfo getStageInfo()
        {
            Map<StageId, StageInfo> stageInfos = stages.values().stream()
                    .map(SqlStageExecution::getStageInfo)
                    .collect(toImmutableMap(StageInfo::getStageId, identity()));

            return buildStageInfo(rootStageId, stageInfos);
        }

        private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
        {
            StageInfo parent = stageInfos.get(stageId);
            checkArgument(parent != null, "No stageInfo for %s", parent);
            List<StageInfo> childStages = children.get(stageId).stream()
                    .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                    .collect(toImmutableList());
            if (childStages.isEmpty()) {
                return parent;
            }
            return new StageInfo(
                    parent.getStageId(),
                    parent.getState(),
                    parent.isRestoring(),
                    parent.getSnapshotId(),
                    parent.getSelf(),
                    parent.getPlan(),
                    parent.getTypes(),
                    parent.getStageStats(),
                    parent.getTasks(),
                    childStages,
                    parent.getTables(),
                    parent.getFailureCause());
        }

        public long getUserMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStageExecution::getUserMemoryReservation)
                    .sum();
        }

        public long getTotalMemoryReservation()
        {
            return stages.values().stream()
                    .mapToLong(SqlStageExecution::getTotalMemoryReservation)
                    .sum();
        }

        public Duration getTotalCpuTime()
        {
            long millis = stages.values().stream()
                    .mapToLong(stage -> stage.getTotalCpuTime().toMillis())
                    .sum();
            return new Duration(millis, MILLISECONDS);
        }
    }

    /**
     * Scheduler for stages that must be executed on coordinator.
     * <p>
     * Scheduling for coordinator only stages must be represented as a separate entity to
     * ensure the coordinator stages/tasks are never restarted in an event of a failure.
     * <p>
     * Coordinator only tasks cannot be restarted due to the nature of operations
     * they perform. For example commit operations for DML statements are performed as a
     * coordinator only task (via {@link io.prestosql.operator.TableFinishOperator}). Today it is
     * not required for a commit operation to be side effect free and idempotent what makes it
     * impossible to safely retry.
     */
    private static class CoordinatorStagesScheduler
    {
        private static final int[] SINGLE_PARTITION = new int[]{0};

        private final QueryStateMachine queryStateMachine;
        private final NodeScheduler nodeScheduler;
        private final Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator;
        private final Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator;
        private final TaskLifecycleListener taskLifecycleListener;
        private final StageManager stageManager;
        private final List<StageExecution> stageExecutions;
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;
        private final SqlTaskManager coordinatorTaskManager;

        private final AtomicBoolean scheduled = new AtomicBoolean();

        public static CoordinatorStagesScheduler create(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                StageManager stageManager,
                FailureDetector failureDetector,
                Executor executor,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                SqlTaskManager coordinatorTaskManager)
        {
            Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCn = createOutputBuffersForStagesConsumedByCoordinator(stageManager);
            Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCn = createBucketToPartitionForStagesConsumedByCoordinator(stageManager);

            TaskLifecycleListener outputTaskLifecycleListener = new QueryOutputTaskLifecycleListener(queryStateMachine);
            // create executions
            ImmutableList.Builder<StageExecution> sqlStageExecutions = ImmutableList.builder();
            for (SqlStageExecution stage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                StageExecution stageExecution = createPipelinedStageExecution(
                        stage,
                        outputBuffersForStagesConsumedByCn,
                        outputTaskLifecycleListener,
                        failureDetector,
                        executor,
                        bucketToPartitionForStagesConsumedByCn.get(stage.getFragment().getId()),
                        0);
                sqlStageExecutions.add(stageExecution);
                outputTaskLifecycleListener = stageExecution.getTaskLifecycleListener();
            }

            CoordinatorStagesScheduler cnStagesScheduler = new CoordinatorStagesScheduler(
                    queryStateMachine,
                    nodeScheduler,
                    outputBuffersForStagesConsumedByCn,
                    bucketToPartitionForStagesConsumedByCn,
                    outputTaskLifecycleListener,
                    stageManager,
                    sqlStageExecutions.build(),
                    distributedStagesScheduler,
                    coordinatorTaskManager);
            cnStagesScheduler.initialize();

            return cnStagesScheduler;
        }

        private static Map<PlanFragmentId, OutputBufferManager> createOutputBuffersForStagesConsumedByCoordinator(StageManager stageManager)
        {
            ImmutableMap.Builder<PlanFragmentId, OutputBufferManager> result = ImmutableMap.builder();

            // create output buffer for output stage
            SqlStageExecution outputStage = stageManager.getOutputStage();
            result.put(outputStage.getFragment().getId(), createSingleStreamOutputBuffer(outputStage));

            // create output buffers for stages consumed by coordinator
            for (SqlStageExecution coordinatorStage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStageExecution childStage : stageManager.getChildren(coordinatorStage.getStageId())) {
                    result.put(childStage.getFragment().getId(), createSingleStreamOutputBuffer(childStage));
                }
            }

            return result.build();
        }

        private static OutputBufferManager createSingleStreamOutputBuffer(SqlStageExecution stage)
        {
            PartitioningHandle partitioningHandle = stage.getFragment().getPartitioningScheme().getPartitioning().getHandle();
            checkArgument(partitioningHandle.isSingleNode(), "partitioning is expected to be single node: " + partitioningHandle);
            return new PartitionedOutputBufferManager(partitioningHandle, 1);
        }

        private static Map<PlanFragmentId, Optional<int[]>> createBucketToPartitionForStagesConsumedByCoordinator(StageManager stageManager)
        {
            ImmutableMap.Builder<PlanFragmentId, Optional<int[]>> result = ImmutableMap.builder();

            SqlStageExecution outputStage = stageManager.getOutputStage();
            result.put(outputStage.getFragment().getId(), Optional.of(SINGLE_PARTITION));

            for (SqlStageExecution coordinatorStage : stageManager.getCoordinatorStagesInTopologicalOrder()) {
                for (SqlStageExecution childStage : stageManager.getChildren(coordinatorStage.getStageId())) {
                    result.put(childStage.getFragment().getId(), Optional.of(SINGLE_PARTITION));
                }
            }

            return result.build();
        }

        private CoordinatorStagesScheduler(
                QueryStateMachine queryStateMachine,
                NodeScheduler nodeScheduler,
                Map<PlanFragmentId, OutputBufferManager> outputBuffersForStagesConsumedByCoordinator,
                Map<PlanFragmentId, Optional<int[]>> bucketToPartitionForStagesConsumedByCoordinator,
                TaskLifecycleListener taskLifecycleListener,
                StageManager stageManager,
                List<StageExecution> stageExecutions,
                AtomicReference<DistributedStagesScheduler> distributedStagesScheduler,
                SqlTaskManager coordinatorTaskManager)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.outputBuffersForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(outputBuffersForStagesConsumedByCoordinator, "outputBuffersForStagesConsumedByCoordinator is null"));
            this.bucketToPartitionForStagesConsumedByCoordinator = ImmutableMap.copyOf(requireNonNull(bucketToPartitionForStagesConsumedByCoordinator, "bucketToPartitionForStagesConsumedByCoordinator is null"));
            this.taskLifecycleListener = requireNonNull(taskLifecycleListener, "taskLifecycleListener is null");
            this.stageManager = requireNonNull(stageManager, "stageManager is null");
            this.stageExecutions = ImmutableList.copyOf(requireNonNull(stageExecutions, "stageExecutions is null"));
            this.distributedStagesScheduler = requireNonNull(distributedStagesScheduler, "distributedStagesScheduler is null");
            this.coordinatorTaskManager = requireNonNull(coordinatorTaskManager, "coordinatorTaskManager is null");
        }

        private void initialize()
        {
            for (StageExecution stageExecution : stageExecutions) {
                stageExecution.addStateChangeListener(state -> {
                    if (queryStateMachine.isDone()) {
                        return;
                    }
                    if (queryStateMachine.getQueryState() == QueryState.STARTING && (state == StageExecution.State.RUNNING || state.isDone())) {
                        queryStateMachine.transitionToRunning();
                    }
                    // if any coordinator stage failed transition directly to failure
                    if (state == StageExecution.State.FAILED) {
                        RuntimeException failureCause = stageExecution.getFailureCause()
                                .map(ExecutionFailureInfo::toException)
                                .orElseGet(() -> new VerifyException(format("stage execution for stage %s is failed by failure cause is not present", stageExecution.getStageId())));
                        stageManager.get(stageExecution.getStageId()).fail(failureCause);
                        queryStateMachine.transitionToFailed(failureCause);
                    }
                    else if (state == StageExecution.State.ABORTED) {
                        // this should never happen, since abort can only be triggered in query clean up after the query is finished
                        stageManager.get(stageExecution.getStageId()).abort();
                        queryStateMachine.transitionToFailed(new PrestoException(GENERIC_INTERNAL_ERROR, "Query stage was aborted"));
                    }
                    else if (state.isDone()) {
                        stageManager.get(stageExecution.getStageId()).finish();
                    }
                });
            }

            for (int currentIndex = 0, nextIndex = 1; nextIndex < stageExecutions.size(); currentIndex++, nextIndex++) {
                StageExecution stageExecution = stageExecutions.get(currentIndex);
                StageExecution childStageExecution = stageExecutions.get(nextIndex);
                Set<SqlStageExecution> childStages = stageManager.getChildren(stageExecution.getStageId());
                verify(childStages.size() == 1, "exactly one child stage is expected");
                SqlStageExecution childStage = getOnlyElement(childStages);
                verify(childStage.getStageId().equals(childStageExecution.getStageId()), "stage execution order doesn't match the stage order");
                stageExecution.addStateChangeListener(newState -> {
                    if (newState == FLUSHING || newState.isDone()) {
                        childStageExecution.cancel();
                    }
                });
            }

            Optional<StageExecution> root = Optional.ofNullable(getFirst(stageExecutions, null));
            root.ifPresent(stageExecution -> stageExecution.addStateChangeListener(state -> {
                if (state == StageExecution.State.FINISHED) {
                    queryStateMachine.transitionToFinishing();
                }
                else if (state == StageExecution.State.CANCELED) {
                    // output stage was canceled
                    queryStateMachine.transitionToCanceled();
                }
            }));

            Optional<StageExecution> last = Optional.ofNullable(getLast(stageExecutions, null));
            last.ifPresent(stageExecution -> stageExecution.addStateChangeListener(newState -> {
                if (newState == FLUSHING || newState.isDone()) {
                    DistributedStagesScheduler stagesScheduler = this.distributedStagesScheduler.get();
                    if (stagesScheduler != null) {
                        stagesScheduler.cancel();
                    }
                }
            }));
        }

        public void schedule()
        {
            if (!scheduled.compareAndSet(false, true)) {
                return;
            }

            /*
             * Tasks have 2 communication links:
             *
             * Task <-> Coordinator (for status updates)
             * Task <-> Downstream Task (for exchanging the task results)
             *
             * In a scenario when a link between a task and a downstream task is broken (while the link between a
             * task and coordinator is not) without failure recovery enabled the downstream task would discover
             * that the communication link is broken and fail a query.
             *
             * However with failure recovery enabled a downstream task is configured to ignore the failures to survive an
             * upstream task failure. That may result into a "deadlock", when the coordinator thinks that a task is active,
             * but since the communication link between the task and it's downstream task is broken nobody is pooling
             * the results leaving it in a blocked state. Thus it is important to notify the scheduler about such
             * communication failures so the scheduler can react and re-schedule a task.
             *
             * Currently only "coordinator" tasks have to survive an upstream task failure (for example a task that performs
             * table commit). Restarting a table commit task introduces another set of challenges (such as making sure the commit
             * operation is always idempotent). Given that only coordinator tasks have to survive a failure there's a shortcut in
             * implementation of the error reporting. The assumption is that scheduling also happens on coordinator, thus no RPC is
             * involved in notifying the coordinator. Whenever it is needed to separate scheduling and coordinator tasks on different
             * nodes an RPC mechanism for this notification has to be implemented.
             *
             * Note: For queries that don't have any coordinator stages the situation is still similar. The exchange client that
             * pulls the final query results has to propagate the same notification if the communication link between the exchange client
             * and one of the output tasks is broken.
             */
            TaskFailureReporter failureReporter = new TaskFailureReporter(distributedStagesScheduler);
            queryStateMachine.addOutputTaskFailureListener(failureReporter);

            String catalogName = queryStateMachine.getSession().getCatalog().orElseThrow(() -> new PrestoException(NOT_FOUND, "Catalog is not present"));
            InternalNode coordinator = nodeScheduler.createNodeSelector(new CatalogName(catalogName), false, ImmutableMap.of()).selectCurrentNode();
            for (StageExecution stageExecution : stageExecutions) {
                Optional<RemoteTask> remoteTask = stageExecution.scheduleTask(
                        coordinator,
                        0,
                        ImmutableMultimap.of(),
                        ImmutableMultimap.of());
                stageExecution.schedulingComplete();
                remoteTask.ifPresent(task -> coordinatorTaskManager.addSourceTaskFailureListener(task.getTaskId(), failureReporter));
            }
        }

        public Map<PlanFragmentId, OutputBufferManager> getOutputBuffersForStagesConsumedByCoordinator()
        {
            return outputBuffersForStagesConsumedByCoordinator;
        }

        public Map<PlanFragmentId, Optional<int[]>> getBucketToPartitionForStagesConsumedByCoordinator()
        {
            return bucketToPartitionForStagesConsumedByCoordinator;
        }

        public TaskLifecycleListener getTaskLifecycleListener()
        {
            return taskLifecycleListener;
        }

        public void cancel()
        {
            stageExecutions.forEach(StageExecution::cancel);
        }

        public void abort()
        {
            stageExecutions.forEach(StageExecution::abort);
        }
    }

    private static class QueryOutputTaskLifecycleListener
            implements TaskLifecycleListener
    {
        private final QueryStateMachine queryStateMachine;

        private QueryOutputTaskLifecycleListener(QueryStateMachine queryStateMachine)
        {
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        }

        @Override
        public void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            Map<TaskId, TaskLocation> bufferLocations = ImmutableMap.of(task.getTaskId(), new TaskLocation(uriBuilderFrom(task.getTaskStatus().getSelf())
                    .appendPath("results")
                    .appendPath("0").build(), task.getInstanceId()));
            queryStateMachine.updateOutputLocations(bufferLocations, false);
        }

        @Override
        public void noMoreTasks(PlanFragmentId fragmentId)
        {
            queryStateMachine.updateOutputLocations(ImmutableMap.of(), true);
        }
    }

    private static class TaskLifecycleListenerBridge
            implements TaskLifecycleListener
    {
        private final TaskLifecycleListener listener;

        @GuardedBy("this")
        private final Set<PlanFragmentId> noMoreSourceTasks = new HashSet<>();
        @GuardedBy("this")
        private boolean done;

        private TaskLifecycleListenerBridge(TaskLifecycleListener listener)
        {
            this.listener = requireNonNull(listener, "listener is null");
        }

        @Override
        public synchronized void taskCreated(PlanFragmentId fragmentId, RemoteTask task)
        {
            checkState(!done, "unexpected state");
            listener.taskCreated(fragmentId, task);
        }

        @Override
        public synchronized void noMoreTasks(PlanFragmentId fragmentId)
        {
            checkState(!done, "unexpected state");
            noMoreSourceTasks.add(fragmentId);
        }

        public synchronized void notifyNoMoreSourceTasks()
        {
            checkState(!done, "unexpected state");
            done = true;
            noMoreSourceTasks.forEach(listener::noMoreTasks);
        }
    }

    private static class TaskFailureReporter
            implements TaskFailureListener
    {
        private final AtomicReference<DistributedStagesScheduler> distributedStagesScheduler;

        private TaskFailureReporter(AtomicReference<DistributedStagesScheduler> distributedStagesScheduler)
        {
            this.distributedStagesScheduler = distributedStagesScheduler;
        }

        @Override
        public void onTaskFailed(TaskId taskId, Throwable failure)
        {
            if (failure instanceof PrestoException && REMOTE_TASK_FAILED.toErrorCode().equals(((PrestoException) failure).getErrorCode())) {
                // This error indicates that a downstream task was trying to fetch results from an upstream task that is marked as failed
                // Instead of failing a downstream task let the coordinator handle and report the failure of an upstream task to ensure correct error reporting
                log.debug("Task failure discovered while fetching task results: %s", taskId);
                return;
            }
            log.warn(failure, "Reported task failure: %s", taskId);
            DistributedStagesScheduler scheduler = this.distributedStagesScheduler.get();
            if (scheduler != null) {
                scheduler.reportTaskFailure(taskId, failure);
            }
        }
    }

    /**
     * Scheduler for stages executed on workers.
     * <p>
     * As opposed to {@link CoordinatorStagesScheduler} this component is designed
     * to facilitate failure recovery.
     * <p>
     * In an event of a failure the system may decide to terminate an active scheduler
     * and create a new one to initiate a new query attempt.
     * <p>
     * Stages scheduled by this scheduler are assumed to be safe to retry.
     * <p>
     * The implementation is responsible for task creation and orchestration as well as
     * split enumeration, split assignment and state transitioning for the tasks scheduled.
     */
    private interface DistributedStagesScheduler
    {
        void schedule();

        void cancelStage(StageId stageId);

        void cancel();

        void abort();

        void reportTaskFailure(TaskId taskId, Throwable failureCause);

        void failTaskRemotely(TaskId taskId, Throwable failureCause);

        void addStateChangeListener(StateMachine.StateChangeListener<DistributedStagesSchedulerState> stateChangeListener);

        Optional<StageFailureInfo> getFailureCause();
    }

    private enum DistributedStagesSchedulerState
    {
        PLANNED(false, false),
        RUNNING(false, false),
        FINISHED(true, false),
        CANCELED(true, false),
        ABORTED(true, true),
        FAILED(true, true);

        public static final Set<DistributedStagesSchedulerState> TERMINAL_STATES = Stream.of(DistributedStagesSchedulerState.values()).filter(DistributedStagesSchedulerState::isDone).collect(toImmutableSet());

        private final boolean doneState;
        private final boolean failureState;

        DistributedStagesSchedulerState(boolean doneState, boolean failureState)
        {
            checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
            this.doneState = doneState;
            this.failureState = failureState;
        }

        /**
         * Is this a terminal state.
         */
        public boolean isDone()
        {
            return doneState;
        }

        /**
         * Is this a non-success terminal state.
         */
        public boolean isFailure()
        {
            return failureState;
        }
    }

    private static class DistributedStagesSchedulerStateMachine
    {
        private final QueryId queryId;
        private final StateMachine<DistributedStagesSchedulerState> state;
        private final AtomicReference<StageFailureInfo> failureCause = new AtomicReference<>();

        public DistributedStagesSchedulerStateMachine(QueryId queryId, Executor executor)
        {
            this.queryId = requireNonNull(queryId, "queryId is null");
            requireNonNull(executor, "executor is null");
            state = new StateMachine<>("Distributed stages scheduler", executor, DistributedStagesSchedulerState.PLANNED, DistributedStagesSchedulerState.TERMINAL_STATES);
        }

        public DistributedStagesSchedulerState getState()
        {
            return state.get();
        }

        public boolean transitionToRunning()
        {
            return state.setIf(DistributedStagesSchedulerState.RUNNING, currentState -> !currentState.isDone());
        }

        public boolean transitionToFinished()
        {
            return state.setIf(DistributedStagesSchedulerState.FINISHED, currentState -> !currentState.isDone());
        }

        public boolean transitionToCanceled()
        {
            return state.setIf(DistributedStagesSchedulerState.CANCELED, currentState -> !currentState.isDone());
        }

        public boolean transitionToAborted()
        {
            return state.setIf(DistributedStagesSchedulerState.ABORTED, currentState -> !currentState.isDone());
        }

        public boolean transitionToFailed(Throwable throwable, Optional<StageId> failedStageId)
        {
            requireNonNull(throwable, "throwable is null");

            failureCause.compareAndSet(null, new StageFailureInfo(toFailure(throwable), failedStageId));
            boolean failed = state.setIf(DistributedStagesSchedulerState.FAILED, currentState -> !currentState.isDone());
            if (failed) {
                log.error(throwable, "Failure in distributed stage for query %s", queryId);
            }
            else {
                log.debug(throwable, "Failure in distributed stage for query %s after finished", queryId);
            }
            return failed;
        }

        public Optional<StageFailureInfo> getFailureCause()
        {
            return Optional.ofNullable(failureCause.get());
        }

        /**
         * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
         * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
         * possible notifications are observed out of order due to the asynchronous execution.
         */
        public void addStateChangeListener(StateMachine.StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            state.addStateChangeListener(stateChangeListener);
        }
    }

    private static class StageFailureInfo
    {
        private final ExecutionFailureInfo failureInfo;
        private final Optional<StageId> failedStageId;

        private StageFailureInfo(ExecutionFailureInfo failureInfo, Optional<StageId> failedStageId)
        {
            this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
            this.failedStageId = requireNonNull(failedStageId, "failedStageId is null");
        }

        public ExecutionFailureInfo getFailureInfo()
        {
            return failureInfo;
        }

        public Optional<StageId> getFailedStageId()
        {
            return failedStageId;
        }
    }

    private static class FaultTolerantDistributedStagesScheduler
            implements DistributedStagesScheduler
    {
        private final DistributedStagesSchedulerStateMachine stateMachine;
        private final QueryStateMachine queryStateMachine;
        private final List<FaultTolerantStageScheduler> schedulers;
        private final SplitSchedulerStats schedulerStats;
        private final NodeAllocator nodeAllocator;

        private final AtomicBoolean started = new AtomicBoolean();

        public static FaultTolerantDistributedStagesScheduler create(
                QueryStateMachine queryStateMachine,
                StageManager stageManager,
                FailureDetector failureDetector,
                TaskSourceFactory taskSourceFactory,
                TaskDescriptorStorage taskDescriptorStorage,
                ExchangeManager exchangeManager,
                NodePartitioningManager nodePartitioningManager,
                TaskLifecycleListener coordinatorTaskLifecycleListener,
                int taskRetryAttemptsOverall,
                int taskRetryAttemptsPerTask,
                int maxTasksWaitingForNodePerStage,
                ScheduledExecutorService scheduledExecutorService,
                SplitSchedulerStats schedulerStats,
                NodeAllocatorService nodeAllocatorService,
                PartitionMemoryEstimatorFactory partitionMemoryEstimatorFactory,
                TaskExecutionStats taskExecutionStats)
        {
            taskDescriptorStorage.initialize(queryStateMachine.getQueryId());
            queryStateMachine.addStateChangeListener(state -> {
                if (state.isDone()) {
                    taskDescriptorStorage.destroy(queryStateMachine.getQueryId());
                }
            });

            DistributedStagesSchedulerStateMachine sqlStateMachine = new DistributedStagesSchedulerStateMachine(queryStateMachine.getQueryId(), scheduledExecutorService);

            Session querySession = queryStateMachine.getSession();
            int partitionCount = getFaultTolerantExecutionPartitionCount(querySession);
            Function<PartitioningHandle, BucketToPartition> bucketToPartitionCache = createBucketToPartitionCache(nodePartitioningManager, querySession, partitionCount);

            ImmutableList.Builder<FaultTolerantStageScheduler> sqlStageSchedulers = ImmutableList.builder();
            Map<PlanFragmentId, Exchange> exchanges = new HashMap<>();
            NodeAllocator queryNodeAllocator = nodeAllocatorService.getNodeAllocator(querySession);

            try {
                // root to children order
                List<SqlStageExecution> distributedStagesInTopologicalOrder = stageManager.getDistributedStagesInTopologicalOrder();
                // children to root order
                List<SqlStageExecution> distributedStagesInReverseTopologicalOrder = reverse(distributedStagesInTopologicalOrder);

                ImmutableSet.Builder<PlanFragmentId> coordinatorConsumedFragmentsBuilder = ImmutableSet.builder();

                checkArgument(taskRetryAttemptsOverall >= 0, "taskRetryAttemptsOverall must be greater than or equal to 0: %s", taskRetryAttemptsOverall);
                AtomicInteger remainingTaskRetryAttemptsOverall = new AtomicInteger(taskRetryAttemptsOverall);
                for (SqlStageExecution stage : distributedStagesInReverseTopologicalOrder) {
                    PlanFragment fragment = stage.getFragment();
                    Optional<SqlStageExecution> parentStage = stageManager.getParent(stage.getStageId());
                    TaskLifecycleListener taskLifecycleListener;
                    Optional<Exchange> exchange;
                    if (!parentStage.isPresent() || parentStage.get().getFragment().getPartitioning().isCoordinatorOnly()) {
                        // output will be consumed by coordinator
                        exchange = Optional.empty();
                        taskLifecycleListener = coordinatorTaskLifecycleListener;
                        coordinatorConsumedFragmentsBuilder.add(fragment.getId());
                    }
                    else {
                        // create external exchange
                        ExchangeContext context = new ExchangeContext(querySession.getQueryId(), new ExchangeId("external-exchange-" + stage.getStageId().getId()));
                        exchange = Optional.of(exchangeManager.createExchange(context, partitionCount));
                        exchanges.put(fragment.getId(), exchange.get());
                        taskLifecycleListener = TaskLifecycleListener.NO_OP;
                    }

                    ImmutableMap.Builder<PlanFragmentId, Exchange> sourceExchanges = ImmutableMap.builder();
                    for (SqlStageExecution childStage : stageManager.getChildren(fragment.getId())) {
                        PlanFragmentId childFragmentId = childStage.getFragment().getId();
                        Exchange sourceExchange = exchanges.get(childFragmentId);
                        verify(sourceExchange != null, "exchange not found for fragment: %s", childFragmentId);
                        sourceExchanges.put(childFragmentId, sourceExchange);
                    }

                    BucketToPartition inputBucketToPartition = bucketToPartitionCache.apply(fragment.getPartitioning());
                    FaultTolerantStageScheduler scheduler = new FaultTolerantStageScheduler(
                            querySession,
                            stage,
                            failureDetector,
                            taskSourceFactory,
                            queryNodeAllocator,
                            taskDescriptorStorage,
                            partitionMemoryEstimatorFactory.createPartitionMemoryEstimator(),
                            taskExecutionStats,
                            taskLifecycleListener,
                            (future, delay) -> scheduledExecutorService.schedule(() -> future.set(null), delay.toMillis(), MILLISECONDS),
                            systemTicker(),
                            exchange,
                            bucketToPartitionCache.apply(fragment.getPartitioningScheme().getPartitioning().getHandle()).getBucketToPartitionMap(),
                            sourceExchanges.build(),
                            inputBucketToPartition.getBucketToPartitionMap(),
                            inputBucketToPartition.getBucketNodeMap(),
                            remainingTaskRetryAttemptsOverall,
                            taskRetryAttemptsPerTask,
                            maxTasksWaitingForNodePerStage);

                    sqlStageSchedulers.add(scheduler);
                }

                Set<PlanFragmentId> coordinatorConsumedFragments = coordinatorConsumedFragmentsBuilder.build();
                sqlStateMachine.addStateChangeListener(state -> {
                    if (state == DistributedStagesSchedulerState.FINISHED) {
                        coordinatorConsumedFragments.forEach(coordinatorTaskLifecycleListener::noMoreTasks);
                    }
                });

                return new FaultTolerantDistributedStagesScheduler(
                        sqlStateMachine,
                        queryStateMachine,
                        sqlStageSchedulers.build(),
                        schedulerStats,
                        queryNodeAllocator);
            }
            catch (Throwable t) {
                for (FaultTolerantStageScheduler scheduler : sqlStageSchedulers.build()) {
                    try {
                        scheduler.abort();
                    }
                    catch (Throwable closeFailure) {
                        if (t != closeFailure) {
                            t.addSuppressed(closeFailure);
                        }
                    }
                }

                try {
                    queryNodeAllocator.close();
                }
                catch (Throwable closeFailure) {
                    if (t != closeFailure) {
                        t.addSuppressed(closeFailure);
                    }
                }

                for (Exchange exchange : exchanges.values()) {
                    try {
                        exchange.close();
                    }
                    catch (Throwable closeFailure) {
                        if (t != closeFailure) {
                            t.addSuppressed(closeFailure);
                        }
                    }
                }
                throw t;
            }
        }

        private static Function<PartitioningHandle, BucketToPartition> createBucketToPartitionCache(NodePartitioningManager nodePartitioningManager, Session session, int partitionCount)
        {
            Map<PartitioningHandle, BucketToPartition> cachingMap = new HashMap<>();
            return partitioningHandle ->
                    cachingMap.computeIfAbsent(
                            partitioningHandle,
                            handle -> createBucketToPartitionMap(session, partitionCount, handle, nodePartitioningManager));
        }

        private static BucketToPartition createBucketToPartitionMap(
                Session session,
                int partitionCount,
                PartitioningHandle partitioningHandle,
                NodePartitioningManager nodePartitioningManager)
        {
            if (partitioningHandle.equals(FIXED_HASH_DISTRIBUTION)) {
                return new BucketToPartition(Optional.of(IntStream.range(0, partitionCount).toArray()), Optional.empty());
            }
            else if (partitioningHandle.getConnectorId().isPresent()) {
                BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, partitioningHandle, true);
                int bucketCount = bucketNodeMap.getBucketCount();
                int[] bucketToPartition = new int[bucketCount];
                if (bucketNodeMap.isDynamic()) {
                    int nextPartitionId = 0;
                    for (int bucket = 0; bucket < bucketCount; bucket++) {
                        bucketToPartition[bucket] = nextPartitionId++ % partitionCount;
                    }
                }
                else {
                    // make sure all buckets mapped to the same node map to the same partition, such that locality requirements are respected in scheduling
                    Map<InternalNode, Integer> nodeToPartition = new HashMap<>();
                    int nextPartitionId = 0;
                    for (int bucket = 0; bucket < bucketCount; bucket++) {
                        InternalNode node = bucketNodeMap.getAssignedNode(bucket)
                                .orElseThrow(() -> new IllegalStateException("Nodes are expected to be assigned for non dynamic BucketNodeMap"));
                        Integer partitionId = nodeToPartition.get(node);
                        if (partitionId == null) {
                            partitionId = nextPartitionId++;
                            nodeToPartition.put(node, partitionId);
                        }
                        bucketToPartition[bucket] = partitionId;
                    }
                }
                return new BucketToPartition(Optional.of(bucketToPartition), Optional.of(bucketNodeMap));
            }
            else {
                return new BucketToPartition(Optional.empty(), Optional.empty());
            }
        }

        private static class BucketToPartition
        {
            private final Optional<int[]> bucketToPartitionMap;
            private final Optional<BucketNodeMap> bucketNodeMap;

            private BucketToPartition(Optional<int[]> bucketToPartitionMap, Optional<BucketNodeMap> bucketNodeMap)
            {
                this.bucketToPartitionMap = requireNonNull(bucketToPartitionMap, "bucketToPartitionMap is null");
                this.bucketNodeMap = requireNonNull(bucketNodeMap, "bucketNodeMap is null");
            }

            public Optional<int[]> getBucketToPartitionMap()
            {
                return bucketToPartitionMap;
            }

            public Optional<BucketNodeMap> getBucketNodeMap()
            {
                return bucketNodeMap;
            }
        }

        private FaultTolerantDistributedStagesScheduler(
                DistributedStagesSchedulerStateMachine stateMachine,
                QueryStateMachine queryStateMachine,
                List<FaultTolerantStageScheduler> schedulers,
                SplitSchedulerStats schedulerStats,
                NodeAllocator nodeAllocator)
        {
            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
            this.schedulers = requireNonNull(schedulers, "schedulers is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.nodeAllocator = requireNonNull(nodeAllocator, "nodeAllocator is null");
        }

        @Override
        public void schedule()
        {
            checkState(started.compareAndSet(false, true), "already started");

            if (schedulers.isEmpty()) {
                stateMachine.transitionToFinished();
                return;
            }

            stateMachine.transitionToRunning();

            try (SetThreadName ignored = new SetThreadName("Query-%s", queryStateMachine.getQueryId())) {
                List<ListenableFuture<Void>> blockedStages = new ArrayList<>();
                while (!isFinishingOrDone(queryStateMachine) && !stateMachine.getState().isDone()) {
                    blockedStages.clear();
                    boolean atLeastOneStageIsNotBlocked = false;
                    boolean allFinished = true;
                    for (FaultTolerantStageScheduler scheduler : schedulers) {
                        if (scheduler.isFinished()) {
                            continue;
                        }
                        allFinished = false;
                        ListenableFuture<Void> blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                            continue;
                        }
                        try {
                            scheduler.schedule();
                        }
                        catch (Throwable t) {
                            fail(t, Optional.of(scheduler.getStageId()));
                            return;
                        }
                        blocked = scheduler.isBlocked();
                        if (!blocked.isDone()) {
                            blockedStages.add(blocked);
                        }
                        else {
                            atLeastOneStageIsNotBlocked = true;
                        }
                    }
                    if (allFinished) {
                        stateMachine.transitionToFinished();
                        return;
                    }
                    // wait for a state change and then schedule again
                    if (!atLeastOneStageIsNotBlocked) {
                        verify(!blockedStages.isEmpty(), "blockedStages is not expected to be empty here");
                        try (TimeStat.BlockTimer timer = schedulerStats.getSleepTime().time()) {
                            try {
                                tryGetFutureValue(whenAnyComplete(blockedStages), 1, SECONDS);
                            }
                            catch (CancellationException e) {
                                log.debug(
                                        "Scheduling has been cancelled for query %s. Query state: %s, Scheduler state: %s",
                                        queryStateMachine.getQueryId(),
                                        queryStateMachine.getQueryState(),
                                        stateMachine.getState());
                            }
                        }
                    }
                }
            }
            catch (Throwable t) {
                fail(t, Optional.empty());
            }
        }

        private static boolean isFinishingOrDone(QueryStateMachine queryStateMachine)
        {
            QueryState queryState = queryStateMachine.getQueryState();
            return queryState == FINISHING || queryState.isDone();
        }

        private void fail(Throwable t, Optional<StageId> failedStageId)
        {
            stateMachine.transitionToFailed(t, failedStageId);
            schedulers.forEach(FaultTolerantStageScheduler::abort);
            closeNodeAllocator();
        }

        @Override
        public void cancelStage(StageId stageId)
        {
            throw new UnsupportedOperationException("partial cancel is not supported in fault tolerant mode");
        }

        @Override
        public void cancel()
        {
            stateMachine.transitionToCanceled();
            schedulers.forEach(FaultTolerantStageScheduler::cancel);
            closeNodeAllocator();
        }

        @Override
        public void abort()
        {
            stateMachine.transitionToAborted();
            schedulers.forEach(FaultTolerantStageScheduler::abort);
            closeNodeAllocator();
        }

        private void closeNodeAllocator()
        {
            try {
                nodeAllocator.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing node allocator for query: %s", queryStateMachine.getQueryId());
            }
        }

        @Override
        public void reportTaskFailure(TaskId taskId, Throwable failureCause)
        {
            for (FaultTolerantStageScheduler scheduler : schedulers) {
                if (scheduler.getStageId().equals(taskId.getStageId())) {
                    scheduler.reportTaskFailure(taskId, failureCause);
                }
            }
        }

        @Override
        public void failTaskRemotely(TaskId taskId, Throwable failureCause)
        {
            for (FaultTolerantStageScheduler scheduler : schedulers) {
                if (scheduler.getStageId().equals(taskId.getStageId())) {
                    scheduler.failTaskRemotely(taskId, failureCause);
                }
            }
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<DistributedStagesSchedulerState> stateChangeListener)
        {
            stateMachine.addStateChangeListener(stateChangeListener);
        }

        @Override
        public Optional<StageFailureInfo> getFailureCause()
        {
            return stateMachine.getFailureCause();
        }
    }
}
