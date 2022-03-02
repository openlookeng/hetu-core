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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
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
import io.prestosql.execution.BasicStageStats;
import io.prestosql.execution.LocationFactory;
import io.prestosql.execution.NodeTaskMap;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStateMachine;
import io.prestosql.execution.RemoteTask;
import io.prestosql.execution.RemoteTaskFactory;
import io.prestosql.execution.SqlStageExecution;
import io.prestosql.execution.StageId;
import io.prestosql.execution.StageInfo;
import io.prestosql.execution.StageState;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStatus;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.metadata.InternalNode;
import io.prestosql.operator.TaskLocation;
import io.prestosql.server.ResourceGroupInfo;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.planner.NodePartitionMap;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.plan.PlanFragmentId;
import io.prestosql.sql.planner.plan.RemoteSourceNode;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.newConcurrentHashSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.MoreFutures.whenAnyComplete;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.prestosql.SystemSessionProperties.getConcurrentLifespansPerNode;
import static io.prestosql.SystemSessionProperties.getWriterMinSize;
import static io.prestosql.SystemSessionProperties.isReuseTableScanEnabled;
import static io.prestosql.execution.BasicStageStats.aggregateBasicStageStats;
import static io.prestosql.execution.SqlStageExecution.createSqlStageExecution;
import static io.prestosql.execution.StageState.ABORTED;
import static io.prestosql.execution.StageState.CANCELED;
import static io.prestosql.execution.StageState.FAILED;
import static io.prestosql.execution.StageState.FINISHED;
import static io.prestosql.execution.StageState.RESUMABLE_FAILURE;
import static io.prestosql.execution.StageState.RUNNING;
import static io.prestosql.execution.StageState.SCHEDULED;
import static io.prestosql.execution.scheduler.SourcePartitionedScheduler.newSourcePartitionedSchedulerAsStageScheduler;
import static io.prestosql.snapshot.SnapshotConfig.calculateTaskCount;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.spi.connector.CatalogName.isInternalSystemConnector;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SCALED_WRITER_DISTRIBUTION;
import static io.prestosql.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static io.prestosql.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static io.prestosql.util.Failures.checkCondition;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
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
    private final Map<PlanNodeId, FixedNodeScheduleData> feederScheduledNodes = new ConcurrentHashMap<>();

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
            Map<StageId, Integer> stageTaskCounts)
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
                stageTaskCounts);
        sqlQueryScheduler.initialize();
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
            Map<StageId, Integer> stageTaskCounts)
    {
        this.queryStateMachine = requireNonNull(queryStateMachine, "queryStateMachine is null");
        this.executionPolicy = requireNonNull(executionPolicy, "schedulerPolicyFactory is null");
        this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
        this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
        this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
        this.summarizeTaskInfo = summarizeTaskInfo;

        this.snapshotManager = snapshotManager;
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            snapshotManager.setRescheduler(this::cancelToResume);
        }

        // todo come up with a better way to build this, or eliminate this map
        ImmutableMap.Builder<StageId, StageScheduler> stageSchedulerBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<StageId, StageLinkage> stageLinkageBuilder = ImmutableMap.builder();

        // Only fetch a distribution once per query to assure all stages see the same machine assignments
        Map<PartitioningHandle, NodePartitionMap> partitioningCache = new HashMap<>();

        OutputBufferId rootBufferId = Iterables.getOnlyElement(rootOutputBuffers.getBuffers().keySet());
        visitedPlanFrags.add(plan.getFragment().getId());
        final boolean isSnapshotEnabled = SystemSessionProperties.isSnapshotEnabled(session);
        List<SqlStageExecution> stageExecutions = createStages(
                (fragmentId, tasks, noMoreExchangeLocations) -> updateQueryOutputLocations(queryStateMachine, rootBufferId, tasks, noMoreExchangeLocations),
                new AtomicInteger(),
                locationFactory,
                plan.withBucketToPartition(Optional.of(new int[1])),
                nodeScheduler,
                remoteTaskFactory,
                session,
                splitBatchSize,
                (partitioningHandle, nodeCount) -> partitioningCache.computeIfAbsent(partitioningHandle, handle -> nodePartitioningManager.getNodePartitioningMap(session, handle, isSnapshotEnabled, nodeCount)),
                nodePartitioningManager,
                queryExecutor,
                schedulerExecutor,
                failureDetector,
                nodeTaskMap,
                stageSchedulerBuilder,
                stageLinkageBuilder,
                isSnapshotEnabled,
                snapshotManager,
                stageTaskCounts);

        SqlStageExecution rootStage = stageExecutions.get(0);
        rootStage.setOutputBuffers(rootOutputBuffers);
        this.rootStageId = rootStage.getStageId();

        this.stages = stageExecutions.stream()
                .collect(toImmutableMap(SqlStageExecution::getStageId, identity()));

        if (isSnapshotEnabled) {
            // Snapshot: add minimum number of tasks to task list in query snapshot manager, so that we don't complete prematurely,
            // e.g. 1 task is schedule and finishes right away, before other tasks are scheduled, then snapshot manager may think
            // snapshot is complete, because all known tasks are covered.
            for (SqlStageExecution stage : stageExecutions) {
                snapshotManager.addNewTask(new TaskId(stage.getStageId(), 0));
            }
        }

        this.stageSchedulers = stageSchedulerBuilder.build();
        this.stageLinkages = stageLinkageBuilder.build();

        this.executor = queryExecutor;
        this.session = session;
        this.currentTimerLevel = 0;
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

                queryStateMachine.transitionToRescheduling();
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
                if (SystemSessionProperties.isSnapshotEnabled(session)) {
                    if (snapshotManager.hasPendingResume()) {
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
                if (state == RESUMABLE_FAILURE) {
                    // Snapshot: One of the stages has a resumable failure. Cancel all stages so they can be rescheduled.
                    cancelToResume();
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
                queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo()));
            }
        });
        for (SqlStageExecution stage : stages.values()) {
            stage.addFinalStageInfoListener(status -> queryStateMachine.updateQueryInfo(Optional.ofNullable(getStageInfo())));
        }
    }

    private static void updateQueryOutputLocations(QueryStateMachine queryStateMachine, OutputBufferId rootBufferId, Set<RemoteTask> tasks, boolean noMoreExchangeLocations)
    {
        Set<TaskLocation> bufferLocations = tasks.stream()
                .map(task -> {
                    URI uri = task.getTaskStatus().getSelf();
                    String instanceId = task.getInstanceId();
                    URI newUri = uriBuilderFrom(uri).appendPath("results").appendPath(rootBufferId.toString()).build();
                    return new TaskLocation(newUri, instanceId);
                }).collect(toImmutableSet());
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
            boolean isSnapshotEnabled,
            QuerySnapshotManager snapshotManager,
            Map<StageId, Integer> stageTaskCounts)
    {
        ImmutableList.Builder<SqlStageExecution> localStages = ImmutableList.builder();

        StageId stageId = new StageId(queryStateMachine.getQueryId(), nextStageId.getAndIncrement());
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
                snapshotManager);

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
            if (isSnapshotEnabled) {
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
                    if (isSnapshotEnabled) {
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
                    isSnapshotEnabled,
                    snapshotManager,
                    stageTaskCounts);
            localStages.addAll(subTree);

            SqlStageExecution childStage = subTree.get(0);
            childStagesBuilder.add(childStage);
            Optional<RemoteSourceNode> parentNode = plan.getFragment().getRemoteSourceNodes().stream().filter(x -> x.getSourceFragmentIds().contains(childStage.getFragment().getId())).findAny();

            checkArgument(parentNode.isPresent(), "Couldn't find parent of a CTE node");
            childStage.setParentId(parentNode.get().getId());
        }
        Set<SqlStageExecution> childStages = childStagesBuilder.build();
        stageExecution.addStateChangeListener(newState -> {
            if (newState.isDone() && newState != StageState.RESCHEDULING) {
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
                    isSnapshotEnabled,
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

        return buildStageInfo(rootStageId, stageInfos);
    }

    private StageInfo buildStageInfo(StageId stageId, Map<StageId, StageInfo> stageInfos)
    {
        StageInfo parent = stageInfos.get(stageId);
        checkArgument(parent != null, "No stageInfo for %s", parent);
        List<StageInfo> childStages = stageLinkages.get(stageId).getChildStageIds().stream()
                .map(childStageId -> buildStageInfo(childStageId, stageInfos))
                .collect(toImmutableList());
        if (childStages.isEmpty()) {
            return parent;
        }
        return new StageInfo(
                parent.getStageId(),
                parent.getState(),
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
        if (started.compareAndSet(false, true)) {
            executor.submit(this::schedule);
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
                    /* Todo(nitin) get groupSize specification from the ResourceGroupManager */
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
                // Snapshot: if state is resumable_failure, then state of stage and query will change soon again. Don't treat as an error.
                if (state != SCHEDULED && state != RUNNING && !state.isDone() && state != RESUMABLE_FAILURE) {
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
                    // Snapshot: when trying to reschedule, then don't close the scheduler (and more importantly, split sources in it)
                    QueryState state = queryStateMachine.getQueryState();
                    if (state != QueryState.RESCHEDULING && state != QueryState.RESUMING) {
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
}
