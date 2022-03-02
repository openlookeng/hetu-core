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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.SetThreadName;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.cost.CostCalculator;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cube.CubeManager;
import io.prestosql.dynamicfilter.DynamicFilterService;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.buffer.OutputBuffers.OutputBufferId;
import io.prestosql.execution.scheduler.ExecutionPolicy;
import io.prestosql.execution.scheduler.NodeScheduler;
import io.prestosql.execution.scheduler.SplitSchedulerStats;
import io.prestosql.execution.scheduler.SqlQueryScheduler;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.failuredetector.FailureDetector;
import io.prestosql.heuristicindex.HeuristicIndexerManager;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.ForScheduler;
import io.prestosql.query.CachedSqlQueryExecution;
import io.prestosql.query.CachedSqlQueryExecutionPlan;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.snapshot.MarkerAnnouncer;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.snapshot.SnapshotUtils;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.CatalogName;
import io.prestosql.spi.connector.StandardWarningCode;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.split.SplitManager;
import io.prestosql.split.SplitSource;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.DistributedExecutionPlanner;
import io.prestosql.sql.planner.InputExtractor;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.sql.planner.PartitioningHandle;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.PlanFragmenter;
import io.prestosql.sql.planner.PlanOptimizers;
import io.prestosql.sql.planner.SimplePlanVisitor;
import io.prestosql.sql.planner.StageExecutionPlan;
import io.prestosql.sql.planner.SubPlan;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.OutputNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.tree.CreateTableAsSelect;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.Insert;
import io.prestosql.sql.tree.InsertCube;
import io.prestosql.sql.tree.Statement;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.HetuConfig;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static io.prestosql.execution.scheduler.SqlQueryScheduler.createSqlQueryScheduler;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static io.prestosql.sql.planner.DistributedExecutionPlanner.Mode.NORMAL;
import static io.prestosql.sql.planner.DistributedExecutionPlanner.Mode.RESUME;
import static io.prestosql.sql.planner.DistributedExecutionPlanner.Mode.SNAPSHOT;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTERS;
import static io.prestosql.statestore.StateStoreConstants.QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class SqlQueryExecution
        implements QueryExecution
{
    private static final Logger log = Logger.get(SqlQueryExecution.class);

    private static final OutputBufferId OUTPUT_BUFFER_ID = new OutputBufferId(0);

    private final QueryStateMachine stateMachine;
    private final String slug;
    private final Metadata metadata;
    private final CubeManager cubeManager;
    private final SqlParser sqlParser;
    private final SplitManager splitManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final NodeScheduler nodeScheduler;
    private final List<PlanOptimizer> planOptimizers;
    private final PlanFragmenter planFragmenter;
    private final RemoteTaskFactory remoteTaskFactory;
    private final LocationFactory locationFactory;
    private final int scheduleSplitBatchSize;
    private final ExecutorService queryExecutor;
    private final ScheduledExecutorService schedulerExecutor;
    private final FailureDetector failureDetector;

    private final AtomicReference<SqlQueryScheduler> queryScheduler = new AtomicReference<>();
    private final AtomicReference<Plan> queryPlan = new AtomicReference<>();
    private final NodeTaskMap nodeTaskMap;
    private final ExecutionPolicy executionPolicy;
    private final SplitSchedulerStats schedulerStats;
    private final Analysis analysis;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;
    private final DynamicFilterService dynamicFilterService;
    private final HeuristicIndexerManager heuristicIndexerManager;
    private final StateStoreProvider stateStoreProvider;
    private final QuerySnapshotManager snapshotManager;
    private final WarningCollector warningCollector;

    public SqlQueryExecution(
            PreparedQuery preparedQuery,
            QueryStateMachine stateMachine,
            String slug,
            Metadata metadata,
            CubeManager cubeManager,
            AccessControl accessControl,
            SqlParser sqlParser,
            SplitManager splitManager,
            NodePartitioningManager nodePartitioningManager,
            NodeScheduler nodeScheduler,
            List<PlanOptimizer> planOptimizers,
            PlanFragmenter planFragmenter,
            RemoteTaskFactory remoteTaskFactory,
            LocationFactory locationFactory,
            int scheduleSplitBatchSize,
            ExecutorService queryExecutor,
            ScheduledExecutorService schedulerExecutor,
            FailureDetector failureDetector,
            NodeTaskMap nodeTaskMap,
            QueryExplainer queryExplainer,
            ExecutionPolicy executionPolicy,
            SplitSchedulerStats schedulerStats,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector,
            DynamicFilterService dynamicFilterService,
            HeuristicIndexerManager heuristicIndexerManager,
            StateStoreProvider stateStoreProvider,
            SnapshotUtils snapshotUtils)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            this.slug = requireNonNull(slug, "slug is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.executionPolicy = requireNonNull(executionPolicy, "executionPolicy is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            this.warningCollector = requireNonNull(warningCollector);

            this.snapshotManager = snapshotUtils.getOrCreateQuerySnapshotManager(stateMachine.getQueryId(), stateMachine.getSession());

            checkArgument(scheduleSplitBatchSize > 0, "scheduleSplitBatchSize must be greater than 0");
            this.scheduleSplitBatchSize = scheduleSplitBatchSize;

            this.stateMachine = requireNonNull(stateMachine, "stateMachine is null");
            this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");

            // clear dynamic filter tasks and data created for this query
            stateMachine.addStateChangeListener(state -> {
                if (isEnableDynamicFiltering(stateMachine.getSession()) && state.isDone()) {
                    dynamicFilterService.clearDynamicFiltersForQuery(stateMachine.getQueryId().getId());
                }
            });

            // analyze query
            requireNonNull(preparedQuery, "preparedQuery is null");
            Analyzer analyzer = new Analyzer(
                    stateMachine.getSession(),
                    metadata,
                    sqlParser,
                    accessControl,
                    Optional.of(queryExplainer),
                    preparedQuery.getParameters(),
                    warningCollector,
                    heuristicIndexerManager,
                    cubeManager);
            this.analysis = analyzer.analyze(preparedQuery.getStatement());

            stateMachine.setUpdateType(analysis.getUpdateType());

            // when the query finishes cache the final query info, and clear the reference to the output stage
            AtomicReference<SqlQueryScheduler> localQueryScheduler = this.queryScheduler;
            stateMachine.addStateChangeListener(state -> {
                //Set the AsyncRunning flag if query is capable of running async
                if (analysis.isAsyncQuery() && state == QueryState.RUNNING) {
                    stateMachine.setRunningAsync(true);
                }

                if (!state.isDone()) {
                    return;
                }

                // Snapshot: query is now done, so clear its entries in the snapshot manager
                if (SystemSessionProperties.isSnapshotEnabled(stateMachine.getSession())) {
                    snapshotManager.doneQuery(state);
                }
                SqlQueryScheduler scheduler = localQueryScheduler.get();
                if (scheduler != null) {
                    scheduler.abort();
                }
            });

            this.remoteTaskFactory = new MemoryTrackingRemoteTaskFactory(requireNonNull(remoteTaskFactory, "remoteTaskFactory is null"), stateMachine);
        }
    }

    @Override
    public String getSlug()
    {
        return slug;
    }

    @Override
    public VersionedMemoryPoolId getMemoryPool()
    {
        return stateMachine.getMemoryPool();
    }

    @Override
    public void setMemoryPool(VersionedMemoryPoolId poolId)
    {
        stateMachine.setMemoryPool(poolId);
    }

    @Override
    public QuerySnapshotManager getQuerySnapshotManager()
    {
        return snapshotManager;
    }

    @Override
    public DataSize getUserMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getUserMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getUserMemoryReservation());
    }

    @Override
    public DataSize getTotalMemoryReservation()
    {
        // acquire reference to scheduler before checking finalQueryInfo, because
        // state change listener sets finalQueryInfo and then clears scheduler when
        // the query finishes.
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalMemoryReservation();
        }
        if (scheduler == null) {
            return new DataSize(0, BYTE);
        }
        return succinctBytes(scheduler.getTotalMemoryReservation());
    }

    @Override
    public DateTime getCreateTime()
    {
        return stateMachine.getCreateTime();
    }

    @Override
    public Optional<DateTime> getExecutionStartTime()
    {
        return stateMachine.getExecutionStartTime();
    }

    @Override
    public DateTime getLastHeartbeat()
    {
        return stateMachine.getLastHeartbeat();
    }

    @Override
    public Optional<DateTime> getEndTime()
    {
        return stateMachine.getEndTime();
    }

    @Override
    public Duration getTotalCpuTime()
    {
        SqlQueryScheduler scheduler = queryScheduler.get();
        Optional<QueryInfo> finalQueryInfo = stateMachine.getFinalQueryInfo();
        if (finalQueryInfo.isPresent()) {
            return finalQueryInfo.get().getQueryStats().getTotalCpuTime();
        }
        if (scheduler == null) {
            return new Duration(0, SECONDS);
        }
        return scheduler.getTotalCpuTime();
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo()
    {
        return stateMachine.getFinalQueryInfo()
                .map(BasicQueryInfo::new)
                .orElseGet(() -> stateMachine.getBasicQueryInfo(Optional.ofNullable(queryScheduler.get()).map(SqlQueryScheduler::getBasicStageStats)));
    }

    private void findMappingFromPlan(Map<String, Set<String>> mapping, PlanNode sourceNode)
    {
        if (sourceNode != null && sourceNode instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) sourceNode;
            Map<Symbol, RowExpression> assignments = projectNode.getAssignments().getMap();
            for (Symbol symbol : assignments.keySet()) {
                if (mapping.containsKey(symbol.getName())) {
                    Set<String> sets = mapping.get(symbol.getName());
                    RowExpression expression = assignments.get(symbol);
                    if (expression instanceof VariableReferenceExpression) {
                        sets.add(((VariableReferenceExpression) expression).getName());
                    }
                    else {
                        sets.add(expression.toString());
                    }
                }
                else {
                    for (Map.Entry<String, Set<String>> entry : mapping.entrySet()) {
                        if (entry.getValue().contains(symbol.getName())) {
                            RowExpression expression = assignments.get(symbol);
                            if (expression instanceof VariableReferenceExpression) {
                                entry.getValue().add(((VariableReferenceExpression) expression).getName());
                            }
                            else {
                                entry.getValue().add(expression.toString());
                            }
                        }
                    }
                }
            }
        }

        for (PlanNode planNode : sourceNode.getSources()) {
            findMappingFromPlan(mapping, planNode);
        }
    }

    private void handleCrossRegionDynamicFilter(PlanRoot plan)
    {
        if (!isCrossRegionDynamicFilterEnabled(getSession()) || plan == null) {
            return;
        }

        StateStore stateStore = stateStoreProvider.getStateStore();
        if (stateStore == null) {
            return;
        }

        String queryId = getSession().getQueryId().getId();
        log.debug("queryId=%s begin to find columnToColumnMapping.", queryId);
        PlanNode outputNode = plan.getRoot().getFragment().getRoot();
        Map<String, Set<String>> columnToSymbolMapping = new HashMap<>();

        if (outputNode != null && outputNode instanceof OutputNode) {
            List<String> queryColumnNames = ((OutputNode) outputNode).getColumnNames();
            List<Symbol> outputSymbols = outputNode.getOutputSymbols();

            Map<String, Set<String>> tmpMapping = new HashMap<>(outputSymbols.size());
            for (Symbol symbol : outputNode.getOutputSymbols()) {
                Set<String> sets = new HashSet();
                sets.add(symbol.getName());
                tmpMapping.put(symbol.getName(), sets);
            }

            for (PlanFragment fragment : plan.getRoot().getAllFragments()) {
                if ("0".equals(fragment.getId().toString())) {
                    continue;
                }

                PlanNode sourceNode = fragment.getRoot();
                findMappingFromPlan(tmpMapping, sourceNode);
            }

            for (int i = 0; i < outputSymbols.size(); i++) {
                columnToSymbolMapping.put(queryColumnNames.get(i), tmpMapping.get(outputSymbols.get(i).getName()));
            }
        }

        // save mapping into stateStore
        StateMap<String, Object> mappingStateMap = (StateMap<String, Object>) stateStore.getOrCreateStateCollection(CROSS_REGION_DYNAMIC_FILTERS, StateCollection.Type.MAP);
        mappingStateMap.put(queryId + QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING, columnToSymbolMapping);
        log.debug("queryId=%s, add columnToSymbolMapping into hazelcast success.", queryId + QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING);
    }

    @Override
    public void start()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            try {
                // transition to planning
                if (!stateMachine.transitionToPlanning()) {
                    // query already started or finished
                    return;
                }

                // analyze query
                PlanRoot plan = analyzeQuery();

                try {
                    handleCrossRegionDynamicFilter(plan);
                }
                catch (Throwable e) {
                    // ignore any exception
                    log.warn("something unexpected happened.. cause: %s", e.getMessage());
                }

                // plan distribution of query
                planDistribution(plan);

                // transition to starting
                if (!stateMachine.transitionToStarting()) {
                    // query already started or finished
                    return;
                }
                stateMachine.addStateChangeListener(state -> {
                    if (state == QueryState.RESUMING) {
                        // Snapshot: old stages/tasks have finished. Ready to resume.
                        try {
                            resumeQuery(plan);
                        }
                        catch (Throwable e) {
                            fail(e);
                            throwIfInstanceOf(e, Error.class);
                            log.warn(e, "Encountered error while rescheduling query");
                        }
                    }
                });
                // if query is not finished, start the scheduler, otherwise cancel it
                SqlQueryScheduler scheduler = queryScheduler.get();

                if (!stateMachine.isDone()) {
                    scheduler.start();
                }
            }
            catch (Throwable e) {
                fail(e);
                throwIfInstanceOf(e, Error.class);
                log.warn(e, "Encountered error while scheduling query");
            }
        }
    }

    private void resumeQuery(PlanRoot plan)
    {
        SqlQueryScheduler oldScheduler = queryScheduler.get();
        try {
            // Wait for previous scheduler to finish.
            // This is important, otherwise the old schedule may close split sources after the new scheduler has started.
            oldScheduler.doneScheduling().get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        log.debug("Rescheduling query %s from a resumable task failure.", getQueryId());
        PartitioningHandle partitioningHandle = plan.getRoot().getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler;
        try {
            scheduler = createResumeScheduler(plan, rootOutputBuffers);
        }
        catch (PrestoException e) {
            if (e.getErrorCode() == NO_NODES_AVAILABLE.toErrorCode()) {
                // Not enough worker to resume all tasks. Retrying from any saved snapshot likely wont' work either.
                // Clear ongoing and existing snapshots and restart.
                snapshotManager.invalidateAllSnapshots();
                scheduler = createResumeScheduler(plan, rootOutputBuffers);
            }
            else {
                throw e;
            }
        }
        queryScheduler.set(scheduler);
        log.debug("Restarting query %s from a resumable task failure.", getQueryId());
        scheduler.start();
        stateMachine.transitionToStarting();
    }

    private SqlQueryScheduler createResumeScheduler(PlanRoot plan, OutputBuffers rootOutputBuffers)
    {
        String resumeMessage = "Query encountered failures. Recovering using the distributed-snapshot feature.";
        warningCollector.add(new PrestoWarning(StandardWarningCode.SNAPSHOT_RECOVERY, resumeMessage));
        // Check if there is a snapshot we can restore to, or restart from beginning,
        // and update marker split sources so they know where to resume from.
        // This MUST be done BEFORE creating the new scheduler, because it resets the snapshotManager internal states.
        OptionalLong snapshotId = snapshotManager.getResumeSnapshotId();
        MarkerAnnouncer announcer = splitManager.getMarkerAnnouncer(stateMachine.getSession());
        announcer.resumeSnapshot(snapshotId.orElse(0));
        // Clear any temporary content that's not part of the snapshot
        resetOutputData(plan, snapshotId);
        // Create a new scheduler, to schedule new stages and tasks
        DistributedExecutionPlanner distributedExecutionPlanner = new DistributedExecutionPlanner(splitManager, metadata);
        StageExecutionPlan executionPlan = distributedExecutionPlanner.plan(plan.getRoot(), stateMachine.getSession(),
                RESUME, snapshotId.isPresent() ? snapshotId.getAsLong() : null, announcer.currentSnapshotId());

        // build the stage execution objects (this doesn't schedule execution)
        return createSqlQueryScheduler(
                stateMachine,
                locationFactory,
                executionPlan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
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
                // Require same number of tasks to be scheduled, but do not require it if starting from beginning
                snapshotId.isPresent() ? queryScheduler.get().getStageTaskCounts() : null);
    }

    private void resetOutputData(PlanRoot plan, OptionalLong snapshotId)
    {
        plan.getRoot().getFragment().getRoot().accept(new SimplePlanVisitor<Void>()
        {
            @Override
            public Void visitTableFinish(TableFinishNode node, Void context)
            {
                super.visitTableFinish(node, context);

                // Find table-finish-node, which contains handle to the table
                if (analysis.getStatement() instanceof CreateTableAsSelect) {
                    metadata.resetCreateForRerun(getSession(), ((TableWriterNode.CreateTarget) node.getTarget()).getHandle(), OptionalLong.of(snapshotManager.computeSnapshotIndex(snapshotId)));
                }
                else {
                    metadata.resetInsertForRerun(getSession(), ((TableWriterNode.InsertTarget) node.getTarget()).getHandle(), OptionalLong.of(snapshotManager.computeSnapshotIndex(snapshotId)));
                }
                return null;
            }
        }, null);
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            stateMachine.addStateChangeListener(stateChangeListener);
        }
    }

    @Override
    public Session getSession()
    {
        return stateMachine.getSession();
    }

    @Override
    public void addFinalQueryInfoListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        stateMachine.addQueryInfoStateChangeListener(stateChangeListener);
    }

    private PlanRoot analyzeQuery()
    {
        try {
            return doAnalyzeQuery();
        }
        catch (StackOverflowError e) {
            throw new PrestoException(NOT_SUPPORTED, "statement is too large (stack overflow during analysis)");
        }
    }

    private PlanRoot doAnalyzeQuery()
    {
        // time analysis phase
        stateMachine.beginAnalysis();
        stateMachine.beginLogicalPlan();

        // plan query
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        Plan plan = createPlan(analysis, stateMachine.getSession(), planOptimizers, idAllocator, metadata, new TypeAnalyzer(sqlParser, metadata), statsCalculator, costCalculator, stateMachine.getWarningCollector());
        queryPlan.set(plan);

        // extract inputs
        List<Input> inputs = new InputExtractor(metadata, stateMachine.getSession()).extractInputs(plan.getRoot());
        stateMachine.setInputs(inputs);

        // extract output
        stateMachine.setOutput(analysis.getTarget());
        stateMachine.endLogicalPlan();

        // fragment the plan
        SubPlan fragmentedPlan = planFragmenter.createSubPlans(stateMachine.getSession(), plan, false, stateMachine.getWarningCollector());

        // record analysis time
        stateMachine.endAnalysis();

        boolean explainAnalyze = analysis.getStatement() instanceof Explain && ((Explain) analysis.getStatement()).isAnalyze();

        if (SystemSessionProperties.isSnapshotEnabled(getSession())) {
            checkSnapshotSupport(getSession());
        }

        return new PlanRoot(fragmentedPlan, !explainAnalyze, extractConnectors(analysis));
    }

    // This method was introduced separate logical planning from query analyzing stage
    // and allow plans to be overwritten by CachedSqlQueryExecution
    protected Plan createPlan(Analysis analysis,
            Session session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator,
            WarningCollector warningCollector)
    {
        LogicalPlanner logicalPlanner = new LogicalPlanner(session, planOptimizers, idAllocator, metadata, typeAnalyzer, statsCalculator, costCalculator, warningCollector);
        return logicalPlanner.plan(analysis, true);
    }

    // Check if snapshot feature conflict with other aspects of the query.
    // If any requirement is not met, then proceed as if snapshot was not enabled,
    // i.e. session.isSnapshotEnabled() and SystemSessionProperties.isSnapshotEnabled(session) return false
    private void checkSnapshotSupport(Session session)
    {
        List<String> reasons = new ArrayList<>();
        // Only support create-table-as-select and insert statements
        Statement statement = analysis.getStatement();
        if (statement instanceof CreateTableAsSelect) {
            if (analysis.isCreateTableAsSelectNoOp()) {
                // Table already exists. Ask catalog if target table supports snapshot
                if (!metadata.isSnapshotSupportedAsOutput(session, analysis.getCreateTableAsSelectNoOpTarget())) {
                    reasons.add("Only support inserting into tables in Hive with ORC format");
                }
            }
            else {
                // Ask catalog if new table supports snapshot
                Map<String, Object> tableProperties = analysis.getCreateTableMetadata().getProperties();
                if (!metadata.isSnapshotSupportedAsNewTable(session, analysis.getTarget().get().getCatalogName(), tableProperties)) {
                    reasons.add("Only support creating tables in Hive with ORC format");
                }
            }
        }
        else if (statement instanceof Insert) {
            // Ask catalog if target table supports snapshot
            if (!metadata.isSnapshotSupportedAsOutput(session, analysis.getInsert().get().getTarget())) {
                reasons.add("Only support inserting into tables in Hive with ORC format");
            }
        }
        else if (statement instanceof InsertCube) {
            reasons.add("INSERT INTO CUBE is not supported, only support CTAS (create table as select) and INSERT INTO (tables) statements");
        }
        else {
            reasons.add("Only support CTAS (create table as select) and INSERT INTO (tables) statements");
        }

        // Doesn't work with the following features
        if (SystemSessionProperties.isReuseTableScanEnabled(session)
                || SystemSessionProperties.isCTEReuseEnabled(session)) {
            reasons.add("No support along with reuse_table_scan or cte_reuse_enabled features");
        }

        // All input tables must support snapshotting
        for (TableHandle tableHandle : analysis.getTables()) {
            if (!metadata.isSnapshotSupportedAsInput(session, tableHandle)) {
                reasons.add("Only support reading from Hive, TPCDS, and TPCH source tables");
                break;
            }
        }

        // Must have more than 1 worker
        if (nodeScheduler.createNodeSelector(null, false, null).selectableNodeCount() == 1) {
            reasons.add("Requires more than 1 worker nodes");
        }

        if (!snapshotManager.getSnapshotUtils().hasStoreClient()) {
            String snapshotProfile = snapshotManager.getSnapshotUtils().getSnapshotProfile();
            if (snapshotProfile == null) {
                reasons.add("Property hetu.experimental.snapshot.profile is not specified");
            }
            else {
                reasons.add("Specified value '" + snapshotProfile + "' for property hetu.experimental.snapshot.profile is not valid");
            }
        }

        if (!reasons.isEmpty()) {
            // Disable snapshot support in the session. If this value has been used before this point,
            // then we may need to remedy those places to disable snapshot as well. Fortunately,
            // most accesses occur before this point, except for classes like ExecutingStatementResource,
            // where the "snapshot enabled" info is retrieved and set in ExchangeClient. This is harmless.
            // The ExchangeClient may still have snapshotEnabled=true while it's disabled in the session.
            // This does not alter ExchangeClient's behavior, because this instance (in coordinator)
            // will never receive any marker.
            session.disableSnapshot();
            String reasonsMessage = "Snapshot feature is disabled: \n" + String.join(". \n", reasons);
            warningCollector.add(new PrestoWarning(StandardWarningCode.SNAPSHOT_NOT_SUPPORTED, reasonsMessage));
        }
    }

    private static Set<CatalogName> extractConnectors(Analysis analysis)
    {
        ImmutableSet.Builder<CatalogName> connectors = ImmutableSet.builder();

        for (TableHandle tableHandle : analysis.getTables()) {
            connectors.add(tableHandle.getCatalogName());
        }

        if (analysis.getInsert().isPresent()) {
            TableHandle target = analysis.getInsert().get().getTarget();
            connectors.add(target.getCatalogName());
        }

        return connectors.build();
    }

    private void planDistribution(PlanRoot plan)
    {
        // time distribution planning
        stateMachine.beginDistributedPlanning();

        // plan the execution on the active nodes
        DistributedExecutionPlanner distributedPlanner = new DistributedExecutionPlanner(splitManager, metadata);
        StageExecutionPlan outputStageExecutionPlan;
        Session session = stateMachine.getSession();
        if (SystemSessionProperties.isSnapshotEnabled(session)) {
            // Snapshot: need to plan different when snapshot is enabled.
            // See the "plan" method for difference between the different modes.
            MarkerAnnouncer announcer = splitManager.getMarkerAnnouncer(session);
            announcer.setSnapshotManager(snapshotManager);
            outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), session, SNAPSHOT, null, announcer.currentSnapshotId());
        }
        else {
            outputStageExecutionPlan = distributedPlanner.plan(plan.getRoot(), session, NORMAL, null, 0);
        }
        stateMachine.endDistributedPlanning();

        // ensure split sources are closed
        stateMachine.addStateChangeListener(state -> {
            if (state.isDone()) {
                closeSplitSources(outputStageExecutionPlan);
            }
        });

        // if query was canceled, skip creating scheduler
        if (stateMachine.isDone()) {
            return;
        }

        // record output field
        stateMachine.setColumns(outputStageExecutionPlan.getFieldNames(), outputStageExecutionPlan.getFragment().getTypes());

        PartitioningHandle partitioningHandle = plan.getRoot().getFragment().getPartitioningScheme().getPartitioning().getHandle();
        OutputBuffers rootOutputBuffers = createInitialEmptyOutputBuffers(partitioningHandle)
                .withBuffer(OUTPUT_BUFFER_ID, BROADCAST_PARTITION_ID)
                .withNoMoreBufferIds();

        // build the stage execution objects (this doesn't schedule execution)
        SqlQueryScheduler scheduler = createSqlQueryScheduler(
                stateMachine,
                locationFactory,
                outputStageExecutionPlan,
                nodePartitioningManager,
                nodeScheduler,
                remoteTaskFactory,
                stateMachine.getSession(),
                plan.isSummarizeTaskInfos(),
                scheduleSplitBatchSize,
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
                null);

        queryScheduler.set(scheduler);

        // if query was canceled during scheduler creation, abort the scheduler
        // directly since the callback may have already fired
        if (stateMachine.isDone()) {
            scheduler.abort();
            queryScheduler.set(null);
        }
    }

    private static void closeSplitSources(StageExecutionPlan plan)
    {
        for (SplitSource source : plan.getSplitSources().values()) {
            try {
                source.close();
            }
            catch (Throwable t) {
                log.warn(t, "Error closing split source");
            }
        }

        for (StageExecutionPlan stage : plan.getSubStages()) {
            closeSplitSources(stage);
        }
    }

    @Override
    public void cancelQuery()
    {
        stateMachine.transitionToCanceled();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            SqlQueryScheduler scheduler = queryScheduler.get();
            if (scheduler != null) {
                scheduler.cancelStage(stageId);
            }
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        stateMachine.transitionToFailed(cause);
    }

    @Override
    public boolean isDone()
    {
        return getState().isDone();
    }

    @Override
    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        stateMachine.addOutputInfoListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return stateMachine.getStateChange(currentState);
    }

    @Override
    public void recordHeartbeat()
    {
        stateMachine.recordHeartbeat();
    }

    @Override
    public void pruneInfo()
    {
        stateMachine.pruneQueryInfo();
    }

    @Override
    public QueryId getQueryId()
    {
        return stateMachine.getQueryId();
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        try (SetThreadName ignored = new SetThreadName("Query-%s", stateMachine.getQueryId())) {
            // acquire reference to scheduler before checking finalQueryInfo, because
            // state change listener sets finalQueryInfo and then clears scheduler when
            // the query finishes.
            SqlQueryScheduler scheduler = queryScheduler.get();

            return stateMachine.getFinalQueryInfo().orElseGet(() -> buildQueryInfo(scheduler));
        }
    }

    @Override
    public boolean isRunningAsync()
    {
        return stateMachine.isRunningAsync();
    }

    @Override
    public QueryState getState()
    {
        return stateMachine.getQueryState();
    }

    @Override
    public Plan getQueryPlan()
    {
        return queryPlan.get();
    }

    private QueryInfo buildQueryInfo(SqlQueryScheduler scheduler)
    {
        Optional<StageInfo> stageInfo = Optional.empty();
        if (scheduler != null) {
            stageInfo = Optional.ofNullable(scheduler.getStageInfo());
        }

        QueryInfo queryInfo = stateMachine.updateQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            // capture the final query state and drop reference to the scheduler
            queryScheduler.set(null);
        }

        return queryInfo;
    }

    private static class PlanRoot
    {
        private final SubPlan root;
        private final boolean summarizeTaskInfos;
        private final Set<CatalogName> connectors;

        public PlanRoot(SubPlan root, boolean summarizeTaskInfos, Set<CatalogName> connectors)
        {
            this.root = requireNonNull(root, "root is null");
            this.summarizeTaskInfos = summarizeTaskInfos;
            this.connectors = ImmutableSet.copyOf(connectors);
        }

        public SubPlan getRoot()
        {
            return root;
        }

        public boolean isSummarizeTaskInfos()
        {
            return summarizeTaskInfos;
        }

        public Set<CatalogName> getConnectors()
        {
            return connectors;
        }
    }

    public static class SqlQueryExecutionFactory
            implements QueryExecutionFactory<QueryExecution>
    {
        private final SplitSchedulerStats schedulerStats;
        private final int scheduleSplitBatchSize;
        private final Metadata metadata;
        private final CubeManager cubeManager;
        private final AccessControl accessControl;
        private final SqlParser sqlParser;
        private final SplitManager splitManager;
        private final NodePartitioningManager nodePartitioningManager;
        private final NodeScheduler nodeScheduler;
        private final List<PlanOptimizer> planOptimizers;
        private final PlanFragmenter planFragmenter;
        private final RemoteTaskFactory remoteTaskFactory;
        private final QueryExplainer queryExplainer;
        private final LocationFactory locationFactory;
        private final ExecutorService queryExecutor;
        private final ScheduledExecutorService schedulerExecutor;
        private final FailureDetector failureDetector;
        private final NodeTaskMap nodeTaskMap;
        private final Map<String, ExecutionPolicy> executionPolicies;
        private final StatsCalculator statsCalculator;
        private final CostCalculator costCalculator;
        private final DynamicFilterService dynamicFilterService;
        private final Optional<Cache<Integer, CachedSqlQueryExecutionPlan>> cache;
        private final HeuristicIndexerManager heuristicIndexerManager;
        private final StateStoreProvider stateStoreProvider;
        private final SnapshotUtils snapshotUtils;

        @Inject
        SqlQueryExecutionFactory(QueryManagerConfig config,
                HetuConfig hetuConfig,
                Metadata metadata,
                CubeManager cubeManager,
                AccessControl accessControl,
                SqlParser sqlParser,
                LocationFactory locationFactory,
                SplitManager splitManager,
                NodePartitioningManager nodePartitioningManager,
                NodeScheduler nodeScheduler,
                PlanOptimizers planOptimizers,
                PlanFragmenter planFragmenter,
                RemoteTaskFactory remoteTaskFactory,
                @ForQueryExecution ExecutorService queryExecutor,
                @ForScheduler ScheduledExecutorService schedulerExecutor,
                FailureDetector failureDetector,
                NodeTaskMap nodeTaskMap,
                QueryExplainer queryExplainer,
                Map<String, ExecutionPolicy> executionPolicies,
                SplitSchedulerStats schedulerStats,
                StatsCalculator statsCalculator,
                CostCalculator costCalculator,
                DynamicFilterService dynamicFilterService,
                HeuristicIndexerManager heuristicIndexerManager,
                StateStoreProvider stateStoreProvider,
                SnapshotUtils snapshotUtils)
        {
            requireNonNull(config, "config is null");
            this.schedulerStats = requireNonNull(schedulerStats, "schedulerStats is null");
            this.scheduleSplitBatchSize = config.getScheduleSplitBatchSize();
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.cubeManager = requireNonNull(cubeManager, "cubeManager is null");
            this.accessControl = requireNonNull(accessControl, "accessControl is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.locationFactory = requireNonNull(locationFactory, "locationFactory is null");
            this.splitManager = requireNonNull(splitManager, "splitManager is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.nodeScheduler = requireNonNull(nodeScheduler, "nodeScheduler is null");
            this.planFragmenter = requireNonNull(planFragmenter, "planFragmenter is null");
            this.remoteTaskFactory = requireNonNull(remoteTaskFactory, "remoteTaskFactory is null");
            this.queryExecutor = requireNonNull(queryExecutor, "queryExecutor is null");
            this.schedulerExecutor = requireNonNull(schedulerExecutor, "schedulerExecutor is null");
            this.failureDetector = requireNonNull(failureDetector, "failureDetector is null");
            this.nodeTaskMap = requireNonNull(nodeTaskMap, "nodeTaskMap is null");
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.executionPolicies = requireNonNull(executionPolicies, "schedulerPolicies is null");
            this.planOptimizers = requireNonNull(planOptimizers, "planOptimizers is null").get();
            this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
            this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
            this.dynamicFilterService = requireNonNull(dynamicFilterService, "dynamicFilterService is null");
            this.heuristicIndexerManager = requireNonNull(heuristicIndexerManager, "heuristicIndexerManager is null");
            this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
            this.snapshotUtils = requireNonNull(snapshotUtils, "snapshotUtils is null");
            this.loadConfigToService(hetuConfig);
            if (hetuConfig.isExecutionPlanCacheEnabled()) {
                this.cache = Optional.of(CacheBuilder.newBuilder()
                        .expireAfterAccess(java.time.Duration.ofMillis(hetuConfig.getExecutionPlanCacheTimeout()))
                        .maximumSize(hetuConfig.getExecutionPlanCacheMaxItems())
                        .build());
            }
            else {
                this.cache = Optional.empty();
            }
        }

        // Loading properties into PropertyService for later reference
        private void loadConfigToService(HetuConfig hetuConfig)
        {
            PropertyService.setProperty(HetuConstant.SPLIT_CACHE_MAP_ENABLED, hetuConfig.isSplitCacheMapEnabled());
            PropertyService.setProperty(HetuConstant.SPLIT_CACHE_STATE_UPDATE_INTERVAL, hetuConfig.getSplitCacheStateUpdateInterval());
        }

        @Override
        public QueryExecution createQueryExecution(
                PreparedQuery preparedQuery,
                QueryStateMachine stateMachine,
                String slug,
                WarningCollector warningCollector)
        {
            String executionPolicyName = SystemSessionProperties.getExecutionPolicy(stateMachine.getSession());
            ExecutionPolicy localExecutionPolicy = executionPolicies.get(executionPolicyName);
            checkArgument(localExecutionPolicy != null, "No execution policy %s", localExecutionPolicy);

            return new CachedSqlQueryExecution(
                    preparedQuery,
                    stateMachine,
                    slug,
                    metadata,
                    cubeManager,
                    accessControl,
                    sqlParser,
                    splitManager,
                    nodePartitioningManager,
                    nodeScheduler,
                    planOptimizers,
                    planFragmenter,
                    remoteTaskFactory,
                    locationFactory,
                    scheduleSplitBatchSize,
                    queryExecutor,
                    schedulerExecutor,
                    failureDetector,
                    nodeTaskMap,
                    queryExplainer,
                    localExecutionPolicy,
                    schedulerStats,
                    statsCalculator,
                    costCalculator,
                    warningCollector,
                    dynamicFilterService,
                    this.cache,
                    heuristicIndexerManager,
                    stateStoreProvider,
                    snapshotUtils);
        }
    }
}
