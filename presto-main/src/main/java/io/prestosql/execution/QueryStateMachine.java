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
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.QueryExecution.QueryOutputInfo;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.memory.VersionedMemoryPoolId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.BlockedReason;
import io.prestosql.operator.OperatorStats;
import io.prestosql.operator.TaskLocation;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.BasicQueryStats;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.eventlistener.StageGcStatistics;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.transaction.TransactionId;
import io.prestosql.transaction.TransactionManager;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.execution.BasicStageStats.EMPTY_STAGE_STATS;
import static io.prestosql.execution.QueryState.DISPATCHING;
import static io.prestosql.execution.QueryState.FAILED;
import static io.prestosql.execution.QueryState.FINISHED;
import static io.prestosql.execution.QueryState.FINISHING;
import static io.prestosql.execution.QueryState.PLANNING;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.execution.QueryState.RESCHEDULING;
import static io.prestosql.execution.QueryState.RESUMING;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.execution.QueryState.STARTING;
import static io.prestosql.execution.QueryState.TERMINAL_QUERY_STATES;
import static io.prestosql.execution.QueryState.WAITING_FOR_RESOURCES;
import static io.prestosql.execution.StageInfo.getAllStages;
import static io.prestosql.memory.LocalMemoryManager.GENERAL_POOL;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static io.prestosql.spi.StandardErrorCode.USER_CANCELED;
import static io.prestosql.util.Failures.toFailure;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger QUERY_STATE_LOG = Logger.get(QueryStateMachine.class);

    private final QueryId queryId;
    private final String query;
    private final Optional<String> preparedQuery;
    private final Session session;
    private final URI self;
    private final ResourceGroupId resourceGroup;
    private final ResourceGroupManager resourceGroupManager;
    private boolean throttlingEnabled;
    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final QueryOutputManager outputManager;

    private final AtomicReference<VersionedMemoryPoolId> memoryPool = new AtomicReference<>(new VersionedMemoryPoolId(GENERAL_POOL, 0));

    private final AtomicLong currentUserMemory = new AtomicLong();
    private final AtomicLong peakUserMemory = new AtomicLong();

    private final AtomicLong currentRevocableMemory = new AtomicLong();
    private final AtomicLong peakRevocableMemory = new AtomicLong();

    // peak of the user + system + revocable memory reservation
    private final AtomicLong currentTotalMemory = new AtomicLong();
    private final AtomicLong peakTotalMemory = new AtomicLong();

    private final AtomicLong peakTaskUserMemory = new AtomicLong();
    private final AtomicLong peakTaskRevocableMemory = new AtomicLong();
    private final AtomicLong peakTaskTotalMemory = new AtomicLong();

    private final QueryStateTimer queryStateTimer;

    private final StateMachine<QueryState> queryState;
    private final AtomicBoolean queryCleanedUp = new AtomicBoolean();

    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final AtomicReference<String> setPath = new AtomicReference<>();

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();

    private final Map<String, SelectedRole> setRoles = new ConcurrentHashMap<>();

    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();

    private final AtomicReference<TransactionId> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();

    private final AtomicReference<String> updateType = new AtomicReference<>();

    private final AtomicReference<ExecutionFailureInfo> failureCause = new AtomicReference<>();

    private final AtomicReference<Set<Input>> inputs = new AtomicReference<>(ImmutableSet.of());
    private final AtomicReference<Optional<Output>> output = new AtomicReference<>(Optional.empty());
    private final StateMachine<Optional<QueryInfo>> finalQueryInfo;

    private final WarningCollector warningCollector;

    private final AtomicBoolean isRunningAsync = new AtomicBoolean();

    private QueryStateMachine(
            String query,
            Optional<String> preparedQuery,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            ResourceGroupManager resourceGroupManager,
            TransactionManager transactionManager,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        this.query = requireNonNull(query, "query is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
        this.session = requireNonNull(session, "session is null");
        this.queryId = session.getQueryId();
        this.self = requireNonNull(self, "self is null");
        this.resourceGroup = requireNonNull(resourceGroup, "resourceGroup is null");
        this.resourceGroupManager = resourceGroupManager;
        this.throttlingEnabled = resourceGroupManager.isGroupRegistered(resourceGroup)
                                    && resourceGroupManager.getSoftReservedMemory(resourceGroup) != Long.MAX_VALUE;
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.queryStateTimer = new QueryStateTimer(ticker);
        this.metadata = requireNonNull(metadata, "metadata is null");

        this.queryState = new StateMachine<>("query " + query, executor, QUEUED, TERMINAL_QUERY_STATES);
        this.finalQueryInfo = new StateMachine<>("finalQueryInfo-" + queryId, executor, Optional.empty());
        this.outputManager = new QueryOutputManager(executor);
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    /**
     * Created QueryStateMachines must be transitioned to terminal states to clean up resources.
     */
    public static QueryStateMachine begin(
            String query,
            Optional<String> preparedQuery,
            Session session,
            URI self,
            ResourceGroupId resourceGroup,
            ResourceGroupManager resourceGroupManager,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        return beginWithTicker(
                query,
                preparedQuery,
                session,
                self,
                resourceGroup,
                resourceGroupManager,
                transactionControl,
                transactionManager,
                accessControl,
                executor,
                Ticker.systemTicker(),
                metadata,
                warningCollector);
    }

    static QueryStateMachine beginWithTicker(
            String query,
            Optional<String> preparedQuery,
            Session inputSession,
            URI self,
            ResourceGroupId resourceGroup,
            ResourceGroupManager resourceGroupManager,
            boolean transactionControl,
            TransactionManager transactionManager,
            AccessControl accessControl,
            Executor executor,
            Ticker ticker,
            Metadata metadata,
            WarningCollector warningCollector)
    {
        Session localSession = inputSession;
        // If there is not an existing transaction, begin an auto commit transaction
        if (!localSession.getTransactionId().isPresent() && !transactionControl) {
            // TODO: make autocommit isolation level a session parameter
            TransactionId transactionId = transactionManager.beginTransaction(true);
            localSession = localSession.beginTransactionId(transactionId, transactionManager, accessControl);
        }

        QueryStateMachine queryStateMachine = new QueryStateMachine(
                query,
                preparedQuery,
                localSession,
                self,
                resourceGroup,
                resourceGroupManager,
                transactionManager,
                executor,
                ticker,
                metadata,
                warningCollector);
        queryStateMachine.addStateChangeListener(newState -> QUERY_STATE_LOG.debug("Query %s is %s", queryStateMachine.getQueryId(), newState));

        return queryStateMachine;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public ResourceGroupId getResourceGroup()
    {
        return resourceGroup;
    }

    public ResourceGroupManager getResourceGroupManager()
    {
        return resourceGroupManager;
    }

    public boolean isThrottlingEnabled()
    {
        return throttlingEnabled;
    }

    public Session getSession()
    {
        return session;
    }

    public long getPeakUserMemoryInBytes()
    {
        return peakUserMemory.get();
    }

    public long getPeakRevocableMemoryInBytes()
    {
        return peakRevocableMemory.get();
    }

    public long getPeakTotalMemoryInBytes()
    {
        return peakTotalMemory.get();
    }

    public long getPeakTaskUserMemory()
    {
        return peakTaskUserMemory.get();
    }

    public long getPeakTaskRevocableMemory()
    {
        return peakTaskRevocableMemory.get();
    }

    public long getPeakTaskTotalMemory()
    {
        return peakTaskTotalMemory.get();
    }

    public WarningCollector getWarningCollector()
    {
        return warningCollector;
    }

    public void updateMemoryUsage(
            long deltaUserMemoryInBytes,
            long deltaRevocableMemoryInBytes,
            long deltaTotalMemoryInBytes,
            long taskUserMemoryInBytes,
            long taskRevocableMemoryInBytes,
            long taskTotalMemoryInBytes)
    {
        currentUserMemory.addAndGet(deltaUserMemoryInBytes);
        currentRevocableMemory.addAndGet(deltaRevocableMemoryInBytes);
        currentTotalMemory.addAndGet(deltaTotalMemoryInBytes);
        peakUserMemory.updateAndGet(currentPeakValue -> Math.max(currentUserMemory.get(), currentPeakValue));
        peakRevocableMemory.updateAndGet(currentPeakValue -> Math.max(currentRevocableMemory.get(), currentPeakValue));
        peakTotalMemory.updateAndGet(currentPeakValue -> Math.max(currentTotalMemory.get(), currentPeakValue));
        peakTaskUserMemory.accumulateAndGet(taskUserMemoryInBytes, Math::max);
        peakTaskRevocableMemory.accumulateAndGet(taskRevocableMemoryInBytes, Math::max);
        peakTaskTotalMemory.accumulateAndGet(taskTotalMemoryInBytes, Math::max);
    }

    public BasicQueryInfo getBasicQueryInfo(Optional<BasicStageStats> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ErrorCode errorCode = null;
        if (state == FAILED) {
            ExecutionFailureInfo localFailureCause = this.failureCause.get();
            if (localFailureCause != null) {
                errorCode = localFailureCause.getErrorCode();
            }
        }

        BasicStageStats stageStats = rootStage.orElse(EMPTY_STAGE_STATS);
        BasicQueryStats queryStats = new BasicQueryStats(
                queryStateTimer.getCreateTime(),
                getEndTime().orElse(null),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getElapsedTime(),
                queryStateTimer.getExecutionTime(),

                stageStats.getTotalDrivers(),
                stageStats.getQueuedDrivers(),
                stageStats.getRunningDrivers(),
                stageStats.getCompletedDrivers(),

                stageStats.getRawInputDataSize(),
                stageStats.getRawInputPositions(),

                stageStats.getCumulativeUserMemory(),
                stageStats.getUserMemoryReservation(),
                stageStats.getTotalMemoryReservation(),
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),

                stageStats.getTotalCpuTime(),
                stageStats.getTotalScheduledTime(),

                stageStats.isFullyBlocked(),
                stageStats.getBlockedReasons(),
                stageStats.getProgressPercentage());

        return new BasicQueryInfo(
                queryId,
                session.toSessionRepresentation(),
                Optional.of(resourceGroup),
                state,
                memoryPool.get().getId(),
                stageStats.isScheduled(),
                self,
                query,
                preparedQuery,
                queryStats,
                errorCode == null ? null : errorCode.getType(),
                errorCode);
    }

    @VisibleForTesting
    QueryInfo getQueryInfo(Optional<StageInfo> rootStage)
    {
        // Query state must be captured first in order to provide a
        // correct view of the query.  For example, building this
        // information, the query could finish, and the task states would
        // never be visible.
        QueryState state = queryState.get();

        ExecutionFailureInfo localFailureCause = null;
        ErrorCode errorCode = null;
        if (state == FAILED) {
            localFailureCause = this.failureCause.get();
            if (localFailureCause != null) {
                errorCode = localFailureCause.getErrorCode();
            }
        }

        boolean completeInfo = getAllStages(rootStage).stream().allMatch(StageInfo::isCompleteInfo);
        boolean isScheduled = isScheduled(rootStage);

        return new QueryInfo(
                queryId,
                session.toSessionRepresentation(),
                state,
                memoryPool.get().getId(),
                isScheduled,
                self,
                outputManager.getQueryOutputInfo().map(QueryOutputInfo::getColumnNames).orElse(ImmutableList.of()),
                query,
                preparedQuery,
                getQueryStats(rootStage),
                Optional.ofNullable(setCatalog.get()),
                Optional.ofNullable(setSchema.get()),
                Optional.ofNullable(setPath.get()),
                setSessionProperties,
                resetSessionProperties,
                setRoles,
                addedPreparedStatements,
                deallocatedPreparedStatements,
                Optional.ofNullable(startedTransactionId.get()),
                clearTransactionId.get(),
                updateType.get(),
                rootStage,
                localFailureCause,
                errorCode,
                warningCollector.getWarnings(),
                inputs.get(),
                output.get(),
                completeInfo,
                Optional.of(resourceGroup),
                isRunningAsync.get());
    }

    private QueryStats getQueryStats(Optional<StageInfo> rootStage)
    {
        int totalTasks = 0;
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

        long outputDataSize = 0;
        long outputPositions = 0;

        long physicalWrittenDataSize = 0;

        ImmutableList.Builder<StageGcStatistics> stageGcStatistics = ImmutableList.builder();

        boolean fullyBlocked = rootStage.isPresent();
        Set<BlockedReason> blockedReasons = new HashSet<>();

        ImmutableList.Builder<OperatorStats> operatorStatsSummary = ImmutableList.builder();
        boolean completeInfo = true;
        for (StageInfo stageInfo : getAllStages(rootStage)) {
            StageStats stageStats = stageInfo.getStageStats();
            totalTasks += stageStats.getTotalTasks();
            runningTasks += stageStats.getRunningTasks();
            completedTasks += stageStats.getCompletedTasks();

            totalDrivers += stageStats.getTotalDrivers();
            queuedDrivers += stageStats.getQueuedDrivers();
            runningDrivers += stageStats.getRunningDrivers();
            blockedDrivers += stageStats.getBlockedDrivers();
            completedDrivers += stageStats.getCompletedDrivers();

            cumulativeUserMemory += stageStats.getCumulativeUserMemory();
            userMemoryReservation += stageStats.getUserMemoryReservation().toBytes();
            revocableMemoryReservation += stageStats.getRevocableMemoryReservation().toBytes();
            totalMemoryReservation += stageStats.getTotalMemoryReservation().toBytes();
            totalScheduledTime += stageStats.getTotalScheduledTime().roundTo(MILLISECONDS);
            totalCpuTime += stageStats.getTotalCpuTime().roundTo(MILLISECONDS);
            totalBlockedTime += stageStats.getTotalBlockedTime().roundTo(MILLISECONDS);
            if (!stageInfo.getState().isDone()) {
                fullyBlocked &= stageStats.isFullyBlocked();
                blockedReasons.addAll(stageStats.getBlockedReasons());
            }

            physicalInputDataSize += stageStats.getPhysicalInputDataSize().toBytes();
            physicalInputPositions += stageStats.getPhysicalInputPositions();

            internalNetworkInputDataSize += stageStats.getInternalNetworkInputDataSize().toBytes();
            internalNetworkInputPositions += stageStats.getInternalNetworkInputPositions();

            PlanFragment plan = stageInfo.getPlan();
            if (plan != null && plan.getPartitionedSourceNodes().stream().anyMatch(TableScanNode.class::isInstance)) {
                rawInputDataSize += stageStats.getRawInputDataSize().toBytes();
                rawInputPositions += stageStats.getRawInputPositions();

                processedInputDataSize += stageStats.getProcessedInputDataSize().toBytes();
                processedInputPositions += stageStats.getProcessedInputPositions();
            }

            physicalWrittenDataSize += stageStats.getPhysicalWrittenDataSize().toBytes();

            stageGcStatistics.add(stageStats.getGcInfo());

            completeInfo = completeInfo && stageInfo.isCompleteInfo();
            operatorStatsSummary.addAll(stageInfo.getStageStats().getOperatorSummaries());
        }

        if (rootStage.isPresent()) {
            StageStats outputStageStats = rootStage.get().getStageStats();
            outputDataSize += outputStageStats.getOutputDataSize().toBytes();
            outputPositions += outputStageStats.getOutputPositions();
        }

        boolean isScheduled = isScheduled(rootStage);

        return new QueryStats(
                queryStateTimer.getCreateTime(),
                getExecutionStartTime().orElse(null),
                getLastHeartbeat(),
                getEndTime().orElse(null),

                queryStateTimer.getElapsedTime(),
                queryStateTimer.getQueuedTime(),
                queryStateTimer.getResourceWaitingTime(),
                queryStateTimer.getDispatchingTime(),
                queryStateTimer.getExecutionTime(),
                queryStateTimer.getAnalysisTime(),
                queryStateTimer.getDistributedPlanningTime(),
                queryStateTimer.getPlanningTime(),
                queryStateTimer.getLogicalPlanningTime(),
                queryStateTimer.getSyntaxAnalysisTime(),
                queryStateTimer.getFinishingTime(),

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
                succinctBytes(getPeakUserMemoryInBytes()),
                succinctBytes(getPeakRevocableMemoryInBytes()),
                succinctBytes(getPeakTotalMemoryInBytes()),
                succinctBytes(getPeakTaskUserMemory()),
                succinctBytes(getPeakTaskRevocableMemory()),
                succinctBytes(getPeakTaskTotalMemory()),

                isScheduled,

                new Duration(totalScheduledTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalCpuTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                new Duration(totalBlockedTime, MILLISECONDS).convertToMostSuccinctTimeUnit(),
                fullyBlocked,
                blockedReasons,

                succinctBytes(physicalInputDataSize),
                physicalInputPositions,
                succinctBytes(internalNetworkInputDataSize),
                internalNetworkInputPositions,
                succinctBytes(rawInputDataSize),
                rawInputPositions,
                succinctBytes(processedInputDataSize),
                processedInputPositions,
                succinctBytes(outputDataSize),
                outputPositions,

                succinctBytes(physicalWrittenDataSize),

                stageGcStatistics.build(),

                operatorStatsSummary.build());
    }

    public VersionedMemoryPoolId getMemoryPool()
    {
        return memoryPool.get();
    }

    public void setMemoryPool(VersionedMemoryPoolId memoryPool)
    {
        this.memoryPool.set(requireNonNull(memoryPool, "memoryPool is null"));
    }

    public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
    {
        outputManager.addOutputInfoListener(listener);
    }

    public void setColumns(List<String> columnNames, List<Type> columnTypes)
    {
        outputManager.setColumns(columnNames, columnTypes);
    }

    public void updateOutputLocations(Set<TaskLocation> newExchangeLocations, boolean noMoreExchangeLocations)
    {
        outputManager.updateOutputLocations(newExchangeLocations, noMoreExchangeLocations);
    }

    public void setInputs(List<Input> inputs)
    {
        requireNonNull(inputs, "inputs is null");
        this.inputs.set(ImmutableSet.copyOf(inputs));
    }

    public void setOutput(Optional<Output> output)
    {
        requireNonNull(output, "output is null");
        this.output.set(output);
    }

    public Map<String, String> getSetSessionProperties()
    {
        return setSessionProperties;
    }

    public void setSetCatalog(String catalog)
    {
        setCatalog.set(requireNonNull(catalog, "catalog is null"));
    }

    public void setSetSchema(String schema)
    {
        setSchema.set(requireNonNull(schema, "schema is null"));
    }

    public void setSetPath(String path)
    {
        requireNonNull(path, "path is null");
        setPath.set(path);
    }

    public String getSetPath()
    {
        return setPath.get();
    }

    public void addSetSessionProperties(String key, String value)
    {
        setSessionProperties.put(requireNonNull(key, "key is null"), requireNonNull(value, "value is null"));
    }

    public void addSetRole(String catalog, SelectedRole role)
    {
        setRoles.put(requireNonNull(catalog, "catalog is null"), requireNonNull(role, "role is null"));
    }

    public Set<String> getResetSessionProperties()
    {
        return resetSessionProperties;
    }

    public void addResetSessionProperties(String name)
    {
        resetSessionProperties.add(requireNonNull(name, "name is null"));
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return addedPreparedStatements;
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return deallocatedPreparedStatements;
    }

    public void addPreparedStatement(String key, String value)
    {
        requireNonNull(key, "key is null");
        requireNonNull(value, "value is null");

        addedPreparedStatements.put(key, value);
    }

    public void removePreparedStatement(String key)
    {
        requireNonNull(key, "key is null");

        if (!session.getPreparedStatements().containsKey(key)) {
            throw new PrestoException(NOT_FOUND, "Prepared statement not found: " + key);
        }
        deallocatedPreparedStatements.add(key);
    }

    public void setStartedTransactionId(TransactionId startedTransactionId)
    {
        checkArgument(!clearTransactionId.get(), "Cannot start and clear transaction ID in the same request");
        this.startedTransactionId.set(startedTransactionId);
    }

    public void clearTransactionId()
    {
        checkArgument(startedTransactionId.get() == null, "Cannot start and clear transaction ID in the same request");
        clearTransactionId.set(true);
    }

    public void setUpdateType(String updateType)
    {
        this.updateType.set(updateType);
    }

    public QueryState getQueryState()
    {
        return queryState.get();
    }

    public boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean transitionToWaitingForResources()
    {
        queryStateTimer.beginWaitingForResources();
        return queryState.setIf(WAITING_FOR_RESOURCES, currentState -> currentState.ordinal() < WAITING_FOR_RESOURCES.ordinal());
    }

    public boolean transitionToDispatching()
    {
        queryStateTimer.beginDispatching();
        return queryState.setIf(DISPATCHING, currentState -> currentState.ordinal() < DISPATCHING.ordinal());
    }

    public boolean transitionToPlanning()
    {
        queryStateTimer.beginPlanning();
        return queryState.setIf(PLANNING, currentState -> currentState.ordinal() < PLANNING.ordinal());
    }

    public boolean transitionToStarting()
    {
        queryStateTimer.beginStarting();
        return queryState.setIf(STARTING, currentState -> {
            if (currentState.ordinal() < STARTING.ordinal()) {
                return true;
            }
            if (currentState == RESUMING) {
                // Snapshot: Also allow to go from "resuming".
                // Inform outputManager to reset some fields.
                outputManager.resetForResume();
                return true;
            }
            return false;
        });
    }

    public boolean transitionToRunning()
    {
        queryStateTimer.beginRunning();
        return queryState.setIf(RUNNING, currentState -> currentState.ordinal() < RUNNING.ordinal());
    }

    public boolean transitionToRescheduling()
    {
        return queryState.setIf(RESCHEDULING, currentState -> currentState == RUNNING);
    }

    public boolean transitionToResuming()
    {
        return queryState.setIf(RESUMING, currentState -> currentState == RESCHEDULING);
    }

    public boolean transitionToFinishing()
    {
        queryStateTimer.beginFinishing();

        if (!queryState.setIf(FINISHING, currentState -> currentState != FINISHING && !currentState.isDone())) {
            return false;
        }

        isRunningAsync.set(false); //reset
        try {
            cleanupQuery();
        }
        catch (Exception e) {
            transitionToFailed(e);
            return true;
        }

        Optional<TransactionId> transactionId = session.getTransactionId();
        if (transactionId.isPresent() && transactionManager.transactionExists(transactionId.get()) && transactionManager.isAutoCommit(transactionId.get())) {
            ListenableFuture<?> commitFuture = transactionManager.asyncCommit(transactionId.get());
            Futures.addCallback(commitFuture, new FutureCallback<Object>()
            {
                @Override
                public void onSuccess(@Nullable Object result)
                {
                    transitionToFinished();
                }

                @Override
                public void onFailure(Throwable throwable)
                {
                    transitionToFailed(throwable);
                }
            }, directExecutor());
        }
        else {
            transitionToFinished();
        }
        return true;
    }

    private void transitionToFinished()
    {
        queryStateTimer.endQuery();

        queryState.setIf(FINISHED, currentState -> !currentState.isDone());
    }

    public boolean transitionToFailed(Throwable throwable)
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        requireNonNull(throwable, "throwable is null");
        failureCause.compareAndSet(null, toFailure(throwable));

        boolean failed = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (failed) {
            QUERY_STATE_LOG.debug(throwable, "Query %s failed", queryId);
            session.getTransactionId().ifPresent(transactionId -> {
                try {
                    if (transactionManager.transactionExists(transactionId) && transactionManager.isAutoCommit(transactionId)) {
                        transactionManager.asyncAbort(transactionId);
                        return;
                    }
                }
                catch (RuntimeException e) {
                    // This shouldn't happen but be safe and just fail the transaction directly
                    QUERY_STATE_LOG.error(e, "Error aborting transaction for failed query. Transaction will be failed directly");
                }
                transactionManager.fail(transactionId);
            });
        }
        else {
            QUERY_STATE_LOG.debug(throwable, "Failure after query %s finished", queryId);
        }

        return failed;
    }

    public boolean transitionToCanceled()
    {
        cleanupQueryQuietly();
        queryStateTimer.endQuery();

        // NOTE: The failure cause must be set before triggering the state change, so
        // listeners can observe the exception. This is safe because the failure cause
        // can only be observed if the transition to FAILED is successful.
        failureCause.compareAndSet(null, toFailure(new PrestoException(USER_CANCELED, "Query was canceled")));

        boolean canceled = queryState.setIf(FAILED, currentState -> !currentState.isDone());
        if (canceled) {
            session.getTransactionId().ifPresent(transactionId -> {
                try {
                    if (transactionManager.transactionExists(transactionId) && transactionManager.isAutoCommit(transactionId)) {
                        transactionManager.asyncAbort(transactionId);
                        return;
                    }
                }
                catch (RuntimeException e) {
                    // This shouldn't happen but be safe and just fail the transaction directly
                    QUERY_STATE_LOG.error(e, "Error aborting transaction for failed query. Transaction will be failed directly");
                }
                transactionManager.fail(transactionId);
            });
        }

        return canceled;
    }

    private void cleanupQuery()
    {
        // only execute cleanup once
        if (queryCleanedUp.compareAndSet(false, true)) {
            metadata.cleanupQuery(session);
            SqlTaskManager.cleanupContext(queryId.getId());
        }
    }

    private void cleanupQueryQuietly()
    {
        try {
            cleanupQuery();
        }
        catch (Throwable t) {
            QUERY_STATE_LOG.error("Error cleaning up query: %s", t);
        }
    }

    /**
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    /**
     * Add a listener for the final query info.  This notification is guaranteed to be fired only once.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor.
     */
    public void addQueryInfoStateChangeListener(StateChangeListener<QueryInfo> stateChangeListener)
    {
        AtomicBoolean done = new AtomicBoolean();
        StateChangeListener<Optional<QueryInfo>> fireOnceStateChangeListener = finalQueryInfo -> {
            if (finalQueryInfo.isPresent() && done.compareAndSet(false, true)) {
                stateChangeListener.stateChanged(finalQueryInfo.get());
            }
        };
        finalQueryInfo.addStateChangeListener(fireOnceStateChangeListener);
    }

    public ListenableFuture<QueryState> getStateChange(QueryState currentState)
    {
        return queryState.getStateChange(currentState);
    }

    public void recordHeartbeat()
    {
        queryStateTimer.recordHeartbeat();
    }

    public void beginSyntaxAnalysis()
    {
        queryStateTimer.beginSyntaxAnalysis();
    }

    public void endSyntaxAnalysis()
    {
        queryStateTimer.endSyntaxAnalysis();
    }

    public void beginAnalysis()
    {
        queryStateTimer.beginAnalyzing();
    }

    public void endAnalysis()
    {
        queryStateTimer.endAnalysis();
    }

    public void beginLogicalPlan()
    {
        queryStateTimer.beginLogicalPlan();
    }

    public void endLogicalPlan()
    {
        queryStateTimer.endLogicalPlan();
    }

    public void beginDistributedPlanning()
    {
        queryStateTimer.beginDistributedPlanning();
    }

    public void endDistributedPlanning()
    {
        queryStateTimer.endDistributedPlanning();
    }

    public DateTime getCreateTime()
    {
        return queryStateTimer.getCreateTime();
    }

    public Optional<DateTime> getExecutionStartTime()
    {
        return queryStateTimer.getExecutionStartTime();
    }

    public DateTime getLastHeartbeat()
    {
        return queryStateTimer.getLastHeartbeat();
    }

    public Optional<DateTime> getEndTime()
    {
        return queryStateTimer.getEndTime();
    }

    private static boolean isScheduled(Optional<StageInfo> rootStage)
    {
        if (!rootStage.isPresent()) {
            return false;
        }
        return getAllStages(rootStage).stream()
                .map(StageInfo::getState)
                // Snapshot: RESCHEDULING, although a done state for stage, should not be deemed as "scheduled".
                .allMatch(state -> (state == StageState.RUNNING) || state.isDone() && state != StageState.RESCHEDULING);
    }

    void setRunningAsync(boolean runningAsync)
    {
        if (queryState.get() == RUNNING && runningAsync) {
            this.isRunningAsync.compareAndSet(false, true);
        }
    }

    boolean isRunningAsync()
    {
        return isRunningAsync.get();
    }

    public Optional<ExecutionFailureInfo> getFailureInfo()
    {
        if (queryState.get() != FAILED) {
            return Optional.empty();
        }
        return Optional.ofNullable(this.failureCause.get());
    }

    public Optional<QueryInfo> getFinalQueryInfo()
    {
        return finalQueryInfo.get();
    }

    public QueryInfo updateQueryInfo(Optional<StageInfo> stageInfo)
    {
        QueryInfo queryInfo = getQueryInfo(stageInfo);
        if (queryInfo.isFinalQueryInfo()) {
            finalQueryInfo.compareAndSet(Optional.empty(), Optional.of(queryInfo));
        }
        else if (SystemSessionProperties.isSnapshotEnabled(session)) {
            if (queryInfo.getState() == RESCHEDULING && queryInfo.areAllStagesDone()) {
                // Snapsoht: All remote tasks have been cancelled. Can start scheduling new ones.
                transitionToResuming();
            }
        }
        return queryInfo;
    }

    public void pruneQueryInfo()
    {
        Optional<QueryInfo> finalInfo = finalQueryInfo.get();
        if (!finalInfo.isPresent() || !finalInfo.get().getOutputStage().isPresent()) {
            return;
        }

        QueryInfo queryInfo = finalInfo.get();
        Optional<StageInfo> prunedOutputStage = queryInfo.getOutputStage().map(outputStage -> new StageInfo(
                outputStage.getStageId(),
                outputStage.getState(),
                outputStage.isRestoring(),
                outputStage.getSelf(),
                null, // Remove the plan
                outputStage.getTypes(),
                outputStage.getStageStats(),
                ImmutableList.of(), // Remove the tasks
                ImmutableList.of(), // Remove the substages
                ImmutableMap.of(), // Remove tables
                outputStage.getFailureCause()));

        QueryInfo prunedQueryInfo = new QueryInfo(
                queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                getMemoryPool().getId(),
                queryInfo.isScheduled(),
                queryInfo.getSelf(),
                queryInfo.getFieldNames(),
                queryInfo.getQuery(),
                queryInfo.getPreparedQuery(),
                pruneQueryStats(queryInfo.getQueryStats()),
                queryInfo.getSetCatalog(),
                queryInfo.getSetSchema(),
                queryInfo.getSetPath(),
                queryInfo.getSetSessionProperties(),
                queryInfo.getResetSessionProperties(),
                queryInfo.getSetRoles(),
                queryInfo.getAddedPreparedStatements(),
                queryInfo.getDeallocatedPreparedStatements(),
                queryInfo.getStartedTransactionId(),
                queryInfo.isClearTransactionId(),
                queryInfo.getUpdateType(),
                prunedOutputStage,
                queryInfo.getFailureInfo(),
                queryInfo.getErrorCode(),
                queryInfo.getWarnings(),
                queryInfo.getInputs(),
                queryInfo.getOutput(),
                queryInfo.isCompleteInfo(),
                queryInfo.getResourceGroupId(), false);
        finalQueryInfo.compareAndSet(finalInfo, Optional.of(prunedQueryInfo));
    }

    private static QueryStats pruneQueryStats(QueryStats queryStats)
    {
        return new QueryStats(
                queryStats.getCreateTime(),
                queryStats.getExecutionStartTime(),
                queryStats.getLastHeartbeat(),
                queryStats.getEndTime(),
                queryStats.getElapsedTime(),
                queryStats.getQueuedTime(),
                queryStats.getResourceWaitingTime(),
                queryStats.getDispatchingTime(),
                queryStats.getExecutionTime(),
                queryStats.getAnalysisTime(),
                queryStats.getDistributedPlanningTime(),
                queryStats.getTotalPlanningTime(),
                queryStats.getTotalLogicalPlanningTime(),
                queryStats.getTotalSyntaxAnalysisTime(),
                queryStats.getFinishingTime(),
                queryStats.getTotalTasks(),
                queryStats.getRunningTasks(),
                queryStats.getCompletedTasks(),
                queryStats.getTotalDrivers(),
                queryStats.getQueuedDrivers(),
                queryStats.getRunningDrivers(),
                queryStats.getBlockedDrivers(),
                queryStats.getCompletedDrivers(),
                queryStats.getCumulativeUserMemory(),
                queryStats.getUserMemoryReservation(),
                queryStats.getRevocableMemoryReservation(),
                queryStats.getTotalMemoryReservation(),
                queryStats.getPeakUserMemoryReservation(),
                queryStats.getPeakRevocableMemoryReservation(),
                queryStats.getPeakTotalMemoryReservation(),
                queryStats.getPeakTaskUserMemory(),
                queryStats.getPeakTaskRevocableMemory(),
                queryStats.getPeakTaskTotalMemory(),
                queryStats.isScheduled(),
                queryStats.getTotalScheduledTime(),
                queryStats.getTotalCpuTime(),
                queryStats.getTotalBlockedTime(),
                queryStats.isFullyBlocked(),
                queryStats.getBlockedReasons(),
                queryStats.getPhysicalInputDataSize(),
                queryStats.getPhysicalInputPositions(),
                queryStats.getInternalNetworkInputDataSize(),
                queryStats.getInternalNetworkInputPositions(),
                queryStats.getRawInputDataSize(),
                queryStats.getRawInputPositions(),
                queryStats.getProcessedInputDataSize(),
                queryStats.getProcessedInputPositions(),
                queryStats.getOutputDataSize(),
                queryStats.getOutputPositions(),
                queryStats.getPhysicalWrittenDataSize(),
                queryStats.getStageGcStatistics(),
                ImmutableList.of()); // Remove the operator summaries as OperatorInfo (especially ExchangeClientStatus) can hold onto a large amount of memory
    }

    public static class QueryOutputManager
    {
        private final Executor executor;

        @GuardedBy("this")
        private final List<Consumer<QueryOutputInfo>> outputInfoListeners = new ArrayList<>();

        @GuardedBy("this")
        private List<String> columnNames;
        @GuardedBy("this")
        private List<Type> columnTypes;
        @GuardedBy("this")
        private final Set<TaskLocation> exchangeLocations = new LinkedHashSet<>();
        @GuardedBy("this")
        private boolean noMoreExchangeLocations;

        public QueryOutputManager(Executor executor)
        {
            this.executor = requireNonNull(executor, "executor is null");
        }

        public void addOutputInfoListener(Consumer<QueryOutputInfo> listener)
        {
            requireNonNull(listener, "listener is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            synchronized (this) {
                outputInfoListeners.add(listener);
                queryOutputInfo = getQueryOutputInfo();
            }
            queryOutputInfo.ifPresent(info -> executor.execute(() -> listener.accept(info)));
        }

        public void setColumns(List<String> columnNames, List<Type> columnTypes)
        {
            requireNonNull(columnNames, "columnNames is null");
            requireNonNull(columnTypes, "columnTypes is null");
            checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes must be the same size");

            Optional<QueryOutputInfo> queryOutputInfo;
            List<Consumer<QueryOutputInfo>> localOutputInfoListeners;
            synchronized (this) {
                checkState(this.columnNames == null && this.columnTypes == null, "output fields already set");
                this.columnNames = ImmutableList.copyOf(columnNames);
                this.columnTypes = ImmutableList.copyOf(columnTypes);

                queryOutputInfo = getQueryOutputInfo();
                localOutputInfoListeners = ImmutableList.copyOf(this.outputInfoListeners);
            }
            queryOutputInfo.ifPresent(info -> fireStateChanged(info, localOutputInfoListeners));
        }

        private void resetForResume()
        {
            // Snapshot: Preprare to restart, by allowing receival of exchange locations
            this.noMoreExchangeLocations = false;
            this.exchangeLocations.clear();
        }

        public void updateOutputLocations(Set<TaskLocation> newExchangeLocations, boolean noMoreExchangeLocations)
        {
            requireNonNull(newExchangeLocations, "newExchangeLocations is null");

            Optional<QueryOutputInfo> queryOutputInfo;
            List<Consumer<QueryOutputInfo>> localOutputInfoListeners;
            synchronized (this) {
                if (this.noMoreExchangeLocations) {
                    checkArgument(this.exchangeLocations.containsAll(newExchangeLocations), "New locations added after no more locations set");
                    return;
                }

                this.exchangeLocations.addAll(newExchangeLocations);
                this.noMoreExchangeLocations = noMoreExchangeLocations;
                queryOutputInfo = getQueryOutputInfo();
                localOutputInfoListeners = ImmutableList.copyOf(this.outputInfoListeners);
            }
            queryOutputInfo.ifPresent(info -> fireStateChanged(info, localOutputInfoListeners));
        }

        private synchronized Optional<QueryOutputInfo> getQueryOutputInfo()
        {
            if (columnNames == null || columnTypes == null) {
                return Optional.empty();
            }
            return Optional.of(new QueryOutputInfo(columnNames, columnTypes, exchangeLocations, noMoreExchangeLocations));
        }

        private void fireStateChanged(QueryOutputInfo queryOutputInfo, List<Consumer<QueryOutputInfo>> outputInfoListeners)
        {
            for (Consumer<QueryOutputInfo> outputInfoListener : outputInfoListeners) {
                executor.execute(() -> outputInfoListener.accept(queryOutputInfo));
            }
        }
    }
}
