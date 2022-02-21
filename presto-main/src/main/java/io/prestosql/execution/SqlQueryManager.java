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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.ExceededCpuLimitException;
import io.prestosql.Session;
import io.prestosql.event.QueryMonitor;
import io.prestosql.execution.QueryExecution.QueryOutputInfo;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.memory.ClusterMemoryManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.queryeditorui.QueryEditorUIModule;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.snapshot.QuerySnapshotManager;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.sql.planner.Plan;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.version.EmbedVersion;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.SystemSessionProperties.getQueryMaxCpuTime;
import static io.prestosql.execution.QueryState.RUNNING;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.QUERY_EXPIRE;
import static io.prestosql.utils.StateUtils.isMultiCoordinatorEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

@ThreadSafe
public class SqlQueryManager
        implements QueryManager
{
    private static final Logger log = Logger.get(SqlQueryManager.class);

    private final ClusterMemoryManager memoryManager;
    private final QueryMonitor queryMonitor;
    private final EmbedVersion embedVersion;
    private final QueryTracker<QueryExecution> queryTracker;

    private final Duration maxQueryCpuTime;

    private final ExecutorService queryExecutor;
    private final ThreadPoolExecutorMBean queryExecutorMBean;

    private final ScheduledExecutorService queryManagementExecutor;
    private final ThreadPoolExecutorMBean queryManagementExecutorMBean;

    private final QueryManagerStats stats = new QueryManagerStats();
    // LocalStateProvider
    private final StateStoreProvider stateStoreProvider;
    private final SessionPropertyManager sessionPropertyManager;

    // Inject LocalStateProvider
    @Inject
    public SqlQueryManager(ClusterMemoryManager memoryManager, QueryMonitor queryMonitor, EmbedVersion embedVersion, QueryManagerConfig queryManagerConfig, StateStoreProvider stateStoreProvider, SessionPropertyManager sessionPropertyManager)
    {
        this.memoryManager = requireNonNull(memoryManager, "memoryManager is null");
        this.queryMonitor = requireNonNull(queryMonitor, "queryMonitor is null");
        this.embedVersion = requireNonNull(embedVersion, "embedVersion is null");

        this.maxQueryCpuTime = queryManagerConfig.getQueryMaxCpuTime();

        this.queryExecutor = newCachedThreadPool(threadsNamed("query-scheduler-%s"));
        this.queryExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryExecutor);

        this.queryManagementExecutor = Executors.newScheduledThreadPool(queryManagerConfig.getQueryManagerExecutorPoolSize(), threadsNamed("query-management-%s"));
        this.queryManagementExecutorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) queryManagementExecutor);

        this.queryTracker = new QueryTracker<>(queryManagerConfig, queryManagementExecutor, stateStoreProvider);
        // Inject LocalStateProvider
        this.stateStoreProvider = stateStoreProvider;
        this.sessionPropertyManager = sessionPropertyManager;
    }

    @PostConstruct
    public void start()
    {
        queryTracker.start();
        queryManagementExecutor.scheduleWithFixedDelay(() -> {
            try {
                killBlockingQuery();
                enforceMemoryLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing memory limits");
            }

            try {
                enforceCpuLimits();
            }
            catch (Throwable e) {
                log.error(e, "Error enforcing query CPU time limits");
            }

            try {
                killExpiredQuery();
            }
            catch (Throwable e) {
                log.error(e, "Error killing expired query");
            }
        }, 1, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop()
    {
        queryTracker.stop();
        queryManagementExecutor.shutdownNow();
        queryExecutor.shutdownNow();
    }

    @Override
    public List<BasicQueryInfo> getQueries()
    {
        return queryTracker.getAllQueries().stream()
                .map(queryExecution -> {
                    try {
                        return queryExecution.getBasicQueryInfo();
                    }
                    catch (RuntimeException ignored) {
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(toImmutableList());
    }

    @Override
    public void addOutputInfoListener(QueryId queryId, Consumer<QueryOutputInfo> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).addOutputInfoListener(listener);
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateChangeListener<QueryState> listener)
    {
        requireNonNull(listener, "listener is null");

        queryTracker.getQuery(queryId).addStateChangeListener(listener);
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(query -> query.getStateChange(currentState))
                .orElseGet(() -> immediateFailedFuture(new NoSuchElementException()));
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        return queryTracker.getQuery(queryId).getQueryInfo();
    }

    @Override
    public Session getQuerySession(QueryId queryId)
            throws NoSuchElementException
    {
        return queryTracker.getQuery(queryId).getSession();
    }

    @Override
    public boolean isQuerySlugValid(QueryId queryId, String slug)
    {
        return queryTracker.getQuery(queryId).getSlug().equals(slug);
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getQueryPlan();
    }

    public void addFinalQueryInfoListener(QueryId queryId, StateChangeListener<QueryInfo> stateChangeListener)
    {
        queryTracker.getQuery(queryId).addFinalQueryInfoListener(stateChangeListener);
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getState();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::recordHeartbeat);
    }

    @Override
    public void createQuery(QueryExecution queryExecution)
    {
        requireNonNull(queryExecution, "queryExecution is null");

        if (!queryTracker.addQuery(queryExecution)) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Query %s already registered", queryExecution.getQueryId()));
        }

        // CreateIndex operations could not register cleanup operations like connectors
        // Therefore a listener is added here to clean up index records on failure
        if (isIndexCreationQuery(queryExecution.getQueryInfo())) {
            queryExecution.addStateChangeListener(state -> {
                try {
                    queryMonitor.indexCreationStateChangeEvent(state, queryExecution.getQueryInfo());
                }
                finally {
                    // execution MUST be added to the expiration queue or there will be a leak
                    queryTracker.expireQuery(queryExecution.getQueryId());
                }
            });
        }

        queryExecution.addFinalQueryInfoListener(finalQueryInfo -> {
            try {
                queryMonitor.queryCompletedEvent(finalQueryInfo);
            }
            finally {
                // execution MUST be added to the expiration queue or there will be a leak
                queryTracker.expireQuery(queryExecution.getQueryId());
            }
        });

        stats.trackQueryStats(queryExecution);

        embedVersion.embedVersion(queryExecution::start).run();
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        requireNonNull(cause, "cause is null");

        queryTracker.tryGetQuery(queryId)
                .ifPresent(query -> query.fail(cause));
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        log.debug("Cancel query %s", queryId);

        queryTracker.tryGetQuery(queryId)
                .ifPresent(QueryExecution::cancelQuery);
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");

        log.debug("Cancel stage %s", stageId);

        queryTracker.tryGetQuery(stageId.getQueryId())
                .ifPresent(query -> query.cancelStage(stageId));
    }

    @Override
    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    @Managed(description = "Query scheduler executor")
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return queryExecutorMBean;
    }

    @Managed(description = "Query query management executor")
    @Nested
    public ThreadPoolExecutorMBean getManagementExecutor()
    {
        return queryManagementExecutorMBean;
    }

    /**
     * Enforce memory limits at the query level
     */
    private void enforceMemoryLimits()
    {
        List<QueryExecution> runningQueries;
        Supplier<List<BasicQueryInfo>> allQueryInfoSupplier;

        Map<String, SharedQueryState> queryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        if (isMultiCoordinatorEnabled() && queryStates != null) {
            // Get all queries from state store
            runningQueries = queryStates.values().stream()
                    .filter(state -> state.getBasicQueryInfo().getState() == RUNNING)
                    .map(state -> new SharedQueryExecution(state, sessionPropertyManager)).collect(Collectors.toList());
            allQueryInfoSupplier = () -> queryStates.values().stream().map(state -> state.getBasicQueryInfo()).collect(Collectors.toList());
        }
        else {
            runningQueries = queryTracker.getAllQueries().stream()
                    .filter(query -> query.getState() == RUNNING)
                    .collect(toImmutableList());
            allQueryInfoSupplier = this::getQueries;
        }

        memoryManager.process(runningQueries, allQueryInfoSupplier);

        // Put the chosen query to kill into state store
        if (isMultiCoordinatorEnabled() && queryStates != null) {
            updateKillingQuery(runningQueries, queryStates);
        }
    }

    /**
     * Enforce query CPU time limits
     */
    private void enforceCpuLimits()
    {
        for (QueryExecution query : queryTracker.getAllQueries()) {
            Duration cpuTime = query.getTotalCpuTime();
            Duration sessionLimit = getQueryMaxCpuTime(query.getSession());
            Duration limit = Ordering.natural().min(maxQueryCpuTime, sessionLimit);
            if (cpuTime.compareTo(limit) > 0) {
                query.fail(new ExceededCpuLimitException(limit));
            }
        }
    }

    /**
     * Check if any query is getting killed and update state store
     *
     * @param runningQueries All the running query executions
     * @param queryStates All the query states
     */
    private void updateKillingQuery(List<QueryExecution> runningQueries, Map<String, SharedQueryState> queryStates)
    {
        for (QueryExecution query : runningQueries) {
            SharedQueryExecution queryExecution = (SharedQueryExecution) query;
            if (((SharedQueryExecution) query).isGettingKilled()) {
                StateMap stateMap = (StateMap<String, String>) stateStoreProvider.getStateStore().getStateCollection(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME);
                if (stateMap != null) {
                    ObjectMapper mapper = new ObjectMapperProvider().get();
                    try {
                        String stateJson = mapper.writeValueAsString(queryStates.get(queryExecution.getQueryId().getId()));
                        stateMap.put(queryExecution.getQueryId().getId(), stateJson);
                    }
                    catch (JsonProcessingException e) {
                        log.warn("Query %s state serialization failed: %s", queryExecution.getQueryId().getId(), e.getMessage());
                    }
                }
                break;
            }
        }
    }

    /**
     * Kill query when cluster is in OOM state
     */
    private void killBlockingQuery()
    {
        if (isMultiCoordinatorEnabled()) {
            return;
        }

        List<QueryExecution> localRunningQueries = queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList());

        Map<String, SharedQueryState> queries = StateCacheStore.get().getCachedStates(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME);
        if (queries != null) {
            for (SharedQueryState query : queries.values()) {
                for (QueryExecution localQuery : localRunningQueries) {
                    if (query.getBasicQueryInfo().getQueryId().equals(localQuery.getQueryId())) {
                        memoryManager.killLocalQuery(localQuery);
                    }
                }
            }
        }
    }

    /**
     * Kill query when expired, state has already been updated in StateFetcher.
     */
    private void killExpiredQuery()
    {
        if (!isMultiCoordinatorEnabled()) {
            return;
        }
        List<QueryExecution> localRunningQueries = queryTracker.getAllQueries().stream()
                .filter(query -> query.getState() == RUNNING)
                .collect(toImmutableList());

        Map<String, SharedQueryState> queries = StateCacheStore.get().getCachedStates(StateStoreConstants.FINISHED_QUERY_STATE_COLLECTION_NAME);
        if (queries != null) {
            Set<String> expiredQueryIds = queries.entrySet().stream()
                    .filter(entry -> isQueryExpired(entry.getValue()))
                    .map(entry -> entry.getKey())
                    .collect(Collectors.toSet());
            if (!expiredQueryIds.isEmpty()) {
                for (QueryExecution localQuery : localRunningQueries) {
                    if (expiredQueryIds.contains(localQuery.getQueryId().getId())) {
                        localQuery.fail(new PrestoException(QUERY_EXPIRE, "Query killed because the query has expired. Please try again in a few minutes."));
                    }
                }
            }
        }
    }

    @Override
    public void checkForQueryPruning(QueryId queryId, QueryInfo queryInfo)
    {
        boolean isUiQuery = queryInfo.getSession().getSource()
                .map(source -> QueryEditorUIModule.UI_QUERY_SOURCE.equals(source))
                .orElse(false);
        if (isUiQuery && queryInfo.getState() == QueryState.FINISHED) {
            // UI related queries need not take up the history space.
            queryTracker.removeQuery(queryId);
        }
    }

    @Override
    public QuerySnapshotManager getQuerySnapshotManager(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getQuerySnapshotManager();
    }

    private boolean isIndexCreationQuery(QueryInfo queryInfo)
    {
        return queryInfo.getQuery().toUpperCase(Locale.ROOT).startsWith("CREATE INDEX");
    }

    private static boolean isQueryExpired(SharedQueryState state)
    {
        BasicQueryInfo info = state.getBasicQueryInfo();
        return info.getState() == QueryState.FAILED && info.getErrorType() == ErrorType.INTERNAL_ERROR && info.getErrorCode().equals(QUERY_EXPIRE.toErrorCode());
    }
}
