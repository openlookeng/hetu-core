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
package io.prestosql.dispatcher;

import com.google.common.util.concurrent.AbstractFuture;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.QueryManagerStats;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryTracker;
import io.prestosql.execution.resourcegroups.ResourceGroupManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.queryeditorui.QueryEditorUIModule;
import io.prestosql.security.AccessControl;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.server.SessionContext;
import io.prestosql.server.SessionPropertyDefaults;
import io.prestosql.server.SessionSupplier;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.SelectionContext;
import io.prestosql.spi.resourcegroups.SelectionCriteria;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.statestore.SharedQueryState;
import io.prestosql.statestore.StateCacheStore;
import io.prestosql.statestore.StateFetcher;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.statestore.StateUpdater;
import io.prestosql.transaction.TransactionManager;
import io.prestosql.utils.HetuConfig;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.spi.StandardErrorCode.QUERY_TEXT_TOO_LARGE;
import static io.prestosql.util.StatementUtils.getQueryType;
import static io.prestosql.util.StatementUtils.isTransactionControlStatement;
import static io.prestosql.utils.StateUtils.isMultiCoordinatorEnabled;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DispatchManager
{
    private static final Logger LOG = Logger.get(DispatchManager.class);

    private final QueryIdGenerator queryIdGenerator;
    private final QueryPreparer queryPreparer;
    private final ResourceGroupManager<?> resourceGroupManager;
    private final DispatchQueryFactory dispatchQueryFactory;
    private final FailedDispatchQueryFactory failedDispatchQueryFactory;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;
    private final SessionSupplier sessionSupplier;
    private final SessionPropertyDefaults sessionPropertyDefaults;
    // Members to support multiple coordinators
    private final StateStoreProvider stateStoreProvider;
    private StateUpdater stateUpdater;
    private StateFetcher stateFetcher;
    private final HetuConfig hetuConfig;
    private final int maxQueryLength;

    private final Executor queryExecutor;

    private final QueryTracker<DispatchQuery> queryTracker;

    private final QueryManagerStats stats = new QueryManagerStats();

    @Inject
    public DispatchManager(
            QueryIdGenerator queryIdGenerator,
            QueryPreparer queryPreparer,
            @SuppressWarnings("rawtypes") ResourceGroupManager resourceGroupManager,
            DispatchQueryFactory dispatchQueryFactory,
            FailedDispatchQueryFactory failedDispatchQueryFactory,
            TransactionManager transactionManager,
            AccessControl accessControl,
            SessionSupplier sessionSupplier,
            SessionPropertyDefaults sessionPropertyDefaults,
            QueryManagerConfig queryManagerConfig,
            DispatchExecutor dispatchExecutor,
            StateStoreProvider stateStoreProvider,
            HetuConfig hetuConfig)
    {
        this.queryIdGenerator = requireNonNull(queryIdGenerator, "queryIdGenerator is null");
        this.queryPreparer = requireNonNull(queryPreparer, "queryPreparer is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
        this.dispatchQueryFactory = requireNonNull(dispatchQueryFactory, "dispatchQueryFactory is null");
        this.failedDispatchQueryFactory = requireNonNull(failedDispatchQueryFactory, "failedDispatchQueryFactory is null");
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.accessControl = requireNonNull(accessControl, "accessControl is null");
        this.sessionSupplier = requireNonNull(sessionSupplier, "sessionSupplier is null");
        this.sessionPropertyDefaults = requireNonNull(sessionPropertyDefaults, "sessionPropertyDefaults is null");
        // StateStoreProvider and load HetuConfig
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
        this.hetuConfig = requireNonNull(hetuConfig, "hetuConfig is null");
        PropertyService.setProperty(HetuConstant.MULTI_COORDINATOR_ENABLED, hetuConfig.isMultipleCoordinatorEnabled());

        requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.maxQueryLength = queryManagerConfig.getMaxQueryLength();

        this.queryExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();

        this.queryTracker = new QueryTracker<>(queryManagerConfig, dispatchExecutor.getScheduledExecutor(), stateStoreProvider);
    }

    @PostConstruct
    public void start()
    {
        // start state services
        startStateServices();
        queryTracker.start();
    }

    @PreDestroy
    public void stop()
    {
        // stop state services
        stopStateServices();
        queryTracker.stop();
    }

    @Managed
    @Flatten
    public QueryManagerStats getStats()
    {
        return stats;
    }

    public QueryId createQueryId()
    {
        return queryIdGenerator.createNextQueryId();
    }

    public ListenableFuture<?> createQuery(QueryId queryId, String slug, SessionContext sessionContext, String query)
    {
        requireNonNull(queryId, "queryId is null");
        requireNonNull(sessionContext, "sessionFactory is null");
        requireNonNull(query, "query is null");
        checkArgument(!query.isEmpty(), "query must not be empty string");
        checkArgument(!queryTracker.tryGetQuery(queryId).isPresent(), "query %s already exists", queryId);

        DispatchQueryCreationFuture queryCreationFuture = new DispatchQueryCreationFuture();
        queryExecutor.execute(() -> {
            try {
                createQueryInternal(queryId, slug, sessionContext, query, resourceGroupManager);
            }
            finally {
                queryCreationFuture.set(null);
            }
        });
        return queryCreationFuture;
    }

    /**
     * Creates and registers a dispatch query with the query tracker.  This method will never fail to register a query with the query
     * tracker.  If an error occurs while creating a dispatch query, a failed dispatch will be created and registered.
     */
    private <C> void createQueryInternal(QueryId queryId, String slug, SessionContext sessionContext, String inputQuery, ResourceGroupManager<C> resourceGroupManager)
    {
        String query = inputQuery;
        Session session = null;
        DispatchQuery dispatchQuery = null;
        try {
            if (query.length() > maxQueryLength) {
                int queryLength = query.length();
                query = query.substring(0, maxQueryLength);
                throw new PrestoException(QUERY_TEXT_TOO_LARGE, format("Query text length (%s) exceeds the maximum length (%s)", queryLength, maxQueryLength));
            }

            // decode session
            session = sessionSupplier.createSession(queryId, sessionContext);

            // prepare query
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, query);

            // select resource group
            Optional<String> queryType = getQueryType(preparedQuery.getStatement().getClass()).map(Enum::name);
            SelectionContext<C> selectionContext = resourceGroupManager.selectGroup(new SelectionCriteria(
                    sessionContext.getIdentity().getPrincipal().isPresent(),
                    sessionContext.getIdentity().getUser(),
                    Optional.ofNullable(sessionContext.getSource()),
                    sessionContext.getClientTags(),
                    sessionContext.getResourceEstimates(),
                    queryType));

            // apply system default session properties (does not override user set properties)
            session = sessionPropertyDefaults.newSessionWithDefaultProperties(session, queryType, selectionContext.getResourceGroupId());

            // mark existing transaction as active
            transactionManager.activateTransaction(session, isTransactionControlStatement(preparedQuery.getStatement()), accessControl);

            dispatchQuery = dispatchQueryFactory.createDispatchQuery(
                    session,
                    query,
                    preparedQuery,
                    slug,
                    selectionContext.getResourceGroupId(),
                    resourceGroupManager);

            boolean queryAdded = queryCreated(dispatchQuery);
            if (queryAdded && !dispatchQuery.isDone()) {
                try {
                    resourceGroupManager.submit(dispatchQuery, selectionContext, queryExecutor);

                    if (PropertyService.getBooleanProperty(HetuConstant.MULTI_COORDINATOR_ENABLED) && stateUpdater != null) {
                        stateUpdater.registerQuery(StateStoreConstants.QUERY_STATE_COLLECTION_NAME, dispatchQuery);
                    }

                    if (LOG.isDebugEnabled()) {
                        long now = System.currentTimeMillis();
                        LOG.debug("query:%s submission started at %s, ended at %s, total time use: %sms",
                                dispatchQuery.getQueryId(),
                                new SimpleDateFormat("HH:mm:ss:SSS").format(dispatchQuery.getCreateTime().toDate()),
                                new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(now)),
                                now - dispatchQuery.getCreateTime().getMillis());
                    }
                }
                catch (Throwable e) {
                    // dispatch query has already been registered, so just fail it directly
                    dispatchQuery.fail(e);
                }
            }
        }
        catch (Throwable throwable) {
            // creation must never fail, so register a failed query in this case
            if (dispatchQuery == null) {
                if (session == null) {
                    session = Session.builder(new SessionPropertyManager())
                            .setQueryId(queryId)
                            .setIdentity(sessionContext.getIdentity())
                            .setSource(sessionContext.getSource())
                            .build();
                }
                DispatchQuery failedDispatchQuery = failedDispatchQueryFactory.createFailedDispatchQuery(session, query, Optional.empty(), throwable);
                queryCreated(failedDispatchQuery);
            }
            else {
                dispatchQuery.fail(throwable);
            }
        }
    }

    private boolean queryCreated(DispatchQuery dispatchQuery)
    {
        boolean queryAdded = queryTracker.addQuery(dispatchQuery);

        boolean isUiQuery = dispatchQuery.getSession().getSource()
                .map(source -> QueryEditorUIModule.UI_QUERY_SOURCE.equals(source))
                .orElse(false);
        // only add state tracking if this query instance will actually be used for the execution
        if (queryAdded) {
            dispatchQuery.addStateChangeListener(newState -> {
                if (newState.isDone()) {
                    if (isUiQuery && newState == QueryState.FINISHED) {
                        // UI related queries need not take up the history space.
                        queryTracker.removeQuery(dispatchQuery.getQueryId());
                    }
                    else {
                        // execution MUST be added to the expiration queue or there will be a leak
                        queryTracker.expireQuery(dispatchQuery.getQueryId());
                    }
                }
            });
            stats.trackQueryStats(dispatchQuery);
        }

        return queryAdded;
    }

    public ListenableFuture<?> waitForDispatched(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchedFuture();
                })
                .orElseGet(() -> immediateFuture(null));
    }

    public List<BasicQueryInfo> getQueries()
    {
        // Get states from state store when multi-coordinator is enabled
        List<BasicQueryInfo> queryInfos;
        if (isMultiCoordinatorEnabled() && StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME) != null) {
            Map<String, SharedQueryState> queryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
            Map<String, SharedQueryState> finishedQueryStates = StateCacheStore.get().getCachedStates(StateStoreConstants.FINISHED_QUERY_STATE_COLLECTION_NAME);

            queryInfos = Stream.concat(
                    queryStates.values().stream(),
                    finishedQueryStates.values().stream())
                    .map(SharedQueryState::getBasicQueryInfo)
                    .collect(Collectors.toList());
        }
        else {
            queryInfos = queryTracker.getAllQueries().stream()
                    .map(DispatchQuery::getBasicQueryInfo)
                    .collect(toImmutableList());
        }
        return queryInfos;
    }

    public boolean isQueryRegistered(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId).isPresent();
    }

    public DispatchQuery getQuery(QueryId queryId)
    {
        return queryTracker.getQuery(queryId);
    }

    public BasicQueryInfo getQueryInfo(QueryId queryId)
    {
        return queryTracker.getQuery(queryId).getBasicQueryInfo();
    }

    public Optional<DispatchInfo> getDispatchInfo(QueryId queryId)
    {
        return queryTracker.tryGetQuery(queryId)
                .map(dispatchQuery -> {
                    dispatchQuery.recordHeartbeat();
                    return dispatchQuery.getDispatchInfo();
                });
    }

    public void cancelQuery(QueryId queryId)
    {
        queryTracker.tryGetQuery(queryId)
                .ifPresent(DispatchQuery::cancel);
    }

    private static class DispatchQueryCreationFuture
            extends AbstractFuture<QueryInfo>
    {
        @Override
        protected boolean set(QueryInfo value)
        {
            return super.set(value);
        }

        @Override
        protected boolean setException(Throwable throwable)
        {
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning)
        {
            // query submission can not be canceled
            return false;
        }
    }

    // Initialize query states related services
    private void startStateServices()
    {
        if (!PropertyService.getBooleanProperty(HetuConstant.MULTI_COORDINATOR_ENABLED)) {
            return;
        }

        if (stateUpdater == null) {
            stateUpdater = new StateUpdater(stateStoreProvider, hetuConfig.getStateUpdateInterval());
        }

        if (stateFetcher == null) {
            stateFetcher = new StateFetcher(stateStoreProvider, hetuConfig.getStateFetchInterval(), hetuConfig.getStateExpireTime());
        }

        // Start state updater
        stateUpdater.start();

        // Start state fetcher
        stateFetcher.registerStateCollection(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
        stateFetcher.registerStateCollection(StateStoreConstants.FINISHED_QUERY_STATE_COLLECTION_NAME);
        stateFetcher.registerStateCollection(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME);
        stateFetcher.registerStateCollection(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME);
        stateFetcher.start();
    }

    // Stop states related services
    private void stopStateServices()
    {
        if (stateUpdater != null) {
            stateUpdater.stop();
        }

        if (stateFetcher != null) {
            stateFetcher.stop();
        }
    }
}
