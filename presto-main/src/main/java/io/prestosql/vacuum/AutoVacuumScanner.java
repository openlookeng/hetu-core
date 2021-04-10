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

package io.prestosql.vacuum;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.datacenter.DataCenterStatementResource;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.execution.QueryManager;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.metadata.Catalog;
import io.prestosql.metadata.CatalogManager;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.server.ForStatementResource;
import io.prestosql.server.protocol.Query;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.security.Identity;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class AutoVacuumScanner
{
    private static final Logger log = Logger.get(AutoVacuumScanner.class);

    private final ScheduledExecutorService autoVacuumScanExecutor;
    private final Duration vacuumScanInterval;
    private final boolean autoVacuumEnabled;
    private final CatalogManager catalogManager;
    private final DispatchManager dispatchManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final QueryManager queryManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final Map<String, Long> vacuumInProgressMap;

    @Inject
    public AutoVacuumScanner(AutoVacuumConfig autoVacuumConfig,
            CatalogManager catalogManager,
            DispatchManager dispatchManager,
            SessionPropertyManager sessionPropertyManager,
            QueryManager queryManager,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            @ForStatementResource BoundedExecutor responseExecutor,
            @ForStatementResource ScheduledExecutorService timeoutExecutor)
    {
        this.autoVacuumScanExecutor = Executors.newScheduledThreadPool(autoVacuumConfig.getVacuumScanThreads());
        this.autoVacuumEnabled = autoVacuumConfig.isAutoVacuumEnabled();
        this.vacuumScanInterval = autoVacuumConfig.getVacuumScanInterval();
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.vacuumInProgressMap = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void start()
    {
        if (!autoVacuumEnabled) {
            log.debug("Auto Vacuum scan is disabled");
            return;
        }
        checkState(!autoVacuumScanExecutor.isShutdown(), "Auto Vacuum scanner has been destroyed");
        log.debug("Start the auto vacuum scan.");

        // start scan.
        autoVacuumScanExecutor.scheduleWithFixedDelay(() -> {
            try {
                scan();
            }
            catch (PrestoException e) {
                log.error(e, "Start the auto vacuum scanner failed.");
            }
        }, vacuumScanInterval.toMillis(), vacuumScanInterval.toMillis(), MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        autoVacuumScanExecutor.shutdownNow();
    }

    void scan()
    {
        List<Catalog> catalogs = catalogManager.getCatalogs();

        for (Catalog catalog : catalogs) {
            try {
                List<ConnectorVacuumTableInfo> tables = getVacuumTableList(catalog);
                if (tables != null && 0 != tables.size()) {
                    for (ConnectorVacuumTableInfo vacuumTable : tables) {
                        try {
                            startVacuum(catalog, vacuumTable.getSchemaTableName(), vacuumTable.isFull());
                        }
                        catch (Exception e) {
                            log.error("error in start Vacuum %s", e.getMessage());
                        }
                    }
                }
            }
            catch (Exception e) {
                log.error("error in scan %s", e.getMessage());
            }
        }
    }

    private void startVacuum(Catalog catalog, String vacuumTable, boolean isFull)
    {
        String catalogNameVacuumTable = catalog.getCatalogName() + "." + vacuumTable;

        if (vacuumInProgressMap.containsKey(catalogNameVacuumTable)) {
            log.debug("return Present in vacuumInProgressMap %s ", catalogNameVacuumTable);
            return;
        }
        long attempts = 0;
        QueryId queryId = dispatchManager.createQueryId();
        String slug = "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        String vacuumQuery;
        if (isFull) {
            vacuumQuery = "vacuum table " + catalogNameVacuumTable + "  full";
        }
        else {
            vacuumQuery = "vacuum table " + catalogNameVacuumTable;
        }
        Session.SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(new Identity("openLooKeng", Optional.empty()))
                .setSource("auto-vacuum");
        Session session = sessionBuilder.build();
        AutoVacuumSessionContext sessionContext = new AutoVacuumSessionContext(session);
        vacuumInProgressMap.put(catalogNameVacuumTable, System.currentTimeMillis());
        log.debug("Query.create queryId %s  catalogNameVacuumTable: %s ", queryId.toString(), catalogNameVacuumTable);
        ListenableFuture<?> lf = waitForDispatched(queryId, slug, sessionContext, vacuumQuery);
        Futures.addCallback(lf, new FutureCallback<Object>()
        {
            @Override
            public void onSuccess(@Nullable Object result)
            {
                try {
                    DispatchQuery dispatchQuery = dispatchManager.getQuery(queryId);
                    dispatchQuery.addStateChangeListener((state) -> {
                        Query query = getQuery(queryId, slug);
                        if ((null != query) && (!dispatchManager.getQueryInfo(queryId).getState().isDone())) {
                            query.waitForResults(attempts, Duration.valueOf("1s"), DataSize.valueOf("1MB"));
                        }

                        if (state.isDone()) {
                            log.debug("STATUS  %s QueryID %s Query %s", state.name(), queryId.toString(), vacuumQuery);
                            vacuumInProgressMap.remove(catalogNameVacuumTable);
                        }
                    });
                }
                catch (Throwable e) {
                    vacuumInProgressMap.remove(catalogNameVacuumTable);
                    log.error("Filed to execute vacuum for table %s QueryID %s", catalogNameVacuumTable, queryId.toString(), e.getMessage());
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                vacuumInProgressMap.remove(catalogNameVacuumTable);
                log.error("Query %s request to start vacuum scan failed at queryId[%s]: %s ", vacuumQuery, queryId, t.getMessage());
            }
        }, directExecutor());
    }

    private ListenableFuture<?> waitForDispatched(QueryId queryId, String slug, AutoVacuumSessionContext sessionContext, String vacuumQuery)
    {
        ListenableFuture<?> querySubmissionFuture;
        // if query query submission has not finished, wait for it to finish
        synchronized (this) {
            querySubmissionFuture = dispatchManager.createQuery(queryId, slug, sessionContext, vacuumQuery);
        }

        if (!querySubmissionFuture.isDone()) {
            return querySubmissionFuture;
        }
        // otherwise, wait for the query to finish
        return dispatchManager.waitForDispatched(queryId);
    }

    private synchronized Query getQuery(QueryId queryId, String slug)
    {
        // this is the first time the query has been accessed on this coordinator
        Session session = null;
        try {
            if (!queryManager.isQuerySlugValid(queryId, slug)) {
                return null;
            }
            session = queryManager.getQuerySession(queryId);
            if (null == session) {
                return null;
            }
        }
        catch (NoSuchElementException e) {
            return null;
        }

        ExchangeClient exchangeClient = this.exchangeClientSupplier.get(
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(),
                        DataCenterStatementResource.class.getSimpleName()));
        return Query.create(session, slug, queryManager, exchangeClient, directExecutor(), timeoutExecutor,
                blockEncodingSerde);
    }

    public List<ConnectorVacuumTableInfo> getVacuumTableList(Catalog catalog)
    {
        try {
            Connector connector = catalog.getConnector(catalog.getConnectorCatalogName());
            ConnectorMetadata connectorMetadata = connector.getConnectorMetadata();
            return connectorMetadata.getTablesForVacuum();
        }
        catch (UnsupportedOperationException e) {
            return null;
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, format("Start vacuum scan failed: %s.", e.getMessage()));
        }
    }
}
