/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.ctematerialization;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.cache.CachedDataManager;
import io.prestosql.datacenter.DataCenterStatementResource;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.exchange.ExchangeId;
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
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.utils.HetuConfig;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.prestosql.SystemSessionProperties.getRetryPolicy;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CteMaterializationScanner
{
    private static final Logger log = Logger.get(CteMaterializationScanner.class);

    private final ScheduledExecutorService cteMaterializationScanService;
    private final CatalogManager catalogManager;
    private final DispatchManager dispatchManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final QueryManager queryManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final BoundedExecutor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final boolean isCteMaterializationEnabled;
    private final boolean isMultiCoordinatorEnabled;
    private final String cachingConnectorName;
    private final String cachingSchemaName;
    private final String cachingUserName;
    private final CachedDataManager cacheDataManager;

    @Inject
    public CteMaterializationScanner(FeaturesConfig featuresConfig,
                                     HetuConfig hetuConfig,
                                     CatalogManager catalogManager,
                                     DispatchManager dispatchManager,
                                     SessionPropertyManager sessionPropertyManager,
                                     QueryManager queryManager,
                                     ExchangeClientSupplier exchangeClientSupplier,
                                     BlockEncodingSerde blockEncodingSerde,
                                     @ForStatementResource BoundedExecutor responseExecutor,
                                     @ForStatementResource ScheduledExecutorService timeoutExecutor,
                                     CachedDataManager cachedDataManager)
    {
        this.isCteMaterializationEnabled = featuresConfig.isCTEMaterializationEnabled();
        this.isMultiCoordinatorEnabled = hetuConfig.isMultipleCoordinatorEnabled();
        this.catalogManager = requireNonNull(catalogManager, "catalogManager is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.cteMaterializationScanService = Executors.newScheduledThreadPool(4);
        this.cachingConnectorName = hetuConfig.getCachingConnectorName();
        this.cachingSchemaName = hetuConfig.getCachingSchemaName();
        this.cachingUserName = hetuConfig.getCachingUserName();
        this.cacheDataManager = requireNonNull(cachedDataManager);
    }

    @PostConstruct
    public void start()
    {
        if (!isMultiCoordinatorEnabled && isCteMaterializationEnabled) {
            checkState(!cteMaterializationScanService.isShutdown(), "Cte Materialization scanner has been destroyed");

            // start scan.
            cteMaterializationScanService.scheduleWithFixedDelay(() -> {
                try {
                    List<String> catalogs = catalogManager.getCatalogs().stream().map(catalog -> catalog.getCatalogName())
                            .filter(catalog -> catalog.equalsIgnoreCase(cachingConnectorName)).collect(Collectors.toList());
                    if (catalogs.size() > 0) {
                        createCteMaterializationSchemaIfNotExists();
                        cacheDataManager.setReady();
                        cteMaterializationScanService.shutdown();
                    }
                }
                catch (PrestoException e) {
                    log.error(e, "Start the cte materialization scanner failed.");
                }
            }, 10000, 5000, MILLISECONDS);
        }
    }

    private void createCteMaterializationSchemaIfNotExists()
    {
        List<Catalog> catalogs = catalogManager.getCatalogs().stream().filter(catalog -> catalog.getCatalogName().equalsIgnoreCase(cachingConnectorName)).collect(Collectors.toList());

        for (Catalog catalog : catalogs) {
            try {
                List<String> tables = getCteCacheTables(catalog, cachingSchemaName);
                if (tables != null && 0 != tables.size()) {
                    for (String cteCachedTable : tables) {
                        try {
                            createCteMaterializationSchema(catalog, cteCachedTable, true);
                        }
                        catch (Exception e) {
                            log.error("error in dropping old cached tables under schema cache %s", e.getMessage());
                        }
                    }
                }
                else {
                    createCteMaterializationSchema(catalog, cachingSchemaName, false);
                }
            }
            catch (Exception e) {
                log.error("error in scan %s", e.getMessage());
            }
        }
    }

    public List<String> getCteCacheTables(Catalog catalog, String cachingSchemaName)
    {
        QueryId queryId = dispatchManager.createQueryId();
        try {
            Connector connector = catalog.getConnector(catalog.getConnectorCatalogName());
            ConnectorMetadata connectorMetadata = connector.getConnectorMetadata();
            Session.SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                    .setQueryId(queryId)
                    .setIdentity(new Identity(cachingUserName, Optional.empty()))
                    .setCatalog(catalog.getCatalogName())
                    .setSchema(cachingSchemaName);
            Session session = sessionBuilder.build();
            List<SchemaTableName> schemaTableNames = connectorMetadata.listTables(session.toConnectorSession(catalog.getConnectorCatalogName()),
                    Optional.of(cachingSchemaName));
            dispatchManager.cancelQuery(queryId);
            return schemaTableNames.stream().map(schemaTableName -> schemaTableName.getTableName()).collect(Collectors.toList());
        }
        catch (UnsupportedOperationException e) {
            return ImmutableList.of();
        }
        catch (Exception e) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, format("Start cte materialization scan failed: %s.", e.getMessage()));
        }
    }

    private void createCteMaterializationSchema(Catalog catalog, String table, boolean isDrop)
    {
        String catalogNameTable = table;

        long attempts = 0;
        QueryId queryId = dispatchManager.createQueryId();
        String slug = "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        String localQuery;
        if (isDrop) {
            localQuery = "drop table \"" + catalogNameTable + "\"";
        }
        else {
            localQuery = "create schema IF NOT EXISTS \"" + table + "\"";
        }
        Session.SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                .setQueryId(queryId)
                .setIdentity(new Identity(cachingUserName, Optional.empty()))
                .setCatalog(catalog.getCatalogName())
                .setSchema(cachingSchemaName)
                .setSource("create-schema");
        Session session = sessionBuilder.build();
        CteMaterializationSessionContext sessionContext = new CteMaterializationSessionContext(session);
        ListenableFuture<?> lf = waitForDispatched(queryId, slug, sessionContext, localQuery);
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
                            log.debug("STATUS  %s QueryID %s Query %s", state.name(), queryId.toString(), localQuery);
                        }
                    });
                }
                catch (Throwable e) {
                    log.error("Filed to execute %s QueryID %s", table, queryId.toString(), e.getMessage());
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                log.error("Query %s request to scan failed at queryId[%s]: %s ", localQuery, queryId, t.getMessage());
            }
        }, directExecutor());
    }

    private ListenableFuture<?> waitForDispatched(QueryId queryId, String slug, CteMaterializationSessionContext sessionContext, String query)
    {
        ListenableFuture<?> querySubmissionFuture;
        // if query query submission has not finished, wait for it to finish
        synchronized (this) {
            querySubmissionFuture = dispatchManager.createQuery(queryId, slug, sessionContext, query);
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
                        DataCenterStatementResource.class.getSimpleName()), null, getRetryPolicy(session), new ExchangeId("direct-exchange-cte-schema-scanner"), session.getQueryId());
        return Query.create(session, slug, queryManager, exchangeClient, directExecutor(), timeoutExecutor,
                blockEncodingSerde);
    }
}
