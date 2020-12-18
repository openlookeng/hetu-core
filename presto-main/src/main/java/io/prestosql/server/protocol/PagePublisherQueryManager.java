/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.server.protocol;

import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.client.DataCenterQueryResults;
import io.prestosql.client.StatementStats;
import io.prestosql.dispatcher.DispatchExecutor;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryManager;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.server.SessionContext;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.StateStoreProvider;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTERS;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PagePublisherQueryManager
{
    private static final Logger log = Logger.get(PagePublisherQueryManager.class);

    private static final DataSize DEFAULT_TARGET_RESULT_SIZE = new DataSize(1, MEGABYTE);
    private static final int MAX_CONCURRENT_SUBSCRIBERS_PER_QUERY = 10;

    private DataSize resultSizeQuota = DEFAULT_TARGET_RESULT_SIZE;
    private final Set<String> queries = Sets.newConcurrentHashSet();
    private final Map<String, PagePublisherQueryRunner> queryRunners = new ConcurrentHashMap<>();
    private static final DataCenterQueryResults FINISHED_RESULTS_DONOT_USE_HEADER = new DataCenterQueryResults("", URI.create(""), null, null, null, null,
            new StatementStats("FINISHED", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null), null,
            Collections.emptyList(), null, false);

    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final BlockEncodingSerde blockEncodingSerde;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final StateStoreProvider stateStoreProvider;
    private final Duration pageConsumerTimeout;
    private final ScheduledExecutorService queryPurger = newSingleThreadScheduledExecutor(threadsNamed("execution-query-purger"));

    public PagePublisherQueryManager(DispatchManager dispatchManager,
            QueryManager queryManager,
            ExchangeClientSupplier exchangeClientSupplier,
            BlockEncodingSerde blockEncodingSerde,
            DispatchExecutor dispatchExecutor,
            StateStoreProvider stateStoreProvider,
            Duration pageConsumerTimeout)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.responseExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getExecutor();
        this.timeoutExecutor = requireNonNull(dispatchExecutor, "dispatchExecutor is null").getScheduledExecutor();
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
        this.pageConsumerTimeout = requireNonNull(pageConsumerTimeout, "pageConsumerTimeout is null");
        this.queryPurger.scheduleWithFixedDelay(
                () -> {
                    try {
                        this.queryRunners.entrySet().removeIf(entry -> {
                            String queryId = entry.getKey();
                            PagePublisherQueryRunner queryRunner = entry.getValue();
                            if (queryRunner.isDone() || queryRunner.isExpired()) {
                                // Either the query has been completed or the query wasn't started for a long time
                                queryRunner.stop();
                                Duration maxAnticipatedDelay = queryRunner.getMaxAnticipatedDelay();
                                // After maxAnticipatedDelay, delete the query from the set
                                this.queryPurger.schedule(() -> {
                                    this.queries.remove(queryId);
                                }, maxAnticipatedDelay.toMillis(), MILLISECONDS);
                                return true;
                            }
                            return false;
                        });
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error removing old queries");
                    }
                },
                200,
                200,
                MILLISECONDS);
    }

    public PagePublisherQueryRunner submit(String globalQueryId, String query, String clientId, Duration maxAnticipatedDelay, SessionContext context)
    {
        PagePublisherQueryRunner runner = this.queryRunners.get(globalQueryId);
        if (runner == null) {
            synchronized (this) {
                runner = this.queryRunners.get(globalQueryId);
                if (runner != null) {
                    runner.register(globalQueryId, clientId);
                    return runner;
                }
                if (this.queries.contains(globalQueryId)) {
                    return null;
                }
                // This is the first time this query is submitted
                runner = new PagePublisherQueryRunner(globalQueryId,
                        query,
                        context,
                        null,
                        this.dispatchManager,
                        this.queryManager,
                        this.responseExecutor,
                        this.timeoutExecutor,
                        this.blockEncodingSerde,
                        this.exchangeClientSupplier,
                        MAX_CONCURRENT_SUBSCRIBERS_PER_QUERY,
                        this.resultSizeQuota,
                        maxAnticipatedDelay,
                        stateStoreProvider,
                        this.pageConsumerTimeout);
                // this.queryPurger.scheduleWithFixedDelay check if the runner is expired {@link isExpired}
                // (if the query is null, need a active customer) and remove from queryRunners.
                // so we need register customer first to add a active customer, then put to queryRunners.
                runner.register(globalQueryId, clientId);
                // it's better to add globalQueryId into queries before to add runner into queryRunners.
                // otherwise, in high concurrency scenarios, something strange will happen,
                // such as when invoke add method, the result of this.queries.contains(globalQueryId) is false.
                this.queries.add(globalQueryId);
                this.queryRunners.put(globalQueryId, runner);
            }
        }
        else {
            runner.register(globalQueryId, clientId);
        }
        return runner;
    }

    public void add(String globalQueryId, String slug, String clientId, PageSubscriber subscriber)
    {
        if (!this.queries.contains(globalQueryId)) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        PagePublisherQueryRunner runner = this.queryRunners.get(globalQueryId);
        if (runner == null) {
            // already finished
            subscriber.send(null, FINISHED_RESULTS_DONOT_USE_HEADER);
            return;
        }
        if (!Objects.equals(runner.getSlug(), slug)) {
            throw new WebApplicationException(Response.Status.NOT_FOUND);
        }
        runner.add(clientId, subscriber);
    }

    public synchronized void cancel(String globalQueryId, String slug)
    {
        PagePublisherQueryRunner runner = this.queryRunners.get(globalQueryId);
        if (runner != null) {
            if (!Objects.equals(runner.getSlug(), slug)) {
                throw new IllegalArgumentException("globalQueryId and slug do not match with each other");
            }
            runner.stop();
            this.queryRunners.remove(globalQueryId);
        }
    }

    public synchronized void saveDynamicFilter(String globalQueryId, Map<String, byte[]> bloomFilters)
    {
        PagePublisherQueryRunner queryRunner = this.queryRunners.get(globalQueryId);
        StateStore stateStore = this.stateStoreProvider.getStateStore();

        if (queryRunner != null && stateStore != null) {
            if (this.stateStoreProvider.getStateStore() == null) {
                return;
            }

            String collectionName = queryRunner.getQueryId().getId() + CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
            synchronized (queryRunner) {
                if (queryRunner.isDone() || queryRunner.isExpired()) {
                    return;
                }

                StateMap map = (StateMap<String, Map<String, byte[]>>) stateStore.getOrCreateStateCollection(CROSS_REGION_DYNAMIC_FILTERS, StateCollection.Type.MAP);

                map.put(collectionName, bloomFilters);
            }
        }
    }

    public synchronized void close()
    {
        for (PagePublisherQueryRunner runner : this.queryRunners.values()) {
            try {
                runner.stop();
            }
            catch (Throwable t) {
                log.warn(t, "Error stopping query runner");
            }
        }
        this.queryRunners.clear();
        this.queries.clear();
        this.queryPurger.shutdownNow();
    }
}
