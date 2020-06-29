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

import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.client.DataCenterQueryResults;
import io.prestosql.client.StatementStats;
import io.prestosql.datacenter.DataCenterStatementResource;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryManager;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.operator.ExchangeClient;
import io.prestosql.operator.ExchangeClientSupplier;
import io.prestosql.server.SessionContext;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.statestore.StateStoreProvider;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.TEXT_PLAIN_TYPE;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

public class PagePublisherQueryRunner
{
    private static final Logger log = Logger.get(PagePublisherQueryRunner.class);

    private static final DataCenterQueryResults RUNNING_RESULTS = new DataCenterQueryResults("", URI.create(""), null, URI.create(""), null, null,
            new StatementStats("RUNNING", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null), null,
            Collections.emptyList(), null, true);
    private static final DataCenterQueryResults FINISHED_RESULTS = new DataCenterQueryResults("", URI.create(""), null, null, null, null,
            new StatementStats("FINISHED", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null), null,
            Collections.emptyList(), null, true);
    private static final DataCenterQueryResults FAILED_RESULTS = new DataCenterQueryResults("", URI.create(""), null, null, null, null,
            new StatementStats("FAILED", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null), null,
            Collections.emptyList(), null, true);
    private static final Ordering<Comparable<Duration>> WAIT_ORDERING = Ordering.natural().nullsLast();
    private static final Duration MAX_WAIT_TIME = new Duration(1, SECONDS);
    private static final Duration MAX_ANTICIPATED_DELAY = new Duration(1, HOURS);

    private final BlockingQueue<DataCenterQueryResults> queryResults;
    private final Map<String, PageConsumer> consumers;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean finishedExecuting = new AtomicBoolean(false);
    private final AtomicBoolean finishedPublishing = new AtomicBoolean(false);
    private final AtomicBoolean error = new AtomicBoolean(false);
    private final AtomicReference<DataCenterQueryResults> failedResult = new AtomicReference<>();
    private final List<PageConsumer> consumersList = new CopyOnWriteArrayList<>();
    private Query query;

    private final DataSize targetResultSize;
    private final String slug;
    private final String statement;
    private final QueryManager queryManager;
    private final DispatchManager dispatchManager;
    private final QueryId queryId;
    private final Duration wait;
    private final Executor executor;
    private final ScheduledExecutorService timeoutExecutor;
    private final BlockEncodingSerde blockEncodingSerde;
    private final SessionContext sessionContext;
    private final ExchangeClientSupplier exchangeClientSupplier;
    private final Duration maxAnticipatedDelay;
    private final String globalQueryId;
    private final StateStoreProvider stateStoreProvider;
    private final Duration pageConsumerTimeout;

    public PagePublisherQueryRunner(String globalQueryId, String statement, SessionContext sessionContext,
            Duration maxWait, DispatchManager dispatchManager, QueryManager queryManager,
            Executor executor, ScheduledExecutorService timeoutExecutor, BlockEncodingSerde blockEncodingSerde,
            ExchangeClientSupplier exchangeClientSupplier, int maxSubscribersLimit, DataSize targetResultSize,
            Duration maxAnticipatedDelay, StateStoreProvider stateStoreProvider, Duration pageConsumerTimeout)
    {
        this.globalQueryId = requireNonNull(globalQueryId, "globalQueryId is null");
        this.statement = requireNonNull(statement, "statement is null");
        this.sessionContext = requireNonNull(sessionContext, "sessionContext is null");
        this.exchangeClientSupplier = requireNonNull(exchangeClientSupplier, "exchangeClientSupplier is null");
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.targetResultSize = requireNonNull(targetResultSize, "targetResultSize is null");
        this.pageConsumerTimeout = requireNonNull(pageConsumerTimeout, "pageConsumerTimeout is null");
        this.queryId = this.dispatchManager.createQueryId();
        this.slug = "x" + randomUUID().toString().toLowerCase(ENGLISH).replace("-", "");
        this.wait = WAIT_ORDERING.min(MAX_WAIT_TIME, maxWait);
        this.maxAnticipatedDelay = WAIT_ORDERING.min(MAX_ANTICIPATED_DELAY, maxAnticipatedDelay);
        this.queryResults = new LinkedBlockingQueue<>(maxSubscribersLimit);
        this.consumers = new HashMap<>(maxSubscribersLimit);
        this.stateStoreProvider = stateStoreProvider;
    }

    public String getSlug()
    {
        return slug;
    }

    public boolean isDone()
    {
        if (!this.running.get() || (this.finishedExecuting.get() && this.finishedPublishing.get()) || this.error.get()) {
            for (PageConsumer consumer : this.consumersList) {
                if (!consumer.isFinished()) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public synchronized boolean isExpired()
    {
        if (this.query != null) {
            // PagePublisherQueryRunner is considered expired only if it is not started for a long time
            for (PageConsumer consumer : this.consumersList) {
                if (consumer.isActive()) {
                    return false;
                }
            }
            // None of the consumers are active
            return true;
        }
        return false;
    }

    public Duration getMaxAnticipatedDelay()
    {
        return this.maxAnticipatedDelay;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public void start()
    {
        if (this.query == null) {
            // Not dispatched yet
            synchronized (this) {
                if (this.query == null) {
                    try {
                        waitForDispatched(this.queryId, slug, this.sessionContext, this.statement);
                        this.query = getQuery(this.queryId, slug);
                        ResultsProducer fetcher = new ResultsProducer(this.queryManager, this.query, this.queryResults, this.running, this.started, this.finishedExecuting, this.error, this.failedResult, this.wait, this.targetResultSize);
                        ResultsPublisher publisher = new ResultsPublisher(this.query, this.queryResults, this.running, this.finishedExecuting, this.finishedPublishing, this.error, this.failedResult, this.consumersList);
                        this.executor.execute(fetcher);
                        this.executor.execute(publisher);
                    }
                    catch (Throwable t) {
                        this.stop();
                    }
                }
            }
        }
    }

    private synchronized Query getQuery(QueryId queryId, String slug)
    {
        // this is the first time the query has been accessed on this coordinator
        Session session;
        try {
            if (!queryManager.isQuerySlugValid(queryId, slug)) {
                throw badRequest(NOT_FOUND, "Query not found");
            }
            session = queryManager.getQuerySession(queryId);
        }
        catch (NoSuchElementException e) {
            throw badRequest(NOT_FOUND, "Query not found");
        }

        ExchangeClient exchangeClient = this.exchangeClientSupplier.get(
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(),
                        DataCenterStatementResource.class.getSimpleName()));
        return Query.create(session, slug, queryManager, exchangeClient, executor, timeoutExecutor,
                blockEncodingSerde);
    }

    private static WebApplicationException badRequest(Response.Status status, String message)
    {
        throw new WebApplicationException(Response.status(status).type(TEXT_PLAIN_TYPE).entity(message).build());
    }

    private void waitForDispatched(QueryId queryId, String slug, SessionContext sessionContext, String query)
    {
        ListenableFuture<?> future = this.dispatchManager.createQuery(queryId, slug, sessionContext, query);
        try {
            future.get();
            this.dispatchManager.waitForDispatched(queryId).get();
        }
        catch (InterruptedException | ExecutionException e) {
            badRequest(INTERNAL_SERVER_ERROR, "Query dispatching was interrupted");
        }
    }

    public synchronized void stop()
    {
        this.running.set(false);
        for (PageConsumer consumer : this.consumersList) {
            consumer.stop();
        }
        queryManager.cancelQuery(queryId);

        // try to remove bloomFilter and columns type from hazelcast
        if (stateStoreProvider.getStateStore() != null) {
            StateCollection collection = stateStoreProvider.getStateStore().getStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);
            if (collection != null) {
                collection.destroy();
            }
        }
    }

    public void register(String queryId, String clientId)
    {
        if (!this.globalQueryId.equals(queryId)) {
            throw new IllegalArgumentException("queryId does not match with the expected queryId:" + this.globalQueryId);
        }
        if (!this.started.get() && this.running.get()) {
            // Cannot register after the query has started
            synchronized (this) {
                if (!this.consumers.containsKey(clientId)) {
                    PageConsumer consumer = new PageConsumer(RUNNING_RESULTS, FINISHED_RESULTS, FAILED_RESULTS, this.pageConsumerTimeout);
                    this.consumers.put(clientId, consumer);
                    this.consumersList.add(consumer);
                }
            }
        }
    }

    public synchronized PageConsumer getConsumer(String clientId)
    {
        return this.consumers.get(clientId);
    }

    public synchronized void add(String clientId, PageSubscriber subscriber)
    {
        PageConsumer consumer = this.consumers.get(clientId);
        if (consumer != null) {
            if (this.query == null) {
                this.executor.execute(this::start);
            }
            consumer.add(subscriber);
        }
        else if (this.error.get()) {
            subscriber.send(this.query, FAILED_RESULTS);
        }
        else {
            // let client to close this split
            subscriber.send(this.query, FINISHED_RESULTS);
        }
    }

    public static class ResultsProducer
            implements Runnable
    {
        private final Query query;
        private final QueryId queryId;
        private final QueryManager queryManager;
        private final BlockingQueue<DataCenterQueryResults> resultsQueue;
        private final AtomicBoolean running;
        private final AtomicBoolean started;
        private final AtomicBoolean finishedExecuting;
        private final AtomicBoolean error;
        private final AtomicReference<DataCenterQueryResults> failedResult;
        private final Duration wait;
        private final DataSize targetResultSize;

        public ResultsProducer(QueryManager queryManager,
                Query query,
                BlockingQueue<DataCenterQueryResults> resultsQueue,
                AtomicBoolean running,
                AtomicBoolean started,
                AtomicBoolean finishedExecuting,
                AtomicBoolean error,
                AtomicReference<DataCenterQueryResults> failedResult,
                Duration wait,
                DataSize targetResultSize)
        {
            this.query = query;
            this.queryId = query.getQueryId();
            this.resultsQueue = resultsQueue;
            this.queryManager = queryManager;
            this.running = running;
            this.started = started;
            this.finishedExecuting = finishedExecuting;
            this.error = error;
            this.failedResult = failedResult;
            this.wait = wait;
            this.targetResultSize = targetResultSize;
        }

        @Override
        public void run()
        {
            long lastToken = 0;
            while (this.running.get()) {
                ListenableFuture<DataCenterQueryResults> queryResultsFuture = this.query.waitForResults(lastToken, this.wait,
                        this.targetResultSize);

                DataCenterQueryResults results;
                try {
                    results = queryResultsFuture.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    this.failedResult.set(new DataCenterQueryResults("", URI.create(""), null, null, null, null,
                            new StatementStats("FAILED", false, false, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, null), null,
                            Collections.emptyList(), null, true));
                    this.error.set(true);
                    return;
                }
                if (results == null || !this.running.get()) {
                    this.finishedExecuting.set(true);
                    return;
                }

                // Mark the query as started so that new consumer registration will be prevented
                this.started.compareAndSet(false, true);

                // If query is failed, broadcast and stop the query runner
                if ("FAILED".equals(results.getStats().getState())) {
                    this.failedResult.set(results);
                    this.error.set(true);
                    return;
                }
                // If query is successful and has valid data, add query results to queue so that
                // subscribers can take them from the queue and send back to client.
                if (!("RUNNING".equals(results.getStats().getState()) && results.getData() == null)) {
                    // Don't bother sending null results
                    while (this.running.get()) {
                        try {
                            if (this.resultsQueue.offer(results, 1, SECONDS)) {
                                break;
                            }
                            // If the queue is full, more likely consumers are slow or busy
                            // Record a heart beat to keep the query active
                            queryManager.recordHeartbeat(queryId);
                        }
                        catch (InterruptedException e) {
                            log.debug(e, "Queue was full, retrying...");
                        }
                    }
                }

                // Fetch next token in a round-robin way
                URI nextToken = results.getNextUri();
                if (nextToken != null) {
                    lastToken = Long.parseLong(nextToken.toString());
                }
                else {
                    this.finishedExecuting.set(true);
                    return;
                }
            }
        }
    }

    public static class ResultsPublisher
            implements Runnable
    {
        private final Query query;
        private final BlockingQueue<DataCenterQueryResults> resultsQueue;
        private final AtomicBoolean running;
        private final AtomicBoolean finishedExecuting;
        private final AtomicBoolean finishedPublishing;
        private final AtomicBoolean error;
        private final List<PageConsumer> consumers;
        private int nextIndex;

        public ResultsPublisher(Query query,
                BlockingQueue<DataCenterQueryResults> resultsQueue,
                AtomicBoolean running,
                AtomicBoolean finishedExecuting,
                AtomicBoolean finishedPublishing,
                AtomicBoolean error,
                AtomicReference<DataCenterQueryResults> failedResult,
                List<PageConsumer> consumers)
        {
            this.query = query;
            this.resultsQueue = resultsQueue;
            this.running = running;
            this.finishedExecuting = finishedExecuting;
            this.finishedPublishing = finishedPublishing;
            this.error = error;
            this.consumers = consumers;
        }

        @Override
        public void run()
        {
            while (this.running.get()) {
                if (this.error.get()) {
                    for (PageConsumer consumer : this.consumers) {
                        consumer.setState(this.query, PageConsumer.State.ERROR);
                    }
                    return;
                }
                if (this.finishedExecuting.get() && this.resultsQueue.isEmpty()) {
                    // No results to send and query finished
                    for (PageConsumer consumer : this.consumers) {
                        consumer.setState(this.query, PageConsumer.State.FINISHED);
                    }
                    this.finishedPublishing.set(true);
                    return;
                }
                else if (!this.resultsQueue.isEmpty()) {
                    // Poll only if there is a consumer
                    PageConsumer consumer = this.nextConsumer(Math.min(2, this.consumers.size()));
                    if (consumer != null) {
                        DataCenterQueryResults results = this.resultsQueue.poll();
                        if (results != null) {
                            consumer.consume(query, results);
                        }
                    }
                    else if (this.consumers.isEmpty()) {
                        // no subscribers
                        this.running.set(false);
                        return;
                    }
                }
            }
        }

        private PageConsumer nextConsumer(int maxAttempts)
        {
            if (maxAttempts <= 0 || this.consumers.isEmpty()) {
                return null;
            }
            this.nextIndex = (this.nextIndex + 1) % this.consumers.size();
            PageConsumer consumer = this.consumers.get(this.nextIndex);
            if (consumer.hasRoom()) {
                return consumer;
            }
            else {
                if (consumer.isFinished() || !consumer.isActive()) {
                    this.consumers.remove(this.nextIndex);
                }
                return nextConsumer(maxAttempts - 1);
            }
        }
    }
}
