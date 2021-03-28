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
package io.prestosql.statestore;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.dispatcher.DispatchQuery;
import io.prestosql.execution.ManagedQueryExecution;
import io.prestosql.execution.QueryState;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.spi.StandardErrorCode.CLUSTER_OUT_OF_MEMORY;
import static io.prestosql.spi.StandardErrorCode.EXCEEDED_GLOBAL_MEMORY_LIMIT;
import static io.prestosql.utils.StateUtils.removeState;

/**
 * State updater service used to update locally registered query states to external state store
 *
 * @since 2019-11-29
 */
public class StateUpdater
{
    private static final Logger LOG = Logger.get(StateUpdater.class);

    private final StateStoreProvider stateStoreProvider;
    private final Duration updateInterval;
    private final Multimap<String, DispatchQuery> states = ArrayListMultimap.create();
    private final ScheduledExecutorService stateUpdateExecutor;
    private ScheduledFuture<?> backgroundTask;

    private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();
    private static final int THREAD_POOL_SIZE = 2;

    public StateUpdater(StateStoreProvider stateStoreProvider, Duration updateInterval)
    {
        this.stateStoreProvider = stateStoreProvider;
        this.updateInterval = updateInterval;
        this.stateUpdateExecutor = Executors.newScheduledThreadPool(THREAD_POOL_SIZE, threadsNamed("state-updater-%s"));
    }

    /**
     * Start background task to update local state to external state store
     */
    public void start()
    {
        checkState(backgroundTask == null, "StateUpdater already started");
        backgroundTask = stateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateStates();
            }
            catch (Exception e) {
                LOG.error("Error updating query states: " + e.getMessage());
            }
        }, updateInterval.toMillis(), updateInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the background state updating task
     */
    public void stop()
    {
        synchronized (this) {
            if (backgroundTask != null) {
                backgroundTask.cancel(true);
                states.clear();
            }
        }
    }

    /**
     * Register a local DispatchQuery to a state collection in external state store
     * State updater service will periodically update the states to state store
     *
     * @param stateCollectionName state collection name
     * @param query DispatchQuery
     */
    public void registerQuery(String stateCollectionName, DispatchQuery query)
    {
        synchronized (states) {
            states.put(stateCollectionName, query);
            query.addStateChangeListener(state -> {
                if (state.isDone()) {
                    queryFinished(query);
                }
            });
        }
    }

    /**
     * Unregister a registered DispatchQuery
     *
     * @param stateCollectionName state collection name
     * @param query DispatchQuery
     */
    public void unregisterQuery(String stateCollectionName, ManagedQueryExecution query)
    {
        synchronized (states) {
            states.remove(stateCollectionName, query);
        }
    }

    /**
     * Update local queued query states to state store
     *
     * @throws JsonProcessingException exception when fail to serialize states to json
     */
    public void updateStates()
            throws JsonProcessingException
    {
        synchronized (states) {
            // State store hasn't been loaded yet
            if (stateStoreProvider.getStateStore() == null) {
                return;
            }

            long start = System.currentTimeMillis();
            LOG.debug("UpdateStates starts at current time milliseconds: %s, at format HH:mm:ss:SSS:%s",
                    start,
                    new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(start)));

            for (String stateCollectionName : states.keySet()) {
                StateCollection stateCollection = stateStoreProvider.getStateStore().getStateCollection(stateCollectionName);
                Set<QueryId> finishedQueries = new HashSet<>();
                for (DispatchQuery query : states.get(stateCollectionName)) {
                    SharedQueryState state = SharedQueryState.create(query);
                    String stateJson = MAPPER.writeValueAsString(state);

                    switch (stateCollection.getType()) {
                        case MAP:
                            ((StateMap) stateCollection).put(state.getBasicQueryInfo().getQueryId().getId(), stateJson);
                            break;
                        default:
                            LOG.error("Unsupported state collection type: %s", stateCollection.getType());
                    }

                    if (state.getBasicQueryInfo().getState() == QueryState.FINISHED || state.getBasicQueryInfo().getState() == QueryState.FAILED) {
                        finishedQueries.add(state.getBasicQueryInfo().getQueryId());
                    }
                }

                // No need to update states for finished queries
                unregisterFinishedQueries(stateCollectionName, finishedQueries);
            }

            long end = System.currentTimeMillis();
            LOG.debug("updateStates ends at current time milliseconds: %s, at format HH:mm:ss:SSS:%s, total time use: %s",
                    end,
                    new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(end)),
                    end - start);
        }
    }

    private void queryFinished(ManagedQueryExecution query)
    {
        // If query killed by OOM remove the query from OOM query state store
        if (isQueryKilledByOOMKiller(query)) {
            removeFromStateStore(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME, query);
        }
    }

    private boolean isQueryKilledByOOMKiller(ManagedQueryExecution query)
    {
        if (!query.getErrorCode().isPresent()) {
            return false;
        }

        if (query.getErrorCode().get().equals(CLUSTER_OUT_OF_MEMORY.toErrorCode()) ||
                query.getErrorCode().get().equals(EXCEEDED_GLOBAL_MEMORY_LIMIT.toErrorCode())) {
            return true;
        }

        return false;
    }

    private void removeFromStateStore(String stateCollectionName, ManagedQueryExecution query)
    {
        // State store hasn't been loaded yet
        if (stateStoreProvider.getStateStore() == null) {
            return;
        }

        synchronized (states) {
            StateCollection stateCollection = stateStoreProvider.getStateStore().getStateCollection(stateCollectionName);
            removeState(stateCollection, Optional.of(query.getBasicQueryInfo().getQueryId()), LOG);
            states.remove(stateCollectionName, query);
        }
    }

    /**
     * For finished or failed queries, no need to keep updating their states to state store
     * so unregister them from state updater
     *
     * @param stateCollectionName state collection name
     * @param queryIds Queries to be unregistered
     */
    private void unregisterFinishedQueries(String stateCollectionName, Set<QueryId> queryIds)
    {
        Iterator<DispatchQuery> iterator = states.get(stateCollectionName).iterator();
        while (iterator.hasNext()) {
            if (queryIds.contains(iterator.next().getQueryId())) {
                iterator.remove();
            }
        }
    }
}
