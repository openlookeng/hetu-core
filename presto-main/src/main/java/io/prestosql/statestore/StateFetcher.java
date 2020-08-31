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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryState;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static io.prestosql.utils.StateUtils.removeState;

/**
 * State fetcher service used to fetch externalized query states from external state store
 *
 * @since 2019-11-29
 */
public class StateFetcher
{
    private static final Logger LOG = Logger.get(StateFetcher.class);

    private final StateStoreProvider stateStoreProvider;
    private final Duration fetchInterval;
    private final Duration stateExpireTime;
    private final Set<String> stateCollections = new HashSet<>();
    private final ScheduledExecutorService stateUpdateExecutor;
    private ScheduledFuture<?> backgroundTask;

    private static final ObjectMapper MAPPER = new ObjectMapperProvider().get();
    private static final int THREAD_POOL_SIZE = 2;

    public StateFetcher(StateStoreProvider stateStoreProvider, Duration fetchInterval, Duration stateExpireTime)
    {
        this.stateStoreProvider = stateStoreProvider;
        this.fetchInterval = fetchInterval;
        this.stateExpireTime = stateExpireTime;
        this.stateUpdateExecutor = Executors.newScheduledThreadPool(THREAD_POOL_SIZE, threadsNamed("state-fetcher-%s"));
    }

    /**
     * Start background task to fetch externalized query states
     */
    public void start()
    {
        checkState(backgroundTask == null, "StateFetcher already started");
        backgroundTask = stateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                fetchStates();
            }
            catch (Exception e) {
                LOG.error("Error fetching query states: " + e.getMessage());
            }
        }, fetchInterval.toMillis(), fetchInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop the background state fetching task
     */
    public void stop()
    {
        synchronized (this) {
            if (backgroundTask != null) {
                backgroundTask.cancel(true);
                stateCollections.clear();
            }
        }
    }

    /**
     * Register state collections to state fetcher service, state fetcher tries to fetch all the states
     * from all the state collections
     *
     * @param stateCollectionName state collection name
     */
    public void registerStateCollection(String stateCollectionName)
    {
        stateCollections.add(stateCollectionName);
    }

    /**
     * Unregister a state collection from state fetcher service
     *
     * @param stateCollectionName state collection name
     */
    public void unregisterStateCollection(String stateCollectionName)
    {
        stateCollections.remove(stateCollectionName);
    }

    /**
     * Fetch state from state store to cache store
     *
     * @throws IOException exception when failed to deserialize states
     */
    public void fetchStates()
            throws IOException
    {
        synchronized (this) {
            // State store hasn't been loaded yet
            if (stateStoreProvider.getStateStore() == null) {
                return;
            }

            long start = System.currentTimeMillis();
            LOG.debug("fetchStates starts at current time milliseconds: %s, at format HH:mm:ss:SSS:%s",
                    start,
                    new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(start)));

            DateTime currentTime = new DateTime(DateTimeZone.UTC);
            for (String stateCollectionName : stateCollections) {
                StateCollection stateCollection = stateStoreProvider.getStateStore().getStateCollection(stateCollectionName);
                if (stateCollection == null) {
                    continue;
                }
                if (stateCollectionName.equals(StateStoreConstants.CPU_USAGE_STATE_COLLECTION_NAME)) {
                    StateCacheStore.get().setCachedStates(stateCollectionName, ((StateMap) stateCollection).getAll());
                    continue;
                }

                if (stateCollection.getType() == StateCollection.Type.MAP) {
                    Map<String, String> states = ((StateMap<String, String>) stateCollection).getAll();

                    ImmutableMap.Builder<String, SharedQueryState> queryStatesBuilder = ImmutableMap.builder();
                    for (Map.Entry<String, String> entry : states.entrySet()) {
                        SharedQueryState state = MAPPER.readerFor(SharedQueryState.class).readValue(entry.getValue());
                        if (isStateExpired(state, currentTime)) {
                            handleExpiredQueryState(state);
                        }
                        queryStatesBuilder.put(entry.getKey(), state);
                    }
                    StateCacheStore.get().setCachedStates(stateCollectionName, queryStatesBuilder.build());
                }
                else {
                    LOG.warn("Unsupported state collection type: %s", stateCollection.getType());
                }
            }
            long end = System.currentTimeMillis();
            LOG.debug("updateStates ends at current time milliseconds: %s, at format HH:mm:ss:SSS:%s, total time use: %s",
                    end,
                    new SimpleDateFormat("HH:mm:ss:SSS").format(new Date(end)),
                    end - start);
        }
    }

    /**
     * Check if state is expired, no need to count expired states
     * expired states are likely from inactive coordinators that are not cleaned properly
     *
     * @param state query state to check
     * @param currentTime current time
     * @return if the query state expires
     */
    private boolean isStateExpired(SharedQueryState state, DateTime currentTime)
    {
        // No need to check query expiry for finished queries
        if (state.getBasicQueryInfo().getState() == QueryState.FINISHED || state.getBasicQueryInfo().getState() == QueryState.FAILED) {
            return false;
        }
        return currentTime.getMillis() - state.getStateUpdateTime().getMillis() > stateExpireTime.toMillis();
    }

    private void handleExpiredQueryState(SharedQueryState state)
    {
        // State store hasn't been loaded yet
        if (stateStoreProvider.getStateStore() == null) {
            return;
        }

        Lock lock = null;
        boolean locked = false;
        try {
            lock = stateStoreProvider.getStateStore().getLock(StateStoreConstants.HANDLE_EXPIRED_QUERY_LOCK_NAME);
            locked = lock.tryLock(StateStoreConstants.DEFAULT_ACQUIRED_LOCK_TIME_MS, TimeUnit.MILLISECONDS);
            if (locked) {
                LOG.debug(String.format("EXPIRED!!! REMOVING... Id: %s, state: %s, uri: %s, query: %s",
                        state.getBasicQueryInfo().getQueryId().getId(),
                        state.getBasicQueryInfo().getState().toString(),
                        state.getBasicQueryInfo().getSelf().toString(),
                        state.getBasicQueryInfo().getQuery()));

                // remove expired query from oom
                StateCollection stateCollection = stateStoreProvider.getStateStore().getStateCollection(StateStoreConstants.OOM_QUERY_STATE_COLLECTION_NAME);
                removeState(stateCollection, Optional.of(state.getBasicQueryInfo().getQueryId()), LOG);

                // update query to failed in stateCollection if exists
                stateCollection = stateStoreProvider.getStateStore().getStateCollection(StateStoreConstants.QUERY_STATE_COLLECTION_NAME);
                if (stateCollection != null && stateCollection.getType().equals(StateCollection.Type.MAP)) {
                    Map<String, String> queryStateMap = ((StateMap<String, String>) stateCollection).getAll();
                    if (queryStateMap.get(state.getBasicQueryInfo().getQueryId().getId()) != null) {
                        BasicQueryInfo oldQueryInfo = state.getBasicQueryInfo();
                        SharedQueryState newState = createNewState(oldQueryInfo, state);

                        String stateJson = MAPPER.writeValueAsString(newState);
                        ((StateMap) stateCollection).put(newState.getBasicQueryInfo().getQueryId().getId(), stateJson);
                    }
                }
            }
        }
        catch (Exception e) {
            LOG.error("Error handleExpiredQueryState: " + e.getMessage());
        }
        finally {
            if (locked) {
                lock.unlock();
            }
        }
    }

    private SharedQueryState createNewState(BasicQueryInfo oldQueryInfo, SharedQueryState oldState)
    {
        BasicQueryInfo newQueryInfo = new BasicQueryInfo(
                oldQueryInfo.getQueryId(),
                oldQueryInfo.getSession(),
                oldQueryInfo.getResourceGroupId(),
                QueryState.FAILED,
                oldQueryInfo.getMemoryPool(),
                oldQueryInfo.isScheduled(),
                oldQueryInfo.getSelf(),
                oldQueryInfo.getQuery(),
                oldQueryInfo.getPreparedQuery(),
                oldQueryInfo.getQueryStats(),
                ErrorType.INTERNAL_ERROR,
                SERVER_SHUTTING_DOWN.toErrorCode()); // now query expired only if coordinator shutdown

        SharedQueryState newState = new SharedQueryState(
                newQueryInfo,
                oldState.getSession(),
                Optional.of(SERVER_SHUTTING_DOWN.toErrorCode()),
                oldState.getUserMemoryReservation(),
                oldState.getTotalMemoryReservation(),
                oldState.getTotalCpuTime(),
                oldState.getStateUpdateTime(),
                oldState.getExecutionStartTime());

        return newState;
    }
}
