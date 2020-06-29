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
package io.prestosql.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.statestore.StateStoreConstants;
import io.prestosql.statestore.StateStoreProvider;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.execution.SplitCacheStateInitializer.InitializationStatus;

/**
 *
 * SplitCacheStateUpdater periodically updates StateStore
 * with the local SplitCacheMap data.
 *
 */
public class SplitCacheStateUpdater
{
    private final Logger log = Logger.get(SplitCacheStateUpdater.class);

    private final Duration updateInterval;
    private final StateStoreProvider provider;
    private final ScheduledExecutorService stateUpdateExecutor;
    private final SplitCacheMap splitCacheMap;
    private final ObjectMapper mapper;
    private final AtomicReference<InitializationStatus> status;
    private ScheduledFuture<?> backgroundTask;

    public SplitCacheStateUpdater(StateStoreProvider provider, SplitCacheMap splitCacheMap, Duration updateInterval, ObjectMapper mapper, AtomicReference<InitializationStatus> status)
    {
        this.provider = provider;
        this.updateInterval = updateInterval;
        this.stateUpdateExecutor = Executors.newScheduledThreadPool(2, threadsNamed("split-cache-state-updater-%s"));
        this.splitCacheMap = splitCacheMap;
        this.mapper = mapper;
        this.status = status;
    }

    public void start()
    {
        checkState(backgroundTask == null, "StateUpdater already started");

        //start background task to update StateStore periodically
        backgroundTask = stateUpdateExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateStateStore();
            }
            catch (Exception e) {
                log.error(e, "Error updating split cache map in state store: " + e.getMessage());
            }
        }, updateInterval.toMillis(), updateInterval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("Split cache map async state update task started.");
    }

    public void updateStateStore()
    {
        StateStore stateStore = provider.getStateStore();
        if (stateStore == null) {
            log.debug("State store not yet initialized.");
            return;
        }

        if (status.get() == InitializationStatus.INITIALIZING) {
            log.debug("Split cache map not yet initialized.");
            return;
        }

        //create state store collection
        if (stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME) == null) {
            stateStore.createStateMap(StateStoreConstants.SPLIT_CACHE_METADATA_NAME, new SplitCacheStateStoreChangesListener(SplitCacheMap.getInstance(), mapper));
        }
        StateMap<String, String> stateMap = (StateMap<String, String>) stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME);

        splitCacheMap.getAndClearDroppedCaches().forEach(stateMap::remove);

        splitCacheMap.tableCacheInfoMap().forEach((fqTableName, localCacheInfo) -> {
            try {
                //Async update works only when new cache predicates added and splits are scheduled or updated
                //It does not perform merge also in case if both local info and state store are updated.
                //The logic is very simple. Only update state store if local copy is more recent than the one in state store.
                TableCacheInfo stateStoreCacheInfo = stateMap.containsKey(fqTableName) ? mapper.readerFor(TableCacheInfo.class).readValue(stateMap.get(fqTableName)) : null;
                if (stateStoreCacheInfo == null || localCacheInfo.getLastUpdated().isAfter(stateStoreCacheInfo.getLastUpdated())) {
                    log.info("Updating state store split cache map for table %s.", fqTableName);
                    String json = mapper.writeValueAsString(localCacheInfo);
                    stateMap.put(fqTableName, json);
                }
                else if (localCacheInfo.getLastUpdated().isBefore(stateStoreCacheInfo.getLastUpdated())) {
                    log.debug("Not updating state store split cache map for table %s. Local copy is outdated. State store split cache map is more recent. Local split cache map should be updated.", fqTableName);
                }
            }
            catch (IOException e) {
                log.error(e, "Unable to update state store split cache map.");
            }
        });
    }

    public void stop()
    {
        if (backgroundTask != null) {
            backgroundTask.cancel(true);
        }
        stateUpdateExecutor.shutdownNow();
    }
}
