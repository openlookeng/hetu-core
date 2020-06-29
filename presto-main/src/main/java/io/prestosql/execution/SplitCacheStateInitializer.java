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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;

/**
 *
 * Initializes the coordinator local
 * SplitCacheMap with the data already available in StateStore.
 *
 */
public class SplitCacheStateInitializer
{
    private final Logger log = Logger.get(SplitCacheStateInitializer.class);
    private final StateStoreProvider provider;
    private final ScheduledExecutorService stateFetchExecutor;
    private final Duration delay;
    private final Duration timeout;
    private final SplitCacheMap splitCacheMap;
    private final ObjectMapper mapper;
    private final AtomicReference<InitializationStatus> initializationState;
    private ScheduledFuture<?> backgroundTask;

    public enum InitializationStatus
    {
        INITIALIZING, COMPLETED, FAILED;
    }

    public SplitCacheStateInitializer(StateStoreProvider provider, SplitCacheMap splitCacheMap, Duration delay, Duration timeout, ObjectMapper mapper, AtomicReference<InitializationStatus> status)
    {
        this.provider = provider;
        this.delay = delay;
        this.timeout = timeout;
        this.splitCacheMap = splitCacheMap;
        this.mapper = mapper;
        this.initializationState = status;
        this.stateFetchExecutor = Executors.newScheduledThreadPool(1, threadsNamed("split-cache-state-fetcher-%s"));
    }

    public void start()
    {
        checkState(backgroundTask == null, "StateFetcher already started");

        //start background task to initialize the splitcache map
        backgroundTask = stateFetchExecutor.scheduleWithFixedDelay(() -> {
            try {
                updateLocal();
            }
            catch (Exception e) {
                log.error(e, "Error updating local split cache map from state store");
            }
        }, delay.toMillis(), delay.toMillis(), TimeUnit.MILLISECONDS);

        //cancel initializer task if already not complete within configured timeout
        stateFetchExecutor.schedule(() -> {
            initializationState.compareAndSet(InitializationStatus.INITIALIZING, InitializationStatus.FAILED);
            log.info("Split cache state initialization %s.", initializationState.get());
            backgroundTask.cancel(true);
        }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void updateLocal()
    {
        StateStore stateStore = provider.getStateStore();
        if (stateStore == null) {
            log.debug("State store not yet initialized. Will retry after %s milli seconds until %s", delay.toMillis(), timeout.toString(TimeUnit.SECONDS));
            return;
        }

        if (initializationState.get() == InitializationStatus.COMPLETED) {
            log.debug("Split cache map already initialized.");
            return;
        }

        //create state store collection
        if (stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME) == null) {
            stateStore.createStateMap(StateStoreConstants.SPLIT_CACHE_METADATA_NAME, new SplitCacheStateStoreChangesListener(SplitCacheMap.getInstance(), mapper));
        }

        StateMap<String, String> stateMap = (StateMap<String, String>) stateStore.getStateCollection(StateStoreConstants.SPLIT_CACHE_METADATA_NAME);
        stateMap.getAll().forEach((fqTableName, stateJson) -> {
            try {
                TableCacheInfo cacheInfo = mapper.readerFor(TableCacheInfo.class).readValue(stateJson);
                log.info("Retrieving cache info for table %s from state store and updating on local copy.", fqTableName);
                splitCacheMap.setTableCacheInfo(fqTableName, cacheInfo);
            }
            catch (Exception e) {
                log.error(e, "Unable to update local split cache map from state store.");
            }
        });
        initializationState.set(InitializationStatus.COMPLETED);
    }

    public void stop()
    {
        if (backgroundTask != null) {
            backgroundTask.cancel(true);
        }
        stateFetchExecutor.shutdownNow();
    }
}
