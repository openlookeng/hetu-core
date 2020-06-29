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

import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Objects.requireNonNull;

/**
 * StateCacheStore caches states from StateStore locally
 *
 * @since 2019-11-29
 */
@ThreadSafe
public class StateCacheStore
{
    private static Logger log = Logger.get(StateCacheStore.class);
    private static volatile StateCacheStore instance;
    private final Map<String, Map> cachedStates = new ConcurrentHashMap<>();

    private StateCacheStore()
    {
        // Only use get() method to obtain instance
    }

    /**
     * Get or create a StateCacheStore instance
     *
     * @return existing or created StateCacheStore instance
     */
    public static StateCacheStore get()
    {
        if (instance == null) {
            synchronized (StateCacheStore.class) {
                if (instance == null) {
                    log.info("Creating new StateCacheStore...");
                    instance = new StateCacheStore();
                    log.info("StateCacheStore created.");
                }
            }
        }
        return instance;
    }

    /**
     * Get cache by cache name from StateCacheStore
     *
     * @param cacheName name of the cache
     * @return map contains all cached states
     */
    public Map getCachedStates(String cacheName)
    {
        requireNonNull(cacheName, "cacheName is null");
        return cachedStates.get(cacheName);
    }

    /**
     * Put states in StateCacheStore
     *
     * @param cacheName name of the cache
     * @param states map contains all states to cache
     */
    public void setCachedStates(String cacheName, Map states)
    {
        requireNonNull(cacheName, "cacheName is null");
        requireNonNull(states, "states is null");

        synchronized (cachedStates) {
            cachedStates.put(cacheName, states);
        }
    }

    /**
     * Clear the StateCacheStore instance
     */
    public void resetCachedStates()
    {
        instance = null;
    }
}
