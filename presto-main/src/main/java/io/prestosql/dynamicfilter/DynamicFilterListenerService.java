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
package io.prestosql.dynamicfilter;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.statestore.StateStoreProvider;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.utils.DynamicFilterUtils.FILTERPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.MERGEMAP;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * StateStore Listener for collecting newly merged DynamicFilter on workers
 */
public class DynamicFilterListenerService
        implements EntryAddedListener<String, Object>
{
    private static final Logger LOG = Logger.get(DynamicFilterListenerService.class);
    private static final long CACHE_MAXIMUM_SIZE = 1_000_000;
    private static final long EXPIRES_AFTER_WRITE_MILLIS = 120_000;
    private static final long REFRESH_MILLIS = 60_000;

    private final LoadingCache<String, Optional<DynamicFilter>> cacheGlobalDynamicFilters;
    private final StateStoreProvider stateStoreProvider;
    private StateMap mergedDynamicFilters;

    @Inject
    public DynamicFilterListenerService(StateStoreProvider stateStoreProvider)
    {
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
        cacheGlobalDynamicFilters = newBuilder()
                .expireAfterWrite(EXPIRES_AFTER_WRITE_MILLIS, MILLISECONDS)
                .refreshAfterWrite(REFRESH_MILLIS, MILLISECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .build(CacheLoader.from(this::loadDynamicFilter));
    }

    @PreDestroy
    public void flushCache()
    {
        cacheGlobalDynamicFilters.invalidateAll();
    }

    private Optional<DynamicFilter> loadDynamicFilter(String cacheKey)
    {
        if (stateStoreProvider.getStateStore() == null || cacheKey == null) {
            return Optional.empty();
        }

        if (mergedDynamicFilters == null) {
            mergedDynamicFilters = stateStoreProvider.getStateStore().createStateMap(MERGEMAP);
        }

        Object newDynamicFilter = mergedDynamicFilters.get(FILTERPREFIX + cacheKey);
        if (newDynamicFilter == null) {
            return Optional.empty();
        }

        LOG.debug("Load dynamic filter from hazelcast:" + cacheKey);
        return transformDynamicFilter(cacheKey, newDynamicFilter);
    }

    public void removeDynamicFilter(String cacheKey)
    {
        cacheGlobalDynamicFilters.asMap().remove(cacheKey);
        LOG.debug("Remove DynamicFilter from cacheGlobalDynamicFilter:" + cacheKey);
    }

    public DynamicFilter getDynamicFilter(String cacheKey)
    {
        Optional<DynamicFilter> dynamicFilter = get(cacheGlobalDynamicFilters, cacheKey);
        return dynamicFilter.orElse(null);
    }

    private static <K, V> V get(LoadingCache<K, V> loadingCache, K cacheKey)
    {
        V value = null;
        try {
            value = loadingCache.get(cacheKey);
        }
        catch (ExecutionException e) {
            LOG.warn("Get dynamic filter from cache is error:" + e.getMessage());
        }

        return value;
    }

    private Optional<DynamicFilter> transformDynamicFilter(String cacheKey, Object newDynamicFilter)
    {
        String filterId = cacheKey.split("-")[0];
        // Global dynamic filters
        if (newDynamicFilter == null) {
            LOG.warn("DynamicFilter added to StateStore is null.");
            return Optional.empty();
        }

        DynamicFilter dynamicFilter = null;
        if (newDynamicFilter instanceof Set) {
            Set hashSetFilter = (Set) newDynamicFilter;
            dynamicFilter = DynamicFilterFactory.create(filterId, null, hashSetFilter, GLOBAL);
            LOG.debug("Got new HashSet DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize());
        }
        else if (newDynamicFilter instanceof byte[]) {
            byte[] serializedBloomFilter = (byte[]) newDynamicFilter;
            dynamicFilter = DynamicFilterFactory.create(filterId, null, serializedBloomFilter, GLOBAL);
            LOG.debug("Got new BloomFilter DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize() + ", byte:" + serializedBloomFilter.length);
        }

        return dynamicFilter == null ? Optional.empty() : Optional.of(dynamicFilter);
    }

    @Override
    public void entryAdded(EntryEvent<String, Object> event)
    {
        String key = event.getKey();
        String cacheKey = key.substring(FILTERPREFIX.length());
        cacheGlobalDynamicFilters.put(cacheKey, transformDynamicFilter(cacheKey, event.getValue()));
    }
}
