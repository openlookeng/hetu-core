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
package io.prestosql.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.utils.HetuConfig;

import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;

import static java.util.Objects.requireNonNull;

public class CachedDataManager
{
    private static final Logger LOG = Logger.get(CachedDataManager.class);

    private final Optional<Cache<CachedDataKey, CachedDataStorage>> dataCache;
    private final CacheStorageMonitor monitor;
    private final Metadata metadata;

    @Inject
    public CachedDataManager(HetuConfig hetuConfig, CacheStorageMonitor monitor, Metadata metadata)
    {
        if (hetuConfig.isExecutionDataCacheEnabled()) {
            this.dataCache = Optional.of(CacheBuilder.newBuilder()
                    .maximumWeight(hetuConfig.getExecutionDataCacheMaxSize())
                    .weigher(new Weigher<CachedDataKey, CachedDataStorage>() {
                        @Override
                        public int weigh(CachedDataKey key, CachedDataStorage value)
                        {
                            return (int) value.getDataSize();
                        }
                    })
                    .build());
        }
        else {
            this.dataCache = Optional.empty();
        }

        this.monitor = requireNonNull(monitor, "monitor is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public boolean isDataCachedEnabled()
    {
        return dataCache.isPresent();
    }

    public CachedDataStorage validateAndGet(CachedDataKey dataKey, Session session)
    {
        if (!dataCache.isPresent()) {
            return null;
        }

        CachedDataStorage object = get(dataKey);
        if (object != null && validateCacheEntry(dataKey, object, session)) {
            return object;
        }

        return null;
    }

    public CachedDataStorage get(CachedDataKey dataKey)
    {
        if (!dataCache.isPresent()) {
            return null;
        }

        return dataCache.get().getIfPresent(dataKey);
    }

    private boolean validateCacheEntry(CachedDataKey key, CachedDataStorage object, Session session)
    {
        if (monitor.checkTableValidity(object, session)) {
            return true;
        }

        /* Drop the cached data table */
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, QualifiedObjectName.valueOf(object.getDataTable()));
        if (tableHandle.isPresent()) {
            // Todo: mark for dropping when reference count reduce
            metadata.dropTable(session, tableHandle.get());
        }
        invalidate(ImmutableSet.of(key));
        return false;
    }

    public void put(CachedDataKey key, CachedDataStorage value)
    {
        if (dataCache.isPresent()) {
            dataCache.get().put(key, value);
        }
    }

    public void invalidate(Set<CachedDataKey> keySet)
    {
        if (dataCache.isPresent()) { /* Add invalidators for storage as cacheWalker */
            dataCache.get().invalidateAll(keySet);
        }
    }

    public void invalidateAll(Set<CachedDataKey> keySet)
    {
        if (dataCache.isPresent()) { /* Add invalidators for storage as cacheWalker */
            dataCache.get().invalidateAll();
        }
    }

    public void cacheWalkAll(BiFunction<CachedDataKey, CachedDataStorage, Void> walker)
    {
        if (dataCache.isPresent() && walker != null) {
            dataCache.get().asMap().forEach((key, value) -> walker.apply(key, value));
        }
    }

    public void cacheWalk(Iterable<CachedDataKey> keySet, BiFunction<CachedDataKey, CachedDataStorage, Void> walker)
    {
        if (dataCache.isPresent() && walker != null) {
            dataCache.get().getAllPresent(keySet).forEach((key, value) -> walker.apply(key, value));
        }
    }
}
