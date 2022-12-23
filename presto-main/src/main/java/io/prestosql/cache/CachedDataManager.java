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
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.execution.QueryIdGenerator;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.security.Identity;
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
    private final String userName;

    @Inject
    public CachedDataManager(HetuConfig hetuConfig,
                             CacheStorageMonitor monitor,
                             Metadata metadata,
                             QueryIdGenerator queryIdGenerator,
                             SessionPropertyManager sessionPropertyManager)
    {
        if (hetuConfig.isExecutionDataCacheEnabled()) {
            Session.SessionBuilder sessionBuilder = Session.builder(sessionPropertyManager)
                    .setIdentity(new Identity(hetuConfig.getCachingUserName(), Optional.empty()))
                    .setSource("auto-vacuum");
            RemovalListener<CachedDataKey, CachedDataStorage> listener = new RemovalListener<CachedDataKey, CachedDataStorage>()
            {
                @Override
                public void onRemoval(RemovalNotification<CachedDataKey, CachedDataStorage> notification)
                {
                    if (notification.wasEvicted()) {
                        LOG.info("CTE Materialized entry evicted, Cause: %s", notification.getCause().name());
                        monitor.stopTableMonitorForModification(notification.getValue(),
                                sessionBuilder.setQueryId(queryIdGenerator.createNextQueryId()).build());
                    }
                }
            };

            this.dataCache = Optional.of(CacheBuilder.newBuilder()
                    .maximumWeight(hetuConfig.getExecutionDataCacheMaxSize())
                    .weigher(new Weigher<CachedDataKey, CachedDataStorage>() {
                        @Override
                        public int weigh(CachedDataKey key, CachedDataStorage value)
                        {
                            return (int) value.getDataSize();
                        }
                    })
                    .removalListener(listener)
                    .build());
        }
        else {
            this.dataCache = Optional.empty();
        }

        this.monitor = requireNonNull(monitor, "monitor is null");
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.userName = requireNonNull(hetuConfig, "hetuConfig is null").getCachingUserName();
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
            /* Increment listener count */
            object.grab();
            return object;
        }

        return null;
    }

    public void done(CachedDataKey dataKey, long cdsTime)
    {
        if (!dataCache.isPresent()) {
            return;
        }

        CachedDataStorage object = get(dataKey);
        if (object != null && object.getCreateTime() == cdsTime) {
            /* decrement listener count */
            object.release();
        }
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
        if (object.getRefCount() <= 0) {
            // mark for dropping when reference count reduce
            Identity identity = session.getIdentity();
            identity = new Identity(userName, identity.getGroups(), identity.getPrincipal(), identity.getRoles(), identity.getExtraCredentials());
            Session newSession = session.withUpdatedIdentity(identity);
            Optional<TableHandle> tableHandle = metadata.getTableHandle(newSession, QualifiedObjectName.valueOf(object.getDataTable()));
            if (tableHandle.isPresent()) {
                metadata.dropTable(newSession, tableHandle.get());
            }
            invalidate(ImmutableSet.of(key), session);
        }
        return false;
    }

    public void put(CachedDataKey key, CachedDataStorage value, Session session)
    {
        if (dataCache.isPresent()) {
            monitor.monitorTableForModification(value, session);
            value.grab();
            dataCache.get().put(key, value);
        }
    }

    public void invalidate(Set<CachedDataKey> keySet, Session session)
    {
        if (dataCache.isPresent()) { /* Add invalidators for storage as cacheWalker */
            cacheWalk(keySet, ((key, cds) -> {
                monitor.stopTableMonitorForModification(cds, session);
                return null;
            }));
            dataCache.get().invalidateAll(keySet);
        }
    }

    public void invalidateAll()
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
