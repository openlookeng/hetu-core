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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cache.elements.CachedDataKey;
import io.prestosql.cache.elements.CachedDataStorage;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.security.Identity;
import io.prestosql.utils.HetuConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class CacheStorageMonitor
{
    private static final Logger LOG = Logger.get(CacheStorageMonitor.class);

    private final Metadata metadata;
    private final Map<CachedDataStorage.TableInfo, List<CachedDataKey>> monitoredTables = new ConcurrentHashMap<>();
    private final AtomicBoolean tableCount = new AtomicBoolean();
    private final String userName;

    @Inject
    public CacheStorageMonitor(HetuConfig hetuConfig, Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.userName = requireNonNull(hetuConfig, "hetuConfig is null").getCachingUserName();
    }

    public boolean checkTableValidity(CachedDataStorage cachedDataStorage, Session session)
    {
        Session newSession = updateIdentity(session);
        AtomicBoolean found = new AtomicBoolean(false);
        cachedDataStorage.getTableInfoMap().entrySet().forEach(es -> {
            TableHandle age = es.getValue().getTableHandle();
            if (metadata.isTableModified(newSession, age)) {
                found.set(true);
            }
        });
        return !found.get();
    }

    public void monitorTableForModification(CachedDataStorage cachedDataStorage, Session session)
    {
        Session newSession = updateIdentity(session);
        cachedDataStorage.getTableInfoMap().entrySet().forEach(es -> {
            Optional<TableHandle> tableHandle = metadata.getTableHandle(newSession, QualifiedObjectName.valueOf(es.getValue().getLocation()));
            if (tableHandle.isPresent()) {
                TableHandle th = metadata.watchTableForModifications(newSession, tableHandle.get());
                es.getValue().setTableHandle(th);
            }
        });
    }

    public void stopTableMonitorForModification(CachedDataStorage cachedDataStorage, Session session)
    {
        Session newSession = updateIdentity(session);
        cachedDataStorage.getTableInfoMap().entrySet().forEach(es -> {
            Optional<TableHandle> tableHandle = metadata.getTableHandle(newSession, QualifiedObjectName.valueOf(es.getValue().getLocation()));
            if (tableHandle.isPresent()) {
                metadata.unwatchTableForModifications(newSession, tableHandle.get());
            }
        });
    }

    public Session updateIdentity(Session session)
    {
        Identity identity = session.getIdentity();
        identity = new Identity(userName, identity.getGroups(), identity.getPrincipal(), identity.getRoles(), identity.getExtraCredentials());
        return session.withUpdatedIdentity(identity);
    }
}
