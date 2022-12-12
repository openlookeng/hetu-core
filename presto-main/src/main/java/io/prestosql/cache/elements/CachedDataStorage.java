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
package io.prestosql.cache.elements;

import io.prestosql.spi.connector.CatalogSchemaTableName;
import io.prestosql.spi.metadata.TableHandle;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.System.currentTimeMillis;

public class CachedDataStorage
{
    private final CachedDataKey identifier;

    public class TableInfo
    {
        private final String location;
        private final long age;
        private final long dataAge;

        private TableHandle tableHandle;

        public TableInfo(String location, long age, long dataAge)
        {
            this.location = location;
            this.age = age;
            this.dataAge = dataAge;
        }

        public long getAge()
        {
            return age;
        }

        public long getDataAge()
        {
            return dataAge;
        }

        public String getLocation()
        {
            return location;
        }

        public TableHandle getTableHandle()
        {
            return tableHandle;
        }

        public void setTableHandle(TableHandle tableHandle)
        {
            this.tableHandle = tableHandle;
        }
    }

    private final CatalogSchemaTableName dataTable;
    private final long createTime = currentTimeMillis();
    private final Map<String, TableInfo> tableInfoMap;

    private long commitTime;
    private long abortTime;
    private long lastAccessTime;
    private long dataSize;
    private long runtime;
    private boolean isNonCachable;
    private AtomicInteger refCount = new AtomicInteger(0);
    private AtomicBoolean isCommitted = new AtomicBoolean(false);
    private AtomicBoolean inProgress = new AtomicBoolean(true);

    Function<Void, Void> commitActions;
    Function<Void, Void> abortActions;

    public CachedDataStorage(CachedDataKey identifier, CatalogSchemaTableName dataTable, Function<Void, Void> commit, Function<Void, Void> abort)
    {
        this.identifier = identifier;
        this.dataTable = dataTable;
        this.commitActions = commit;
        this.abortActions = abort;

        this.tableInfoMap = identifier.getTables()
                .stream()
                .collect(toImmutableMap(s -> s, v -> new TableInfo(v, 0, 0)));
    }

    public CachedDataKey getIdentifier()
    {
        return identifier;
    }

    public synchronized void commit(long runtime, long dataSize)
    {
        checkArgument(!isCommitted.get(), "result is already committed.");
        commitTime = currentTimeMillis();
        isCommitted.compareAndSet(false, true);
        inProgress.compareAndSet(true, false);
        this.runtime = runtime - createTime;
        this.dataSize = dataSize;
        if (commitActions != null) {
            this.commitActions.apply(null);
        }
    }

    public synchronized void abort()
    {
        checkArgument(!isCommitted.get(), "result is already committed.");
        abortTime = currentTimeMillis();
        inProgress.compareAndSet(true, false);
        if (abortActions != null) {
            this.abortActions.apply(null);
        }
    }

    public synchronized void reset()
    {
        isCommitted.compareAndSet(true, false);
        inProgress.compareAndSet(false, true);
    }

    public boolean isNonCachable()
    {
        return isNonCachable;
    }

    public synchronized void setNonCachable(boolean nonCachable)
    {
        isNonCachable = nonCachable;
        inProgress.set(false);
        isCommitted.set(false);
    }

    public boolean isCommitted()
    {
        return isCommitted.get();
    }

    public int getRefCount()
    {
        return refCount.get();
    }

    public boolean inProgress()
    {
        return inProgress.get();
    }

    public long getDataSize()
    {
        return dataSize;
    }

    public CatalogSchemaTableName getDataTable()
    {
        return dataTable;
    }

    public long getCreateTime()
    {
        return createTime;
    }

    public Map<String, TableInfo> getTableInfoMap()
    {
        return tableInfoMap;
    }

    public long getCommitTime()
    {
        return commitTime;
    }

    public long getLastAccessTime()
    {
        return lastAccessTime;
    }

    public long getRuntime()
    {
        return runtime;
    }

    public int grab()
    {
        this.lastAccessTime = currentTimeMillis();
        return this.refCount.incrementAndGet();
    }

    public int release()
    {
        return this.refCount.decrementAndGet();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(dataTable, dataSize, createTime, tableInfoMap, commitTime, runtime, lastAccessTime);
    }

    @Override
    public boolean equals(Object obj)
    {
        return super.equals(obj);
    }

    @Override
    public String toString()
    {
        return super.toString();
    }
}
