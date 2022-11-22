/*
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
package io.prestosql.plugin.hive;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.SchemaTableName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class CachingDirectoryLister
        implements DirectoryLister
{
    private final Cache<Path, List<PrestoFileStatus>> cache;
    private final Set<SchemaTableName> tableNames;

    @Inject
    public CachingDirectoryLister(HiveConfig hiveClientConfig)
    {
        this(hiveClientConfig.getFileStatusCacheExpireAfterWrite(), hiveClientConfig.getFileStatusCacheMaxSize(), hiveClientConfig.getFileStatusCacheTables());
    }

    public CachingDirectoryLister(Duration expireAfterWrite, long maxSize, List<String> tables)
    {
        this.cache = CacheBuilder.newBuilder()
                .maximumWeight(maxSize)
                .weigher((Weigher<Path, List<PrestoFileStatus>>) (key, value) -> value.size())
                .expireAfterWrite(expireAfterWrite.toMillis(), TimeUnit.MILLISECONDS)
                .recordStats()
                .build();
        this.tableNames = tables.stream()
                .map(CachingDirectoryLister::parseTableName)
                .collect(Collectors.toSet());
    }

    private static SchemaTableName parseTableName(String tableName)
    {
        String[] parts = tableName.split("\\.");
        checkArgument(parts.length == 2, "Invalid schemaTableName: %s", tableName);
        return new SchemaTableName(parts[0], parts[1]);
    }

    @Override
    public RemoteIterator<PrestoFileStatus> list(FileSystem fs, Table table, Path path)
            throws IOException
    {
        List<PrestoFileStatus> files = cache.getIfPresent(path);
        if (files != null) {
            return simpleRemoteIterator(files);
        }
        RemoteIterator<PrestoFileStatus> iterator = new PrestoFileStatusRemoteIterator(fs.listLocatedStatus(path));

        if (!tableNames.contains(table.getSchemaTableName())) {
            return iterator;
        }
        return cachingRemoteIterator(iterator, path);
    }

    private RemoteIterator<PrestoFileStatus> cachingRemoteIterator(RemoteIterator<PrestoFileStatus> iterator, Path path)
    {
        return new RemoteIterator<PrestoFileStatus>()
        {
            private final List<PrestoFileStatus> files = new ArrayList<>();

            @Override
            public boolean hasNext()
                    throws IOException
            {
                boolean hasNext = iterator.hasNext();
                if (!hasNext) {
                    cache.put(path, ImmutableList.copyOf(files));
                }
                return hasNext;
            }

            @Override
            public PrestoFileStatus next()
                    throws IOException
            {
                PrestoFileStatus next = iterator.next();
                files.add(next);
                return next;
            }
        };
    }

    private static RemoteIterator<PrestoFileStatus> simpleRemoteIterator(List<PrestoFileStatus> files)
    {
        return new RemoteIterator<PrestoFileStatus>()
        {
            private final Iterator<PrestoFileStatus> iterator = ImmutableList.copyOf(files).iterator();

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }

            @Override
            public PrestoFileStatus next()
            {
                return iterator.next();
            }
        };
    }

    @Managed
    public void flushCache()
    {
        cache.invalidateAll();
    }

    @Managed
    public Double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }
}
