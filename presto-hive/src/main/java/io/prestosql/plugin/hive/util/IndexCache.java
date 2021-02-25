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
package io.prestosql.plugin.hive.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexNotCreatedException;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.HetuConstant.KILOBYTE;

public class IndexCache
{
    private static final Logger LOG = Logger.get(IndexCache.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Hive-IndexCache-pool-%d").setDaemon(true).build();
    protected static final List<String> INDEX_TYPES = ImmutableList.of("MINMAX", "BLOOM", "BITMAP");

    private static ScheduledExecutorService executor;

    private Long loadDelay; // in millisecond
    private LoadingCache<IndexCacheKey, List<IndexMetadata>> cache;
    private List<IndexRecord> indexRecords;

    @Inject
    public IndexCache(CacheLoader loader, IndexClient indexClient)
    {
        // If the static variables have not been initialized
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            loadDelay = PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY).toMillis();
            // in millisecond
            long refreshRate = Math.max(loadDelay / 2, 5000L);
            int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), PropertyService.getLongProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS).intValue());
            executor = Executors.newScheduledThreadPool(numThreads, threadFactory);
            CacheBuilder<IndexCacheKey, List<IndexMetadata>> cacheBuilder = CacheBuilder.newBuilder()
                    .removalListener(e -> ((List<IndexMetadata>) e.getValue()).forEach(i -> {
                        try {
                            i.getIndex().close();
                        }
                        catch (IOException ioException) {
                            LOG.debug(ioException, "Failed to close index " + i);
                        }
                    }))
                    .expireAfterWrite(PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_TTL).toMillis(), TimeUnit.MILLISECONDS)
                    .maximumWeight(PropertyService.getLongProperty(HetuConstant.FILTER_CACHE_MAX_MEMORY))
                    .weigher((indexCacheKey, indices) -> {
                        int memorySize = 0;
                        for (IndexMetadata indexMetadata : indices) {
                            // HetuConstant.FILTER_CACHE_MAX_MEMORY is set in KBs
                            // convert index size to KB
                            memorySize += (indexMetadata.getIndex().getMemoryUsage() / KILOBYTE);
                        }
                        return memorySize;
                    });
            if (PropertyService.getBooleanProperty(HetuConstant.FILTER_CACHE_SOFT_REFERENCE)) {
                cacheBuilder.softValues();
            }
            executor.scheduleAtFixedRate(() -> {
                try {
                    if (cache.size() > 0) {
                        // only refresh cache is it's not empty
                        List<IndexRecord> newRecords = indexClient.getAllIndexRecords();

                        if (indexRecords != null) {
                            for (IndexRecord old : indexRecords) {
                                boolean found = false;
                                for (IndexRecord now : newRecords) {
                                    if (now.name.equals(old.name)) {
                                        found = true;
                                        if (now.lastModifiedTime != old.lastModifiedTime) {
                                            // index record has been updated. evict
                                            evictFromCache(old);
                                            LOG.debug("Index for {%s} has been evicted from cache because the index has been updated.", old);
                                        }
                                    }
                                }
                                // old record is gone. evict from cache
                                if (!found) {
                                    evictFromCache(old);
                                    LOG.debug("Index for {%s} has been evicted from cache because the index has been dropped.", old);
                                }
                            }
                        }

                        indexRecords = newRecords;
                    }
                }
                catch (Exception e) {
                    LOG.debug(e, "Error using index records to refresh cache");
                }
            }, loadDelay, refreshRate, TimeUnit.MILLISECONDS);
            cache = cacheBuilder.build(loader);
        }
    }

    // Override the loadDelay, for testing
    public IndexCache(CacheLoader<IndexCacheKey, List<IndexMetadata>> loader, Long loadDelay, IndexClient indexClient)
    {
        this(loader, indexClient);
        this.loadDelay = loadDelay;
    }

    public List<IndexMetadata> getIndices(String catalog, String table, HiveSplit hiveSplit, TupleDomain<HiveColumnHandle> effectivePredicate, List<HiveColumnHandle> partitions)
    {
        if (cache == null || catalog == null || table == null || hiveSplit == null || effectivePredicate == null) {
            return Collections.emptyList();
        }

        long lastModifiedTime = hiveSplit.getLastModifiedTime();
        Path path = new Path(hiveSplit.getPath());

        URI pathUri = URI.create(path.toString().replaceAll(" ", "%20"));
        String tableFqn = catalog + "." + table;

        // for each split, load indexes for each predicate (if the predicate contains an indexed column)
        List<IndexMetadata> splitIndexes = new LinkedList<>();
        effectivePredicate.getDomains().get().keySet().stream()
                // if the domain column is a partition column, skip it
                .filter(key -> partitions == null || !partitions.contains(key))
                .map(HiveColumnHandle::getName)
                .map(String::toLowerCase)
                .forEach(column -> {
                    // security check required before using values in a Path
                    // e.g. catalog.schema.table or dc.catalog.schema.table
                    if (!tableFqn.matches("([\\p{Alnum}_]+\\.){2,3}[\\p{Alnum}_]+")) {
                        LOG.warn("Invalid table name " + tableFqn);
                        return;
                    }

                    if (!column.matches("[\\p{Alnum}_]+")) {
                        LOG.warn("Invalid column name " + column);
                        return;
                    }

                    for (String indexType : INDEX_TYPES) {
                        String indexCacheKeyPath = Paths.get(tableFqn, column, indexType, pathUri.getRawPath()).toString();
                        IndexCacheKey indexCacheKey = new IndexCacheKey(indexCacheKeyPath, lastModifiedTime);
                        // check if cache contains the key
                        List<IndexMetadata> predicateIndexes = cache.getIfPresent(indexCacheKey);

                        // if cache didn't contain the key, it has not been loaded, load it asynchronously
                        if (predicateIndexes == null) {
                            executor.schedule(() -> {
                                try {
                                    cache.get(indexCacheKey);
                                    LOG.debug("Loaded index for %s.", indexCacheKeyPath);
                                }
                                catch (ExecutionException e) {
                                    if (e.getCause() instanceof IndexNotCreatedException) {
                                        // Do nothing. Index not registered.
                                    }
                                    else if (LOG.isDebugEnabled()) {
                                        LOG.debug(e, "Unable to load index for %s. ", indexCacheKeyPath);
                                    }
                                }
                            }, loadDelay, TimeUnit.MILLISECONDS);
                        }
                        else {
                            // if key was present in cache, we still need to check if the index is validate based on the lastModifiedTime
                            // the index is only valid if the lastModifiedTime of the split matches the index's lastModifiedTime
                            for (IndexMetadata index : predicateIndexes) {
                                if (index.getLastModifiedTime() != lastModifiedTime) {
                                    cache.invalidate(indexCacheKey);
                                    predicateIndexes = Collections.emptyList();
                                    break;
                                }
                            }

                            // cache contained the key
                            splitIndexes.addAll(predicateIndexes);
                        }
                    }
                });

        return splitIndexes;
    }

    @VisibleForTesting
    protected long getCacheSize()
    {
        return cache.size();
    }

    private void evictFromCache(IndexRecord record)
    {
        String recordInCacheKey = String.format("%s/%s/%s", record.qualifiedTable, String.join(",", record.columns), record.indexType);
        for (IndexCacheKey key : cache.asMap().keySet()) {
            if (key.getPath().startsWith(recordInCacheKey)) {
                cache.invalidate(key);
            }
        }
    }
}
