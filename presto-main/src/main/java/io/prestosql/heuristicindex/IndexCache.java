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
package io.prestosql.heuristicindex;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.hetu.core.common.heuristicindex.IndexCacheKey;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexNotCreatedException;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.service.PropertyService;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.HetuConstant.KILOBYTE;

public class IndexCache
{
    private static final Logger LOG = Logger.get(IndexCache.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Main-IndexCache-pool-%d").setDaemon(true).build();
    protected static final List<String> INDEX_TYPES = ImmutableList.of("BLOOM", "MINMAX");

    private static ScheduledExecutorService executor;

    private Long loadDelay; // in millisecond
    private LoadingCache<IndexCacheKey, List<IndexMetadata>> cache;
    private List<IndexRecord> indexRecords;

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
                    .removalListener(e -> ((List<IndexMetadata>) e.getValue()).stream().forEach(i -> {
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

    public List<IndexMetadata> getIndices(String table, String column, Split split)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        URI splitUri = URI.create(split.getConnectorSplit().getFilePath().replaceAll(" ", "%20"));
        long lastModifiedTime = split.getConnectorSplit().getLastModifiedTime();
        List<IndexMetadata> indices = new LinkedList<>();

        for (String indexType : INDEX_TYPES) {
            String filterKeyPath = table + "/" + column + "/" + indexType + splitUri.getRawPath();
            IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime);
            //it is possible to return multiple SplitIndexMetadata due to the range mismatch, especially in the case
            //where the split has a wider range than the original splits used for index creation
            // check if cache contains the key
            List<IndexMetadata> indexOfThisType;

            // if cache didn't contain the key, it has not been loaded, load it asynchronously
            indexOfThisType = cache.getIfPresent(filterKey);

            if (indexOfThisType == null) {
                executor.schedule(() -> {
                    try {
                        cache.get(filterKey);
                        LOG.debug("Loaded index for %s.", filterKey);
                    }
                    catch (ExecutionException e) {
                        if (e.getCause() instanceof IndexNotCreatedException) {
                            // Do nothing. Index not registered.
                        }
                        else if (LOG.isDebugEnabled()) {
                            LOG.debug(e, "Unable to load index for %s. ", filterKey);
                        }
                    }
                }, loadDelay, TimeUnit.MILLISECONDS);
            }

            if (indexOfThisType != null) {
                // if key was present in cache, we still need to check if the index is validate based on the lastModifiedTime
                // the index is only valid if the lastModifiedTime of the split matches the index's lastModifiedTime
                for (IndexMetadata index : indexOfThisType) {
                    if (index.getLastModifiedTime() != lastModifiedTime) {
                        cache.invalidate(filterKey);
                        indexOfThisType = Collections.emptyList();
                        break;
                    }
                }
                indices.addAll(indexOfThisType);
            }
        }

        return indices;
    }

    public List<IndexMetadata> getIndices(String table, String column, String indexType, Set<String> partitions, long lastModifiedTime)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        List<IndexMetadata> indices = new LinkedList<>();
        if (!partitions.isEmpty()) {
            for (String partition : partitions) {
                String filterKeyPath = table + "/" + column + "/" + indexType + "/" + partition;
                IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, Index.Level.PARTITION.name());
                List<IndexMetadata> result = loadIndex(filterKey);
                if (result != null) {
                    indices.addAll(result);
                }
            }

            if (!indices.isEmpty()) {
                // if partition-level index exists, return. otherwise keep loading table-level index
                return indices;
            }
        }

        String filterKeyPath = table + "/" + column + "/" + indexType;
        IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, Index.Level.PARTITION.name());
        List<IndexMetadata> result = loadIndex(filterKey);
        if (result != null) {
            indices.addAll(result);
        }

        return indices;
    }

    private List<IndexMetadata> loadIndex(IndexCacheKey cacheKey)
    {
        //it is possible to return multiple SplitIndexMetadata due to the range mismatch, especially in the case
        //where the split has a wider range than the original splits used for index creation
        // check if cache contains the key
        List<IndexMetadata> partitionIndexList;

        // if cache didn't contain the key, it has not been loaded, load it asynchronously
        partitionIndexList = cache.getIfPresent(cacheKey);

        if (partitionIndexList == null) {
            executor.schedule(() -> {
                try {
                    cache.get(cacheKey);
                    LOG.debug("Loaded index for %s.", cacheKey);
                }
                catch (ExecutionException e) {
                    if (e.getCause() instanceof IndexNotCreatedException) {
                        // Do nothing. Index not registered.
                    }
                    else if (LOG.isDebugEnabled()) {
                        LOG.debug(e, "Unable to load index for %s. ", cacheKey);
                    }
                }
            }, loadDelay, TimeUnit.MILLISECONDS);
        }
        return partitionIndexList;
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
