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
import com.google.common.cache.Weigher;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.hetu.core.common.heuristicindex.IndexCacheKey;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.service.PropertyService;

import java.net.URI;
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
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Main-IndexCache-pool-%d").setDaemon(true).build();
    private static final List<String> INDEX_TYPES = ImmutableList.of("bloom", "minmax");

    private static ScheduledExecutorService executor;

    private Long loadDelay; // in millisecond
    private LoadingCache<IndexCacheKey, List<IndexMetadata>> cache;

    public IndexCache(CacheLoader loader)
    {
        // If the static variables have not been initialized
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            loadDelay = PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY).toMillis();
            int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), PropertyService.getLongProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS).intValue());
            executor = Executors.newScheduledThreadPool(numThreads, threadFactory);
            CacheBuilder cacheBuilder = CacheBuilder.newBuilder()
                    .expireAfterWrite(PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_TTL).toMillis(), TimeUnit.MILLISECONDS)
                    .maximumWeight(PropertyService.getLongProperty(HetuConstant.FILTER_CACHE_MAX_MEMORY))
                    .weigher((Weigher<IndexCacheKey, List<IndexMetadata>>) (indexCacheKey, indices) -> {
                        int memorySize = 0;
                        for (IndexMetadata indexMetadata : indices) {
                            // HetuConstant.FILTER_CACHE_MAX_MEMORY is set in KBs
                            // convert index size to KB
                            memorySize += (indexMetadata.getIndex().getMemorySize() / KILOBYTE);
                        }
                        return memorySize;
                    });
            if (PropertyService.getBooleanProperty(HetuConstant.FILTER_CACHE_SOFT_REFERENCE)) {
                cacheBuilder.softValues();
            }
            cache = cacheBuilder.build(loader);
        }
    }

    public List<IndexMetadata> getIndices(String table, String column, Split split)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        URI splitUri = URI.create(split.getConnectorSplit().getFilePath());
        long lastModifiedTime = split.getConnectorSplit().getLastModifiedTime();
        List<IndexMetadata> indices = new LinkedList<>();

        for (String indexType : INDEX_TYPES) {
            String filterKeyPath = table + "/" + column + "/" + indexType + splitUri.getPath();
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
                        if (LOG.isDebugEnabled()) {
                            LOG.debug(e, "Unable to load index for %s. ", filterKey);
                        }
                    }
                }, loadDelay, TimeUnit.MILLISECONDS);
            }

            if (indexOfThisType != null) {
                // if key was present in cache, we still need to check if the index is validate based on the lastModifiedTime
                // the index is only valid if the lastModifiedTime of the split matches the index's lastModifiedTime
                for (IndexMetadata index : indexOfThisType) {
                    if (index.getLastUpdated() != lastModifiedTime) {
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

    @VisibleForTesting
    protected long getCacheSize()
    {
        return cache.size();
    }
}
