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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.log.Logger;
import io.hetu.core.common.heuristicindex.IndexCacheKey;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.service.PropertyService;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class IndexCache
{
    private static final Logger LOG = Logger.get(IndexCache.class);
    private static final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Main-IndexCache-pool-%d").setDaemon(true).build();

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
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_TTL).toMillis(), TimeUnit.MILLISECONDS)
                    .maximumSize(PropertyService.getLongProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE))
                    .build(loader);
        }
    }

    public List<IndexMetadata> getIndices(String table, String column, Split split)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        URI splitUri = URI.create(split.getConnectorSplit().getFilePath());
        String filterKeyPath = getCacheKey(table, column, splitUri.getPath());
        long lastModifiedTime = split.getConnectorSplit().getLastModifiedTime();
        IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, "MINMAX", "BLOOM");
        //it is possible to return multiple SplitIndexMetadata due to the range mismatch, especially in the case
        //where the split has a wider range than the original splits used for index creation
        // check if cache contains the key
        List<IndexMetadata> indices;

        // if cache didn't contain the key, it has not been loaded, load it asynchronously
        indices = cache.getIfPresent(filterKey);

        if (indices == null) {
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

        if (indices != null) {
            // if key was present in cache, we still need to check if the index is validate based on the lastModifiedTime
            // the index is only valid if the lastModifiedTime of the split matches the index's lastModifiedTime
            for (IndexMetadata index : indices) {
                if (index.getLastUpdated() != lastModifiedTime) {
                    cache.invalidate(filterKey);
                    indices = Collections.emptyList();
                    break;
                }
            }
        }

        return indices == null ? Collections.emptyList() : indices;
    }

    private String getCacheKey(String tableName, String columnName, String filePath)
    {
        return tableName + "/" + columnName + filePath;
    }
}
