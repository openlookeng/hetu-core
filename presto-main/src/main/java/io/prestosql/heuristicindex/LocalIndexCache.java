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
import com.google.common.util.concurrent.UncheckedExecutionException;
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

public class LocalIndexCache
{
    private static final Logger LOG = Logger.get(LocalIndexCache.class);
    private final int loadDelay = 5 * 1000; // ms

    private boolean loadAsync = true;
    private static LoadingCache<IndexCacheKey, List<IndexMetadata>> cache;

    private ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Main-LocalIndexCache-pool-%d").setDaemon(true).build();
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, threadFactory);

    public LocalIndexCache(CacheLoader loader)
    {
        this(loader, true);
    }

    public LocalIndexCache(CacheLoader loader, boolean loadAsync)
    {
        if (cache == null && PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .maximumSize(PropertyService.getLongProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE))
                    .build(loader);
        }

        this.loadAsync = loadAsync;
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
        if (loadAsync) {
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
                            LOG.debug("Unable to load index for %s. %s", filterKey, e.getLocalizedMessage());
                        }
                    }
                }, loadDelay, TimeUnit.MILLISECONDS);
            }
        }
        else {
            try {
                indices = cache.get(filterKey);
                LOG.debug("Loaded index for %s.", filterKey);
            }
            catch (UncheckedExecutionException | ExecutionException e) {
                indices = null;
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Unable to load index for %s. %s", filterKey, e.getLocalizedMessage());
                }
            }
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
        return String.format("%s/%s%s", tableName, columnName, filePath);
    }
}
