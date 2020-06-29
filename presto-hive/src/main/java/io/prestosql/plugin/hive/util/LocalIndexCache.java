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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.service.PropertyService;
import org.apache.hadoop.fs.Path;

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

public class LocalIndexCache
        implements IndexCache
{
    private static final Logger LOG = Logger.get(LocalIndexCache.class);

    protected static final long DEFAULT_LOAD_DELAY = 5000;

    private LoadingCache<IndexCacheKey, List<SplitIndexMetadata>> cache;

    private ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("Hive-LocalIndexCache-pool-%d").setDaemon(true).build();
    private ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, threadFactory);

    private long loadDelay;

    public LocalIndexCache(CacheLoader loader, long loadDelay)
    {
        this.loadDelay = loadDelay;
        if (cache == null && PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            cache = CacheBuilder.newBuilder()
                    .expireAfterWrite(10, TimeUnit.MINUTES)
                    .maximumSize(PropertyService.getLongProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE))
                    .build(loader);
        }
    }

    @Inject
    public LocalIndexCache(CacheLoader loader)
    {
        this(loader, DEFAULT_LOAD_DELAY);
    }

    public List<SplitIndexMetadata> getIndices(String catalog, String table, HiveSplit hiveSplit, TupleDomain<HiveColumnHandle> effectivePredicate, List<HiveColumnHandle> partitions)
    {
        if (cache == null || catalog == null || table == null || hiveSplit == null || effectivePredicate == null) {
            return Collections.emptyList();
        }

        long lastModifiedTime = hiveSplit.getLastModifiedTime();
        Path path = new Path(hiveSplit.getPath());

        URI pathUri = URI.create(path.toString());
        String tableFqn = String.format("%s.%s", catalog, table);

        // for each split, load indexes for each predicate (if the predicate contains an indexed column)
        List<SplitIndexMetadata> splitIndexes = new LinkedList<>();
        effectivePredicate.getDomains().get().keySet().stream()
                    // if the domain column is a partition column, skip it
                    .filter(key -> partitions == null || !partitions.contains(key))
                    .map(HiveColumnHandle::getName)
                    .map(String::toLowerCase).forEach(column -> {
                        String indexCacheKeyPath = Paths.get(tableFqn, column, pathUri.getPath()).toString();
                        IndexCacheKey indexCacheKey = new IndexCacheKey(indexCacheKeyPath, lastModifiedTime, "bitmap");
                        // check if cache contains the key
                        List<SplitIndexMetadata> predicateIndexes = cache.getIfPresent(indexCacheKey);

                        // if cache didn't contain the key, it has not been loaded, load it asynchronously
                        if (predicateIndexes == null) {
                            executor.schedule(() -> {
                                try {
                                    cache.get(indexCacheKey);
                                    LOG.debug("Loaded index for %s.", indexCacheKeyPath);
                                }
                                catch (ExecutionException e) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Unable to load index for %s. %s", indexCacheKeyPath, e.getLocalizedMessage());
                                    }
                                }
                            }, loadDelay, TimeUnit.MILLISECONDS);
                        }
                        else {
                            // if key was present in cache, we still need to check if the index is validate based on the lastModifiedTime
                            // the index is only valid if the lastModifiedTime of the split matches the index's lastModifiedTime
                            for (SplitIndexMetadata index : predicateIndexes) {
                                if (index.getLastUpdated() != lastModifiedTime) {
                                    cache.invalidate(indexCacheKey);
                                    predicateIndexes = Collections.emptyList();
                                    break;
                                }
                            }

                            // cache contained the key
                            if (predicateIndexes != null) {
                                splitIndexes.addAll(predicateIndexes);
                            }
                        }
                    });

        return splitIndexes;
    }
}
