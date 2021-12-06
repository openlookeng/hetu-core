/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.units.Duration;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.IndexNotCreatedException;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.service.PropertyService;
import org.eclipse.jetty.util.URIUtil;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static io.prestosql.spi.HetuConstant.KILOBYTE;
import static io.prestosql.spi.heuristicindex.IndexCacheKey.LAST_MODIFIED_TIME_PLACE_HOLDER;

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
        this(loader, indexClient, true);
    }

    /**
     * during some testing we don't want auto refresh occurring
     */
    @VisibleForTesting
    protected IndexCache(CacheLoader loader, IndexClient indexClient, boolean autoRefreshEnabled)
    {
        // If the static variables have not been initialized
        if (PropertyService.getBooleanProperty(HetuConstant.FILTER_ENABLED)) {
            loadDelay = PropertyService.getDurationProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY).toMillis();
            // in millisecond
            long refreshRate = Math.max(loadDelay / 2, 5000L);
            int numThreads = Math.min(Runtime.getRuntime().availableProcessors(), PropertyService.getLongProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS).intValue());
            if (executor == null) {
                executor = Executors.newScheduledThreadPool(numThreads, threadFactory);
            }
            CacheBuilder<IndexCacheKey, List<IndexMetadata>> cacheBuilder = CacheBuilder.newBuilder()
                    .removalListener(e -> {
                        try {
                            if (!((IndexCacheKey) e.getKey()).skipCloseIndex()) {
                                for (IndexMetadata i : ((List<IndexMetadata>) e.getValue())) {
                                    i.getIndex().close();
                                }
                            }
                        }
                        catch (IOException ioException) {
                            LOG.debug(ioException, "Failed to close index:", e);
                        }
                    })
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

            if (autoRefreshEnabled) {
                executor.scheduleAtFixedRate(() -> {
                    // This thread automatically keep the cache updated every 5 secs in the background.
                    try {
                        List<IndexRecord> newRecords = indexClient.getAllIndexRecords();
                        boolean success = autoUpdateCache(newRecords);
                        if (success) {
                            LOG.debug("Cache refreshed");
                        }
                    }
                    catch (Exception e) {
                        LOG.debug(e, "Error using index records to refresh cache");
                    }
                }, loadDelay, refreshRate, TimeUnit.MILLISECONDS);
            }

            cache = cacheBuilder.build(loader);
        }
    }

    // only called on "SHOW INDEX" statement
    public void readUsage(HashMap<IndexRecord, Long> indexRecordMemoryUse, HashMap<IndexRecord, Long> indexRecordDiskUse)
    {
        try {
            for (Map.Entry<IndexCacheKey, List<IndexMetadata>> entry : cache.asMap().entrySet()) {
                IndexRecord curRecord = entry.getKey().getRecord();
                if (indexRecordMemoryUse.containsKey(curRecord)) {
                    for (IndexMetadata indexMetadata : entry.getValue()) {
                        indexRecordMemoryUse.put(curRecord, indexRecordMemoryUse.get(curRecord) + indexMetadata.getIndex().getMemoryUsage());
                        indexRecordDiskUse.put(curRecord, indexRecordDiskUse.get(curRecord) + indexMetadata.getIndex().getDiskUsage());
                    }
                }
            }
        }
        catch (Exception e) {
            LOG.debug(e, "Error updating memory or disk usage");
        }
    }

    private boolean autoUpdateCache(List<IndexRecord> newRecords)
    {
        // Three cases are checked:
        // 1. if new index record is created, add it to cache
        // 2. if the index in cache is updated, update the cache
        // 3. if the index in cache is outdated, evict it from cache

        if (newRecords == null && indexRecords == null) {
            return false;
        }

        HashMap<String, Long> oldIndexMap = new HashMap<>();
        if (indexRecords != null) {
            for (IndexRecord oldIndexRecord : indexRecords) {
                oldIndexMap.put(oldIndexRecord.name, oldIndexRecord.lastModifiedTime);
            }
        }
        boolean dropped = false;
        boolean created = false;
        boolean updated = false;
        HashMap<String, Long> newIndexMap = new HashMap<>();
        if (newRecords != null) {
            for (IndexRecord newIndexRecord : newRecords) {
                newIndexMap.put(newIndexRecord.name, newIndexRecord.lastModifiedTime);
                if (oldIndexMap.containsKey(newIndexRecord.name)) {
                    if (oldIndexMap.get(newIndexRecord.name) != newIndexRecord.lastModifiedTime) {
                        // update operation
                        updated = true;
                        evictFromCache(newIndexRecord);
                        if (newIndexRecord.isAutoloadEnabled()) {
                            LOG.debug("Index %s was updated: reloading to cache...", newIndexRecord.name);
                            Duration timeElapsed = loadIndexToCache(newIndexRecord);
                            LOG.debug("Index %s was reloaded to cache. (Time elapsed: %s)", newIndexRecord.name, timeElapsed.toString());
                        }
                    }
                }
                else {
                    // create operation
                    created = true;
                    if (newIndexRecord.isAutoloadEnabled()) {
                        LOG.debug("New index %s was created: loading to cache...", newIndexRecord.name);
                        Duration timeElapsed = loadIndexToCache(newIndexRecord);
                        LOG.debug("New index %s was loaded to cache. (Time elapsed: %s)", newIndexRecord.name, timeElapsed.toString());
                    }
                }
            }
        }

        if (indexRecords != null) {
            for (IndexRecord oldIndexRecord : indexRecords) {
                if (!newIndexMap.containsKey(oldIndexRecord.name)) {
                    // drop operation
                    dropped = true;
                    evictFromCache(oldIndexRecord);
                    LOG.debug("Index %s was dropped: evicting from cache.", oldIndexRecord.name);
                }
            }
        }
        indexRecords = newRecords;
        return (dropped || created || updated);
    }

    /**
     * Loads the provided index record into cache.
     * This method blocks until the loading is complete.
     */
    public Duration loadIndexToCache(IndexRecord record)
    {
        long before = System.currentTimeMillis();
        String table = record.qualifiedTable;
        String column = String.join(",", record.columns);
        String type = record.indexType;
        CreateIndexMetadata.Level level = record.getLevel();

        String filterKeyPath = table + "/" + column + "/" + type;
        IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, LAST_MODIFIED_TIME_PLACE_HOLDER, record, level);
        filterKey.setNoCloseFlag(true);
        Future<?> future = executor.submit(() -> {
            List<IndexMetadata> allLoaded;
            try {
                // Load index for the whole table with dummy last modified time first
                allLoaded = cache.get(filterKey);
                // Then 1. replace the filterKey with the actual last modified time read from index
                // 2. for PARTITION and STRIPE index, the loaded whole table index should also be broken to stripe/partition indices
                switch (level) {
                    case STRIPE:
                        // break index key from table/column/type to several table/column/type/split-path
                        for (IndexMetadata index : allLoaded) {
                            String indexUri = index.getUri();
                            IndexCacheKey newKey = new IndexCacheKey(filterKeyPath + indexUri, index.getLastModifiedTime(), record);
                            cache.asMap().putIfAbsent(newKey, new ArrayList<>());
                            cache.asMap().get(newKey).add(index);
                        }
                        cache.invalidate(filterKey);
                        break;
                    case PARTITION:
                        // break index key from table/column/type to several table/column/type/partition
                        for (IndexMetadata index : allLoaded) {
                            Path indexUri = Paths.get(index.getUri());
                            String partition = null;
                            // get partition name from path if present
                            for (int i = 0; i < indexUri.getNameCount(); i++) {
                                if (indexUri.getName(i).toString().contains("=")) {
                                    partition = indexUri.getName(i).toString();
                                    break;
                                }
                            }
                            if (partition != null) {
                                IndexCacheKey newKey = new IndexCacheKey(filterKeyPath + "/" + partition, index.getLastModifiedTime(), record);
                                cache.asMap().putIfAbsent(newKey, new ArrayList<>());
                                cache.asMap().get(newKey).add(index);
                            }
                        }
                        cache.invalidate(filterKey);
                        break;
                    case TABLE:
                        // no need to break index, and lastModifiedTime is not used for TABLE level, but we need to
                        // toggle back the noCloseFlag, as it was previously set to true.
                        filterKey.setNoCloseFlag(false);
                        break;
                }
            }
            catch (ExecutionException e) {
                LOG.debug("Failed to load into cache: " + filterKey, e);
            }
        });
        // block until loading is complete
        try {
            future.get();
        }
        catch (InterruptedException | ExecutionException e) {
            LOG.debug("Failed to load into cache: " + filterKey, e);
        }
        long msElapsed = System.currentTimeMillis() - before;
        return new Duration(msElapsed, TimeUnit.MILLISECONDS);
    }

    public List<IndexMetadata> getIndices(String table, String column, Split split)
    {
        return getIndices(table, column, split, Collections.emptyMap());
    }

    public List<IndexMetadata> getIndices(String table, String column, Split split, Map<String, IndexRecord> indexRecordKeyToRecordMap)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        URI splitUri = URI.create(URIUtil.encodePath(split.getConnectorSplit().getFilePath()));
        long lastModifiedTime = split.getConnectorSplit().getLastModifiedTime();
        List<IndexMetadata> indices = new LinkedList<>();

        for (String indexType : INDEX_TYPES) {
            String filterKeyPath = table + "/" + column + "/" + indexType + splitUri.getRawPath();
            String indexRecordKey = table + "/" + column + "/" + indexType;
            IndexRecord record = indexRecordKeyToRecordMap.get(indexRecordKey);
            IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, record);
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
            else {
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

    public List<IndexMetadata> getIndices(String table, String column, String indexType, Set<String> partitions, long lastModifiedTime, Map<String, IndexRecord> indexRecordKeyToRecordMap)
    {
        if (cache == null) {
            return Collections.emptyList();
        }

        List<IndexMetadata> indices = new LinkedList<>();
        if (!partitions.isEmpty()) {
            for (String partition : partitions) {
                String filterKeyPath = table + "/" + column + "/" + indexType + "/" + partition;
                IndexRecord record = indexRecordKeyToRecordMap.get(filterKeyPath);
                IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, record, CreateIndexMetadata.Level.PARTITION);
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
        IndexRecord record = indexRecordKeyToRecordMap.get(filterKeyPath);
        IndexCacheKey filterKey = new IndexCacheKey(filterKeyPath, lastModifiedTime, record, CreateIndexMetadata.Level.TABLE);
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
