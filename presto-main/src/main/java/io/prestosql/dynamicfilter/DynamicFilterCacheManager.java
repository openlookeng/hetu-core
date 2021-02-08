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
package io.prestosql.dynamicfilter;

import com.google.common.cache.Cache;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import io.airlift.log.Logger;
import io.prestosql.execution.TaskId;
import io.prestosql.spi.dynamicfilter.DynamicFilter;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.Set;

import static com.google.common.cache.CacheBuilder.newBuilder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Cache manager for merged Dynamic Filters, being used by all the tasks running on same worker
 * Currently the merged Dynamic Filters are added by {@link io.prestosql.dynamicfilter.DynamicFilterListener}
 */
public class DynamicFilterCacheManager
{
    private static final Logger LOG = Logger.get(DynamicFilterCacheManager.class);
    private static final long CACHE_MAXIMUM_SIZE = 10_000L;
    private static final long EXPIRES_AFTER_WRITE_MILLIS = 600_000L;
    private static final long EXPIRES_AFTER_ACCESS_MILLIS = 120_000L;

    private final Cache<String, DynamicFilter> cacheGlobalDynamicFilters;
    private final Cache<String, Map<String, byte[]>> cacheBloomFilters;
    private final Cache<String, Map<String, Set<String>>> cacheMapping;
    private final SetMultimap<String, TaskId> filterAppliedTasks = HashMultimap.create();

    @Inject
    public DynamicFilterCacheManager()
    {
        cacheGlobalDynamicFilters = newBuilder()
                .expireAfterAccess(EXPIRES_AFTER_ACCESS_MILLIS, MILLISECONDS)
                .expireAfterWrite(EXPIRES_AFTER_WRITE_MILLIS, MILLISECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .build();
        cacheBloomFilters = newBuilder()
                .expireAfterAccess(EXPIRES_AFTER_ACCESS_MILLIS, MILLISECONDS)
                .expireAfterWrite(EXPIRES_AFTER_WRITE_MILLIS, MILLISECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .build();
        cacheMapping = newBuilder()
                .expireAfterAccess(EXPIRES_AFTER_ACCESS_MILLIS, MILLISECONDS)
                .expireAfterWrite(EXPIRES_AFTER_WRITE_MILLIS, MILLISECONDS)
                .maximumSize(CACHE_MAXIMUM_SIZE)
                .build();
    }

    @PreDestroy
    public void invalidateCache()
    {
        cacheGlobalDynamicFilters.invalidateAll();
        cacheBloomFilters.invalidateAll();
        cacheMapping.invalidateAll();
    }

    /**
     * Only remove the cached filter when there is no related tasks remaining
     *
     * @param cacheKey Id of the dynamic filter and query id
     * @param taskId TaskId of the task to unregister
     */
    public synchronized void removeDynamicFilter(String cacheKey, TaskId taskId)
    {
        filterAppliedTasks.remove(cacheKey, taskId);
        LOG.debug("Removed task " + taskId + " for filter " + cacheKey);
        if (!filterAppliedTasks.containsKey(cacheKey)) {
            cacheGlobalDynamicFilters.asMap().remove(cacheKey);
            cacheBloomFilters.asMap().remove(cacheKey);
            cacheMapping.asMap().remove(cacheKey);
            LOG.debug("Removed cached DynamicFilter:" + cacheKey);
        }
    }

    public DynamicFilter getDynamicFilter(String cacheKey)
    {
        return cacheGlobalDynamicFilters.getIfPresent(cacheKey);
    }

    public void cacheDynamicFilter(String filterId, DynamicFilter dynamicFilter)
    {
        cacheGlobalDynamicFilters.put(filterId, dynamicFilter);
    }

    public Map<String, byte[]> getBloomFitler(String cacheKey)
    {
        return cacheBloomFilters.getIfPresent(cacheKey);
    }

    public Map<String, Set<String>> getMapping(String cacheKey)
    {
        return cacheMapping.getIfPresent(cacheKey);
    }

    public synchronized void cacheBloomFilters(String cacheKey, Map<String, byte[]> bloomFilters)
    {
        cacheBloomFilters.put(cacheKey, bloomFilters);
    }

    public synchronized void cacheMapping(String cacheKey, Map<String, Set<String>> mapping)
    {
        cacheMapping.put(cacheKey, mapping);
    }

    /**
     * Each dynamic filter could be used by multiple tasks
     * So we need to keep track of which tasks needs the filter
     *
     * @param cacheKey Id of the dynamic filter and query id
     * @param taskId TaskId of the task to register
     */
    public void registerTask(String cacheKey, TaskId taskId)
    {
        filterAppliedTasks.put(cacheKey, taskId);
        LOG.debug("Registered task " + taskId + " with filter " + cacheKey);
    }

    public static String createCacheKey(String filterId, String queryId)
    {
        return filterId + "-" + queryId;
    }
}
