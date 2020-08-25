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
package io.prestosql.orc;

import com.google.common.cache.Cache;
import com.google.inject.Inject;
import org.weakref.jmx.Managed;

public class FileTailCacheStatsLister
{
    private final Cache<?, ?> cache;

    @Inject
    public FileTailCacheStatsLister(OrcCacheStore orcCacheStore)
    {
        this.cache = orcCacheStore.getFileTailCache();
    }

    @Managed
    public long getEvictionCount()
    {
        return cache.stats().evictionCount();
    }

    @Managed
    public long getHitCount()
    {
        return cache.stats().hitCount();
    }

    @Managed
    public double getHitRate()
    {
        return cache.stats().hitRate();
    }

    @Managed
    public long getLoadCount()
    {
        return cache.stats().loadCount();
    }

    @Managed
    public long getLoadExceptionCount()
    {
        return cache.stats().loadExceptionCount();
    }

    @Managed
    public long getLoadSuccessCount()
    {
        return cache.stats().loadSuccessCount();
    }

    @Managed
    public double getLoadExceptionRate()
    {
        return cache.stats().loadExceptionRate();
    }

    @Managed
    public double getAverageLoadPenalty()
    {
        return cache.stats().averageLoadPenalty();
    }

    @Managed
    public long getMissCount()
    {
        return cache.stats().missCount();
    }

    @Managed
    public double getMissRate()
    {
        return cache.stats().missRate();
    }

    @Managed
    public long getRequestCount()
    {
        return cache.stats().requestCount();
    }

    @Managed
    public long getTotalLoadTime()
    {
        return cache.stats().totalLoadTime();
    }

    @Managed
    public long getSize()
    {
        return cache.size();
    }
}
