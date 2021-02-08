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

import io.airlift.log.Logger;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;

import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.utils.DynamicFilterUtils.FILTERPREFIX;
import static java.util.Objects.requireNonNull;

/**
 * StateStore Listener for collecting newly merged DynamicFilter on workers
 */
public class DynamicFilterListener
        implements EntryAddedListener<String, Object>
{
    private static final Logger LOG = Logger.get(DynamicFilterListener.class);

    private final DynamicFilterCacheManager dynamicFilterCacheManager;

    public DynamicFilterListener(DynamicFilterCacheManager cache)
    {
        this.dynamicFilterCacheManager = requireNonNull(cache, "cache is null");
    }

    private Optional<DynamicFilter> transformDynamicFilter(String cacheKey, Object newDynamicFilter)
    {
        String filterId = cacheKey.split("-")[0];
        // Global dynamic filters
        if (newDynamicFilter == null) {
            LOG.warn("DynamicFilter added to StateStore is null.");
            return Optional.empty();
        }

        DynamicFilter dynamicFilter = null;
        if (newDynamicFilter instanceof Set) {
            dynamicFilter = DynamicFilterFactory.create(filterId, null, (Set<?>) newDynamicFilter, GLOBAL);
            LOG.debug("Got new HashSet DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize());
        }
        else if (newDynamicFilter instanceof byte[]) {
            dynamicFilter = DynamicFilterFactory.create(filterId, null, (byte[]) newDynamicFilter, GLOBAL);
            LOG.debug("Got new BloomFilter DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize());
        }

        return dynamicFilter == null ? Optional.empty() : Optional.of(dynamicFilter);
    }

    @Override
    public void entryAdded(EntryEvent<String, Object> event)
    {
        String key = event.getKey();
        String cacheKey = key.substring(FILTERPREFIX.length());
        transformDynamicFilter(cacheKey, event.getValue())
                .ifPresent(filter -> dynamicFilterCacheManager.cacheDynamicFilter(cacheKey, filter));
    }
}
