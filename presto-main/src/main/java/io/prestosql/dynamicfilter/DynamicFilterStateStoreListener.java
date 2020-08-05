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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;
import io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType;

import java.util.Map;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.HASHSET;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.utils.DynamicFilterUtils.FILTERPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.getDynamicFilterDataType;
import static java.util.Objects.requireNonNull;

/**
 * StateStore Listener for collecting newly merged DynamicFilter on workers
 */
public class DynamicFilterStateStoreListener
        implements EntryAddedListener<String, Object>
{
    private final String queryId;
    private final DynamicFilterDataType dynamicFilterDataType;
    private final Map<String, DynamicFilter> cachedDynamicFilters;
    private static final Logger LOG = Logger.get(DynamicFilterStateStoreListener.class);

    public DynamicFilterStateStoreListener(
            Map<String, DynamicFilter> cachedDynamicFilters,
            String queryId, DynamicFilterDataType dynamicFilterDataType)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.cachedDynamicFilters = requireNonNull(cachedDynamicFilters, "cachedDynamicFilters is null");
        this.dynamicFilterDataType = requireNonNull(dynamicFilterDataType, "dynamicFilterDataType is null");
    }

    @Override
    public void entryAdded(EntryEvent<String, Object> event)
    {
        String key = event.getKey();
        String cacheKey = key.substring(FILTERPREFIX.length());
        if (key.contains(queryId) && !cachedDynamicFilters.containsKey(cacheKey)) {
            cacheDynamicFilters(cacheKey, event.getValue());
        }
    }

    private void cacheDynamicFilters(String cacheKey, Object newDynamicFilter)
    {
        String filterId = cacheKey.split("-")[0];
        // Global dynamic filters
        if (newDynamicFilter == null) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "DynamicFilter added to StateStore is null");
        }

        DynamicFilter.DataType dataType = getDynamicFilterDataType(GLOBAL, dynamicFilterDataType);
        if (dataType == HASHSET) {
            Set hashSetFilter = (Set) newDynamicFilter;
            DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, null, hashSetFilter, GLOBAL);
            LOG.debug("Got new HashSet DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize());
            cachedDynamicFilters.put(filterId, dynamicFilter);
        }
        else if (dataType == BLOOM_FILTER) {
            byte[] serializedBloomFilter = (byte[]) newDynamicFilter;
            DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, null, serializedBloomFilter, GLOBAL);
            LOG.debug("Got new BloomFilter DynamicFilter from state store: " + filterId + ", size: " + dynamicFilter.getSize());
            cachedDynamicFilters.put(filterId, dynamicFilter);
        }
    }
}
