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
import io.prestosql.spi.statestore.listener.EntryAddedListener;
import io.prestosql.spi.statestore.listener.EntryEvent;

import java.util.Map;
import java.util.Set;

import static io.prestosql.statestore.StateStoreConstants.CROSS_LAYER_DYNAMIC_FILTER;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static io.prestosql.statestore.StateStoreConstants.QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING;

public class CrossRegionDynamicFilterListener
        implements EntryAddedListener<String, Object>
{
    private static final Logger LOG = Logger.get(CrossRegionDynamicFilterListener.class);
    private final DynamicFilterCacheManager dynamicFilterCacheManager;

    public CrossRegionDynamicFilterListener(DynamicFilterCacheManager dynamicFilterCacheManager)
    {
        this.dynamicFilterCacheManager = dynamicFilterCacheManager;
    }

    @Override
    public void entryAdded(EntryEvent<String, Object> event)
    {
        if (event.getKey().contains(CROSS_LAYER_DYNAMIC_FILTER) || event.getKey().contains(CROSS_REGION_DYNAMIC_FILTER_COLLECTION)) {
            dynamicFilterCacheManager.cacheBloomFilters(event.getKey(), (Map<String, byte[]>) event.getValue());
            LOG.debug("cached cross region BloomFilter DynamicFilter from state store, key is %s.", event.getKey());
        }
        else if (event.getKey().contains(QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING)) {
            dynamicFilterCacheManager.cacheMapping(event.getKey(), (Map<String, Set<String>>) event.getValue());
            LOG.debug("cached column-mapping from state store, key is %s.", event.getKey());
        }
    }
}
