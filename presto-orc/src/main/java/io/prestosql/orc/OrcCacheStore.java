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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import io.airlift.units.DataSize;
import io.prestosql.orc.metadata.RowGroupIndex;
import io.prestosql.orc.metadata.StripeFooter;
import io.prestosql.orc.metadata.statistics.HashableBloomFilter;
import io.prestosql.spi.block.Block;

import java.time.Duration;
import java.util.List;

public class OrcCacheStore
{
    public static final OrcCacheStore CACHE_NOTHING = new OrcCacheStore(null,
            null,
            null,
            null,
            null);

    private Cache<OrcDataSourceId, OrcFileTail> fileTailCache;
    private Cache<OrcStripeFooterCacheKey, StripeFooter> stripeFooterCache;
    private Cache<OrcRowIndexCacheKey, List<RowGroupIndex>> rowIndexCache;
    private Cache<OrcBloomFilterCacheKey, List<HashableBloomFilter>> bloomFiltersCache;
    private Cache<OrcRowDataCacheKey, Block> rowDataCache;

    private OrcCacheStore()
    {
        //do nothing
    }

    private OrcCacheStore(Cache<OrcDataSourceId, OrcFileTail> fileTailCache,
                          Cache<OrcStripeFooterCacheKey, StripeFooter> stripeFooterCache,
                          Cache<OrcRowIndexCacheKey, List<RowGroupIndex>> rowIndexCache,
                          Cache<OrcBloomFilterCacheKey, List<HashableBloomFilter>> bloomFiltersCache,
                          Cache<OrcRowDataCacheKey, Block> rowDataCache)
    {
        this.fileTailCache = fileTailCache;
        this.stripeFooterCache = stripeFooterCache;
        this.rowIndexCache = rowIndexCache;
        this.bloomFiltersCache = bloomFiltersCache;
        this.rowDataCache = rowDataCache;
    }

    public Cache<OrcDataSourceId, OrcFileTail> getFileTailCache()
    {
        return fileTailCache;
    }

    public Cache<OrcStripeFooterCacheKey, StripeFooter> getStripeFooterCache()
    {
        return stripeFooterCache;
    }

    public Cache<OrcRowIndexCacheKey, List<RowGroupIndex>> getRowIndexCache()
    {
        return rowIndexCache;
    }

    public Cache<OrcBloomFilterCacheKey, List<HashableBloomFilter>> getBloomFiltersCache()
    {
        return bloomFiltersCache;
    }

    public Cache<OrcRowDataCacheKey, Block> getRowDataCache()
    {
        return rowDataCache;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder()
        {
            //default constructor
        }

        public OrcCacheStore newCacheStore(long fileTailMaximumSize, Duration fileTailTtl,
                                           long stripeFooterMaximumSize, Duration stripeFooterTtl,
                                           long rowIndexMaximumSize, Duration rowIndexTtl,
                                           long bloomFiltersMaximumSize, Duration bloomFiltersTtl,
                                           DataSize rowDataMaximumWeight, Duration rowDataTtl)
        {
            OrcCacheStore store = new OrcCacheStore();
            store.fileTailCache = buildOrcFileTailCache(fileTailMaximumSize, fileTailTtl);
            store.stripeFooterCache = buildOrcStripeFooterCache(stripeFooterMaximumSize, stripeFooterTtl);
            store.rowIndexCache = buildOrcRowGroupIndexCache(rowIndexMaximumSize, rowIndexTtl);
            store.bloomFiltersCache = buildOrcBloomFilterCache(bloomFiltersMaximumSize, bloomFiltersTtl);
            store.rowDataCache = buildOrcRowDataCache(rowDataMaximumWeight, rowDataTtl);
            return store;
        }

        private Cache<OrcDataSourceId, OrcFileTail> buildOrcFileTailCache(long maximumSize, Duration ttl)
        {
            return CacheBuilder.newBuilder()
                    .maximumSize(maximumSize)
                    .expireAfterAccess(ttl)
                    .build();
        }

        private Cache<OrcStripeFooterCacheKey, StripeFooter> buildOrcStripeFooterCache(long maximumSize, Duration ttl)
        {
            return CacheBuilder.newBuilder()
                    .maximumSize(maximumSize)
                    .expireAfterAccess(ttl)
                    .build();
        }

        private Cache<OrcRowIndexCacheKey, List<RowGroupIndex>> buildOrcRowGroupIndexCache(long maximumSize,
                                                                                           Duration ttl)
        {
            return CacheBuilder.newBuilder()
                    .maximumSize(maximumSize)
                    .expireAfterAccess(ttl)
                    .recordStats()
                    .build();
        }

        private Cache<OrcBloomFilterCacheKey, List<HashableBloomFilter>> buildOrcBloomFilterCache(long maximumSize,
                                                                                          Duration ttl)
        {
            return CacheBuilder.newBuilder()
                    .maximumSize(maximumSize)
                    .expireAfterAccess(ttl)
                    .build();
        }

        private Cache<OrcRowDataCacheKey, Block> buildOrcRowDataCache(DataSize maximumWeight, Duration ttl)
        {
            return CacheBuilder.newBuilder()
                    .maximumWeight(maximumWeight.toBytes())
                    .weigher(
                            (Weigher<OrcRowDataCacheKey, Block>) (orcRowDataCacheKey, block) -> (int) block.getSizeInBytes())
                    .expireAfterAccess(ttl)
                    .build();
        }
    }
}
