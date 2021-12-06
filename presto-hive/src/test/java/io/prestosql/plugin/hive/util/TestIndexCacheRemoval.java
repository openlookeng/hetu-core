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
package io.prestosql.plugin.hive.util;

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.service.PropertyService;
import io.prestosql.testing.NoOpIndexClient;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

/**
 * test methods should be in a synchronized (this) {} block to prevent conflicts between testcases
 */
public class TestIndexCacheRemoval
{
    private final String catalog = "test_catalog";
    private final String table = "schema_name.table_name";
    private final long testLastModifiedTime = 1;
    private final String testPath = "/user/hive/schema.db/table/001.orc";
    private final String testPath2 = "/user/hive/schema.db/table/002.orc";
    private final long loadDelay = 1000;
    private final long numberOfIndexTypes = IndexCache.INDEX_TYPES.size();
    private final Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
    private final String column = "column_name";
    private TupleDomain<HiveColumnHandle> effectivePredicate;
    private List<HiveColumnHandle> testPartitions = Collections.emptyList();
    private HiveColumnHandle testColumnHandle;

    @BeforeClass
    public void setupBeforeClass()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE, "local-config-default");
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_MAX_MEMORY, (long) (new DataSize(numberOfIndexTypes * 2, KILOBYTE).getValue(KILOBYTE)));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_TTL, new Duration(10, TimeUnit.MINUTES));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY, new Duration(loadDelay, TimeUnit.MILLISECONDS));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS, 2L);
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_SOFT_REFERENCE, false);

        testColumnHandle = mock(HiveColumnHandle.class);
        when(testColumnHandle.getName()).thenReturn(column);
        effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(testColumnHandle, domain));
    }

    @Test
    public void testExpiredCacheIndices() throws Exception
    {
        synchronized (this) {
            HiveSplit testHiveSplit;
            testHiveSplit = mock(HiveSplit.class);
            when(testHiveSplit.getPath()).thenReturn(testPath);
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);

            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            Index index = mock(Index.class);
            when(indexMetadata.getIndex()).then(new Returns(index));
            when(index.getMemoryUsage()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

            List<IndexMetadata> expectedIndices = new LinkedList<>();
            expectedIndices.add(indexMetadata);

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).then(new Returns(expectedIndices));

            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay, new NoOpIndexClient());
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 2000);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), numberOfIndexTypes);

            // now the index is in the cache, but changing the lastmodified date of the split should invalidate it
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime + 1);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
        }
    }

    @Test
    public void testIndexCacheEviction() throws Exception
    {
        synchronized (this) {
            HiveSplit testHiveSplit;
            testHiveSplit = mock(HiveSplit.class);
            when(testHiveSplit.getPath()).thenReturn(testPath);
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay, new NoOpIndexClient());

            // get index for split1
            IndexMetadata indexMetadata1 = mock(IndexMetadata.class);
            when(indexMetadata1.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            Index index1 = mock(Index.class);
            when(indexMetadata1.getIndex()).thenReturn(index1);
            when(index1.getMemoryUsage()).thenReturn(new DataSize(2, KILOBYTE).toBytes());

            List<IndexMetadata> expectedIndices1 = new LinkedList<>();
            expectedIndices1.add(indexMetadata1);
            when(indexCacheLoader.load(any())).then(new Returns(expectedIndices1));

            // each index is has memory usage of 2, and limit is 2*types of idx, so all should be loaded
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 2000);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), numberOfIndexTypes);
            assertEquals(actualSplitIndex.get(0), indexMetadata1);
            assertEquals(indexCache.getCacheSize(), numberOfIndexTypes);

            // get index for split2
            when(testHiveSplit.getPath()).thenReturn(testPath2);
            IndexMetadata indexMetadata2 = mock(IndexMetadata.class);
            when(indexMetadata2.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            Index index2 = mock(Index.class);
            when(indexMetadata2.getIndex()).thenReturn(index2);
            when(index2.getMemoryUsage()).thenReturn(new DataSize(2, KILOBYTE).toBytes());

            // previous indexes should be evicted bc cache was at max weight limit and new ones should be added
            List<IndexMetadata> expectedIndices2 = new LinkedList<>();
            expectedIndices2.add(indexMetadata2);
            when(indexCacheLoader.load(any())).then(new Returns(expectedIndices2));

            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            assertEquals(indexCache.getCacheSize(), numberOfIndexTypes);
            Thread.sleep(loadDelay + 2000);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), numberOfIndexTypes);
            assertEquals(actualSplitIndex.get(0), indexMetadata2);
            assertEquals(indexCache.getCacheSize(), numberOfIndexTypes);

            // get index for split1
            when(testHiveSplit.getPath()).thenReturn(testPath);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            assertEquals(indexCache.getCacheSize(), numberOfIndexTypes);
        }
    }
}
