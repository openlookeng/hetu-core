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

import com.google.common.collect.ImmutableList;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestIndexCache
{
    private final String catalog = "test_catalog";
    private final String column = "column_name";
    private final String table = "schema_name.table_name";
    private final long testLastModifiedTime = 1;
    private final String testPath = "/user/hive/schema.db/table/001.orc";
    private final String testPath2 = "/user/hive/schema.db/table/002.orc";
    private TupleDomain<HiveColumnHandle> effectivePredicate;
    private TupleDomain<HiveColumnHandle> effectivePredicateForPartition;
    private HiveColumnHandle partitionColumnHandle;
    private HiveSplit testHiveSplit;
    private List<HiveColumnHandle> testPartitions = Collections.emptyList();
    private final long loadDelay = 0;

    @BeforeClass
    public void setupBeforeClass()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE, "local-config-default");
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_MAX_MEMORY, (long) (new DataSize(10, KILOBYTE).getValue(KILOBYTE)));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_TTL, new Duration(10, TimeUnit.MINUTES));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_DELAY, new Duration(loadDelay, TimeUnit.MILLISECONDS));
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_LOADING_THREADS, 2L);
        PropertyService.setProperty(HetuConstant.FILTER_CACHE_SOFT_REFERENCE, false);

        Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
        HiveColumnHandle testColumnHandle = mock(HiveColumnHandle.class);
        when(testColumnHandle.getName()).thenReturn(column);
        testHiveSplit = mock(HiveSplit.class);
        when(testHiveSplit.getPath()).thenReturn(testPath);
        effectivePredicate = TupleDomain.withColumnDomains(ImmutableMap.of(testColumnHandle, domain));

        partitionColumnHandle = mock(HiveColumnHandle.class);
        // partition column should be filtered out this should never get called
        when(partitionColumnHandle.getName()).thenThrow(Exception.class);

        effectivePredicateForPartition = TupleDomain.withColumnDomains(ImmutableMap.of(testColumnHandle, domain,
                        partitionColumnHandle, domain));
    }

    @Test
    public void testIndexCacheGetIndices() throws Exception
    {
        synchronized (this) {
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            List<IndexMetadata> expectedIndices = new LinkedList<>();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices.add(indexMetadata);
            Index index = mock(Index.class);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).thenReturn(expectedIndices);

            IndexCache indexCache = new IndexCache(indexCacheLoader);
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit,
                    effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 1);
        }
    }

    @Test
    public void testIndexCacheThrowsExecutionException() throws Exception
    {
        synchronized (this) {
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            List<IndexMetadata> expectedIndices = new LinkedList<>();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices.add(indexMetadata);
            Index index = mock(Index.class);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).thenThrow(ExecutionException.class);

            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay);
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit,
                    effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                    testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
        }
    }

    @Test
    public void testExpiredCacheIndices() throws Exception
    {
        synchronized (this) {
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            List<IndexMetadata> expectedIndices = new LinkedList<>();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices.add(indexMetadata);
            Index index = mock(Index.class);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).thenReturn(expectedIndices);

            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay);
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit,
                    effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                    testPartitions);
            assertEquals(actualSplitIndex.size(), 1);

            // now the index is in the cache, but changing the lastmodified date of the split should invalidate it
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime + 1);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                    testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
        }
    }

    @Test
    public void testIndexCacheIWIthPartitions() throws Exception
    {
        synchronized (this) {
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            List<HiveColumnHandle> partitionColumns = ImmutableList.of(partitionColumnHandle);

            List<IndexMetadata> expectedIndices = new LinkedList<>();
            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices.add(indexMetadata);
            Index index = mock(Index.class);
            when(indexMetadata.getIndex()).thenReturn(index);
            when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).thenReturn(expectedIndices);

            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay);
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit,
                    effectivePredicateForPartition, partitionColumns);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicateForPartition,
                    partitionColumns);

            assertEquals(actualSplitIndex.size(), 1);
        }
    }

    @Test
    public void testIndexCacheEviction() throws Exception
    {
        synchronized (this) {
            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay);
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);

            //get index for split1
            List<IndexMetadata> expectedIndices1 = new LinkedList<>();
            IndexMetadata indexMetadata1 = mock(IndexMetadata.class);
            when(indexMetadata1.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices1.add(indexMetadata1);
            Index index1 = mock(Index.class);
            when(indexMetadata1.getIndex()).thenReturn(index1);
            when(index1.getMemorySize()).thenReturn(new DataSize(5, KILOBYTE).toBytes());
            when(indexCacheLoader.load(any())).thenReturn(expectedIndices1);

            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit,
                    effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                    testPartitions);
            assertEquals(actualSplitIndex.size(), 1);
            assertEquals(actualSplitIndex.get(0), expectedIndices1.get(0));
            assertEquals(indexCache.getCacheSize(), 1);

            //get index for split2
            when(testHiveSplit.getPath()).thenReturn(testPath2);
            List<IndexMetadata> expectedIndices2 = new LinkedList<>();
            IndexMetadata indexMetadata2 = mock(IndexMetadata.class);
            when(indexMetadata2.getLastModifiedTime()).thenReturn(testLastModifiedTime);
            expectedIndices2.add(indexMetadata2);
            Index index2 = mock(Index.class);
            when(indexMetadata2.getIndex()).thenReturn(index2);
            when(index2.getMemorySize()).thenReturn(new DataSize(8, KILOBYTE).toBytes());
            when(indexCacheLoader.load(any())).thenReturn(expectedIndices2);

            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            assertEquals(indexCache.getCacheSize(), 1);
            Thread.sleep(loadDelay + 500);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 1);
            assertEquals(actualSplitIndex.get(0), expectedIndices2.get(0));
            assertEquals(indexCache.getCacheSize(), 1);

            // get index for split1
            when(testHiveSplit.getPath()).thenReturn(testPath);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            assertEquals(indexCache.getCacheSize(), 1);
        }
    }
}
