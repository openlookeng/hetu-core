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
import java.util.concurrent.ExecutionException;
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
public class TestIndexCacheFetch
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
    public void testIndexCacheGetIndices() throws Exception
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

            IndexCache indexCache = new IndexCache(indexCacheLoader, new NoOpIndexClient());
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 2000);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), numberOfIndexTypes);
            assertEquals(actualSplitIndex.get(0), expectedIndices.get(0));
        }
    }

    @Test
    public void testIndexCacheThrowsExecutionException()
            throws Exception
    {
        synchronized (this) {
            HiveSplit testHiveSplit;
            testHiveSplit = mock(HiveSplit.class);
            when(testHiveSplit.getPath()).thenReturn(testPath);
            when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);

            IndexMetadata indexMetadata = mock(IndexMetadata.class);
            when(indexMetadata.getLastModifiedTime()).thenReturn(testLastModifiedTime);

            List<IndexMetadata> expectedIndices = new LinkedList<>();
            expectedIndices.add(indexMetadata);

            IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
            when(indexCacheLoader.load(any())).thenThrow(ExecutionException.class);

            IndexCache indexCache = new IndexCache(indexCacheLoader, loadDelay, new NoOpIndexClient());
            List<IndexMetadata> actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
            Thread.sleep(loadDelay + 2000);
            actualSplitIndex = indexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate, testPartitions);
            assertEquals(actualSplitIndex.size(), 0);
        }
    }
}
