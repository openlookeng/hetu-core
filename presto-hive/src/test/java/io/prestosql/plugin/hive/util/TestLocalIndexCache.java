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
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.service.PropertyService;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.prestosql.spi.predicate.Range.range;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestLocalIndexCache
{
    private void setProperties()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
        PropertyService.setProperty(HetuConstant.INDEXSTORE_FILESYSTEM_PROFILE, "local-config-default");
        PropertyService.setProperty(HetuConstant.FILTER_MAX_INDICES_IN_CACHE, 10L);
    }

    @Test
    public void testLocalIndexCacheGetIndices() throws Exception
    {
        setProperties();

        String catalog = "test_catalog";
        String column = "column_name";
        String table = "table_name";
        long testLastModifiedTime = 1;
        String testPath = "/user/hive/schema.db/table/001.orc";
        List<HiveColumnHandle> testPartitions = Collections.emptyList();

        Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
        HiveColumnHandle testColumnHandle = mock(HiveColumnHandle.class);
        when(testColumnHandle.getName()).thenReturn(column);
        HiveSplit testHiveSplit = mock(HiveSplit.class);
        when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        when(testHiveSplit.getPath()).thenReturn(testPath);
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(testColumnHandle, domain));
        List<IndexMetadata> expectedIndices = new LinkedList<>();
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        expectedIndices.add(indexMetadata);

        LocalIndexCacheLoader localIndexCacheLoader = mock(LocalIndexCacheLoader.class);
        when(localIndexCacheLoader.load(any())).thenReturn(expectedIndices);
        LocalIndexCache localIndexCache = new LocalIndexCache(localIndexCacheLoader);
        List<IndexMetadata> actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit,
                effectivePredicate, testPartitions);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(LocalIndexCache.DEFAULT_LOAD_DELAY + 500);
        actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                testPartitions);
        assertEquals(actualSplitIndex.size(), 1);
    }

    @Test
    public void testLocalIndexCacheThrowsExecutionException() throws Exception
    {
        setProperties();

        String catalog = "test_catalog";
        String column = "column_name";
        String table = "table_name";
        long testLastModifiedTime = 1;
        String testPath = "/user/hive/schema.db/table/001.orc";
        List<HiveColumnHandle> testPartitions = Collections.emptyList();

        Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
        HiveColumnHandle testColumnHandle = mock(HiveColumnHandle.class);
        when(testColumnHandle.getName()).thenReturn(column);
        HiveSplit testHiveSplit = mock(HiveSplit.class);
        when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        when(testHiveSplit.getPath()).thenReturn(testPath);
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(testColumnHandle, domain));
        List<IndexMetadata> expectedIndices = new LinkedList<>();
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        expectedIndices.add(indexMetadata);

        LocalIndexCacheLoader localIndexCacheLoader = mock(LocalIndexCacheLoader.class);
        when(localIndexCacheLoader.load(any())).thenThrow(ExecutionException.class);

        long loadDelay = 1000;
        LocalIndexCache localIndexCache = new LocalIndexCache(localIndexCacheLoader, loadDelay);
        List<IndexMetadata> actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit,
                effectivePredicate, testPartitions);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                testPartitions);
        assertEquals(actualSplitIndex.size(), 0);
    }

    @Test
    public void testExpiredCacheIndices() throws Exception
    {
        setProperties();

        String catalog = "test_catalog";
        String column = "column_name";
        String table = "table_name";
        long testLastModifiedTime = 1;
        String testPath = "/user/hive/schema.db/table/001.orc";
        List<HiveColumnHandle> testPartitions = Collections.emptyList();

        Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
        HiveColumnHandle testColumnHandle = mock(HiveColumnHandle.class);
        when(testColumnHandle.getName()).thenReturn(column);
        HiveSplit testHiveSplit = mock(HiveSplit.class);
        when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        when(testHiveSplit.getPath()).thenReturn(testPath);
        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(testColumnHandle, domain));
        List<IndexMetadata> expectedIndices = new LinkedList<>();
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        expectedIndices.add(indexMetadata);

        LocalIndexCacheLoader localIndexCacheLoader = mock(LocalIndexCacheLoader.class);
        when(localIndexCacheLoader.load(any())).thenReturn(expectedIndices);
        long loadDelay = 1000;
        LocalIndexCache localIndexCache = new LocalIndexCache(localIndexCacheLoader, loadDelay);
        List<IndexMetadata> actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit,
                effectivePredicate, testPartitions);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                testPartitions);
        assertEquals(actualSplitIndex.size(), 1);

        // now the index is in the cache, but changing the lastmodified date of the split should invalidate it
        when(testHiveSplit.getLastModifiedTime()).thenReturn(++testLastModifiedTime);
        actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                testPartitions);
        assertEquals(actualSplitIndex.size(), 0);
    }

    @Test
    public void testLocalIndexCacheIWIthPartitions() throws Exception
    {
        setProperties();

        String catalog = "test_catalog";
        String column = "column_name";
        String table = "table_name";
        long testLastModifiedTime = 1;
        String testPath = "/user/hive/schema.db/table/001.orc";

        Domain domain = Domain.create(ValueSet.ofRanges(range(INTEGER, 0L, true, 100L, true)), false);
        HiveColumnHandle indexColumnHandle = mock(HiveColumnHandle.class);
        when(indexColumnHandle.getName()).thenReturn("indexColumn");

        HiveColumnHandle partitionColumnHandle = mock(HiveColumnHandle.class);
        // partition column should be filtered out this should never get called
        when(partitionColumnHandle.getName()).thenThrow(Exception.class);

        List<HiveColumnHandle> partitionColumns = ImmutableList.of(partitionColumnHandle);

        TupleDomain<HiveColumnHandle> effectivePredicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(indexColumnHandle, domain,
                        partitionColumnHandle, domain));

        HiveSplit testHiveSplit = mock(HiveSplit.class);
        when(testHiveSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        when(testHiveSplit.getPath()).thenReturn(testPath);

        List<IndexMetadata> expectedIndices = new LinkedList<>();
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        expectedIndices.add(indexMetadata);

        LocalIndexCacheLoader localIndexCacheLoader = mock(LocalIndexCacheLoader.class);
        when(localIndexCacheLoader.load(any())).thenReturn(expectedIndices);

        long loadDelay = 1000;
        LocalIndexCache localIndexCache = new LocalIndexCache(localIndexCacheLoader, loadDelay);
        List<IndexMetadata> actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit,
                effectivePredicate, partitionColumns);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = localIndexCache.getIndices(catalog, table, testHiveSplit, effectivePredicate,
                partitionColumns);

        assertEquals(actualSplitIndex.size(), 1);
    }
}
