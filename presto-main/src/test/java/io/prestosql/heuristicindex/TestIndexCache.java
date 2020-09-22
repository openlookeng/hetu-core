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
package io.prestosql.heuristicindex;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.connector.CatalogName;
import io.prestosql.execution.Lifespan;
import io.prestosql.metadata.Split;
import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.heuristicindex.Index;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.service.PropertyService;
import org.mockito.internal.stubbing.answers.Returns;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
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
    private final long loadDelay = 1000;
    private Split split;
    ConnectorSplit connectorSplit;

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

        CatalogName catalogName = new CatalogName(catalog);
        connectorSplit = mock(ConnectorSplit.class);
        Lifespan lifespan = mock(Lifespan.class);
        split = new Split(catalogName, connectorSplit, lifespan);
        when(connectorSplit.getFilePath()).thenReturn(testPath);
    }

    @Test
    public void testIndexCacheGetIndices() throws Exception
    {
        when(connectorSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        Index index = mock(Index.class);
        when(indexMetadata.getIndex()).then(new Returns(index));
        when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

        List<IndexMetadata> expectedIndices = new LinkedList<>();
        expectedIndices.add(indexMetadata);

        IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
        when(indexCacheLoader.load(any())).then(new Returns(expectedIndices));

        IndexCache indexCache = new IndexCache(indexCacheLoader);
        List<IndexMetadata> actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 2);
        assertEquals(actualSplitIndex.get(0), expectedIndices.get(0));
    }

    @Test
    public void testIndexCacheThrowsExecutionException() throws Exception
    {
        when(connectorSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).then(new Returns(testLastModifiedTime));

        List<IndexMetadata> expectedIndices = new LinkedList<>();
        expectedIndices.add(indexMetadata);

        IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
        when(indexCacheLoader.load(any())).thenThrow(ExecutionException.class);

        IndexCache indexCache = new IndexCache(indexCacheLoader);
        List<IndexMetadata> actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
    }

    @Test
    public void testExpiredCacheIndices() throws Exception
    {
        when(connectorSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        IndexMetadata indexMetadata = mock(IndexMetadata.class);
        when(indexMetadata.getLastUpdated()).thenReturn(testLastModifiedTime);
        Index index = mock(Index.class);
        when(indexMetadata.getIndex()).then(new Returns(index));
        when(index.getMemorySize()).thenReturn(new DataSize(1, KILOBYTE).toBytes());

        List<IndexMetadata> expectedIndices = new LinkedList<>();
        expectedIndices.add(indexMetadata);

        IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
        when(indexCacheLoader.load(any())).then(new Returns(expectedIndices));

        IndexCache indexCache = new IndexCache(indexCacheLoader);
        List<IndexMetadata> actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 2);

        // now the index is in the cache, but changing the lastmodified date of the split should invalidate it
        when(indexMetadata.getLastUpdated()).then(new Returns(testLastModifiedTime + 1));
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
    }

    @Test
    public void testIndexCacheEviction() throws Exception
    {
        when(connectorSplit.getLastModifiedTime()).thenReturn(testLastModifiedTime);
        IndexCacheLoader indexCacheLoader = mock(IndexCacheLoader.class);
        IndexCache indexCache = new IndexCache(indexCacheLoader);

        //get index for split1
        IndexMetadata indexMetadata1 = mock(IndexMetadata.class);
        when(indexMetadata1.getLastUpdated()).thenReturn(testLastModifiedTime);
        Index index1 = mock(Index.class);
        when(indexMetadata1.getIndex()).thenReturn(index1);
        when(index1.getMemorySize()).thenReturn(new DataSize(5, KILOBYTE).toBytes());

        List<IndexMetadata> expectedIndices = new LinkedList<>();
        expectedIndices.add(indexMetadata1);
        when(indexCacheLoader.load(any())).then(new Returns(expectedIndices));

        List<IndexMetadata> actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 2);
        assertEquals(actualSplitIndex.get(0), indexMetadata1);
        assertEquals(indexCache.getCacheSize(), 2);

        //get index for split2
        when(connectorSplit.getFilePath()).thenReturn(testPath2);
        IndexMetadata indexMetadata2 = mock(IndexMetadata.class);
        when(indexMetadata2.getLastUpdated()).thenReturn(testLastModifiedTime);
        Index index2 = mock(Index.class);
        when(indexMetadata2.getIndex()).thenReturn(index2);
        when(index2.getMemorySize()).thenReturn(new DataSize(8, KILOBYTE).toBytes());

        List<IndexMetadata> expectedIndices2 = new LinkedList<>();
        expectedIndices2.add(indexMetadata2);
        when(indexCacheLoader.load(any())).then(new Returns(expectedIndices2));
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        assertEquals(indexCache.getCacheSize(), 2);
        Thread.sleep(loadDelay + 500);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 1);
        assertEquals(actualSplitIndex.get(0), indexMetadata2);
        assertEquals(indexCache.getCacheSize(), 1);

        //get index for split1
        when(connectorSplit.getFilePath()).thenReturn(testPath);
        actualSplitIndex = indexCache.getIndices(table, column, split);
        assertEquals(actualSplitIndex.size(), 0);
        assertEquals(indexCache.getCacheSize(), 1);
    }
}
