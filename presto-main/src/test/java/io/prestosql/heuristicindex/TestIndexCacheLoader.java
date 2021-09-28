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
package io.prestosql.heuristicindex;

import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.heuristicindex.IndexCacheKey;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.service.PropertyService;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestIndexCacheLoader
{
    @BeforeTest
    private void setProperties()
    {
        PropertyService.setProperty(HetuConstant.FILTER_ENABLED, true);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoLastModifiedTime() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        IndexCacheLoader indexCacheLoader = new IndexCacheLoader(indexclient);

        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", 1);

        // throw exception to produce "no last modified time file found" behaviour
        when(indexclient.getLastModifiedTime((indexCacheKey.getPath()))).thenThrow(Exception.class);

        indexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoMatchingLastModifiedTime() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        IndexCacheLoader indexCacheLoader = new IndexCacheLoader(indexclient);

        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", 1L);

        // return different last modified time to simulate expired index
        when(indexclient.getLastModifiedTime((indexCacheKey.getPath()))).thenReturn(2L);

        indexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoValidIndexFilesFoundException() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        IndexCacheLoader indexCacheLoader = new IndexCacheLoader(indexclient);

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModifiedTime((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenThrow(Exception.class);

        indexCacheLoader.load(indexCacheKey);
    }

    @Test(expectedExceptions = Exception.class)
    public void testNoValidIndexFilesFound() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        IndexCacheLoader indexCacheLoader = new IndexCacheLoader(indexclient);

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModifiedTime((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenReturn(Collections.emptyList());

        indexCacheLoader.load(indexCacheKey);
    }

    @Test
    public void testIndexFound() throws Exception
    {
        IndexClient indexclient = mock(IndexClient.class);
        IndexCacheLoader indexCacheLoader = new IndexCacheLoader(indexclient);

        List<IndexMetadata> expectedSplitIndexes = new LinkedList<>();
        expectedSplitIndexes.add(mock(IndexMetadata.class));

        long lastModifiedTime = 1L;
        IndexCacheKey indexCacheKey = new IndexCacheKey("/path/to/split", lastModifiedTime);
        when(indexclient.getLastModifiedTime((indexCacheKey.getPath()))).thenReturn(lastModifiedTime);
        when(indexclient.readSplitIndex((indexCacheKey.getPath()))).thenReturn(expectedSplitIndexes);

        List<IndexMetadata> actualSplitIndexes = indexCacheLoader.load(indexCacheKey);
        assertEquals(expectedSplitIndexes.size(), actualSplitIndexes.size());
    }
}
