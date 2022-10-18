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
package io.prestosql.spi.heuristicindex;

import io.prestosql.spi.connector.CreateIndexMetadata;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class IndexCacheKeyTest
{
    @Mock
    private IndexRecord mockRecord;

    private IndexCacheKey indexCacheKeyUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        indexCacheKeyUnderTest = new IndexCacheKey("path", 0L, mockRecord, CreateIndexMetadata.Level.STRIPE);
    }

    @Test
    public void testSkipCloseIndex() throws Exception
    {
        assertTrue(indexCacheKeyUnderTest.skipCloseIndex());
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(indexCacheKeyUnderTest.equals("o"));
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", indexCacheKeyUnderTest.toString());
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, indexCacheKeyUnderTest.hashCode());
    }
}
