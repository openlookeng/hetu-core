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

import io.hetu.core.spi.heuristicindex.SplitIndexMetadata;
import org.testng.annotations.Test;

import java.util.LinkedList;
import java.util.List;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

public class TestIndexManager
{
    @Test
    public void testGetIndices()
    {
        LocalIndexCache localIndexCache = mock(LocalIndexCache.class);
        List<SplitIndexMetadata> expectedSplitsIndexes = new LinkedList<>();
        expectedSplitsIndexes.add(mock(SplitIndexMetadata.class));
        when(localIndexCache.getIndices(any(), any(), any(), any(), any())).thenReturn(expectedSplitsIndexes);

        IndexManager indexManager = new IndexManager(localIndexCache);
        List<SplitIndexMetadata> actualSplitIndex = indexManager.getIndices(any(), any(), any(), any(), any());
        assertEquals(actualSplitIndex.size(), expectedSplitsIndexes.size());
    }
}
