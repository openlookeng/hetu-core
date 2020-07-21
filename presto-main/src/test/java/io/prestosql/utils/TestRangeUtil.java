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
package io.prestosql.utils;

import io.prestosql.spi.heuristicindex.IndexMetadata;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestRangeUtil
{
    @Test
    public void testSubArray()
            throws Exception
    {
        long[] arr = {1, 2, 8, 10, 10, 12, 19};
        List result = RangeUtil.subArray(createIndexList(arr), 7, 13);
        assertEquals(6, result.size());
        List expected = createIndexList(new long[] {2, 8, 10, 10, 12, 19});
        assertTrue(compareIndexLists(expected, result));

        result = RangeUtil.subArray(createIndexList(arr), 1, 10);
        assertEquals(4, result.size());
        List expected2 = createIndexList(new long[] {1, 2, 8, 10});
        assertTrue(compareIndexLists(expected2, result));
    }

    @Test
    public void testSubArray2()
            throws Exception
    {
        long[] arr = {0, 20000, 40000, 60000, 80000, 100000, 120000};
        List result = RangeUtil.subArray(createIndexList(arr), 1000, 60000);
        List expected = createIndexList(new long[] {0, 20000, 40000, 60000});
        assertTrue(compareIndexLists(expected, result));
    }

    @Test
    public void testSubArray3()
            throws Exception
    {
        long[] arr = {0, 20000};
        List result = RangeUtil.subArray(createIndexList(arr), 0, 20000);
        List expected = createIndexList(new long[] {0, 20000});
        assertTrue(compareIndexLists(expected, result));
    }

    @Test
    public void testSubArrayOutOfStartRange()
            throws Exception
    {
        List<IndexMetadata> indices = createIndexList(new long[] {1, 2, 8, 10, 10, 12, 19});
        List<IndexMetadata> result = RangeUtil.subArray(indices, 0, 19);
        assertEquals(7, result.size());
        assertTrue(compareIndexLists(indices, result));
    }

    @Test
    public void testSubArrayOutOfEndRange()
            throws Exception
    {
        List<IndexMetadata> indices = createIndexList(new long[] {1, 2, 8, 10, 10, 12, 19});
        List<IndexMetadata> result = RangeUtil.subArray(indices, 1, 20);
        assertEquals(7, result.size());
        assertTrue(compareIndexLists(indices, result));
    }

    @Test
    public void testFindStart()
            throws Exception
    {
        /* Driver program to check above functions */

        long[] arr = {1, 2, 8, 10, 10, 12, 19};
        int n = arr.length;
        List<IndexMetadata> indices = createIndexList(arr);

        assertEquals(0, RangeUtil.ceilSearch(indices, 0, n - 1, 0));
        assertEquals(0, RangeUtil.ceilSearch(indices, 0, n - 1, -1));
        assertEquals(2, RangeUtil.ceilSearch(indices, 0, n - 1, 7));
        assertEquals(2, RangeUtil.ceilSearch(indices, 0, n - 1, 8));
        assertEquals(6, RangeUtil.ceilSearch(indices, 0, n - 1, 19));
        assertEquals(-1, RangeUtil.ceilSearch(indices, 0, n - 1, 20));
    }

    @Test
    public void testFindEnd()
            throws Exception
    {
        /* Driver program to check above functions */

        long[] arr = {1, 2, 8, 10, 10, 12, 19};
        int n = arr.length;
        List<IndexMetadata> indices = createIndexList(arr);

        assertEquals(-1, RangeUtil.floorSearch(indices, 0, n - 1, 0));
        assertEquals(-1, RangeUtil.floorSearch(indices, 0, n - 1, -1));
        assertEquals(1, RangeUtil.floorSearch(indices, 0, n - 1, 7));
        assertEquals(2, RangeUtil.floorSearch(indices, 0, n - 1, 8));
        assertEquals(6, RangeUtil.floorSearch(indices, 0, n - 1, 19));
        assertEquals(6, RangeUtil.floorSearch(indices, 0, n - 1, 20));
    }

    // Create fake ExternalIndex list given the array of start offsets
    private List<IndexMetadata> createIndexList(long[] arr)
    {
        List<IndexMetadata> list = new ArrayList<>();
        for (long i : arr) {
            IndexMetadata index = new IndexMetadata(null, null, null, null, null, i, 0);
            list.add(index);
        }
        return list;
    }

    // Compare 2 ExternalIndex lists
    private boolean compareIndexLists(List<IndexMetadata> expected, List<IndexMetadata> actual)
            throws Exception
    {
        for (int i = 0; i < expected.size(); i++) {
            if (RangeUtil.getOrder(expected.get(i)) != RangeUtil.getOrder(actual.get(i))) {
                return false;
            }
        }
        return true;
    }
}
