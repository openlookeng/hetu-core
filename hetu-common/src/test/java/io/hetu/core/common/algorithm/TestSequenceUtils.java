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

package io.hetu.core.common.algorithm;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSequenceUtils
{
    List<Integer> l1 = ImmutableList.of(1, 3, 4, 5, 7);
    List<Integer> l2 = ImmutableList.of(1, 2, 3, 6);
    List<Integer> l3 = ImmutableList.of(5);
    Iterator<Integer> emptyIterator = Collections.emptyIterator();

    private static void assertIteratorSame(Iterator iterator1, Iterator iterator2)
    {
        while (iterator1.hasNext()) {
            assertTrue(iterator2.hasNext());
            assertEquals(iterator2.next(), iterator1.next());
        }
        if (iterator2.hasNext()) {
            System.out.println(iterator2.next());
        }
        assertFalse(iterator2.hasNext());
    }

    @Test
    public void testUnion()
    {
        assertIteratorSame(SequenceUtils.union(ImmutableList.of(l1.iterator(), l2.iterator(), l3.iterator(), emptyIterator)),
                ImmutableList.of(1, 2, 3, 4, 5, 6, 7).iterator());
    }

    @Test
    public void testMergeWithDup()
    {
        assertIteratorSame(SequenceUtils.merge(ImmutableList.of(l1.iterator(), l2.iterator(), l3.iterator(), emptyIterator), true),
                ImmutableList.of(1, 1, 2, 3, 3, 4, 5, 5, 6, 7).iterator());
        assertIteratorSame(SequenceUtils.merge(ImmutableList.of(l1.iterator(), l3.iterator(), emptyIterator), true),
                ImmutableList.of(1, 3, 4, 5, 5, 7).iterator());
    }

    @Test
    public void testIntersect()
    {
        assertIteratorSame(SequenceUtils.intersect(ImmutableList.of(l1.iterator(), l2.iterator())),
                ImmutableList.of(1, 3).iterator());
        assertIteratorSame(SequenceUtils.intersect(ImmutableList.of(l1.iterator(), l3.iterator())),
                ImmutableList.of(5).iterator());
        assertIteratorSame(SequenceUtils.intersect(ImmutableList.of(l1.iterator(), l2.iterator(), l3.iterator())),
                ImmutableList.of().iterator());
        assertIteratorSame(SequenceUtils.intersect(ImmutableList.of(l1.iterator(), l2.iterator(), l3.iterator(), emptyIterator)),
                ImmutableList.of().iterator());
    }
}
