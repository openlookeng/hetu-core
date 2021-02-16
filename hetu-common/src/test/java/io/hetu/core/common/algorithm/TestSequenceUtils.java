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

import java.util.ArrayList;
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

    private static <T1, T2> void assertIteratorSame(Iterator<T1> iterator1, Iterator<T2> iterator2)
    {
        while (iterator1.hasNext()) {
            assertTrue(iterator2.hasNext());
            T1 v1 = iterator1.next();
            T2 v2 = iterator2.next();
            System.out.println(v1 + "---" + v2);
            assertEquals(v1, v2);
        }
        System.out.println();
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

    @Test
    public void testRepeatingValues()
    {
        List<String> l = new ArrayList<>(10000);
        for (int i = 0; i < 10000; i++) {
            l.add(String.valueOf(10000));
        }
        Iterator<String> it = SequenceUtils.union(ImmutableList.of(l.iterator()));
        // detect stack overflow
        while (it.hasNext()) {
            it.next();
        }
    }

    @Test
    public void testDupAsEnd()
    {
        List<Integer> i1 = ImmutableList.of(1, 4, 5, 5, 7, 7, 7);
        List<Integer> i2 = ImmutableList.of(2, 2, 2, 3, 5, 5, 5);
        List<Integer> i3 = ImmutableList.of(6, 6, 6, 6);
        List<Integer> i4 = ImmutableList.of(3);

        assertIteratorSame(SequenceUtils.union(ImmutableList.of(i1.iterator(), i2.iterator(), i3.iterator(), i4.iterator())),
                ImmutableList.of(1, 2, 3, 4, 5, 6, 7).iterator());
        assertIteratorSame(SequenceUtils.merge(ImmutableList.of(i1.iterator(), i2.iterator(), i3.iterator(), i4.iterator()), true),
                ImmutableList.of(1, 2, 2, 2, 3, 3, 4, 5, 5, 5, 5, 5, 6, 6, 6, 6, 7, 7, 7).iterator());
    }
}
