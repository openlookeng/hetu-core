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

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;

/**
 * Algorithms for sequence operations (merge, union, intersect)
 *
 * @author Han
 */
public class SequenceUtils
{
    private SequenceUtils() {}

    /**
     * Union k sorted iterators into one by keeping the order and only one copy of each element.
     *
     * This method finishes in O(nlogK) time, where n is the number of elements in one iterator and k is the number of iterators.
     *
     * @param iterators the iterators to be unioned
     * @param <T> the type of values in iterators
     * @return the unioned iterator
     */
    public static <T extends Comparable<T>> Iterator<T> union(List<Iterator<T>> iterators)
    {
        return merge(iterators, false);
    }

    public static <T extends Comparable<T>> Iterator<T> union(Iterator<T> iterator1, Iterator<T> iterator2)
    {
        List<Iterator<T>> iterators = new ArrayList<>(2);
        iterators.add(iterator1);
        iterators.add(iterator2);
        return union(iterators);
    }

    /**
     * Merge k sorted iterators into one by keeping the order and only one copy of each element.
     *
     * This method finishes in O(nlogK) time, where n is the number of elements in one iterator and k is the number of iterators.
     *
     * @param iterators the iterators to be merged
     * @param <T> the type of values in iterators
     * @param keepDuplication whether to keep duplicated values. For (1,2) and (2,4), if this is set to true then (1,2,2,4) will be returned otherwise it returns (1,2,4)
     * @return the merged iterator
     */
    public static <T extends Comparable<T>> Iterator<T> merge(List<Iterator<T>> iterators, boolean keepDuplication)
    {
        PriorityQueue<PeekingIterator<T>> pq = new PriorityQueue<>(Comparator.comparing(PeekingIterator::peek));

        for (Iterator<T> itr : iterators) {
            if (itr.hasNext()) {
                pq.add(Iterators.peekingIterator(itr));
            }
        }

        return new Iterator<T>()
        {
            T next;

            public boolean hasNext()
            {
                if (pq.isEmpty()) {
                    return false;
                }
                else if (keepDuplication) {
                    PeekingIterator<T> curr = pq.poll();
                    next = curr.next();
                    if (curr.hasNext()) {
                        pq.add(curr);
                    }
                    return true;
                }
                else {
                    PeekingIterator<T> curr = pq.poll();
                    next = curr.next();
                    if (curr.hasNext()) {
                        pq.add(curr);
                    }
                    while (!pq.isEmpty()) {
                        PeekingIterator<T> it = pq.poll();
                        if (it.peek().equals(next)) {
                            it.next();
                            if (it.hasNext()) {
                                pq.add(it);
                            }
                        }
                        else {
                            pq.add(it);
                            return true;
                        }
                    }
                    return true;
                }
            }

            public T next()
            {
                return next;
            }
        };
    }

    public static <T extends Comparable<T>> Iterator<T> merge(Iterator<T> iterator1, Iterator<T> iterator2, boolean keepDuplication)
    {
        List<Iterator<T>> iterators = new ArrayList<>(2);
        iterators.add(iterator1);
        iterators.add(iterator2);
        return merge(iterators, keepDuplication);
    }

    /**
     * Intersect k sorted iterators to find the common values in them.
     *
     * In worst case, initialization and next() method finishes in O(\sum{n_k}) time, where n_k is the number of elements in the k-th iterator.
     *
     * The hasNext() operation always finishes in O(1) time.
     *
     * @param iterators the iterators to be intersected
     * @param <T> the type of values in iterators
     * @return the intersected iterator
     */
    public static <T extends Comparable<T>> Iterator<T> intersect(List<Iterator<T>> iterators)
    {
        return new Iterator<T>()
        {
            T nextVal;

            @Override
            public boolean hasNext()
            {
                return nextVal != null;
            }

            @Override
            public T next()
            {
                T commonValToReturn = nextVal;
                search();
                return commonValToReturn;
            }

            void search()
            {
                nextVal = null;
                T[] heads = (T[]) new Comparable[iterators.size()];
                T min = null;
                int minIn = -1;

                for (int i = 0; i < iterators.size(); i++) {
                    if (!iterators.get(i).hasNext()) {
                        return;
                    }
                    heads[i] = iterators.get(i).next();
                    if (min == null || heads[i].compareTo(min) < 0) {
                        min = heads[i];
                        minIn = i;
                    }
                }
                while (true) {
                    nextVal = allEquals(heads);
                    if (nextVal != null) {
                        break;
                    }
                    else {
                        if (!iterators.get(minIn).hasNext()) {
                            break;
                        }
                        heads[minIn] = iterators.get(minIn).next();
                        min = heads[minIn];
                        for (int i = 0; i < heads.length; i++) {
                            if (heads[i].compareTo(min) < 0) {
                                min = heads[i];
                                minIn = i;
                            }
                        }
                    }
                }
            }

            {
                search();
            }
        };
    }

    public static <T extends Comparable<T>> Iterator<T> intersect(Iterator<T> iterator1, Iterator<T> iterator2)
    {
        List<Iterator<T>> iterators = new ArrayList<>(2);
        iterators.add(iterator1);
        iterators.add(iterator2);
        return intersect(iterators);
    }

    private static <T> T allEquals(T[] arr)
    {
        if (arr.length < 1) {
            return null;
        }

        T current = arr[0];
        for (int i = 1; i < arr.length; i++) {
            if (!arr[i].equals(current)) {
                return null;
            }
        }
        return current;
    }
}
