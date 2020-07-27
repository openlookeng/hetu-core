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

import java.util.List;

public class RangeUtil
{
    private RangeUtil() {}

    //returns all of the elements in a sorted array where the first element value <= start and last element >= end
    public static List<IndexMetadata> subArray(List<IndexMetadata> arr, long start, long end)
    {
        try {
            int first = floorSearch(arr, 0, arr.size() - 1, start);
            int last = ceilSearch(arr, 0, arr.size() - 1, end);

            if (first == -1) {
                return arr;
            }

            // If end is greater than the last index, set last to the last index
            if (last == -1) {
                last = arr.size() - 1;
            }

            return arr.subList(first, last + 1);
        }
        catch (Exception e) {
            return arr;
        }
    }

    public static int ceilSearch(List<IndexMetadata> arr, int low, int high, long x)
            throws Exception
    {
        int mid;

      /* If x is smaller than or equal to the
         first element, then return the first element */
        if (x <= getOrder(arr.get(low))) {
            return low;
        }

      /* If x is greater than the last
         element, then return -1 */
        if (x > getOrder(arr.get(high))) {
            return -1;
        }

      /* get the index of middle element
         of arr[low..high]*/
        mid = (low + high) / 2;  /* low + (high - low)/2 */

      /* If x is same as middle element,
         then return mid */
        if (getOrder(arr.get(mid)) == x) {
            return mid;
        }

      /* If x is greater than arr[mid], then
         either arr[mid + 1] is ceiling of x or
         ceiling lies in arr[mid+1...high] */
        else if (getOrder(arr.get(mid)) < x) {
            if (mid + 1 <= high && x <= getOrder(arr.get(mid + 1))) {
                return mid + 1;
            }
            else {
                return ceilSearch(arr, mid + 1, high, x);
            }
        }

      /* If x is smaller than arr[mid],
         then either arr[mid] is ceiling of x
         or ceiling lies in arr[mid-1...high] */
        else {
            if (mid - 1 >= low && x > getOrder(arr.get(mid - 1))) {
                return mid;
            }
            else {
                return ceilSearch(arr, low, mid - 1, x);
            }
        }
    }

    /* Function to get index of floor of x in
    arr[low..high] */
    public static int floorSearch(List<IndexMetadata> arr, int low, int high, long x)
            throws Exception
    {
        // If low and high cross each other
        if (low > high) {
            return -1;
        }

        // If last element is smaller than x
        if (x >= getOrder(arr.get(high))) {
            return high;
        }

        // Find the middle point
        int mid = (low + high) / 2;

        // If middle point is floor.
        if (getOrder(arr.get(mid)) == x) {
            return mid;
        }

        // If x lies between mid-1 and mid
        if (mid > 0 && getOrder(arr.get(mid - 1)) <= x && x < getOrder(arr.get(mid))) {
            return mid - 1;
        }

        // If x is smaller than mid, floor
        // must be in left half.
        if (x < getOrder(arr.get(mid))) {
            return floorSearch(arr, low, mid - 1, x);
        }

        // If mid-1 is not floor and x is
        // greater than arr[mid],
        return floorSearch(arr, mid + 1, high, x);
    }

    public static long getOrder(IndexMetadata index)
            throws Exception

    {
        return index.getSplitStart();
    }
}
