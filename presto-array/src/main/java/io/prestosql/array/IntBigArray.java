/*
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
package io.prestosql.array;

import io.airlift.slice.SizeOf;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.Arrays;

import static io.airlift.slice.SizeOf.sizeOfIntArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class IntBigArray
        implements Restorable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntBigArray.class).instanceSize();
    private static final long SIZE_OF_SEGMENT = sizeOfIntArray(BigArrays.SEGMENT_SIZE);

    private final int initialValue;

    private int[][] array;
    private int capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public IntBigArray()
    {
        this(0);
    }

    /**
     * Creates a new big array containing one initial segment filled with the specified default value
     */
    public IntBigArray(int initialValue)
    {
        this.initialValue = initialValue;
        array = new int[BigArrays.INITIAL_SEGMENTS][];
        allocateNewSegment();
    }

    public int[][] getSegments()
    {
        return array;
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(array) + (segments * SIZE_OF_SEGMENT);
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    public int get(long index)
    {
        return array[BigArrays.segment(index)][BigArrays.offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, int value)
    {
        array[BigArrays.segment(index)][BigArrays.offset(index)] = value;
    }

    /**
     * Increments the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void increment(long index)
    {
        array[BigArrays.segment(index)][BigArrays.offset(index)]++;
    }

    /**
     * Adds the specified value to the specified element of this big array.
     *
     * @param index a position in this big array.
     * @param value the value
     */
    public void add(long index, int value)
    {
        array[BigArrays.segment(index)][BigArrays.offset(index)] += value;
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        if (capacity > length) {
            return;
        }

        grow(length);
    }

    public void fill(int value)
    {
        for (int[] ints : array) {
            if (ints == null) {
                return;
            }
            Arrays.fill(ints, value);
        }
    }

    private void grow(long length)
    {
        // how many segments are required to get to the length?
        int requiredSegments = BigArrays.segment(length) + 1;

        // grow base array if necessary
        if (array.length < requiredSegments) {
            array = Arrays.copyOf(array, requiredSegments);
        }

        // add new segments
        while (segments < requiredSegments) {
            allocateNewSegment();
        }
    }

    private void allocateNewSegment()
    {
        int[] newSegment = new int[BigArrays.SEGMENT_SIZE];
        if (initialValue != 0) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += BigArrays.SEGMENT_SIZE;
        segments++;
    }

    public void sort(int from, int to, IntComparator comparator)
    {
        IntBigArrays.quickSort(array, from, to, comparator);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        IntBigArrayState myState = new IntBigArrayState();
        int[][] capturedArray = new int[this.array.length][];
        for (int i = 0; i < this.array.length; i++) {
            if (this.array[i] != null) {
                capturedArray[i] = this.array[i].clone();
            }
        }
        myState.array = capturedArray;
        myState.capacity = capacity;
        myState.segments = segments;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        IntBigArrayState myState = (IntBigArrayState) state;
        this.array = myState.array;
        this.capacity = myState.capacity;
        this.segments = myState.segments;
    }

    private static class IntBigArrayState
            implements Serializable
    {
        private int[][] array;
        private int capacity;
        private int segments;
    }
}
