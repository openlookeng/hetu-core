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

import static io.airlift.slice.SizeOf.sizeOfLongArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class LongBigArray
        implements Restorable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongBigArray.class).instanceSize();
    private static final long SIZE_OF_SEGMENT = sizeOfLongArray(BigArrays.SEGMENT_SIZE);

    private final long initialValue;

    private long[][] array;
    private int capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public LongBigArray()
    {
        this(0L);
    }

    /**
     * Creates a new big array containing one initial segment filled with the specified default value
     */
    public LongBigArray(long initialValue)
    {
        this.initialValue = initialValue;
        array = new long[BigArrays.INITIAL_SEGMENTS][];
        allocateNewSegment();
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
    public long get(long index)
    {
        return array[BigArrays.segment(index)][BigArrays.offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, long value)
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
    public void add(long index, long value)
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
        long[] newSegment = new long[BigArrays.SEGMENT_SIZE];
        if (initialValue != 0) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += BigArrays.SEGMENT_SIZE;
        segments++;
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        LongBigArrayState myState = new LongBigArrayState();
        long[][] capturedArray = new long[this.array.length][];
        for (int i = 0; i < this.array.length; i++) {
            if (this.array[i] != null) {
                capturedArray[i] = this.array[i].clone();
            }
        }
        myState.array = capturedArray;
        myState.capacity = this.capacity;
        myState.segments = this.segments;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        LongBigArrayState myState = (LongBigArrayState) state;
        this.array = myState.array;
        this.capacity = myState.capacity;
        this.segments = myState.segments;
    }

    private static class LongBigArrayState
            implements Serializable
    {
        private long[][] array;
        private int capacity;
        private int segments;
    }
}
