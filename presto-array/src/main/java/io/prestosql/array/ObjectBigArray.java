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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOfObjectArray;

// Note: this code was forked from fastutil (http://fastutil.di.unimi.it/)
// Copyright (C) 2010-2013 Sebastiano Vigna
public final class ObjectBigArray<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ObjectBigArray.class).instanceSize();
    private static final long SIZE_OF_SEGMENT = sizeOfObjectArray(BigArrays.SEGMENT_SIZE);

    private final Object initialValue;

    private Object[][] array;
    private int capacity;
    private int segments;

    /**
     * Creates a new big array containing one initial segment
     */
    public ObjectBigArray()
    {
        this(null);
    }

    public ObjectBigArray(Object initialValue)
    {
        this.initialValue = initialValue;
        array = new Object[BigArrays.INITIAL_SEGMENTS][];
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
    @SuppressWarnings("unchecked")
    public T get(long index)
    {
        return (T) array[BigArrays.segment(index)][BigArrays.offset(index)];
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public void set(long index, T value)
    {
        array[BigArrays.segment(index)][BigArrays.offset(index)] = value;
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
        Object[] newSegment = new Object[BigArrays.SEGMENT_SIZE];
        if (initialValue != null) {
            Arrays.fill(newSegment, initialValue);
        }
        array[segments] = newSegment;
        capacity += BigArrays.SEGMENT_SIZE;
        segments++;
    }

    /**
     * BlockBigArray utilizes ObjectBigArray internally. In actual use cases, BlockBigArray stores
     * the same Block instance in many different locations, so we only serialize a new Block once
     * and record its location of appearances to be more efficient.
     */
    //Consecutive appearances of same block on row row, from column startCol to column endCol
    private static class Location
            implements Serializable
    {
        private final int start;
        private final int end;

        private Location(int start, int end)
        {
            this.start = start;
            this.end = end;
        }
    }

    private static class BlockObjectBigArrayState
    {
        private Map<byte[], List<Location>> array;
        private int capacity;
    }

    public Object captureBlockBigArray(BlockEncodingSerdeProvider serdeProvider)
    {
        BlockObjectBigArrayState myState = new BlockObjectBigArrayState();
        myState.capacity = capacity;
        myState.array = new HashMap<>();
        Map<Block, List<Location>> blockLocationMap = new HashMap<>();
        int start = -1;
        Block oldBlock = null;

        for (int i = 0; i < capacity; i++) {
            Block currentBlock = (Block) get(i);
            if (currentBlock != null) {
                //start new chain
                if (oldBlock == null) {
                    start = i;
                    oldBlock = currentBlock;
                }
                //end previous chain and start new chain
                else if (!oldBlock.equals(currentBlock)) {
                    storeBlockLocation(blockLocationMap, oldBlock, start, i);
                    start = i;
                    oldBlock = currentBlock;
                }
            }
            else {
                //end previous chain
                if (oldBlock != null) {
                    storeBlockLocation(blockLocationMap, oldBlock, start, i);
                    oldBlock = null;
                    start = -1;
                }
            }
        }

        //chain continues all the way to the end
        if (oldBlock != null) {
            storeBlockLocation(blockLocationMap, oldBlock, start, capacity);
        }

        for (Map.Entry<Block, List<Location>> entry : blockLocationMap.entrySet()) {
            myState.array.put(serializeBlock(entry.getKey(), serdeProvider.getBlockEncodingSerde()), entry.getValue());
        }

        return myState;
    }

    public void restoreBlockBigArray(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BlockObjectBigArrayState myState = (BlockObjectBigArrayState) state;
        array = new Object[BigArrays.INITIAL_SEGMENTS][];
        this.segments = 0;
        this.capacity = 0;
        this.ensureCapacity(myState.capacity);

        for (Map.Entry<byte[], List<Location>> entry : myState.array.entrySet()) {
            Slice inputSlice = Slices.wrappedBuffer(entry.getKey());
            Block block = serdeProvider.getBlockEncodingSerde().readBlock(inputSlice.getInput());
            for (int i = 0; i < entry.getValue().size(); i++) {
                Location location = entry.getValue().get(i);
                for (int j = location.start; j < location.end; j++) {
                    set(j, (T) block);
                }
            }
        }
    }

    private void storeBlockLocation(Map<Block, List<Location>> blockLocationMap, Block block, int start, int end)
    {
        //Seen this block previously, save its new chain of appearances.
        if (blockLocationMap.containsKey(block)) {
            blockLocationMap.get(block).add(new Location(start, end));
        }
        //Never seen this block, create new map entry.
        else {
            blockLocationMap.put(block, new ArrayList<>(Collections.singletonList(new Location(start, end))));
        }
    }

    private byte[] serializeBlock(Block block, BlockEncodingSerde serde)
    {
        SliceOutput output = new DynamicSliceOutput(0);
        serde.writeBlock(output, block);
        return output.getUnderlyingSlice().getBytes();
    }

    public Object capture(Function<Object, Object> captureFunction)
    {
        ObjectBigArrayState myState = new ObjectBigArrayState();
        myState.array = new Object[array.length][];
        for (int i = 0; i < array.length; i++) {
            if (array[i] != null) {
                myState.array[i] = new Object[array[i].length];
                for (int j = 0; j < array[i].length; j++) {
                    if (array[i][j] != null) {
                        myState.array[i][j] = captureFunction.apply(array[i][j]);
                    }
                }
            }
        }
        myState.capacity = capacity;
        myState.segments = segments;
        return myState;
    }

    public void restore(Function<Object, Object> restoreFunction, Object state)
    {
        ObjectBigArrayState myState = (ObjectBigArrayState) state;
        Object[][] restoredArray = new Object[myState.array.length][];
        for (int i = 0; i < myState.array.length; i++) {
            if (myState.array[i] != null) {
                restoredArray[i] = new Object[myState.array[i].length];
                for (int j = 0; j < myState.array[i].length; j++) {
                    if (myState.array[i][j] != null) {
                        restoredArray[i][j] = restoreFunction.apply(myState.array[i][j]);
                    }
                }
            }
        }
        this.array = restoredArray;
        this.capacity = myState.capacity;
        this.segments = myState.segments;
    }

    private static class ObjectBigArrayState
            implements Serializable
    {
        private Object[][] array;
        private int capacity;
        private int segments;
    }
}
