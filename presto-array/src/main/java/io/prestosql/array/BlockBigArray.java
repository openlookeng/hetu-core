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

import io.prestosql.spi.block.Block;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.snapshot.Restorable;
import io.prestosql.spi.snapshot.RestorableConfig;
import org.openjdk.jol.info.ClassLayout;

import java.io.Serializable;

@RestorableConfig(uncapturedFields = {"trackedObjects"})
public final class BlockBigArray
        implements Restorable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BlockBigArray.class).instanceSize();
    private final ObjectBigArray<Block> array;
    private final ReferenceCountMap trackedObjects = new ReferenceCountMap();
    private long sizeOfBlocks;

    public BlockBigArray()
    {
        array = new ObjectBigArray<>();
    }

    public BlockBigArray(Block block)
    {
        array = new ObjectBigArray<>(block);
    }

    /**
     * Returns the size of this big array in bytes.
     */
    public long sizeOf()
    {
        return INSTANCE_SIZE + array.sizeOf() + sizeOfBlocks + trackedObjects.sizeOf();
    }

    /**
     * Returns the element of this big array at specified index.
     *
     * @param index a position in this big array.
     * @return the element of this big array at the specified position.
     */
    public Block get(long index)
    {
        return array.get(index);
    }

    /**
     * Sets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public <T> void set(long index, Block<T> value)
    {
        Block<T> currentValue = array.get(index);
        if (currentValue != null) {
            currentValue.retainedBytesForEachPart((object, size) -> {
                if (currentValue == object) {
                    // track instance size separately as the reference count for an instance is always 1
                    sizeOfBlocks -= size;
                    return;
                }
                if (trackedObjects.decrementAndGet(object) == 0) {
                    // decrement the size only when it is the last reference
                    sizeOfBlocks -= size;
                }
            });
        }
        if (value != null) {
            value.retainedBytesForEachPart((object, size) -> {
                if (value == object) {
                    // track instance size separately as the reference count for an instance is always 1
                    sizeOfBlocks += size;
                    return;
                }
                if (trackedObjects.incrementAndGet(object) == 1) {
                    // increment the size only when it is the first reference
                    sizeOfBlocks += size;
                }
            });
        }
        array.set(index, value);
    }

    /**
     * Resets the element of this big array at specified index.
     *
     * @param index a position in this big array.
     */
    public <T> void reset(long index)
    {
        Block<T> currentValue = array.get(index);
        if (currentValue != null) {
            currentValue.retainedBytesForEachPart((object, size) -> {
                if (currentValue == object) {
                    // track instance size separately as the reference count for an instance is always 1
                    sizeOfBlocks -= size;
                    return;
                }
                if (trackedObjects.decrementAndGet(object) == 0) {
                    // decrement the size only when it is the last reference
                    sizeOfBlocks -= size;
                }
            });
        }
        array.reset(index);
    }

    /**
     * Ensures this big array is at least the specified length.  If the array is smaller, segments
     * are added until the array is larger then the specified length.
     */
    public void ensureCapacity(long length)
    {
        array.ensureCapacity(length);
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        BlockBigArrayState myState = new BlockBigArrayState();
        myState.array = array.captureBlockBigArray(serdeProvider);
        myState.sizeOfBlocks = sizeOfBlocks;
        return myState;
    }

    @Override
    public void restore(Object state, BlockEncodingSerdeProvider serdeProvider)
    {
        BlockBigArrayState myState = (BlockBigArrayState) state;
        this.array.restoreBlockBigArray(myState.array, serdeProvider);
        this.sizeOfBlocks = myState.sizeOfBlocks;
    }

    private static class BlockBigArrayState
            implements Serializable
    {
        private Object array;
        private long sizeOfBlocks;
    }
}
