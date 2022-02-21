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
package io.prestosql.spi.block;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoSerializable;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.util.BloomFilter;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.compactArray;
import static io.prestosql.spi.block.BlockUtil.compactOffsets;
import static io.prestosql.spi.block.BlockUtil.compactSlice;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock<byte[]>
        implements KryoSerializable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize();

    protected int arrayOffset;
    private int positionCount;
    private Slice slice;
    protected int[] offsets;
    @Nullable
    protected boolean[] valueIsNull;

    private long retainedSizeInBytes;
    private long sizeInBytes;
    private boolean isInitialized;

    public VariableWidthBlock()
    {
        arrayOffset = 0;
    }

    public VariableWidthBlock(int positionCount, Slice slice, int[] offsets, Optional<boolean[]> valueIsNull)
    {
        this(0, positionCount, slice, offsets, valueIsNull.orElse(null));
    }

    VariableWidthBlock(int arrayOffset, int positionCount, Slice slice, int[] offsets, boolean[] valueIsNull)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        this.arrayOffset = arrayOffset;
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (slice == null) {
            throw new IllegalArgumentException("slice is null");
        }
        this.slice = slice;

        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = offsets[arrayOffset + positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);

        this.isInitialized = true;
    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets[position + arrayOffset];
    }

    @Override
    public int getSliceLength(int position)
    {
        checkReadablePosition(position);
        return getPositionOffset(position + 1) - getPositionOffset(position);
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    protected boolean isEntryNull(int position)
    {
        return valueIsNull != null && valueIsNull[position + arrayOffset];
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return offsets[arrayOffset + position + length] - offsets[arrayOffset + position] + ((Integer.BYTES + Byte.BYTES) * (long) length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        long finalSizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                finalSizeInBytes += offsets[arrayOffset + i + 1] - offsets[arrayOffset + i];
            }
        }
        return finalSizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(slice, slice.getRetainedSize());
        consumer.accept(offsets, sizeOf(offsets));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        int finalLength = 0;
        for (int i = offset; i < offset + length; i++) {
            finalLength += getSliceLength(positions[i]);
        }
        SliceOutput newSlice = Slices.allocate(finalLength).getOutput();
        int[] newOffsets = new int[length + 1];
        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }

        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            if (!isEntryNull(position)) {
                newSlice.writeBytes(slice, getPositionOffset(position), getSliceLength(position));
            }
            else if (newValueIsNull != null) {
                newValueIsNull[i] = true;
            }
            newOffsets[i + 1] = newSlice.size();
        }
        return new VariableWidthBlock(0, length, newSlice.slice(), newOffsets, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        return slice;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        return new VariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        int finalPositionOffset = positionOffset + arrayOffset;

        int[] newOffsets = compactOffsets(offsets, finalPositionOffset, length);
        Slice newSlice = compactSlice(slice, offsets[finalPositionOffset], newOffsets[length]);
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, finalPositionOffset, length);

        if (newOffsets == offsets && newSlice == slice && newValueIsNull == valueIsNull) {
            return this;
        }
        return new VariableWidthBlock(0, length, newSlice, newOffsets, newValueIsNull);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("VariableWidthBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append(", slice=").append(slice);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean[] filter(BloomFilter filter, boolean[] validPositions)
    {
        for (int i = 0; i < positionCount; i++) {
            int pos = i + arrayOffset;
            if (valueIsNull != null && valueIsNull[pos]) {
                validPositions[i] = validPositions[i] && filter.test((byte[]) null);
            }
            else {
                byte[] bytes = slice.slice(offsets[pos], offsets[pos + 1] - offsets[pos]).getBytes();
                validPositions[i] = validPositions[i] && filter.test(bytes);
            }
        }
        return validPositions;
    }

    @Override
    public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test)
    {
        int matchCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull != null && valueIsNull[positions[i] + arrayOffset]) {
                if (test.apply(null)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
            else {
                byte[] value = slice.slice(offsets[positions[i] + arrayOffset], offsets[positions[i] + arrayOffset + 1] - offsets[positions[i] + arrayOffset]).getBytes();
                if (test.apply(value)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
        }

        return matchCount;
    }

    @Override
    public byte[] get(int position)
    {
        if (valueIsNull != null && valueIsNull[position + arrayOffset]) {
            return null;
        }
        return slice.slice(offsets[position + arrayOffset], offsets[position + arrayOffset + 1] - offsets[position + arrayOffset]).getBytes();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        VariableWidthBlock other = (VariableWidthBlock) obj;
        return Objects.equals(this.arrayOffset, other.arrayOffset) &&
                Objects.equals(this.positionCount, other.positionCount) &&
                Objects.equals(this.slice, other.slice) &&
                Arrays.equals(this.offsets, other.offsets) &&
                Arrays.equals(this.valueIsNull, other.valueIsNull) &&
                Objects.equals(this.retainedSizeInBytes, other.retainedSizeInBytes) &&
                Objects.equals(this.sizeInBytes, other.sizeInBytes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arrayOffset, positionCount, slice, Arrays.hashCode(offsets), Arrays.hashCode(valueIsNull), retainedSizeInBytes, sizeInBytes);
    }

    @Override
    public void write(Kryo kryo, Output output)
    {
        /* # of positions
        *  [Length Per Position]
        *  [nulls as bits]
        *  [buffer]
        */
        output.write(getPositionCount());
        output.writeInts(offsets, arrayOffset, positionCount + 1);
        output.writeBoolean(mayHaveNull());
        if (mayHaveNull()) {
            output.writeBooleans(valueIsNull, 0, positionCount);
        }
        output.write(offsets[arrayOffset + positionCount] - offsets[arrayOffset]);
        output.write(slice.byteArray(), offsets[arrayOffset],
                offsets[arrayOffset + positionCount] - offsets[arrayOffset]);
    }

    @Override
    public void read(Kryo kryo, Input input)
    {
        if (isInitialized) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Already initialized block");
        }

        positionCount = input.read();
        offsets = input.readInts(positionCount + 1);
        if (input.readBoolean()) {
            valueIsNull = input.readBooleans(positionCount);
        }
        int blockSize = input.read();
        slice = Slices.wrappedBuffer(input.readBytes(blockSize));

        isInitialized = true;
        sizeInBytes = offsets[arrayOffset + positionCount] - offsets[arrayOffset] + ((Integer.BYTES + Byte.BYTES) * (long) positionCount);
        retainedSizeInBytes = INSTANCE_SIZE + slice.getRetainedSize() + sizeOf(valueIsNull) + sizeOf(offsets);
    }
}
