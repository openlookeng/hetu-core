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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.util.BloomFilter;
import nova.hetu.omnicache.vector.VarcharVec;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.compactArray;
import static io.prestosql.spi.block.BlockUtil.compactOffsets;
import static io.prestosql.spi.block.BlockUtil.compactSlice;

public class VariableWidthBlock
        extends AbstractVariableWidthBlock<byte[]>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(VariableWidthBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    private final Slice slice;
    private final int[] offsets;
    protected final VarcharVec varcharVec;
    protected final boolean isVecMode = true;
    @Nullable
    protected final boolean[] valueIsNull;

    protected final long retainedSizeInBytes;
    protected final long sizeInBytes;

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
        if (isVecMode) {
            byte[] data = slice.getBytes();
            this.varcharVec = new VarcharVec(data.length, offsets.length);
            this.varcharVec.setData(data);
            int[] lengths = new int [offsets.length];
            for (int i=0; i< (positionCount +1); i++) {
                if (i < offsets.length -1) {
                    lengths[i] = offsets[i + 1] - offsets[i];
                }
            }
            this.varcharVec.set(offsets, lengths);
        }

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

    }

    public VariableWidthBlock(VarcharVec varcharVec, int[] offsets, int[] lengths, boolean[] valueIsNull)
    {
        this.arrayOffset = 0;
        this.positionCount = 0;
        if (varcharVec == null) {
            throw new IllegalArgumentException("varcharVec is null");
        }
        this.varcharVec = varcharVec;

        for (int i=0; i < offsets.length; i++) {
            this.varcharVec.set(i, offsets[i], lengths[i]);
        }

        this.slice = null;
        if (offsets.length - arrayOffset < (positionCount + 1)) {
            throw new IllegalArgumentException("offsets length is less than positionCount");
        }
        this.offsets = offsets;

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("valueIsNull length is less than positionCount");
        }
        this.valueIsNull = valueIsNull;

        sizeInBytes = varcharVec.capacity();
        retainedSizeInBytes = varcharVec.capacity();

    }

    @Override
    protected final int getPositionOffset(int position)
    {
        return offsets[position + arrayOffset];
    }

    @Override
    public int getSliceLength(int position)
    {
       // System.out.println("GetSlice Length::" + position);
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
        System.out.println("Get position size in bytes:::");
        long sizeInBytes = 0;
        int usedPositionCount = 0;
        for (int i = 0; i < positions.length; ++i) {
            if (positions[i]) {
                usedPositionCount++;
                sizeInBytes += offsets[arrayOffset + i + 1] - offsets[arrayOffset + i];
            }
        }
        return sizeInBytes + (Integer.BYTES + Byte.BYTES) * (long) usedPositionCount;
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
        if(isVecMode) {
            return vecCopyPositions(positions, offset, length);
        }
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

    private Block vecCopyPositions(int[] positions, int offset, int length) {
        int finalLength = 0;
        for (int i = 0; i < positions.length; i++) {
            finalLength += this.varcharVec.getLength(positions[i]);
        }

        VarcharVec newVec = new VarcharVec(finalLength, length);
        int[] offsets = new int[positions.length];
        int[] lengths = new int[positions.length];
        int newOffset = 0;

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }

        for (int i = 0; i < length; i++) {
            int position = positions[i];
            if (!isEntryNull(position)) {
                byte[] data = this.varcharVec.getDataAtOffset(position);
                offsets[i] = newOffset;
                lengths[i] = data.length;
                newVec.setData(newOffset, data);
                newOffset = newOffset + data.length;
            }
            else if (newValueIsNull != null) {
                newValueIsNull[i] = true;
            }
        }
        newVec.set(offsets, lengths);
        return new VariableWidthBlock(newVec, offsets, lengths, newValueIsNull);
    }

    @Override
    protected Slice getRawSlice(int position)
    {
        if (isVecMode) {
            return Slices.wrappedBuffer(varcharVec.getData(0, varcharVec.capacity()));
        }
        return slice;
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if (isVecMode) {
            return getRegionVec(positionOffset, length);
        }
        return new VariableWidthBlock(positionOffset + arrayOffset, length, slice, offsets, valueIsNull);
    }

    private Block getRegionVec(int positionOffset, int length)
    {
        VarcharVec newVec = (VarcharVec) this.varcharVec.slice(positionOffset, positionOffset + length);
        return new VariableWidthBlock(newVec, newVec.getOffsets(), newVec.getLengths(), valueIsNull);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);

        if(isVecMode) {
            return copyRegionVec(positionOffset, length);
        }

        positionOffset += arrayOffset;

        int[] newOffsets = compactOffsets(offsets, positionOffset, length);
        Slice newSlice = compactSlice(slice, offsets[positionOffset], newOffsets[length]);
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);

        if (newOffsets == offsets && newSlice == slice && newValueIsNull == valueIsNull) {
            return this;
        }
        return new VariableWidthBlock(0, length, newSlice, newOffsets, newValueIsNull);
    }

    public Block copyRegionVec(int positionOffset, int length)
    {
        VarcharVec newVec = (VarcharVec) this.varcharVec.slice(positionOffset, positionOffset + length);
        return new VariableWidthBlock(newVec, newVec.getOffsets(), newVec.getLengths(), valueIsNull);
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
            byte[] value = slice.slice(offsets[i + arrayOffset], offsets[i + arrayOffset + 1] - offsets[i + arrayOffset]).getBytes();
            validPositions[i] = validPositions[i] && filter.test(value);
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
        if (isVecMode) {
            System.out.println("Reading data from vector::::");
            return varcharVec.getData(position);
        }
        return slice.slice(offsets[position + arrayOffset], offsets[position + arrayOffset + 1] - offsets[position + arrayOffset]).getBytes();
    }
}
