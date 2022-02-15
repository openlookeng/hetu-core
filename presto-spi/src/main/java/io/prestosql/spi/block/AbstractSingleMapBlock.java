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

public abstract class AbstractSingleMapBlock<T>
        implements Block<T>
{
    abstract int getOffset();

    abstract Block getRawKeyBlock();

    abstract Block getRawValueBlock();

    private int getAbsolutePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
        return position + getOffset();
    }

    @Override
    public boolean isNull(int position)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            if (getRawKeyBlock().isNull(absolutePosition / 2)) {
                throw new IllegalStateException("Map key is null");
            }
            return false;
        }
        else {
            return getRawValueBlock().isNull(absolutePosition / 2);
        }
    }

    @Override
    public byte getByte(int position, int offset)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getByte(absolutePosition / 2, offset);
        }
        else {
            return getRawValueBlock().getByte(absolutePosition / 2, offset);
        }
    }

    @Override
    public short getShort(int position, int offset)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getShort(absolutePosition / 2, offset);
        }
        else {
            return getRawValueBlock().getShort(absolutePosition / 2, offset);
        }
    }

    @Override
    public int getInt(int position, int offset)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getInt(absolutePosition / 2, offset);
        }
        else {
            return getRawValueBlock().getInt(absolutePosition / 2, offset);
        }
    }

    @Override
    public long getLong(int position, int offset)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getLong(absolutePosition / 2, offset);
        }
        else {
            return getRawValueBlock().getLong(absolutePosition / 2, offset);
        }
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getSlice(absolutePosition / 2, offset, length);
        }
        else {
            return getRawValueBlock().getSlice(absolutePosition / 2, offset, length);
        }
    }

    @Override
    public String getString(int position, int offset, int length)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getString(absolutePosition / 2, offset, length);
        }

        return getRawValueBlock().getString(absolutePosition / 2, offset, length);
    }

    @Override
    public int getSliceLength(int position)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getSliceLength(absolutePosition / 2);
        }
        else {
            return getRawValueBlock().getSliceLength(absolutePosition / 2);
        }
    }

    @Override
    public int compareTo(int position, int offset, int length, Block otherBlock, int otherPosition, int otherOffset, int otherLength)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().compareTo(absolutePosition / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
        else {
            return getRawValueBlock().compareTo(absolutePosition / 2, offset, length, otherBlock, otherPosition, otherOffset, otherLength);
        }
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().bytesEqual(absolutePosition / 2, offset, otherSlice, otherOffset, length);
        }
        else {
            return getRawValueBlock().bytesEqual(absolutePosition / 2, offset, otherSlice, otherOffset, length);
        }
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().bytesCompare(absolutePosition / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
        else {
            return getRawValueBlock().bytesCompare(absolutePosition / 2, offset, length, otherSlice, otherOffset, otherLength);
        }
    }

    @Override
    public void writeBytesTo(int position, int offset, int length, BlockBuilder blockBuilder)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            getRawKeyBlock().writeBytesTo(absolutePosition / 2, offset, length, blockBuilder);
        }
        else {
            getRawValueBlock().writeBytesTo(absolutePosition / 2, offset, length, blockBuilder);
        }
    }

    @Override
    public boolean equals(int position, int offset, Block otherBlock, int otherPosition, int otherOffset, int length)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().equals(absolutePosition / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
        else {
            return getRawValueBlock().equals(absolutePosition / 2, offset, otherBlock, otherPosition, otherOffset, length);
        }
    }

    @Override
    public long hash(int position, int offset, int length)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().hash(absolutePosition / 2, offset, length);
        }
        else {
            return getRawValueBlock().hash(absolutePosition / 2, offset, length);
        }
    }

    @Override
    public <T> T getObject(int position, Class<T> clazz)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return (T) getRawKeyBlock().getObject(absolutePosition / 2, clazz);
        }
        else {
            return (T) getRawValueBlock().getObject(absolutePosition / 2, clazz);
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            getRawKeyBlock().writePositionTo(absolutePosition / 2, blockBuilder);
        }
        else {
            getRawValueBlock().writePositionTo(absolutePosition / 2, blockBuilder);
        }
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getSingleValueBlock(absolutePosition / 2);
        }
        else {
            return getRawValueBlock().getSingleValueBlock(absolutePosition / 2);
        }
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        int absolutePosition = getAbsolutePosition(position);
        if (absolutePosition % 2 == 0) {
            return getRawKeyBlock().getEstimatedDataSizeForStats(absolutePosition / 2);
        }
        else {
            return getRawValueBlock().getEstimatedDataSizeForStats(absolutePosition / 2);
        }
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new UnsupportedOperationException();
    }
}
