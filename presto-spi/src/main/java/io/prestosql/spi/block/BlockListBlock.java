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
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Objects;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BlockListBlock<T>
        implements Block<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayBlock.class).instanceSize();
    private final Block[] blocks;
    private final int blockCount;
    private final int positionCount;
    private final int[] positionMap;

    private volatile long sizeInBytes;
    private final long retainedSizeInBytes;
    private final boolean hasNulls;

    public BlockListBlock(Block[] blocks, int blockCount, int positionCount)
    {
        this.blocks = requireNonNull(blocks, "Block array list cannot be null");
        if (blocks.length == 0 || blocks.length < blockCount || blockCount < 0) {
            throw new IllegalArgumentException("Block length cannot be 0 and blockCount count cannot be less than 0");
        }
        this.blockCount = blockCount;
        this.positionCount = positionCount;
        this.positionMap = new int[blockCount + 1];

        long finalRetainedSizeInBytes = INSTANCE_SIZE + sizeOf(blocks);
        int positionIdx = 0;
        this.positionMap[0] = 0;
        boolean finalHasNulls = false;

        String blockEncodingName = blocks[0].getEncodingName();
        for (int i = 0; i < blockCount; i++) {
            Block block = blocks[i];
            if (!block.getEncodingName().equalsIgnoreCase(blockEncodingName)) {
                throw new IllegalArgumentException("All blocks should be of same type");
            }

            finalRetainedSizeInBytes += block.getRetainedSizeInBytes();
            positionIdx += block.getPositionCount();
            this.positionMap[i + 1] = positionIdx;

            finalHasNulls = finalHasNulls || block.mayHaveNull();
        }

        this.hasNulls = finalHasNulls;

        if (positionIdx != positionCount) {
            throw new IllegalArgumentException("positionCount not matching the positions in the block");
        }

        this.retainedSizeInBytes = finalRetainedSizeInBytes;
    }

    private int lookupBlockForPosition(int position)
    {
        checkValidPosition(position);

        for (int i = 0; i < blockCount; i++) {
            if (position < positionMap[i + 1]) {
                return i;
            }
        }

        throw new IllegalArgumentException("Not found");
    }

    private void checkValidPosition(int position)
    {
        if (position < 0 || position >= this.positionCount) {
            throw new IllegalArgumentException("positionCount out of bounds");
        }
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        int blockIdx = lookupBlockForPosition(position);
        blockBuilder.appendStructureInternal(blocks[blockIdx], position - positionMap[blockIdx]);
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getSingleValueBlock(position - positionMap[blockIdx]);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public synchronized long getSizeInBytes()
    {
        if (sizeInBytes < 0) {
            for (int i = 0; i < blockCount; i++) {
                sizeInBytes += blocks[i].getSizeInBytes();
            }
        }

        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getRegionSizeInBytes(position - positionMap[blockIdx], length);
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        checkValidPosition(positions.length);
        long totalSize = 0;
        int positionIdx = 0;
        boolean[] used;
        Block block;

        for (int i = 0; i < blockCount; i++) {
            block = blocks[i];
            used = new boolean[block.getPositionCount()];
            System.arraycopy(positions, positionIdx, used, 0, used.length);

            totalSize += block.getPositionsSizeInBytes(used);
        }

        return totalSize;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getEstimatedDataSizeForStats(position - positionMap[blockIdx]);
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        for (int i = 0; i < blockCount; i++) {
            Block block = blocks[i];
            consumer.accept(block, block.getRetainedSizeInBytes());
        }

        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return blocks[0].getEncodingName();
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        throw new IllegalArgumentException("Not Supported");
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        if (positionOffset < 0 || positionOffset + length > positionCount) {
            throw new IllegalArgumentException("Not Supported");
        }

        int finalLength = length;
        int blockIdx = lookupBlockForPosition(positionOffset);
        Block[] newBlocks = new Block[blockCount];
        int newBlockCount = 0;
        int newPositionCount = 0;
        Block block;

        for (int i = blockIdx; i < blockCount && finalLength > 0; i++) {
            block = blocks[i];

            /* if region is within the block return the block else wrap the candidate blocks */
            if (block.getPositionCount() > (positionOffset - positionMap[blockIdx]) + finalLength) {
                newBlocks[newBlockCount++] = block.getRegion(positionOffset - positionMap[blockIdx], finalLength);
                newPositionCount += finalLength;
                finalLength -= finalLength;
            }
            else {
                newBlocks[newBlockCount++] = block;
                newPositionCount += block.getPositionCount();
                finalLength -= block.getPositionCount();
            }
        }

        if (newBlockCount <= 1) {
            return newBlocks[0];
        }

        return new BlockListBlock(newBlocks, newBlockCount, newPositionCount);
    }

    @Override
    public Block copyRegion(int position, int length)
    {
        throw new IllegalArgumentException("Not Supported");
    }

    @Override
    public boolean isNull(int position)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].isNull(position - positionMap[blockIdx]);
    }

    @Override
    public boolean mayHaveNull()
    {
        return hasNulls;
    }

    @Override
    public byte getByte(int position, int offset)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getByte(position - positionMap[blockIdx], offset);
    }

    @Override
    public int getSliceLength(int position)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getSliceLength(position - positionMap[blockIdx]);
    }

    @Override
    public Slice getSlice(int position, int offset, int length)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getSlice(position - positionMap[blockIdx], offset, length);
    }

    @Override
    public short getShort(int position, int offset)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getShort(position - positionMap[blockIdx], offset);
    }

    @Override
    public int getInt(int position, int offset)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getInt(position - positionMap[blockIdx], offset);
    }

    @Override
    public long getLong(int position, int offset)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getLong(position - positionMap[blockIdx], offset);
    }

    @Override
    public String getString(int position, int offset, int length)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].getString(position - positionMap[blockIdx], offset, length);
    }

    @Override
    public <R> R getObject(int position, Class<R> clazz)
    {
        int blockIdx = lookupBlockForPosition(position);
        return (R) blocks[blockIdx].getObject(position - positionMap[blockIdx], clazz);
    }

    @Override
    public boolean bytesEqual(int position, int offset, Slice otherSlice, int otherOffset, int length)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].bytesEqual(position - positionMap[blockIdx], offset,
                otherSlice, otherOffset, length);
    }

    @Override
    public int bytesCompare(int position, int offset, int length, Slice otherSlice, int otherOffset, int otherLength)
    {
        int blockIdx = lookupBlockForPosition(position);
        return blocks[blockIdx].bytesCompare(position - positionMap[blockIdx], offset, length,
                otherSlice, otherOffset, otherLength);
    }

    @Override
    public T get(int position)
    {
        int blockIdx = lookupBlockForPosition(position);
        return (T) blocks[blockIdx].get(position - positionMap[blockIdx]);
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
        BlockListBlock other = (BlockListBlock) obj;
        return Objects.equals(this.INSTANCE_SIZE, other.INSTANCE_SIZE) &&
                Arrays.equals(this.blocks, other.blocks) &&
                Objects.equals(this.blockCount, other.blockCount) &&
                Objects.equals(this.positionCount, other.positionCount) &&
                Arrays.equals(this.positionMap, other.positionMap) &&
                Objects.equals(this.sizeInBytes, other.sizeInBytes) &&
                Objects.equals(this.retainedSizeInBytes, other.retainedSizeInBytes) &&
                Objects.equals(this.hasNulls, other.hasNulls);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(INSTANCE_SIZE, Arrays.hashCode(blocks), blockCount, positionCount, Arrays.hashCode(positionMap), sizeInBytes, retainedSizeInBytes, hasNulls);
    }
}
