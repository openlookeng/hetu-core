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
package io.prestosql.orc.reader;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.ShortArrayBlock;
import io.prestosql.spi.block.ShortArrayBlockBuilder;
import io.prestosql.spi.type.Type;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

//TODO: Rajeev: To be combined with LongSelectiveColumnReader
abstract class AbstractLongSelectiveColumnReader<T>
        implements SelectiveColumnReader<T>
{
    protected final boolean outputRequired;
    @Nullable
    protected final Type outputType;

    @Nullable
    protected long[] values;
    @Nullable
    protected boolean[] nulls;
    @Nullable
    protected int[] outputPositions;
    protected int outputPositionCount;

    protected AbstractLongSelectiveColumnReader(Optional<Type> outputType)
    {
        this.outputRequired = outputType.isPresent();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block<T> mergeBlocks(List<Block<T>> blocks, int positionCount)
    {
        if (outputType == BIGINT) {
            LongArrayBlockBuilder blockBuilder = new LongArrayBlockBuilder(null, positionCount);
            blocks.stream().forEach(block -> {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeLong(block.getLong(i, 0));
                    }
                }
            });

            return blockBuilder.build();
        }

        if (outputType == INTEGER) {
            IntArrayBlockBuilder blockBuilder = new IntArrayBlockBuilder(null, positionCount);
            blocks.stream().forEach(block -> {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeInt(block.getInt(i, 0));
                    }
                }
            });

            return blockBuilder.build();
        }

        if (outputType == SMALLINT) {
            ShortArrayBlockBuilder blockBuilder = new ShortArrayBlockBuilder(null, positionCount);
            blocks.stream().forEach(block -> {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    if (block.isNull(i)) {
                        blockBuilder.appendNull();
                    }
                    else {
                        blockBuilder.writeShort(block.getShort(i, 0));
                    }
                }
            });

            return blockBuilder.build();
        }

        throw new UnsupportedOperationException("Unsupported type: " + outputType);
    }

    protected Block buildOutputBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        if (outputType == BIGINT) {
            return getLongArrayBlock(positions, positionCount, includeNulls);
        }

        if (outputType == INTEGER || outputType == DATE) {
            return getIntArrayBlock(positions, positionCount, includeNulls);
        }

        if (outputType == SMALLINT) {
            return getShortArrayBlock(positions, positionCount, includeNulls);
        }

        throw new UnsupportedOperationException("Unsupported type: " + outputType);
    }

    private Block getLongArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        if (positionCount == outputPositionCount) {
            LongArrayBlock block;
            if (includeNulls) {
                block = new LongArrayBlock(positionCount, Optional.ofNullable(nulls), values);
                nulls = null;
            }
            else {
                block = new LongArrayBlock(positionCount, Optional.empty(), values);
            }
            values = null;
            return block;
        }

        long[] valuesCopy = new long[positionCount];
        boolean[] nullsCopy = null;

        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[positionIndex] = this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new LongArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    private Block getIntArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        int[] valuesCopy = new int[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        // Consider for current column, position details are as below:
        //        p[0] 3
        //        p[1] 5
        //        p[2] 7
        //        p[3] 9
        // And input positions are as below:
        //        p[0] 5
        //        p[1] 9
        // So we should loop on current column position count to find appropriate position index and then corresponding
        // to that index get the value from the value array values.
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[positionIndex] = toIntExact(this.values[i]);
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new IntArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    private Block getShortArrayBlock(int[] positions, int positionCount, boolean includeNulls)
    {
        short[] valuesCopy = new short[positionCount];
        boolean[] nullsCopy = null;
        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[positionIndex] = (short) this.values[i];
            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }

            nextPosition = positions[positionIndex];
        }

        return new ShortArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    protected void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        if (values == null || values.length < capacity) {
            values = new long[capacity];
        }

        if (recordNulls) {
            if (nulls == null || nulls.length < capacity) {
                nulls = new boolean[capacity];
            }
        }
    }

    protected void ensureOutputPositionsCapacity(int capacity)
    {
        if (outputPositions == null || outputPositions.length < capacity) {
            outputPositions = new int[capacity];
        }
    }
}
