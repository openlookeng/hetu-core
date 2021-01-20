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

import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

public class ShortDecimalSelectiveColumnReader
        extends AbstractDecimalSelectiveColumnReader<Short>
{
    public ShortDecimalSelectiveColumnReader(
            OrcType orcType,
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            LocalMemoryContext systemMemoryContext)
    {
        super(orcType, streamDescriptor, filter, outputType, systemMemoryContext, 1);
    }

    @Override
    protected int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        long[] data = new long[1];
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }
            if (presentStream != null && !presentStream.nextBit()) {
                nulls[i] = true;
            }
            else {
                dataStream.nextShortDecimal(data, 1);
                values[i] = Decimals.rescale(data[0], (int) scaleStream.next(), this.scale);
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    @Override
    protected int readWithFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters)
            throws IOException
    {
        int streamPosition = 0;
        outputPositionCount = 0;
        long[] data = new long[1];
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                dataStream.nextShortDecimal(data, 1);
                long rescale = Decimals.rescale(data[0], (int) scaleStream.next(), this.scale);
                if (filters == null || filters.get(0).testLong(rescale)) {
                    if (outputRequired) {
                        values[outputPositionCount] = rescale;
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }

                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            streamPosition++;
        }
        return streamPosition;
    }

    @Override
    protected int readWithOrFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        int streamPosition = 0;
        outputPositionCount = 0;
        long[] data = new long[1];
        boolean checkNulls = filters != null && filters.stream().anyMatch(f -> f.testNull());
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        nulls[outputPositionCount] = true;
                    }
                    if (accumulator != null && checkNulls) {
                        accumulator.set(position);
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                dataStream.nextShortDecimal(data, 1);
                long rescale = Decimals.rescale(data[0], (int) scaleStream.next(), this.scale);
                if ((accumulator != null && accumulator.get(position))
                        || filters == null || filters.stream().anyMatch(f -> f.testLong(rescale))) {
                    if (accumulator != null) {
                        accumulator.set(position);
                    }
                }

                if (outputRequired) {
                    values[outputPositionCount] = rescale;
                    if (nullsAllowed && presentStream != null) {
                        nulls[outputPositionCount] = false;
                    }
                }

                outputPositions[outputPositionCount] = position;
                outputPositionCount++;
            }
            streamPosition++;
        }
        return streamPosition;
    }

    @Override
    protected void copyValues(int[] positions, int positionCount, long[] valuesCopy, boolean[] nullsCopy)
    {
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
    }

    @Override
    protected void compactValues(int[] positions, int positionCount, boolean compactNulls)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            values[positionIndex] = values[i];
            if (compactNulls) {
                nulls[positionIndex] = nulls[i];
            }
            outputPositions[positionIndex] = nextPosition;

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    protected Block makeBlock(int positionCount, boolean includeNulls, boolean[] nulls, long[] values)
    {
        return new LongArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
    }

    @Override
    public Block<Short> mergeBlocks(List<Block<Short>> blocks, int positionCount)
    {
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
}
