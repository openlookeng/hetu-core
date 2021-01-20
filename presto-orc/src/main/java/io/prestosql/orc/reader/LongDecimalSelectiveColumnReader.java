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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.UnsafeSlice;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static io.prestosql.spi.type.UnscaledDecimal128Arithmetic.rescale;

public class LongDecimalSelectiveColumnReader
        extends AbstractDecimalSelectiveColumnReader<long[]>
{
    public LongDecimalSelectiveColumnReader(
            OrcType orcType,
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            LocalMemoryContext systemMemoryContext)
    {
        super(orcType, streamDescriptor, filter, outputType, systemMemoryContext, 2);
    }

    @Override
    protected int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        long[] data = new long[2];
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
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
                dataStream.nextLongDecimal(data, 1);
                Slice decimal = Slices.wrappedLongArray(data[0], data[1]);
                rescale(decimal, this.scale - (int) scaleStream.next(), rescaledDecimal);
                values[2 * i] = UnsafeSlice.getLongUnchecked(rescaledDecimal, 0);
                values[2 * i + 1] = UnsafeSlice.getLongUnchecked(rescaledDecimal, Long.BYTES);
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
        long[] data = new long[2];
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
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
                int scale = (int) scaleStream.next();
                dataStream.nextLongDecimal(data, 1);
                Slice decimal = Slices.wrappedLongArray(data[0], data[1]);
                rescale(decimal, this.scale - scale, rescaledDecimal);
                long low = UnsafeSlice.getLongUnchecked(rescaledDecimal, 0);
                long high = UnsafeSlice.getLongUnchecked(rescaledDecimal, Long.BYTES);
                if (filters.get(0).testDecimal(low, high)) {
                    if (outputRequired) {
                        values[2 * outputPositionCount] = low;
                        values[2 * outputPositionCount + 1] = high;
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
        long[] data = new long[2];
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
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
                int scale = (int) scaleStream.next();
                dataStream.nextLongDecimal(data, 1);
                Slice decimal = Slices.wrappedLongArray(data[0], data[1]);
                rescale(decimal, this.scale - scale, rescaledDecimal);
                long low = UnsafeSlice.getLongUnchecked(rescaledDecimal, 0);
                long high = UnsafeSlice.getLongUnchecked(rescaledDecimal, Long.BYTES);
                if ((accumulator != null && accumulator.get(position))
                        || filters == null || filters.stream().anyMatch(f -> f.testDecimal(low, high))) {
                    if (accumulator != null) {
                        accumulator.set(position);
                    }
                }

                if (outputRequired) {
                    values[2 * outputPositionCount] = low;
                    values[2 * outputPositionCount + 1] = high;
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
    protected void copyValues(int[] positions, int positionsCount, long[] valuesCopy, boolean[] nullsCopy)
    {
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[2 * positionIndex] = this.values[2 * i];
            valuesCopy[2 * positionIndex + 1] = this.values[2 * i + 1];

            if (nullsCopy != null) {
                nullsCopy[positionIndex] = this.nulls[i];
            }
            positionIndex++;

            if (positionIndex >= positionsCount) {
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

            values[2 * positionIndex] = values[2 * i];
            values[2 * positionIndex + 1] = values[2 * i + 1];
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
    protected Block<long[]> makeBlock(int positionCount, boolean includeNulls, boolean[] nulls, long[] values)
    {
        return new Int128ArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
    }
}
