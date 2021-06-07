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
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ByteArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public class BooleanSelectiveColumnReader
        implements SelectiveColumnReader<Byte>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BooleanSelectiveColumnReader.class).instanceSize();

    private final OrcColumn columnDesc;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<BooleanInputStream> dataStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;
    @Nullable
    private byte[] values;
    @Nullable
    private boolean[] nulls;
    @Nullable
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;

    private LocalMemoryContext systemMemoryContext;

    public BooleanSelectiveColumnReader(
            OrcColumn columnDesc,
            Optional<TupleDomainFilter> filter,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        requireNonNull(filter, "filter is null");
        this.columnDesc = requireNonNull(columnDesc, "stream is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nullsAllowed = this.filter == null || this.filter.testNull();
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(BooleanInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(columnDesc, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(columnDesc, DATA, BooleanInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(columnDesc)
                .toString();
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        nulls = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        if (this.filter != null) {
            ensureOutputPositionsCapacity(positionCount);
        }
        else {
            outputPositions = positions;
        }

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else if (this.filter == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            outputPositionCount = 0;
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
                    boolean value = dataStream.nextBit();
                    if (this.filter == null || this.filter.testBoolean(value)) {
                        if (outputRequired) {
                            values[outputPositionCount] = (byte) (value ? 1 : 0);
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
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    @Override
    public int readOr(int offset, int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator) throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        if (filters != null) {
            ensureOutputPositionsCapacity(positionCount);
        }
        else {
            outputPositions = positions;
        }

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        boolean checkNulls = filters != null && filters.stream().anyMatch(f -> f.testNull());
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
            if (checkNulls && accumulator != null) {
                accumulator.set(positions[0], streamPosition);
            }
        }
        else if (filters == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else {
            outputPositionCount = 0;
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
                    boolean value = dataStream.nextBit();
                    if ((accumulator != null && accumulator.get(position))
                            || filters == null || filters.stream().anyMatch(f -> f.testBoolean(value))) {
                        if (accumulator != null) {
                            accumulator.set(position);
                        }
                    }

                    if (outputRequired) {
                        values[outputPositionCount] = (byte) (value ? 1 : 0);
                        if (nullsAllowed && presentStream != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }

                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
                streamPosition++;
            }
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);

        if (nullsAllowed) {
            outputPositionCount = positionCount;
            if (outputPositions != positions) {
                System.arraycopy(positions, 0, outputPositions, 0, outputPositionCount);
            }
            allNulls = true;
        }
        else {
            outputPositionCount = 0;
        }

        return positions[positionCount - 1] + 1;
    }

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        // filter == null implies outputRequired == true
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
                values[i] = (byte) (dataStream.nextBit() ? 1 : 0);
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
        }
    }

    private void ensureValuesCapacity(int capacity, boolean recordNulls)
    {
        if (values == null || values.length < capacity) {
            values = new byte[capacity];
        }

        if (recordNulls) {
            if (nulls == null || nulls.length < capacity) {
                nulls = new boolean[capacity];
            }
        }
    }

    private void ensureOutputPositionsCapacity(int capacity)
    {
        if (outputPositions == null || outputPositions.length < capacity) {
            outputPositions = new int[capacity];
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(BOOLEAN.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        if (positionCount == outputPositionCount) {
            Block block = new ByteArrayBlock(positionCount, Optional.ofNullable(nulls), values);
            nulls = null;
            values = null;
            return block;
        }

        byte[] valuesCopy = new byte[positionCount];
        boolean[] nullsCopy = null;
        if (nullsAllowed && presentStream != null) {
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

        return new ByteArrayBlock(positionCount, Optional.ofNullable(nullsCopy), valuesCopy);
    }

    @Override
    public Block<Byte> mergeBlocks(List<Block<Byte>> blocks, int positionCount)
    {
        byte[] valuesCopy = new byte[positionCount];
        boolean[] nullsCopy = new boolean[positionCount];
        AtomicInteger index = new AtomicInteger(0);
        blocks.stream().forEach(block -> {
            for (int i = 0; i < block.getPositionCount(); i++) {
                nullsCopy[index.get()] = block.isNull(i);
                valuesCopy[index.getAndIncrement()] = block.getByte(i, 0);
            }
        });

        return new ByteArrayBlock(positionCount, Optional.empty(), valuesCopy);
    }
}
