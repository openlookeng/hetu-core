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
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public class LongSelectiveColumnReader
        extends AbstractLongSelectiveColumnReader<Long>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongSelectiveColumnReader.class).instanceSize();

    private final OrcColumn streamDescriptor;
    @Nullable
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private int readOffset;

    private boolean allNulls;

    private LocalMemoryContext systemMemoryContext;

    public LongSelectiveColumnReader(
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            LocalMemoryContext systemMemoryContext)
    {
        super(outputType);
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");

        nullsAllowed = this.filter == null || this.filter.testNull();
    }

    // Read values as per the given position. Below are input.
    // offset: Starting offset to read data.
    // positions: Array of position from which data to be read;
    // positionCount: Number of position in the positions array.
    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        // If this column is not part of projection, then value will not be stored.
        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        // If there is filter on this column then require space to store position. Number of position can not be more
        // positionCount
        if (this.filter != null) {
            ensureOutputPositionsCapacity(positionCount);
        }
        else {
            // If no filter means no rows will be filtered and hence positions will remains same as input.
            outputPositions = positions;
        }

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition = 0;
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
        }
        else {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    // If current streamPosition is behind the first element to be read, then skip difference.
                    skip(position - streamPosition);
                    streamPosition = position;
                }

                if (presentStream != null && !presentStream.nextBit()) {
                    if (nullsAllowed) {
                        if (outputRequired) {
                            nulls[outputPositionCount] = true;
                        }
                        if (this.filter != null) {
                            outputPositions[outputPositionCount] = position;
                        }
                        outputPositionCount++;
                    }
                }
                else {
                    long value = dataStream.next();
                    if (this.filter == null || this.filter.testLong(value)) {
                        if (outputRequired) {
                            values[outputPositionCount] = value;
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

        // At the end, streamPosition may be different from  outputPositionCount as not all element would have
        // qualified the filter.
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

        // If this column is not part of projection, then value will not be stored.
        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        // If there is filter on this column then require space to store position. Number of position can not be more
        // positionCount
        if (filters != null) {
            ensureOutputPositionsCapacity(positionCount);
        }
        else {
            // If no filter means no rows will be filtered and hence positions will remains same as input.
            outputPositions = positions;
        }

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        outputPositionCount = 0;
        int streamPosition = 0;
        boolean checkNulls = filters != null && filters.stream().anyMatch(f -> f.testNull());
        if (dataStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
            if (checkNulls && accumulator != null) {
                accumulator.set(positions[0], streamPosition);
            }
        }
        else {
            for (int i = 0; i < positionCount; i++) {
                int position = positions[i];
                if (position > streamPosition) {
                    // If current streamPosition is behind the first element to be read, then skip difference.
                    skip(position - streamPosition);
                    streamPosition = position;
                }

                if (presentStream != null && !presentStream.nextBit()) {
                    if (nullsAllowed) {
                        if (outputRequired) {
                            nulls[outputPositionCount] = true;
                        }
                        if (filters != null) {
                            outputPositions[outputPositionCount] = position;
                        }
                        if (accumulator != null && checkNulls) {
                            accumulator.set(position);
                        }
                        outputPositionCount++;
                    }
                }
                else {
                    long value = dataStream.next();
                    if ((accumulator != null && accumulator.get(position))
                            || filters == null || filters.size() <= 0 || filters.stream().anyMatch(f -> f.testLong(value))) {
                        if (accumulator != null) {
                            accumulator.set(position);
                        }
                    }

                    if (outputRequired) {
                        values[outputPositionCount] = value;
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

        // At the end, streamPosition may be different from  outputPositionCount as not all element would have
        // qualified the filter.
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

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public Block getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        return buildOutputBlock(positions, positionCount, nullsAllowed && presentStream != null);
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dataStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);
        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        values = null;
        nulls = null;
        outputPositions = null;
        dataStreamSource = null;
        dataStream = null;
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(values) + sizeOf(nulls) + sizeOf(outputPositions);
    }
}
