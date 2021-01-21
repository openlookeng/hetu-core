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

import com.google.common.collect.ImmutableList;
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
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
import static io.prestosql.orc.metadata.Stream.StreamKind.SECONDARY;
import static io.prestosql.orc.reader.ApacheHiveTimestampDecoder.decodeTimestamp;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static java.util.Objects.requireNonNull;

public class TimestampSelectiveColumnReader
        implements SelectiveColumnReader<Long>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampSelectiveColumnReader.class).instanceSize();
    private static final Block NULL_BLOCK = TIMESTAMP.createBlockBuilder(null, 1).appendNull().build();

    private final OrcColumn streamDescriptor;
    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final boolean outputRequired;
    private final LocalMemoryContext systemMemoryContext;
    private final long baseTimestampInSeconds;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<LongInputStream> secondsStreamSource = missingStreamSource(LongInputStream.class);
    private InputStreamSource<LongInputStream> nanosStreamSource = missingStreamSource(LongInputStream.class);

    @Nullable
    private BooleanInputStream presentStream;
    private LongInputStream secondsStream;
    private LongInputStream nanosStream;
    private boolean rowGroupOpen;

    private int readOffset;
    @Nullable
    private long[] values;
    @Nullable
    private boolean[] nulls;
    @Nullable
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean allNulls;
    private boolean valuesInUse;

    public TimestampSelectiveColumnReader(
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            DateTimeZone hiveStorageTimeZone,
            boolean outputRequired,
            LocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputRequired;
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nullsAllowed = this.filter == null || this.filter.testNull();
        this.baseTimestampInSeconds = new DateTime(2015, 1, 1, 0, 0, requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null")).getMillis() / 1000;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        secondsStreamSource = missingStreamSource(LongInputStream.class);
        nanosStreamSource = missingStreamSource(LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        secondsStream = null;
        nanosStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        secondsStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, LongInputStream.class);
        nanosStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        secondsStream = null;
        nanosStream = null;
        rowGroupOpen = false;
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
        secondsStream = secondsStreamSource.openStream();
        nanosStream = nanosStreamSource.openStream();
        rowGroupOpen = true;
    }

    @Override
    public int read(int offset, int[] positions, int positionCount, TupleDomainFilter filter)
            throws IOException
    {
        return readOr(offset, positions, positionCount,
                (this.filter == null) ? null : ImmutableList.of(this.filter),
                null);
    }

    @Override
    public int readOr(int offset, int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator) throws IOException
    {
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (!rowGroupOpen) {
            openRowGroup();
        }

        allNulls = false;

        if (outputRequired) {
            ensureValuesCapacity(positionCount, nullsAllowed && presentStream != null);
        }

        if (filters != null) {
            if (outputPositions == null || outputPositions.length < positionCount) {
                outputPositions = new int[positionCount];
            }
        }
        else {
            outputPositions = positions;
        }

        // account memory used by values, nulls and outputPositions
        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        int streamPosition = 0;
        if (secondsStream == null && nanosStream == null && presentStream != null) {
            streamPosition = readAllNulls(positions, positionCount);
            if (filters != null && filters.get(0).testNull() && accumulator != null) {
                accumulator.set(positions[0], streamPosition);
            }
        }
        else if (filters == null) {
            streamPosition = readNoFilter(positions, positionCount);
        }
        else if (accumulator == null) {
            streamPosition = readWithFilter(positions, positionCount, filters);
        }
        else {
            streamPosition = readWithOrFilter(positions, positionCount, filters, accumulator);
        }

        readOffset = offset + streamPosition;
        return outputPositionCount;
    }

    private int readWithOrFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        int streamPosition = 0;
        outputPositionCount = 0;
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
                long value = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
                if ((accumulator != null && accumulator.get(position))
                        || filters == null || filters.stream().anyMatch(f -> f.testLong(value))) {
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
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters)
            throws IOException
    {
        int streamPosition = 0;
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
                long value = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
                if (filters == null || filters.get(0).testLong(value)) {
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
        return streamPosition;
    }

    private int readAllNulls(int[] positions, int positionCount)
            throws IOException
    {
        presentStream.skip(positions[positionCount - 1]);
        if (nullsAllowed) {
            outputPositionCount = positionCount;
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
                values[i] = decodeTimestamp(secondsStream.next(), nanosStream.next(), baseTimestampInSeconds);
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
        if (secondsStream == null && nanosStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            secondsStream.skip(dataToSkip);
            nanosStream.skip(dataToSkip);
        }
        else {
            secondsStream.skip(items);
            nanosStream.skip(items);
        }
    }

    private void ensureValuesCapacity(int capacity, boolean recordNulls)
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
        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (allNulls) {
            return new RunLengthEncodedBlock(NULL_BLOCK, outputPositionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;
        if (positionCount == outputPositionCount) {
            Block block = new LongArrayBlock(positionCount, Optional.ofNullable(includeNulls ? nulls : null), values);
            nulls = null;
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

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        nulls = null;

        presentStream = null;
        presentStreamSource = null;
        secondsStream = null;
        secondsStreamSource = null;
        nanosStream = null;
        nanosStreamSource = null;

        systemMemoryContext.close();
    }

    @Override
    public Block mergeBlocks(List<Block<Long>> blocks, int positionCount)
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
