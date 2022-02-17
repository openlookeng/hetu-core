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
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.ByteArrayInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.LENGTH;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.SliceSelectiveColumnReader.computeTruncatedLength;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class SliceDirectSelectiveColumnReader
        implements SelectiveColumnReader<byte[]>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDirectSelectiveColumnReader.class).instanceSize();
    private static final int ONE_GIGABYTE = toIntExact(new DataSize(1, GIGABYTE).toBytes());

    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;

    private final OrcColumn streamDescriptor;
    private final boolean outputRequired;
    private final Type outputType;
    private final boolean isCharType;
    private final int maxCodePointCount;

    private int readOffset;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private BooleanInputStream presentStream;
    private InputStreamSource<ByteArrayInputStream> dataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private ByteArrayInputStream dataStream;
    private InputStreamSource<LongInputStream> lengthStreamSource = missingStreamSource(LongInputStream.class);
    private LongInputStream lengthStream;

    private boolean rowGroupOpen;
    private LocalMemoryContext systemMemoryContext;
    private boolean[] nulls;

    private int[] outputPositions;
    private int outputPositionCount;
    private boolean outputPositionsReadOnly;

    private boolean allNulls;           // true if all requested positions are null
    private boolean[] isNullVector;     // isNull flags for all positions up to the last positions requested in read()
    private int[] lengthVector;         // lengths for all positions up to the last positions requested in read()
    private int lengthIndex;            // index into lengthVector array
    private int[] offsets;              // offsets of requested positions only; specifies position boundaries for the data array
    private byte[] data;                // data for requested positions only
    private Slice dataAsSlice;          // data array wrapped in Slice
    private boolean valuesInUse;
    private final OrcType orcType;

    public SliceDirectSelectiveColumnReader(OrcType orcType, OrcColumn streamDescriptor, Optional<TupleDomainFilter> filter, Optional<Type> outputType, LocalMemoryContext newLocalMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = newLocalMemoryContext;
        this.nullsAllowed = this.filter == null || this.filter.testNull();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
        this.outputRequired = outputType.isPresent();
        this.orcType = orcType;
        this.isCharType = orcType.getOrcTypeKind() == OrcType.OrcTypeKind.CHAR;
        this.maxCodePointCount = orcType.getLength().orElse(-1);
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
        if (!rowGroupOpen) {
            openRowGroup();
        }

        checkState(!valuesInUse, "BlockLease hasn't been closed yet");

        if (filters != null) {
            if (outputPositions == null || outputPositions.length < positionCount) {
                outputPositions = new int[positionCount];
            }
        }
        else {
            outputPositions = positions;
            outputPositionsReadOnly = true;
        }

        systemMemoryContext.setBytes(getRetainedSizeInBytes());

        if (readOffset < offset) {
            skip(offset - readOffset);
        }

        prepareForNextRead(positionCount, positions);

        int streamPosition;

        if (lengthStream == null) {
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

    private int readNoFilter(int[] positions, int positionCount)
            throws IOException
    {
        int streamPosition = 0;
        allNulls = false;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skipData(streamPosition, position - streamPosition);
                streamPosition = position;
            }

            int offset = offsets[i];
            if (presentStream != null && isNullVector[position]) {
                if (offsets != null) {
                    offsets[i + 1] = offset;
                }
                nulls[i] = true;
            }
            else {
                int length = lengthVector[lengthIndex];
                int truncatedLength = 0;
                if (length > 0) {
                    dataStream.next(data, offset, offset + length);
                    truncatedLength = computeTruncatedLength(dataAsSlice, offset, length, maxCodePointCount, isCharType);
                }
                offsets[i + 1] = offset + truncatedLength;
                lengthIndex++;
                if (presentStream != null) {
                    nulls[i] = false;
                }
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithOrFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        allNulls = false;
        int streamPosition = 0;
        int dataToSkip = 0;
        boolean checkNulls = filters != null && filters.stream().anyMatch(f -> f.testNull());

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skipData(streamPosition, position - streamPosition);
                streamPosition = position;
            }

            // offset required to stored multiple position data in the same buffer.
            // Actually offset is the point from where next applied filtered data will be stored.
            int offset = outputRequired ? offsets[outputPositionCount] : 0;
            if (isNullVector != null && isNullVector[position]) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        offsets[outputPositionCount + 1] = offset;
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
                int length = lengthVector[lengthIndex];
                int dataOffset = outputRequired ? offset : 0;
                if (true) {
                    if (dataStream != null) {
                        dataStream.skip(dataToSkip);
                        dataToSkip = 0;
                        dataStream.next(data, dataOffset, dataOffset + length);
                        if ((accumulator != null && accumulator.get(position))
                                || filters == null || filters.stream().anyMatch(f -> f.testBytes(data, dataOffset, length))) {
                            if (accumulator != null) {
                                accumulator.set(position);
                            }
                        }
                    }
                    else {
                        if ((accumulator != null && accumulator.get(position))
                                || filters == null || filters.stream().anyMatch(f -> f.testBytes("".getBytes(StandardCharsets.UTF_8), 0, 0))) {
                            if (accumulator != null) {
                                accumulator.set(position);
                            }
                        }
                    }

                    if (outputRequired) {
                        int truncatedLength = 0;
                        if (dataStream != null) {
                            truncatedLength = computeTruncatedLength(dataAsSlice, dataOffset, length, maxCodePointCount, isCharType);
                        }

                        offsets[outputPositionCount + 1] = offset + truncatedLength;
                        if (nullsAllowed && isNullVector != null) {
                            nulls[outputPositionCount] = false;
                        }
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
                else {
                    dataToSkip += length;
                }
                lengthIndex++;
            }

            streamPosition++;
        }
        if (dataToSkip > 0) {
            dataStream.skip(dataToSkip);
        }
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters)
            throws IOException
    {
        allNulls = false;
        int streamPosition = 0;
        int dataToSkip = 0;

        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skipData(streamPosition, position - streamPosition);
                streamPosition = position;
            }

            // offset required to stored multiple position data in the same buffer.
            // Actually offset is the point from where next applied filtered data will be stored.
            int offset = outputRequired ? offsets[outputPositionCount] : 0;
            if (isNullVector != null && isNullVector[position]) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        offsets[outputPositionCount + 1] = offset;
                        nulls[outputPositionCount] = true;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int length = lengthVector[lengthIndex];
                int dataOffset = outputRequired ? offset : 0;
                if (isCharType || filters.get(0).testLength(length)) {
                    if (dataStream != null) {
                        dataStream.skip(dataToSkip);
                        dataToSkip = 0;
                        dataStream.next(data, dataOffset, dataOffset + length);
                        int truncatedLength = computeTruncatedLength(dataAsSlice, dataOffset, length, maxCodePointCount, isCharType);
                        if (filters == null || filters.get(0).testBytes(data, dataOffset, truncatedLength)) {
                            if (outputRequired) {
                                offsets[outputPositionCount + 1] = offset + truncatedLength;
                                if (nullsAllowed && isNullVector != null) {
                                    nulls[outputPositionCount] = false;
                                }
                            }
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount++;
                        }
                    }
                    else {
                        if (filters == null || filters.get(0).testBytes("".getBytes(StandardCharsets.UTF_8), 0, 0)) {
                            if (outputRequired) {
                                offsets[outputPositionCount + 1] = offset;
                                if (nullsAllowed && isNullVector != null) {
                                    nulls[outputPositionCount] = false;
                                }
                            }
                            outputPositions[outputPositionCount] = position;
                            outputPositionCount++;
                        }
                    }
                }
                else {
                    dataToSkip += length;
                }
                lengthIndex++;
            }

            streamPosition++;
        }
        if (dataToSkip > 0) {
            dataStream.skip(dataToSkip);
        }
        return streamPosition;
    }

    private int readAllNulls(int[] positions, int positionCount)
    {
        if (nullsAllowed) {
            outputPositionCount = positionCount;
            if (outputPositions != positions) {
                System.arraycopy(positions, 0, outputPositions, 0, outputPositionCount);
            }
        }
        else {
            outputPositionCount = 0;
        }

        allNulls = true;
        return positions[positionCount - 1] + 1;
    }

    private void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int lengthToSkip = presentStream.countBitsSet(items);
            dataStream.skip(lengthStream.sum(lengthToSkip));
        }
        else {
            dataStream.skip(lengthStream.sum(items));
        }
    }

    private void skipData(int start, int items)
            throws IOException
    {
        int dataToSkip = 0;
        for (int i = 0; i < items; i++) {
            if (isNullVector == null || !isNullVector[start + i]) {
                dataToSkip += lengthVector[lengthIndex];
                lengthIndex++;
            }
        }
        dataStream.skip(dataToSkip);
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
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;

        if (positionCount != outputPositionCount) {
            compactValues(positions, positionCount, includeNulls);
        }

        Block block = new VariableWidthBlock(positionCount, dataAsSlice, offsets, Optional.ofNullable(includeNulls ? nulls : null));
        dataAsSlice = null;
        data = null;
        offsets = null;
        nulls = null;
        return block;
    }

    private void compactValues(int[] positions, int positionCount, boolean includeNulls)
    {
        if (outputPositionsReadOnly) {
            outputPositions = Arrays.copyOf(outputPositions, outputPositionCount);
            outputPositionsReadOnly = false;
        }
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            int length = offsets[i + 1] - offsets[i];
            if (length > 0) {
                System.arraycopy(data, offsets[i], data, offsets[positionIndex], length);
            }
            offsets[positionIndex + 1] = offsets[positionIndex] + length;
            outputPositions[positionIndex] = nextPosition;

            if (includeNulls) {
                nulls[positionIndex] = nulls[i];
            }

            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        outputPositionCount = positionCount;
    }

    @Override
    public void close()
    {
        dataAsSlice = null;
        data = null;
        lengthVector = null;
        isNullVector = null;
        offsets = null;
        outputPositions = null;
        systemMemoryContext.close();
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        lengthStream = lengthStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        lengthStreamSource = missingStreamSource(LongInputStream.class);
        dataStreamSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        lengthStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, ByteArrayInputStream.class);

        readOffset = 0;

        presentStream = null;
        lengthStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(offsets) + sizeOf(outputPositions) + sizeOf(data) + sizeOf(nulls) + sizeOf(lengthVector) + sizeOf(isNullVector);
    }

    private void prepareForNextRead(int positionCount, int[] positions)
            throws IOException
    {
        lengthIndex = 0;
        outputPositionCount = 0;

        int totalLength = 0;
        int maxLength = 0;

        int totalPositions = positions[positionCount - 1] + 1;
        int nullCount = 0;
        if (presentStream != null) {
            if (isNullVector == null || isNullVector.length < totalPositions) {
                isNullVector = new boolean[totalPositions];
            }
            nullCount = presentStream.getUnsetBits(totalPositions, isNullVector);
        }

        if (lengthStream != null) {
            int nonNullCount = totalPositions - nullCount;
            if (lengthVector == null || lengthVector.length < nonNullCount) {
                lengthVector = new int[nonNullCount];
            }

            lengthStream.nextIntVector(nonNullCount, lengthVector, 0);

            //TODO calculate totalLength for only requested positions
            for (int i = 0; i < nonNullCount; i++) {
                totalLength += lengthVector[i];
                maxLength = Math.max(maxLength, lengthVector[i]);
            }

            if (totalLength > ONE_GIGABYTE) {
                throw new PrestoException(
                        GENERIC_INTERNAL_ERROR,
                        format("Values in column \"%s\" are too large to process for Presto. %s column values are larger than 1GB [%s]",
                                orcType.getFieldName(0), positionCount,
                                streamDescriptor.getOrcDataSourceId()));
            }
        }

        if (outputRequired) {
            if (presentStream != null && nullsAllowed) {
                nulls = new boolean[positionCount];
            }
            if (data == null || data.length < totalLength) {
                data = new byte[totalLength];
            }
            if (offsets == null || offsets.length < totalLength + 1) {
                offsets = new int[totalPositions + 1];
            }
        }
        else {
            if (data == null || data.length < maxLength) {
                data = new byte[maxLength];
            }
        }

        dataAsSlice = Slices.wrappedBuffer(data);
    }
}
