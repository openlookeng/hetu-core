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
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.ByteArrayInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.VariableWidthBlock;
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
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.CHAR;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.DICTIONARY_DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.LENGTH;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.SliceSelectiveColumnReader.computeTruncatedLength;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class SliceDictionarySelectiveColumnReader
        implements SelectiveColumnReader<byte[]>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionarySelectiveColumnReader.class).instanceSize();

    private static final byte[] EMPTY_DICTIONARY_DATA = new byte[0];
    // add one extra entry for null after stripe/rowGroup dictionary
    private static final int[] EMPTY_DICTIONARY_OFFSETS = new int[2];

    private final TupleDomainFilter filter;
    private final boolean nullsAllowed;
    private final Type outputType;
    private final boolean outputRequired;
    private final OrcColumn streamDescriptor;
    private final int maxCodePointCount;
    private final boolean isCharType;

    private byte[] stripeDictionaryData = EMPTY_DICTIONARY_DATA;
    private int[] stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
    private byte[] currentDictionaryData = EMPTY_DICTIONARY_DATA;
    private int[] stripeDictionaryLength = new int[0];
    private int[] rowGroupDictionaryLength = new int[0];

    private int readOffset;

    private VariableWidthBlock dictionaryBlock = new VariableWidthBlock(1, wrappedBuffer(EMPTY_DICTIONARY_DATA), EMPTY_DICTIONARY_OFFSETS, Optional.of(new boolean[] {true}));

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private BooleanInputStream presentStream;

    private BooleanInputStream inDictionaryStream;

    private InputStreamSource<ByteArrayInputStream> stripeDictionaryDataStreamSource = missingStreamSource(ByteArrayInputStream.class);
    private InputStreamSource<LongInputStream> stripeDictionaryLengthStreamSource = missingStreamSource(LongInputStream.class);
    private boolean stripeDictionaryOpen;
    private int stripeDictionarySize;

    private InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    private LongInputStream dataStream;

    private boolean rowGroupOpen;
    private LocalMemoryContext systemMemoryContext;

    private int[] values;
    private boolean allNulls;
    private int[] outputPositions;
    private int outputPositionCount;
    private boolean outputPositionsReadOnly;
    private boolean valuesInUse;
    private final OrcType orcType;

    public SliceDictionarySelectiveColumnReader(OrcType orcType, OrcColumn streamDescriptor, Optional<TupleDomainFilter> filter, Optional<Type> outputType, LocalMemoryContext systemMemoryContext)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = requireNonNull(filter, "filter is null").orElse(null);
        this.systemMemoryContext = systemMemoryContext;
        this.nullsAllowed = this.filter == null || this.filter.testNull();
        this.outputType = requireNonNull(outputType, "outputType is null").orElse(null);
        this.maxCodePointCount = orcType == null ? 0 : orcType.getLength().orElse(-1);
        this.isCharType = orcType.getOrcTypeKind() == CHAR;
        this.outputRequired = outputType.isPresent();
        this.orcType = orcType;
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

        if (outputRequired) {
            if (values == null || values.length < positionCount) {
                values = new int[positionCount];
            }
        }

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

        outputPositionCount = 0;
        int streamPosition;

        if (dataStream == null && presentStream != null) {
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
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                values[i] = dictionaryBlock.getPositionCount() - 1;
            }
            else {
                values[i] = toIntExact(dataStream.next());
            }
            streamPosition++;
        }
        outputPositionCount = positionCount;
        return streamPosition;
    }

    private int readWithFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters)
            throws IOException
    {
        int streamPosition = 0;
        for (int i = 0; i < positionCount; i++) {
            int position = positions[i];
            if (position > streamPosition) {
                skip(position - streamPosition);
                streamPosition = position;
            }

            if (presentStream != null && !presentStream.nextBit()) {
                if (nullsAllowed) {
                    if (outputRequired) {
                        values[outputPositionCount] = dictionaryBlock.getPositionCount() - 1;
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int index = toIntExact(dataStream.next());
                int currentPosLength = dictionaryBlock.getSliceLength(index);
                if (filters.get(0).testLength(currentPosLength)) {
                    Slice data = dictionaryBlock.getSlice(index, 0, currentPosLength);
                    if (filters == null || filters.get(0).testBytes(data.getBytes(), 0, currentPosLength)) {
                        if (outputRequired) {
                            values[outputPositionCount] = index;
                        }
                        outputPositions[outputPositionCount] = position;
                        outputPositionCount++;
                    }
                }
            }

            streamPosition++;
        }
        return streamPosition;
    }

    private int readWithOrFilter(int[] positions, int positionCount, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException
    {
        int streamPosition = 0;
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
                        values[outputPositionCount] = dictionaryBlock.getPositionCount() - 1;
                    }
                    if (accumulator != null && checkNulls) {
                        accumulator.set(position);
                    }
                    outputPositions[outputPositionCount] = position;
                    outputPositionCount++;
                }
            }
            else {
                int index = toIntExact(dataStream.next());
                int currentPosLength = dictionaryBlock.getSliceLength(index);
                if (filters.get(0).testLength(currentPosLength)) {
                    Slice data = dictionaryBlock.getSlice(index, 0, currentPosLength);
                    if ((accumulator != null && accumulator.get(position))
                            || filters == null || filters.stream().anyMatch(f -> f.testBytes(data.getBytes(), 0, currentPosLength))) {
                        if (accumulator != null) {
                            accumulator.set(position);
                        }
                    }
                }

                if (outputRequired) {
                    values[outputPositionCount] = index;
                }
                outputPositions[outputPositionCount] = position;
                outputPositionCount++;
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
        if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(dataToSkip);
            }
            dataStream.skip(dataToSkip);
        }
        else {
            if (inDictionaryStream != null) {
                inDictionaryStream.skip(items);
            }
            dataStream.skip(items);
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
            return new RunLengthEncodedBlock(outputType.createBlockBuilder(null, 1).appendNull().build(), outputPositionCount);
        }

        if (positionCount == outputPositionCount) {
            return new DictionaryBlock(positionCount, dictionaryBlock, values);
        }

        int[] valuesCopy = new int[positionCount];
        int positionIndex = 0;
        int nextPosition = positions[positionIndex];
        for (int i = 0; i < outputPositionCount; i++) {
            if (outputPositions[i] < nextPosition) {
                continue;
            }

            valuesCopy[positionIndex] = this.values[i];
            positionIndex++;
            if (positionIndex >= positionCount) {
                break;
            }
            nextPosition = positions[positionIndex];
        }

        return new DictionaryBlock(positionCount, dictionaryBlock, valuesCopy);
    }

    private void openRowGroup()
            throws IOException
    {
        // read the dictionary
        if (!stripeDictionaryOpen) {
            if (stripeDictionarySize > 0) {
                // resize the dictionary lengths array if necessary
                if (stripeDictionaryLength.length < stripeDictionarySize) {
                    stripeDictionaryLength = new int[stripeDictionarySize];
                }

                // read the lengths
                LongInputStream lengthStream = stripeDictionaryLengthStreamSource.openStream();
                if (lengthStream == null) {
                    throw new OrcCorruptionException(streamDescriptor.getOrcDataSourceId(), "Dictionary is not empty but dictionary length stream is not present");
                }
                lengthStream.nextIntVector(stripeDictionarySize, stripeDictionaryLength, 0);

                long dataLength = 0;
                for (int i = 0; i < stripeDictionarySize; i++) {
                    dataLength += stripeDictionaryLength[i];
                }

                // we must always create a new dictionary array because the previous dictionary may still be referenced
                stripeDictionaryData = new byte[toIntExact(dataLength)];
                // add one extra entry for null
                stripeDictionaryOffsetVector = new int[stripeDictionarySize + 2];

                // read dictionary values
                ByteArrayInputStream dictionaryDataStream = stripeDictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, stripeDictionarySize, stripeDictionaryLength, 0, stripeDictionaryData, stripeDictionaryOffsetVector, maxCodePointCount, isCharType);
            }
            else {
                stripeDictionaryData = EMPTY_DICTIONARY_DATA;
                stripeDictionaryOffsetVector = EMPTY_DICTIONARY_OFFSETS;
            }
        }
        stripeDictionaryOpen = true;

        // there is no row group dictionary so use the stripe dictionary
        setDictionaryBlockData(stripeDictionaryData, stripeDictionaryOffsetVector, stripeDictionarySize + 1);

        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    // Reads dictionary into data and offsetVector
    private static void readDictionary(
            @Nullable ByteArrayInputStream dictionaryDataStream,
            int dictionarySize,
            int[] dictionaryLengthVector,
            int offsetVectorOffset,
            byte[] data,
            int[] offsetVector,
            int maxCodePointCount,
            boolean isCharType)
            throws IOException
    {
        Slice slice = wrappedBuffer(data);

        // initialize the offset if necessary;
        // otherwise, use the previous offset
        if (offsetVectorOffset == 0) {
            offsetVector[0] = 0;
        }

        // truncate string and update offsets
        for (int i = 0; i < dictionarySize; i++) {
            int offsetIndex = offsetVectorOffset + i;
            int offset = offsetVector[offsetIndex];
            int length = dictionaryLengthVector[i];

            int truncatedLength;
            if (length > 0) {
                // read data without truncation
                dictionaryDataStream.next(data, offset, offset + length);
                // adjust offsets with truncated length
                truncatedLength = computeTruncatedLength(slice, offset, length, maxCodePointCount, isCharType);
                verify(truncatedLength >= 0);
            }
            else {
                truncatedLength = 0;
            }
            offsetVector[offsetIndex + 1] = offsetVector[offsetIndex] + truncatedLength;
        }
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        stripeDictionaryDataStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayInputStream.class);
        stripeDictionaryLengthStreamSource = dictionaryStreamSources.getInputStreamSource(streamDescriptor, LENGTH, LongInputStream.class);

        stripeDictionarySize = encoding.get(streamDescriptor.getColumnId()).getDictionarySize();

        stripeDictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

        readOffset = 0;

        presentStream = null;
        inDictionaryStream = null;
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
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void close()
    {
        values = null;
        outputPositions = null;
        currentDictionaryData = null;
        rowGroupDictionaryLength = null;
        stripeDictionaryData = null;
        stripeDictionaryLength = null;
        systemMemoryContext.close();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(currentDictionaryData) + sizeOf(values) + sizeOf(outputPositions);
    }

    private void setDictionaryBlockData(byte[] dictionaryData, int[] dictionaryOffsets, int positionCount)
    {
        verify(positionCount > 0);
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (currentDictionaryData != dictionaryData) {
            boolean[] isNullVector = new boolean[positionCount];
            isNullVector[positionCount - 1] = true;
            dictionaryOffsets[positionCount] = dictionaryOffsets[positionCount - 1];
            dictionaryBlock = new VariableWidthBlock(positionCount, wrappedBuffer(dictionaryData), dictionaryOffsets, Optional.of(isNullVector));
            currentDictionaryData = dictionaryData;
        }
    }
}
