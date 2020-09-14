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
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.DecimalInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.time.ZoneId;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.metadata.Stream.StreamKind.SECONDARY;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public abstract class AbstractDecimalSelectiveColumnReader<T>
        implements SelectiveColumnReader<T>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractDecimalSelectiveColumnReader.class).instanceSize();

    protected final TupleDomainFilter filter;
    protected final boolean nullsAllowed;
    protected final boolean outputRequired;
    protected final int scale;

    protected long[] values;
    protected boolean[] nulls;
    protected int[] outputPositions;
    protected int outputPositionCount;
    protected BooleanInputStream presentStream;
    protected DecimalInputStream dataStream;
    protected LongInputStream scaleStream;

    private final int valuesPerPosition;
    private final Block nullBlock;
    private final OrcColumn streamDescriptor;
    private final LocalMemoryContext systemMemoryContext;

    private int readOffset;
    private boolean rowGroupOpen;
    private boolean allNulls;
    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    private InputStreamSource<DecimalInputStream> dataStreamSource = missingStreamSource(DecimalInputStream.class);
    private InputStreamSource<LongInputStream> scaleStreamSource = missingStreamSource(LongInputStream.class);

    public AbstractDecimalSelectiveColumnReader(
            OrcType orcType,
            OrcColumn streamDescriptor,
            Optional<TupleDomainFilter> filter,
            Optional<Type> outputType,
            LocalMemoryContext systemMemoryContext,
            int valuesPerPosition)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "streamDescriptor is null");
        this.filter = filter.orElse(null);
        this.outputRequired = outputType.isPresent();
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
        this.nullsAllowed = this.filter == null || this.filter.testNull();
        this.scale = orcType.getScale().get();
        this.nullBlock = outputType.map(type -> type.createBlockBuilder(null, 1).appendNull().build()).orElse(null);
        this.valuesPerPosition = valuesPerPosition;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(DecimalInputStream.class);
        scaleStreamSource = missingStreamSource(LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, PRESENT, BooleanInputStream.class);
        dataStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, DATA, DecimalInputStream.class);
        scaleStreamSource = dataStreamSources.getInputStreamSource(streamDescriptor, SECONDARY, LongInputStream.class);
        readOffset = 0;
        presentStream = null;
        dataStream = null;
        scaleStream = null;
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
        dataStream = dataStreamSource.openStream();
        scaleStream = scaleStreamSource.openStream();
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
        outputPositionCount = 0;
        if (dataStream == null && scaleStream == null && presentStream != null) {
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

    protected void skip(int items)
            throws IOException
    {
        if (dataStream == null) {
            presentStream.skip(items);
        }
        else if (presentStream != null) {
            int dataToSkip = presentStream.countBitsSet(items);
            dataStream.skip(dataToSkip);
            scaleStream.skip(dataToSkip);
        }
        else {
            dataStream.skip(items);
            scaleStream.skip(items);
        }
    }

    @Override
    public int[] getReadPositions()
    {
        return outputPositions;
    }

    @Override
    public Block<T> getBlock(int[] positions, int positionCount)
    {
        checkArgument(outputPositionCount > 0, "outputPositionCount must be greater than zero");
        checkState(outputRequired, "This stream reader doesn't produce output");
        checkState(positionCount <= outputPositionCount, "Not enough values");

        if (allNulls) {
            return new RunLengthEncodedBlock(nullBlock, outputPositionCount);
        }

        boolean includeNulls = nullsAllowed && presentStream != null;

        if (positionCount == outputPositionCount) {
            Block block = makeBlock(positionCount, nullsAllowed, nulls, values);
            nulls = null;
            values = null;
            return block;
        }

        long[] valuesCopy = new long[valuesPerPosition * positionCount];
        boolean[] nullsCopy = null;

        if (includeNulls) {
            nullsCopy = new boolean[positionCount];
        }

        copyValues(positions, positionCount, valuesCopy, nullsCopy);

        return makeBlock(positionCount, includeNulls, nullsCopy, valuesCopy);
    }

    private void ensureValuesCapacity(int capacity, boolean nullAllowed)
    {
        if (values == null || values.length < capacity) {
            values = new long[valuesPerPosition * capacity];
        }

        if (nullAllowed) {
            if (nulls == null || nulls.length < capacity) {
                nulls = new boolean[capacity];
            }
        }
    }

    abstract void copyValues(int[] positions, int positionsCount, long[] valuesCopy, boolean[] nullsCopy);

    abstract Block<T> makeBlock(int positionCount, boolean includeNulls, boolean[] nulls, long[] values);

    abstract void compactValues(int[] positions, int positionCount, boolean compactNulls);

    abstract int readNoFilter(int[] positions, int position)
            throws IOException;

    abstract int readWithFilter(int[] positions, int position, List<TupleDomainFilter> filters)
            throws IOException;

    abstract int readWithOrFilter(int[] positions, int position, List<TupleDomainFilter> filters, BitSet accumulator)
            throws IOException;

    @Override
    public void close()
    {
        values = null;
        nulls = null;
        outputPositions = null;

        presentStream = null;
        presentStreamSource = null;
        dataStream = null;
        dataStreamSource = null;
        scaleStream = null;
        scaleStreamSource = null;
        systemMemoryContext.close();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
}
