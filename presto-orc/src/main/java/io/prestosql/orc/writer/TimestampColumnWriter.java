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
package io.prestosql.orc.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.prestosql.orc.checkpoint.BooleanStreamCheckpoint;
import io.prestosql.orc.checkpoint.LongStreamCheckpoint;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.CompressedMetadataWriter;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.RowGroupIndex;
import io.prestosql.orc.metadata.Stream;
import io.prestosql.orc.metadata.Stream.StreamKind;
import io.prestosql.orc.metadata.statistics.ColumnStatistics;
import io.prestosql.orc.metadata.statistics.TimestampStatisticsBuilder;
import io.prestosql.orc.stream.LongOutputStream;
import io.prestosql.orc.stream.LongOutputStreamV2;
import io.prestosql.orc.stream.PresentOutputStream;
import io.prestosql.orc.stream.StreamDataOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static io.prestosql.orc.metadata.CompressionKind.NONE;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.SECONDARY;
import static java.time.ZoneOffset.UTC;
import static java.util.Objects.requireNonNull;

public class TimestampColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TimestampColumnWriter.class).instanceSize();
    private static final int MILLIS_PER_SECOND = 1000;
    private static final int MILLIS_TO_NANOS_TRAILING_ZEROS = 5;

    private final OrcColumnId columnId;
    private final Type type;
    private final boolean compressed;
    private final ColumnEncoding columnEncoding;
    private final LongOutputStream secondsStream;
    private final LongOutputStream nanosStream;
    private final PresentOutputStream presentStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();
    private final long baseTimestampInSeconds;

    private final Supplier<TimestampStatisticsBuilder> statisticsBuilderSupplier;
    private TimestampStatisticsBuilder statisticsBuilder;

    private boolean closed;

    public TimestampColumnWriter(OrcColumnId columnId, Type type, CompressionKind compression, int bufferSize, Supplier<TimestampStatisticsBuilder> statisticsBuilderSupplier)
    {
        this.columnId = requireNonNull(columnId, "columnId is null");
        this.type = requireNonNull(type, "type is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.secondsStream = new LongOutputStreamV2(compression, bufferSize, true, DATA);
        this.nanosStream = new LongOutputStreamV2(compression, bufferSize, false, SECONDARY);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        this.baseTimestampInSeconds = OffsetDateTime.of(2015, 1, 1, 0, 0, 0, 0, UTC).toEpochSecond();
        this.statisticsBuilderSupplier = requireNonNull(statisticsBuilderSupplier, "statisticsBuilderSupplier is null");
        this.statisticsBuilder = statisticsBuilderSupplier.get();
    }

    @Override
    public Map<OrcColumnId, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(columnId, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        secondsStream.recordCheckpoint();
        nanosStream.recordCheckpoint();
    }

    @Override
    public void writeBlock(Block block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                long value = type.getLong(block, position);

                // It is a flaw in ORC encoding that uses normal integer division to compute seconds,
                // and floor modulus to compute nano seconds.
                long seconds = (value / MILLIS_PER_SECOND) - baseTimestampInSeconds;
                long millis = Math.floorMod(value, MILLIS_PER_SECOND);
                // The "sub-second" value (i.e., the nanos value) typically has a large number of trailing
                // zero, because many systems, like Presto, only record millisecond or microsecond precision
                // timestamps. To optimize storage, if the value has more than two trailing zeros, the trailing
                // decimal zero digits are removed, and the last three bits are used to record how many zeros
                // were removed (minus one):
                //   # Trailing 0s   Last 3 Bits   Example nanos       Example encoding
                //         0            0b000        123456789     (123456789 << 3) | 0b000
                //         1            0b000        123456780     (123456780 << 3) | 0b000
                //         2            0b001        123456700       (1234567 << 3) | 0b001
                //         3            0b010        123456000        (123456 << 3) | 0b010
                //         4            0b011        123450000         (12345 << 3) | 0b011
                //         5            0b100        123400000          (1234 << 3) | 0b100
                //         6            0b101        123000000           (123 << 3) | 0b101
                //         7            0b110        120000000            (12 << 3) | 0b110
                //         8            0b111        100000000             (1 << 3) | 0b111
                //
                // In Presto, we only have millisecond precision.
                // Therefore, we always use the encoding for 6 trailing zeros (except when input is zero).
                // For simplicity, we don't dynamically use 6, 7, 8 depending on the circumstance.
                long encodedNanos = millis == 0 ? 0 : (millis << 3) | MILLIS_TO_NANOS_TRAILING_ZEROS;

                secondsStream.writeLong(seconds);
                nanosStream.writeLong(encodedNanos);
                statisticsBuilder.addValue(value);
            }
        }
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroupColumnStatistics.add(statistics);
        statisticsBuilder = statisticsBuilderSupplier.get();
        return ImmutableMap.of(columnId, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        secondsStream.close();
        nanosStream.close();
        presentStream.close();
    }

    @Override
    public Map<OrcColumnId, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(columnId, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<StreamDataOutput> getIndexStreams(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> secondsCheckpoints = secondsStream.getCheckpoints();
        List<LongStreamCheckpoint> nanosCheckpoints = nanosStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            LongStreamCheckpoint secondsCheckpoint = secondsCheckpoints.get(groupId);
            LongStreamCheckpoint nanosCheckpoint = nanosCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createTimestampColumnPositionList(compressed, secondsCheckpoint, nanosCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        Slice slice = metadataWriter.writeRowIndexes(rowGroupIndexes.build());
        Stream stream = new Stream(columnId, StreamKind.ROW_INDEX, slice.length(), false);
        return ImmutableList.of(new StreamDataOutput(slice, stream));
    }

    private static List<Integer> createTimestampColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint secondsCheckpoint,
            LongStreamCheckpoint nanosCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(secondsCheckpoint.toPositionList(compressed));
        positionList.addAll(nanosCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<StreamDataOutput> getDataStreams()
    {
        checkState(closed);

        ImmutableList.Builder<StreamDataOutput> outputDataStreams = ImmutableList.builder();
        presentStream.getStreamDataOutput(columnId).ifPresent(outputDataStreams::add);
        outputDataStreams.add(secondsStream.getStreamDataOutput(columnId));
        outputDataStreams.add(nanosStream.getStreamDataOutput(columnId));
        return outputDataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return secondsStream.getBufferedBytes() + nanosStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        long retainedBytes = INSTANCE_SIZE + secondsStream.getRetainedBytes() + nanosStream.getRetainedBytes() + presentStream.getRetainedBytes();
        for (ColumnStatistics statistics : rowGroupColumnStatistics) {
            retainedBytes += statistics.getRetainedSizeInBytes();
        }
        return retainedBytes;
    }

    @Override
    public void reset()
    {
        closed = false;
        secondsStream.reset();
        nanosStream.reset();
        presentStream.reset();
        statisticsBuilder = statisticsBuilderSupplier.get();
        rowGroupColumnStatistics.clear();
    }

    @Override
    public List<StreamDataOutput> getBloomFilters(CompressedMetadataWriter metadataWriter)
            throws IOException
    {
        List<BloomFilter> bloomFilters = rowGroupColumnStatistics.stream()
                .map(ColumnStatistics::getBloomFilter)
                .filter(Objects::nonNull)
                .collect(toImmutableList());

        if (!bloomFilters.isEmpty()) {
            Slice slice = metadataWriter.writeBloomFilters(bloomFilters);
            Stream stream = new Stream(columnId, StreamKind.BLOOM_FILTER_UTF8, slice.length(), false);
            return ImmutableList.of(new StreamDataOutput(slice, stream));
        }

        return ImmutableList.of();
    }
}
