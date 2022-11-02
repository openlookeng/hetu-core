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

import io.hetu.core.common.util.DataSizeOfUtil;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.ByteArrayInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.Int128ArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.IcebergUuidType;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.ZoneId;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class UuidColumnReader
        implements ColumnReader
{
    private static final int INSTANCE_SIZE = toIntExact(ClassLayout.parseClass(UuidColumnReader.class).instanceSize());
    private static final int ONE_GIGABYTE = toIntExact(DataSizeOfUtil.of(1, GIGABYTE).toBytes());

    private final OrcColumn column;

    private int readOffset;
    private int nextBatchSize;

    private InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    private BooleanInputStream presentStream;

    private InputStreamSource<ByteArrayInputStream> dataByteSource = missingStreamSource(ByteArrayInputStream.class);
    @Nullable
    private ByteArrayInputStream dataStream;

    private boolean rowGroupOpen;

    public UuidColumnReader(OrcColumn column)
    {
        this.column = requireNonNull(column, "column is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public Block readBlock()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            skipToReadOffset();
            readOffset = 0;
        }

        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            // since dataStream is null, all values are null
            presentStream.skip(nextBatchSize);
            Block nullValueBlock = createAllNullsBlock();
            nextBatchSize = 0;
            return nullValueBlock;
        }

        boolean[] isNullVector = null;
        int nullCount = 0;
        if (presentStream != null) {
            isNullVector = new boolean[nextBatchSize];
            nullCount = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullCount == nextBatchSize) {
                // all nulls
                Block nullValueBlock = createAllNullsBlock();
                nextBatchSize = 0;
                return nullValueBlock;
            }

            if (nullCount == 0) {
                isNullVector = null;
            }
        }

        int numberOfLongValues = toIntExact(nextBatchSize * 2L);
        int totalByteLength = toIntExact((long) numberOfLongValues * Long.BYTES);

        int currentBatchSize = nextBatchSize;
        nextBatchSize = 0;
        if (totalByteLength == 0) {
            return new Int128ArrayBlock(currentBatchSize, Optional.empty(), new long[0]);
        }
        if (totalByteLength > ONE_GIGABYTE) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR,
                    format("Values in column \"%s\" are too large to process for Trino. %s column values are larger than 1GB [%s]", column.getPath(), nextBatchSize, column.getOrcDataSourceId()));
        }
        if (dataStream == null) {
            throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
        }

        if (isNullVector == null) {
            long[] values = readNonNullLongs(numberOfLongValues);
            return new Int128ArrayBlock(currentBatchSize, Optional.empty(), values);
        }

        int nonNullCount = currentBatchSize - nullCount;
        long[] values = readNullableLongs(isNullVector, nonNullCount);
        return new Int128ArrayBlock(currentBatchSize, Optional.of(isNullVector), values);
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataByteSource = missingStreamSource(ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(InputStreamSources dataStreamSources)
    {
        presentStreamSource = dataStreamSources.getInputStreamSource(column, PRESENT, BooleanInputStream.class);
        dataByteSource = dataStreamSources.getInputStreamSource(column, DATA, ByteArrayInputStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(column)
                .toString();
    }

    @Override
    public void close()
    {
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }

    private void skipToReadOffset()
            throws IOException
    {
        int dataReadOffset = readOffset;
        if (presentStream != null) {
            // skip ahead the present bit reader, but count the set bits
            // and use this as the skip size for the dataStream
            dataReadOffset = presentStream.countBitsSet(readOffset);
        }
        if (dataReadOffset > 0) {
            if (dataStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
            }
            // dataReadOffset deals with positions. Each position is 2 longs in the dataStream.
            long dataSkipSize = dataReadOffset * 2L * Long.BYTES;

            dataStream.skip(dataSkipSize);
        }
    }

    private long[] readNullableLongs(boolean[] isNullVector, int nonNullCount)
            throws IOException
    {
        byte[] data = new byte[nonNullCount * 2 * Long.BYTES];

        dataStream.next(data, 0, data.length);

        int[] offsets = new int[isNullVector.length];
        int offsetPosition = 0;
        for (int i = 0; i < isNullVector.length; i++) {
            offsets[i] = Math.min(offsetPosition * 2 * Long.BYTES, data.length - Long.BYTES * 2);
            offsetPosition += isNullVector[i] ? 0 : 1;
        }

        long[] values = new long[isNullVector.length * 2];

        for (int i = 0; i < isNullVector.length; i++) {
            int isNonNull = isNullVector[i] ? 0 : 1;
            values[i * 2] = (long) bytesToLong(data, offsets[i], false) * isNonNull;
            values[i * 2 + 1] = (long) bytesToLong(data, offsets[i] + Long.BYTES, false) * isNonNull;
        }
        return values;
    }

    private long[] readNonNullLongs(int valueCount)
            throws IOException
    {
        byte[] data = new byte[valueCount * Long.BYTES];

        dataStream.next(data, 0, data.length);

        long[] values = new long[valueCount];
        for (int i = 0; i < valueCount; i++) {
            values[i] = bytesToLong(data, i * Long.BYTES, false);
        }
        return values;
    }

    public long bytesToLong(byte[] input, int offset, boolean littleEndian)
    {
        // 将byte[] 封装为 ByteBuffer
        ByteBuffer buffer = ByteBuffer.wrap(input, offset, 8);
        if (littleEndian) {
            // ByteBuffer.order(ByteOrder) 方法指定字节序,即大小端模式(BIG_ENDIAN/LITTLE_ENDIAN)
            // ByteBuffer 默认为大端(BIG_ENDIAN)模式
            buffer.order(ByteOrder.LITTLE_ENDIAN);
        }
        return buffer.getLong();
    }

    private Block createAllNullsBlock()
    {
        return RunLengthEncodedBlock.create(IcebergUuidType.UUID, BigDecimal.ZERO, BigDecimal.ZERO.intValue());
    }

    private void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataByteSource.openStream();

        rowGroupOpen = true;
    }
}
