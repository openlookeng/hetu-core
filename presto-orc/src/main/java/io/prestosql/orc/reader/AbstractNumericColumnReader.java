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
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.metadata.ColumnEncoding;
import io.prestosql.orc.metadata.ColumnMetadata;
import io.prestosql.orc.stream.BooleanInputStream;
import io.prestosql.orc.stream.InputStreamSource;
import io.prestosql.orc.stream.InputStreamSources;
import io.prestosql.orc.stream.LongInputStream;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.DateType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.ZoneId;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.metadata.Stream.StreamKind.DATA;
import static io.prestosql.orc.metadata.Stream.StreamKind.PRESENT;
import static io.prestosql.orc.reader.ReaderUtils.verifyStreamType;
import static io.prestosql.orc.stream.MissingInputStreamSource.missingStreamSource;
import static java.util.Objects.requireNonNull;

public abstract class AbstractNumericColumnReader<T>
        implements ColumnReader<T>
{
    protected static final int INSTANCE_SIZE = ClassLayout.parseClass(AbstractNumericColumnReader.class).instanceSize();

    protected final OrcColumn column;

    protected int readOffset;
    protected int nextBatchSize;

    protected InputStreamSource<BooleanInputStream> presentStreamSource = missingStreamSource(BooleanInputStream.class);
    @Nullable
    protected BooleanInputStream presentStream;
    protected boolean[] nullVector = new boolean[0];

    protected InputStreamSource<LongInputStream> dataStreamSource = missingStreamSource(LongInputStream.class);
    @Nullable
    protected LongInputStream dataStream;

    protected boolean rowGroupOpen;

    protected final LocalMemoryContext systemMemoryContext;

    /**
     * FIXME: KEN: why do we need to pass in type? isn't it implied already?
     * @param column
     * @param systemMemoryContext
     * @throws OrcCorruptionException
     */
    public AbstractNumericColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        requireNonNull(type, "type is null");
        verifyStreamType(column, type, t -> t instanceof BigintType || t instanceof IntegerType || t instanceof SmallintType || t instanceof DateType);
        this.column = requireNonNull(column, "column is null");
        this.systemMemoryContext = requireNonNull(systemMemoryContext, "systemMemoryContext is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    protected void openRowGroup()
            throws IOException
    {
        presentStream = presentStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    @Override
    public void startStripe(ZoneId fileTimeZone, ZoneId storageTimeZone, InputStreamSources dictionaryStreamSources, ColumnMetadata<ColumnEncoding> encoding)
    {
        presentStreamSource = missingStreamSource(BooleanInputStream.class);
        dataStreamSource = missingStreamSource(LongInputStream.class);

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
        dataStreamSource = dataStreamSources.getInputStreamSource(column, DATA, LongInputStream.class);

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
        systemMemoryContext.close();
        nullVector = null;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(nullVector);
    }
}
