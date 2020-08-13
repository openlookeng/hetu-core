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
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.orc.reader.ReaderUtils.minNonNullValueSize;
import static io.prestosql.orc.reader.ReaderUtils.unpackIntNulls;

public class IntegerColumnReader
        extends AbstractNumericColumnReader<Integer>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IntegerColumnReader.class).instanceSize();

    private int[] intNonNullValueTemp = new int[0];

    /**
     * FIXME: KEN: why do we need to pass in type? isn't it implied already?
     * @param column
     * @param systemMemoryContext
     * @throws OrcCorruptionException
     */
    public IntegerColumnReader(Type type, OrcColumn column, LocalMemoryContext systemMemoryContext)
            throws OrcCorruptionException
    {
        super(type, column, systemMemoryContext);
    }

    @Override
    public Block readBlock()
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup();
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the data reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is not null but data stream is missing");
                }
                dataStream.skip(readOffset);
            }
        }

        Block block;
        if (dataStream == null) {
            if (presentStream == null) {
                throw new OrcCorruptionException(column.getOrcDataSourceId(), "Value is null but present stream is missing");
            }
            presentStream.skip(nextBatchSize);
            block = RunLengthEncodedBlock.create(IntegerType.INTEGER, null, nextBatchSize);
        }
        else if (presentStream == null) {
            block = readNonNullBlock();
        }
        else {
            boolean[] isNull = new boolean[nextBatchSize];
            int nullCount = presentStream.getUnsetBits(nextBatchSize, isNull);
            if (nullCount == 0) {
                block = readNonNullBlock();
            }
            else if (nullCount != nextBatchSize) {
                block = readNullBlock(isNull, nextBatchSize - nullCount);
            }
            else {
                block = RunLengthEncodedBlock.create(IntegerType.INTEGER, null, nextBatchSize);
            }
        }

        readOffset = 0;
        nextBatchSize = 0;

        return block;
    }

    protected Block readNonNullBlock()
            throws IOException
    {
        verify(dataStream != null);
        int[] values = new int[nextBatchSize];
        dataStream.next(values, nextBatchSize);
        return new IntArrayBlock(nextBatchSize, Optional.empty(), values);
    }

    protected Block readNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        return intReadNullBlock(isNull, nonNullCount);
    }

    private Block intReadNullBlock(boolean[] isNull, int nonNullCount)
            throws IOException
    {
        verify(dataStream != null);
        int minNonNullValueSize = minNonNullValueSize(nonNullCount);
        if (intNonNullValueTemp.length < minNonNullValueSize) {
            intNonNullValueTemp = new int[minNonNullValueSize];
            systemMemoryContext.setBytes(sizeOf(intNonNullValueTemp));
        }

        dataStream.next(intNonNullValueTemp, nonNullCount);

        int[] result = unpackIntNulls(intNonNullValueTemp, isNull);

        return new IntArrayBlock(nextBatchSize, Optional.of(isNull), result);
    }

    @Override
    public boolean filterTest(TupleDomainFilter filter, Integer value)
    {
        return filter.testLong(value);
    }
}
