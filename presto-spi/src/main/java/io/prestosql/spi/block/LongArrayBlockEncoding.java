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
package io.prestosql.spi.block;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;

public class LongArrayBlockEncoding
        extends AbstractBlockEncoding<LongArrayBlock>
{
    public static final String NAME = "LONG_ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        int positionCount = block.getPositionCount();
        sliceOutput.appendInt(positionCount);

        encodeNullsAsBits(sliceOutput, block);

        for (int position = 0; position < positionCount; position++) {
            if (!block.isNull(position)) {
                sliceOutput.writeLong(block.getLong(position, 0));
            }
        }
    }

    @Override
    public Block<Long> readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        long[] values = new long[positionCount];
        for (int position = 0; position < positionCount; position++) {
            if (valueIsNull == null || !valueIsNull[position]) {
                values[position] = sliceInput.readLong();
            }
        }

        return new LongArrayBlock(0, positionCount, valueIsNull, values);
    }

    @Override
    public void write(Kryo kryo, Output output, LongArrayBlock block)
    {
        int positionCount = block.getPositionCount();
        output.writeInt(positionCount);

        output.writeBoolean(block.mayHaveNull());
        if (block.mayHaveNull()) {
            output.writeBooleans(block.valueIsNull, 0, positionCount);
        }

        output.writeLongs(block.values, block.arrayOffset, positionCount);
    }

    @Override
    public LongArrayBlock read(Kryo kryo, Input input, Class<? extends LongArrayBlock> aClass)
    {
        int positionCount = input.readInt();
        boolean[] valuesIsNull = null;
        if (input.readBoolean()) {
            valuesIsNull = input.readBooleans(positionCount);
        }
        long[] values = input.readLongs(positionCount);
        return new LongArrayBlock(0, positionCount, valuesIsNull, values);
    }

    /**
     * Read a block from the specified input.  The returned
     * block should begin at the specified position.
     *
     * @param blockEncodingSerde
     * @param input
     */
    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, InputStream input)
    {
        if (!(blockEncodingSerde.getContext() instanceof Kryo) || !(input instanceof Input)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Generic readblock not supported for LongArrayBlock");
        }

        return this.read((Kryo) blockEncodingSerde.getContext(), (Input) input, LongArrayBlock.class);
    }

    /**
     * Write the specified block to the specified output
     *
     * @param blockEncodingSerde
     * @param output
     * @param block
     */
    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, OutputStream output, Block block)
    {
        if (!(blockEncodingSerde.getContext() instanceof Kryo) || !(output instanceof Output)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Generic write not supported for LongArrayBlock");
        }

        this.write((Kryo) blockEncodingSerde.getContext(), (Output) output, (LongArrayBlock) block);
    }
}
