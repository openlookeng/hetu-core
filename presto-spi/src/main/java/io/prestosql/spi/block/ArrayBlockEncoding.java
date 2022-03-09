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

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

import static io.prestosql.spi.block.ArrayBlock.createArrayBlockInternal;
import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;

public class ArrayBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ARRAY";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;

        int positionCount = arrayBlock.getPositionCount();

        int offsetBase = arrayBlock.getOffsetBase();
        int[] offsets = arrayBlock.getOffsets();

        int valuesStartOffset = offsets[offsetBase];
        int valuesEndOffset = offsets[offsetBase + positionCount];
        Block values = arrayBlock.getRawElementBlock().getRegion(valuesStartOffset, valuesEndOffset - valuesStartOffset);
        blockEncodingSerde.writeBlock(sliceOutput, values);

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(offsets[offsetBase + position] - valuesStartOffset);
        }
        encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        Block values = blockEncodingSerde.readBlock(sliceInput);

        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets));
        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);
        return createArrayBlockInternal(0, positionCount, valueIsNull, offsets, values);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, InputStream inputStream)
    {
        if (!(inputStream instanceof Input)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong inputStream for SingleMap ReadBlock");
        }

        Input input = (Input) inputStream;
        Block values = blockEncodingSerde.readBlock(input);
        int positionCount = input.readInt();
        int[] offsets = input.readInts(positionCount + 1);

        boolean[] valueIsNull = null;
        if (input.readBoolean()) {
            valueIsNull = input.readBooleans(positionCount);
        }

        return createArrayBlockInternal(0, positionCount, valueIsNull, offsets, values);
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, OutputStream outputStream, Block block)
    {
        if (!(outputStream instanceof Output)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong outputStream for SingleMap WriteBlock");
        }

        Output output = (Output) outputStream;
        AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;

        int positionCount = arrayBlock.getPositionCount();

        int offsetBase = arrayBlock.getOffsetBase();
        int[] offsets = arrayBlock.getOffsets();

        int valuesStartOffset = offsets[offsetBase];
        int valuesEndOffset = offsets[offsetBase + positionCount];
        Block values = arrayBlock.getRawElementBlock().getRegion(valuesStartOffset, valuesEndOffset - valuesStartOffset);
        blockEncodingSerde.writeBlock(output, values);

        output.writeInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            output.writeInt(offsets[offsetBase + position] - valuesStartOffset);
        }

        output.writeBoolean(block.mayHaveNull());
        if (block.mayHaveNull()) {
            output.writeBooleans(arrayBlock.getValueIsNull(), offsetBase, positionCount);
        }
    }
}
