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
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.prestosql.spi.block.EncoderUtil.decodeNullBits;
import static io.prestosql.spi.block.EncoderUtil.encodeNullsAsBits;

public class VariableWidthBlockEncoding
        extends AbstractBlockEncoding<VariableWidthBlock>
{
    public static final String NAME = "VARIABLE_WIDTH";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        // The down casts here are safe because it is the block itself the provides this encoding implementation.
        AbstractVariableWidthBlock variableWidthBlock = (AbstractVariableWidthBlock) block;

        int positionCount = variableWidthBlock.getPositionCount();
        sliceOutput.appendInt(positionCount);

        // offsets
        int totalLength = 0;
        for (int position = 0; position < positionCount; position++) {
            int length = variableWidthBlock.getSliceLength(position);
            totalLength += length;
            sliceOutput.appendInt(totalLength);
        }

        encodeNullsAsBits(sliceOutput, variableWidthBlock);

        sliceOutput
                .appendInt(totalLength)
                .writeBytes(variableWidthBlock.getRawSlice(0), variableWidthBlock.getPositionOffset(0), totalLength);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();

        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(Slices.wrappedIntArray(offsets), SIZE_OF_INT, positionCount * SIZE_OF_INT);

        boolean[] valueIsNull = decodeNullBits(sliceInput, positionCount).orElse(null);

        int blockSize = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(blockSize);

        return new VariableWidthBlock(0, positionCount, slice, offsets, valueIsNull);
    }

    @Override
    public void write(Kryo kryo, Output output, VariableWidthBlock block)
    {
        int positionCount = block.getPositionCount();
        output.writeInt(positionCount);
        output.writeInts(block.offsets, block.arrayOffset, positionCount + 1);

        output.writeBoolean(block.mayHaveNull());
        if (block.mayHaveNull()) {
            output.writeBooleans(block.valueIsNull, 0, positionCount);
        }

        int totalSize = block.offsets[block.arrayOffset + positionCount] - block.offsets[block.arrayOffset];
        output.writeInt(totalSize);
        if (totalSize != 0) {
            output.write(block.getRawSlice(0).byteArray(), block.offsets[block.arrayOffset], totalSize);
        }
    }

    @Override
    public VariableWidthBlock read(Kryo kryo, Input input, Class<? extends VariableWidthBlock> aClass)
    {
        int positionCount = input.readInt();
        int[] offsets = input.readInts(positionCount + 1);
        boolean[] valuesIsNull = null;
        if (input.readBoolean()) {
            valuesIsNull = input.readBooleans(positionCount);
        }
        int blockSize = input.readInt();
        Slice slice;
        if (blockSize != 0) {
            slice = Slices.wrappedBuffer(input.readBytes(blockSize));
        }
        else {
            slice = Slices.EMPTY_SLICE;
        }
        return new VariableWidthBlock(0, positionCount, slice, offsets, valuesIsNull);
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
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Generic readblock not supported for VariableWidthBlock");
        }

        return this.read((Kryo) blockEncodingSerde.getContext(), (Input) input, VariableWidthBlock.class);
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
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Generic write not supported for VariableWidthBlock");
        }

        this.write((Kryo) blockEncodingSerde.getContext(), (Output) output, (VariableWidthBlock) block);
    }
}
