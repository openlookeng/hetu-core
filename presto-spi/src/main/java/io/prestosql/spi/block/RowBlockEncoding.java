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
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;

import static io.airlift.slice.Slices.wrappedIntArray;
import static io.prestosql.spi.block.RowBlock.createRowBlockInternal;

public class RowBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ROW";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        AbstractRowBlock rowBlock = (AbstractRowBlock) block;
        int numFields = rowBlock.numFields;

        int positionCount = rowBlock.getPositionCount();

        int offsetBase = rowBlock.getOffsetBase();
        int[] fieldBlockOffsets = rowBlock.getFieldBlockOffsets();
        int startFieldBlockOffset = fieldBlockOffsets[offsetBase];
        int endFieldBlockOffset = fieldBlockOffsets[offsetBase + positionCount];

        sliceOutput.appendInt(numFields);
        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(sliceOutput, rowBlock.getRawFieldBlocks()[i].getRegion(startFieldBlockOffset, endFieldBlockOffset - startFieldBlockOffset));
        }

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(fieldBlockOffsets[offsetBase + position] - startFieldBlockOffset);
        }
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }

        int positionCount = sliceInput.readInt();
        int[] fieldBlockOffsets = new int[positionCount + 1];
        sliceInput.readBytes(wrappedIntArray(fieldBlockOffsets));
        boolean[] rowIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount).orElseGet(() -> new boolean[positionCount]);
        return createRowBlockInternal(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, InputStream inputStream)
    {
        if (!(inputStream instanceof Input)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong inputStream for SingleRow ReadBlock");
        }

        Input input = (Input) inputStream;
        int numFields = input.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < fieldBlocks.length; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(input);
        }

        int positionCount = input.readInt();
        int[] fieldBlockOffsets = input.readInts(positionCount + 1);
        boolean[] rowIsNull;

        if (input.readBoolean()) {
            rowIsNull = input.readBooleans(positionCount);
        }
        else {
            rowIsNull = new boolean[positionCount];
        }

        return createRowBlockInternal(0, positionCount, rowIsNull, fieldBlockOffsets, fieldBlocks);
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, OutputStream outputStream, Block block)
    {
        if (!(outputStream instanceof Output)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong outputStream for RowBlock WriteBlock");
        }

        Output output = (Output) outputStream;
        AbstractRowBlock rowBlock = (AbstractRowBlock) block;
        int numFields = rowBlock.numFields;

        int positionCount = rowBlock.getPositionCount();

        int offsetBase = rowBlock.getOffsetBase();
        int[] fieldBlockOffsets = rowBlock.getFieldBlockOffsets();
        int startFieldBlockOffset = fieldBlockOffsets[offsetBase];
        int endFieldBlockOffset = fieldBlockOffsets[offsetBase + positionCount];

        output.writeInt(numFields);
        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(output, rowBlock.getRawFieldBlocks()[i].getRegion(startFieldBlockOffset, endFieldBlockOffset - startFieldBlockOffset));
        }

        output.writeInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            output.writeInt(fieldBlockOffsets[offsetBase + position] - startFieldBlockOffset);
        }

        output.writeBoolean(block.mayHaveNull());
        if (block.mayHaveNull()) {
            output.writeBooleans(rowBlock.getRowIsNull(), offsetBase, positionCount);
        }
    }
}
