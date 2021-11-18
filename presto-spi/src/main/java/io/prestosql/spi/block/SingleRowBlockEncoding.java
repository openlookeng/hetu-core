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

public class SingleRowBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "ROW_ELEMENT";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        SingleRowBlock singleRowBlock = (SingleRowBlock) block;
        int numFields = singleRowBlock.getNumFields();
        int rowIndex = singleRowBlock.getRowIndex();
        sliceOutput.appendInt(numFields);
        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(sliceOutput, singleRowBlock.getRawFieldBlock(i).getRegion(rowIndex, 1));
        }
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        int numFields = sliceInput.readInt();
        Block[] fieldBlocks = new Block[numFields];
        for (int i = 0; i < fieldBlocks.length; i++) {
            fieldBlocks[i] = blockEncodingSerde.readBlock(sliceInput);
        }
        return new SingleRowBlock(0, fieldBlocks);
    }

    /**
     * Read a block from the specified input.  The returned
     * block should begin at the specified position.
     *
     * @param blockEncodingSerde
     * @param inputStream
     */
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

        return new SingleRowBlock(0, fieldBlocks);
    }

    /**
     * Write the specified block to the specified output
     *
     * @param blockEncodingSerde
     * @param outputStream
     * @param block
     */
    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, OutputStream outputStream, Block block)
    {
        if (!(outputStream instanceof Output)) {
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong outputStream for SingleRowBlock WriteBlock");
        }

        Output output = (Output) outputStream;
        SingleRowBlock singleRowBlock = (SingleRowBlock) block;
        int numFields = singleRowBlock.getNumFields();
        int rowIndex = singleRowBlock.getRowIndex();
        output.writeInt(numFields);
        for (int i = 0; i < numFields; i++) {
            blockEncodingSerde.writeBlock(output, singleRowBlock.getRawFieldBlock(i).getRegion(rowIndex, 1));
        }
    }
}
