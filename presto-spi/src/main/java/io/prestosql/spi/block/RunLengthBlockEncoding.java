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

public class RunLengthBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "RLE";

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;

        // write the run length
        sliceOutput.writeInt(rleBlock.getPositionCount());

        // write the value
        blockEncodingSerde.writeBlock(sliceOutput, rleBlock.getValue());
    }

    @Override
    public RunLengthEncodedBlock readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        // read the run length
        int positionCount = sliceInput.readInt();

        // read the value
        Block value = blockEncodingSerde.readBlock(sliceInput);

        return new RunLengthEncodedBlock(value, positionCount);
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
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong inputStream for RLE ReadBlock");
        }

        Input input = (Input) inputStream;
        int positionCount = input.readInt();

        Block value = blockEncodingSerde.readBlock(input);
        return new RunLengthEncodedBlock(value, positionCount);
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
            throw new PrestoException(StandardErrorCode.NOT_SUPPORTED, "Wrong outputStream for RLE WriteBlock");
        }

        RunLengthEncodedBlock rleBlock = (RunLengthEncodedBlock) block;
        Output output = (Output) outputStream;

        // write the run length
        output.writeInt(rleBlock.getPositionCount());

        // write the value
        blockEncodingSerde.writeBlock(output, rleBlock.getValue());
    }
}
