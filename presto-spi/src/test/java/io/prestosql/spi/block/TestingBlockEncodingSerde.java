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
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.TYPE_NOT_FOUND;
import static java.nio.charset.StandardCharsets.UTF_8;

// This class is exactly the same as BlockEncodingManager. They are in SPI and don't have access to InternalBlockEncodingSerde.
public final class TestingBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final ConcurrentMap<String, BlockEncoding> blockEncodings = new ConcurrentHashMap<>();
    private final Kryo kryo = new Kryo();

    public TestingBlockEncodingSerde()
    {
        addBlockEncoding(new VariableWidthBlockEncoding());
        addBlockEncoding(new ByteArrayBlockEncoding());
        addBlockEncoding(new ShortArrayBlockEncoding());
        addBlockEncoding(new IntArrayBlockEncoding());
        addBlockEncoding(new LongArrayBlockEncoding());
        addBlockEncoding(new Int128ArrayBlockEncoding());
        addBlockEncoding(new DictionaryBlockEncoding());
        addBlockEncoding(new ArrayBlockEncoding());
        addBlockEncoding(new RowBlockEncoding());
        addBlockEncoding(new SingleRowBlockEncoding());
        addBlockEncoding(new RunLengthBlockEncoding());
        addBlockEncoding(new LazyBlockEncoding());
    }

    private void addBlockEncoding(BlockEncoding blockEncoding)
    {
        blockEncodings.put(blockEncoding.getName(), blockEncoding);
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        checkArgument(blockEncoding != null, "Unknown block encoding %s", encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
        while (true) {
            // get the encoding name
            String encodingName = block.getEncodingName();

            // look up the encoding factory
            BlockEncoding blockEncoding = blockEncodings.get(encodingName);

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            Block tmpBlock = block;
            if (replacementBlock.isPresent()) {
                tmpBlock = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the block to the output
            blockEncoding.writeBlock(this, output, tmpBlock);

            break;
        }
    }

    @Override
    public Block readBlock(InputStream inputStream)
    {
        if (!(inputStream instanceof Input)) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "This interface should not be called in this flow");
        }

        Input input = (Input) inputStream;
        String encodingName;
        // read the encoding name
        encodingName = readLengthPrefixedString((Input) input);

        // look up the encoding factory
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        Serializer<?> serializer = getSerializerFromBlockEncoding(blockEncoding);
        if (serializer == null) {
            throw new PrestoException(TYPE_NOT_FOUND, "BlockEncoding Type not implemented: " + blockEncoding);
        }
        return (Block) serializer.read(kryo, input, null);
    }

    @Override
    public void writeBlock(OutputStream outputStream, Block block)
    {
        if (!(outputStream instanceof Output)) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "This interface should not be called in this flow");
        }

        Output output = (Output) outputStream;

        String encodingName = block.getEncodingName();
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);
        Serializer<Block<?>> serializer = getSerializerFromBlockEncoding(blockEncoding);
        if (serializer == null) {
            throw new PrestoException(TYPE_NOT_FOUND, "BlockEncoding Type not implemented: " + blockEncoding);
        }

        // write the name to the output
        writeLengthPrefixedString(output, encodingName);

        // write the block to the output
        serializer.write(kryo, output, block);
    }

    private Serializer<Block<?>> getSerializerFromBlockEncoding(BlockEncoding blockEncoding)
    {
        if (blockEncoding instanceof AbstractBlockEncoding) {
            return (Serializer<Block<?>>) blockEncoding;
        }
        return null;
    }

    private static String readLengthPrefixedString(Input input)
    {
        int length = input.readInt();
        byte[] bytes = input.readBytes(length);
        return new String(bytes, UTF_8);
    }

    private static void writeLengthPrefixedString(Output output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }

    private static String readLengthPrefixedString(SliceInput input)
    {
        int length = input.readInt();
        byte[] bytes = new byte[length];
        input.readBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private static void writeLengthPrefixedString(SliceOutput output, String value)
    {
        byte[] bytes = value.getBytes(UTF_8);
        output.writeInt(bytes.length);
        output.writeBytes(bytes);
    }
}
