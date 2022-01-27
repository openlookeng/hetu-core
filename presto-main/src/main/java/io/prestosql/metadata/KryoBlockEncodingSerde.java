/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.metadata;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.StandardErrorCode;
import io.prestosql.spi.block.AbstractBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.io.InputStream;
import java.io.OutputStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class KryoBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Kryo kryo;

    public KryoBlockEncodingSerde(FunctionAndTypeManager metadata, Kryo kryo)
    {
        this.functionAndTypeManager = requireNonNull(metadata, "metadata is null");
        this.kryo = kryo; /*Todo(nitin) make it singleton inject time initializer */
    }

    public Kryo getKryo()
    {
        return kryo;
    }

    @Override
    public Object getContext()
    {
        return getKryo();
    }

    /**
     * Read a block encoding from the input.
     *
     * @param inputStream
     */
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
        BlockEncoding blockEncoding = functionAndTypeManager.getBlockEncoding(encodingName);
        Serializer<?> serializer = getSerializerFromBlockEncoding(blockEncoding);
        if (serializer == null) {
            return (Block) blockEncoding.readBlock(this, inputStream);
        }
        return (Block) serializer.read(kryo, input, null);
    }

    /**
     * Write a blockEncoding to the output.
     *
     * @param outputStream
     * @param block
     */
    @Override
    public void writeBlock(OutputStream outputStream, Block block)
    {
        if (!(outputStream instanceof Output)) {
            throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR,
                    "This interface should not be called in this flow");
        }

        Output output = (Output) outputStream;

        String encodingName = block.getEncodingName();
        BlockEncoding blockEncoding = functionAndTypeManager.getBlockEncoding(encodingName);
        Serializer<Block<?>> serializer = getSerializerFromBlockEncoding(blockEncoding);

        // write the name to the output
        writeLengthPrefixedString(output, encodingName);

        if (serializer == null) {
            blockEncoding.writeBlock(this, outputStream, block);
        }
        else {
            // write the block to the output
            serializer.write(kryo, output, block);
        }
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
}
