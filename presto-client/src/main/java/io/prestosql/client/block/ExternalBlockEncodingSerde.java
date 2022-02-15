/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.client.block;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.ArrayBlockEncoding;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncoding;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.ByteArrayBlockEncoding;
import io.prestosql.spi.block.DictionaryBlockEncoding;
import io.prestosql.spi.block.Int128ArrayBlockEncoding;
import io.prestosql.spi.block.IntArrayBlockEncoding;
import io.prestosql.spi.block.LazyBlockEncoding;
import io.prestosql.spi.block.LongArrayBlockEncoding;
import io.prestosql.spi.block.MapBlockEncoding;
import io.prestosql.spi.block.RowBlockEncoding;
import io.prestosql.spi.block.RunLengthBlockEncoding;
import io.prestosql.spi.block.ShortArrayBlockEncoding;
import io.prestosql.spi.block.SingleMapBlockEncoding;
import io.prestosql.spi.block.SingleRowBlockEncoding;
import io.prestosql.spi.block.VariableWidthBlockEncoding;
import io.prestosql.spi.type.TypeManager;

import java.util.Map;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public final class ExternalBlockEncodingSerde
        implements BlockEncodingSerde
{
    private final Map<String, BlockEncoding> blockEncodings;

    public ExternalBlockEncodingSerde(TypeManager typeManager)
    {
        blockEncodings = ImmutableMap.<String, BlockEncoding>builder().put(VariableWidthBlockEncoding.NAME,
                new VariableWidthBlockEncoding())
                .put(ByteArrayBlockEncoding.NAME, new ByteArrayBlockEncoding())
                .put(ShortArrayBlockEncoding.NAME, new ShortArrayBlockEncoding())
                .put(IntArrayBlockEncoding.NAME, new IntArrayBlockEncoding())
                .put(LongArrayBlockEncoding.NAME, new LongArrayBlockEncoding())
                .put(Int128ArrayBlockEncoding.NAME, new Int128ArrayBlockEncoding())
                .put(DictionaryBlockEncoding.NAME, new DictionaryBlockEncoding())
                .put(ArrayBlockEncoding.NAME, new ArrayBlockEncoding())
                .put(MapBlockEncoding.NAME, new MapBlockEncoding(typeManager))
                .put(SingleMapBlockEncoding.NAME, new SingleMapBlockEncoding(typeManager))
                .put(RowBlockEncoding.NAME, new RowBlockEncoding())
                .put(SingleRowBlockEncoding.NAME, new SingleRowBlockEncoding())
                .put(RunLengthBlockEncoding.NAME, new RunLengthBlockEncoding())
                .put(LazyBlockEncoding.NAME, new LazyBlockEncoding())
                .build();
    }

    @Override
    public Block readBlock(SliceInput input)
    {
        // read the encoding name
        String encodingName = readLengthPrefixedString(input);

        // look up the encoding factory
        BlockEncoding blockEncoding = blockEncodings.get(encodingName);

        // load read the encoding factory from the output stream
        return blockEncoding.readBlock(this, input);
    }

    @Override
    public void writeBlock(SliceOutput output, Block inputBlock)
    {
        Block block = inputBlock;
        while (true) {
            // get the encoding name
            String encodingName = block.getEncodingName();

            // look up the BlockEncoding
            BlockEncoding blockEncoding = blockEncodings.get(encodingName);

            // see if a replacement block should be written instead
            Optional<Block> replacementBlock = blockEncoding.replacementBlockForWrite(block);
            if (replacementBlock.isPresent()) {
                block = replacementBlock.get();
                continue;
            }

            // write the name to the output
            writeLengthPrefixedString(output, encodingName);

            // write the block to the output
            blockEncoding.writeBlock(this, output, block);

            break;
        }
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
