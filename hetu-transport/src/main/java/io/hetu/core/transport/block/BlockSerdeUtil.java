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
package io.hetu.core.transport.block;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.util.Reflection.methodHandle;

public final class BlockSerdeUtil
{
    public static final MethodHandle READ_BLOCK = methodHandle(BlockSerdeUtil.class, "readBlock", BlockEncodingSerde.class, Slice.class);

    private BlockSerdeUtil()
    {
    }

    public static Block readBlock(BlockEncodingSerde blockEncodingSerde, Slice slice)
    {
        return readBlock(blockEncodingSerde, slice.getInput());
    }

    public static Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput input)
    {
        return blockEncodingSerde.readBlock(input);
    }

    public static Block readBlock(Kryo kryo, Serializer blockEncodingSerde, Input input)
    {
        return (Block) blockEncodingSerde.read(kryo, input, null);
    }

    public static void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput output, Block block)
    {
        blockEncodingSerde.writeBlock(output, block);
    }

    public static void writeBlock(Kryo kryo, Serializer blockEncodingSerde, Output output, Block block)
    {
        blockEncodingSerde.write(kryo, output, block);
    }
}
