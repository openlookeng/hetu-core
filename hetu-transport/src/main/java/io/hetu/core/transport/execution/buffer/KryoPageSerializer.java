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
package io.hetu.core.transport.execution.buffer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class KryoPageSerializer
        extends PagesSerde
{
    BlockEncodingSerde serde;
    Serializer<Page> serializer = new Serializer<Page>()
    {
        @Override
        public void write(Kryo kryo, Output output, Page page)
        {
            output.writeInt(page.getPositionCount());
            output.writeInt(page.getChannelCount());
            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                serde.writeBlock(output, page.getBlock(channel));
            }

            if (page.getPageMetadata().size() > 0) {
                String pageProperties = page.getPageMetadata().toString();
                byte[] propertiesByte = pageProperties
                        .replaceAll(",", System.lineSeparator())
                        .substring(1, pageProperties.length() - 1)
                        .getBytes(UTF_8);
                output.writeInt(propertiesByte.length);
                output.writeBytes(propertiesByte);
            }
            else {
                output.writeInt(0);
            }
        }

        @Override
        public Page read(Kryo kryo, Input input, Class<? extends Page> aClass)
        {
            int positionCount = input.readInt();
            int numberOfBlocks = input.readInt();
            Block[] blocks = new Block[numberOfBlocks];
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = serde.readBlock(input);
            }

            int propSize = input.readInt();
            if (propSize > 0) {
                byte[] pageMetadataBytes = input.readBytes(propSize);
                Properties pros = new Properties();
                try {
                    pros.load(new ByteArrayInputStream(pageMetadataBytes));
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }

                return new Page(positionCount, pros, blocks);
            }

            return new Page(positionCount, blocks);
        }
    };

    KryoPageSerializer(BlockEncodingSerde serde)
    {
        super(serde, Optional.empty(), Optional.empty(), Optional.empty());
        this.serde = requireNonNull(serde, "Serde Cannot be null");
    }

    @Override
    public void serialize(OutputStream output, Page page)
    {
        checkArgument(output instanceof Output, "Page serializer does not support (" + output.getClass().getSimpleName() + ") for writing");
        serializer.write(null, (Output) output, page);
    }

    @Override
    public Page deserialize(InputStream input)
    {
        checkArgument(input instanceof Input, "Page serializer does not support (" + input.getClass().getSimpleName() + ") for reading");
        return serializer.read(null, (Input) input, Page.class);
    }
}
