/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.airlift.compress.Compressor;
import io.airlift.compress.Decompressor;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.spiller.SpillCipher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class SliceStreamPageSerde
        extends PagesSerde
{
    private final BlockEncodingSerde serde;

    public SliceStreamPageSerde(BlockEncodingSerde blockEncodingSerde, Optional<Compressor> compressor, Optional<Decompressor> decompressor, Optional<SpillCipher> spillCipher)
    {
        super(blockEncodingSerde, compressor, decompressor, spillCipher);
        this.serde = blockEncodingSerde;
    }

    @Override
    public void serialize(OutputStream output, Page page)
    {
        checkArgument(output instanceof SliceOutput, "Page serializer does not support (" + output.getClass().getSimpleName() + ") for writing");
        writePage((SliceOutput) output, page);
    }

    @Override
    public Page deserialize(InputStream input)
    {
        checkArgument(input instanceof SliceInput, "Page serializer does not support (" + input.getClass().getSimpleName() + ") for reading");
        return readPage((SliceInput) input);
    }

    private void writePage(SliceOutput output, Page page)
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

    public Page readPage(SliceInput input)
    {
        int positionCount = input.readInt();
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = serde.readBlock(input);
        }

        int propSize = input.readInt();
        if (propSize > 0) {
            byte[] pageMetadataBytes = new byte[propSize];
            input.readBytes(pageMetadataBytes);
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
}
