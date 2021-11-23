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
package io.hetu.core.transport.execution.buffer;

import io.airlift.compress.zstd.ZstdCompressor;
import io.airlift.compress.zstd.ZstdDecompressor;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.spiller.SpillCipher;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PagesSerdeFactory
{
    private final BlockEncodingSerde blockEncodingSerde;
    private final boolean compressionEnabled;
    private final boolean directSerDe;

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled)
    {
        this(blockEncodingSerde, compressionEnabled, false);
    }

    public PagesSerdeFactory(BlockEncodingSerde blockEncodingSerde, boolean compressionEnabled, boolean directSerDe)
    {
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        this.compressionEnabled = compressionEnabled;
        this.directSerDe = directSerDe;
    }

    public PagesSerde createPagesSerde()
    {
        return createPagesSerdeInternal(Optional.empty(), false, false);
    }

    public PagesSerde createPagesSerdeForSpill(Optional<SpillCipher> spillCipher, boolean useDirect, boolean useKryo)
    {
        return createPagesSerdeInternal(spillCipher, useDirect, useKryo);
    }

    private PagesSerde createPagesSerdeInternal(Optional<SpillCipher> spillCipher, boolean useDirect, boolean useKryo)
    {
        if (directSerDe || useDirect) {
            if (useKryo) {
                return new KryoPageSerializer(blockEncodingSerde);
            }

            return new SliceStreamPageSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), spillCipher);
        }

        if (compressionEnabled) {
            return new PagesSerde(blockEncodingSerde, Optional.of(new ZstdCompressor()), Optional.of(new ZstdDecompressor()), spillCipher);
        }

        return new PagesSerde(blockEncodingSerde, Optional.empty(), Optional.empty(), spillCipher);
    }
}
