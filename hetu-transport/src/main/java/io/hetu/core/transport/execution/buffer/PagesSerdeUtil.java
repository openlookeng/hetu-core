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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.AbstractIterator;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.function.Predicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.transport.block.BlockSerdeUtil.readBlock;
import static io.hetu.core.transport.block.BlockSerdeUtil.writeBlock;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class PagesSerdeUtil
{
    public static final int DEFAULT_NUM_PAGES_PREFETCH = 1;

    private PagesSerdeUtil()
    {
    }

    static void writeRawPage(Kryo kryo, Page page, Output output, Serializer serde)
    {
        output.write(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            writeBlock(kryo, serde, output, page.getBlock(channel));
        }
    }

    static Page readRawPage(Kryo kryo, int positionCount, Input input, Serializer blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(kryo, blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    static void writeRawPage(Page page, SliceOutput output, BlockEncodingSerde serde)
    {
        output.writeInt(page.getChannelCount());
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            writeBlock(serde, output, page.getBlock(channel));
        }
    }

    static Page readRawPage(int positionCount, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, blocks);
    }

    static Page readRawPage(int positionCount, Properties pageMetadata, SliceInput input, BlockEncodingSerde blockEncodingSerde)
    {
        int numberOfBlocks = input.readInt();
        Block[] blocks = new Block[numberOfBlocks];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = readBlock(blockEncodingSerde, input);
        }

        return new Page(positionCount, pageMetadata, blocks);
    }

    public static void writeSerializedPage(SliceOutput output, SerializedPage page)
    {
        output.writeInt(page.getPositionCount());
        output.writeByte(page.getPageCodecMarkers());
        output.writeInt(page.getUncompressedSizeInBytes());
        output.writeInt(page.getSizeInBytes());
        output.writeBytes(page.getSlice());

        if (page.getPageMetadata().size() != 0) {
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

    private static SerializedPage readSerializedPage(SliceInput sliceInput)
    {
        int positionCount = sliceInput.readInt();
        PageCodecMarker.MarkerSet markers = PageCodecMarker.MarkerSet.fromByteValue(sliceInput.readByte());
        int uncompressedSizeInBytes = sliceInput.readInt();
        int sizeInBytes = sliceInput.readInt();
        Slice slice = sliceInput.readSlice(toIntExact((sizeInBytes)));

        int propertiesLength = sliceInput.readInt();
        if (propertiesLength != 0) {
            byte[] pageMetadataBytes = new byte[propertiesLength];
            sliceInput.readBytes(pageMetadataBytes);
            Properties pros = new Properties();
            try {
                pros.load(new ByteArrayInputStream(pageMetadataBytes));
                return new SerializedPage(slice, markers, positionCount, uncompressedSizeInBytes, pros);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return new SerializedPage(slice, markers, positionCount, uncompressedSizeInBytes);
    }

    public static long writeSerializedPages(SliceOutput sliceOutput, Iterable<SerializedPage> pages)
    {
        Iterator<SerializedPage> pageIterator = pages.iterator();
        long size = 0;
        while (pageIterator.hasNext()) {
            SerializedPage page = pageIterator.next();
            writeSerializedPage(sliceOutput, page);
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Page... pages)
    {
        return writePages(serde, sliceOutput, asList(pages).iterator());
    }

    public static long writePages(PagesSerde serde, SliceOutput sliceOutput, Iterator<Page> pages)
    {
        long size = 0;
        while (pages.hasNext()) {
            Page page = pages.next();
            writeSerializedPage(sliceOutput, serde.serialize(page));
            size += page.getSizeInBytes();
        }
        return size;
    }

    public static Iterator<Page> readPages(GenericPagesSerde serde, SliceInput sliceInput, int spillPrefetchReadPages)
    {
        return new PageReader(serde, sliceInput, spillPrefetchReadPages);
    }

    public static Iterator<Page> readPages(GenericPagesSerde serde, SliceInput sliceInput)
    {
        return new PageReader(serde, sliceInput);
    }

    public static Iterator<Page> readPagesDirect(GenericPagesSerde serde, InputStream input, Predicate<InputStream> eof, int spillPrefetchReadPages)
    {
        return new PageReaderDirect(serde, input, eof, spillPrefetchReadPages);
    }

    private static class PageReader
            extends AbstractIterator<Page>
    {
        private final GenericPagesSerde serde;
        private final SliceInput input;
        private final int spillPrefetchReadPages;
        LinkedList<Page> prefetchedPages;

        PageReader(GenericPagesSerde serde, SliceInput input)
        {
            this(serde, input, DEFAULT_NUM_PAGES_PREFETCH);
        }

        PageReader(GenericPagesSerde serde, SliceInput input, int spillPrefetchReadPages)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
            checkArgument(spillPrefetchReadPages > 0, "spillPrefetchReadPages cannot be less then 1");
            this.prefetchedPages = new LinkedList<>();
            this.spillPrefetchReadPages = spillPrefetchReadPages;
        }

        @Override
        protected Page computeNext()
        {
            if (!input.isReadable() && prefetchedPages.size() == 0) {
                return endOfData();
            }
            if (prefetchedPages.size() == 0) {
                for (int i = 0; i < spillPrefetchReadPages && input.isReadable(); i++) {
                    prefetchedPages.add(serde.deserialize(readSerializedPage(input)));
                }
            }
            return prefetchedPages.removeFirst();
        }
    }

    private static class PageReaderDirect
            extends AbstractIterator<Page>
    {
        private final GenericPagesSerde serde;
        private final InputStream input;
        private final Predicate<InputStream> eof;
        private final int spillPrefetchReadPages;
        LinkedList<Page> prefetchedPages;

        PageReaderDirect(GenericPagesSerde serde, InputStream input, Predicate<InputStream> eof)
        {
            this(serde, input, eof, DEFAULT_NUM_PAGES_PREFETCH);
        }

        PageReaderDirect(GenericPagesSerde serde, InputStream input, Predicate<InputStream> eof, int spillPrefetchReadPages)
        {
            this.serde = requireNonNull(serde, "serde is null");
            this.input = requireNonNull(input, "input is null");
            this.eof = requireNonNull(eof, "End of data needs to passed");
            checkArgument(spillPrefetchReadPages > 0, "spillPrefetchReadPages cannot be less then 1");
            this.prefetchedPages = new LinkedList<>();
            this.spillPrefetchReadPages = spillPrefetchReadPages;
        }

        @Override
        protected Page computeNext()
        {
            if (eof.test(input) && prefetchedPages.size() == 0) {
                return endOfData();
            }

            if (prefetchedPages.size() == 0) {
                for (int i = 0; i < spillPrefetchReadPages && !eof.test(input); i++) {
                    prefetchedPages.add(serde.deserialize(input));
                }
            }
            return prefetchedPages.removeFirst();
        }
    }

    public static Iterator<SerializedPage> readSerializedPages(SliceInput sliceInput)
    {
        return new SerializedPageReader(sliceInput);
    }

    private static class SerializedPageReader
            extends AbstractIterator<SerializedPage>
    {
        private final SliceInput input;

        SerializedPageReader(SliceInput input)
        {
            this.input = requireNonNull(input, "input is null");
        }

        @Override
        protected SerializedPage computeNext()
        {
            if (!input.isReadable()) {
                return endOfData();
            }

            return readSerializedPage(input);
        }
    }
}
