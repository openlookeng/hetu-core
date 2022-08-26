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
package io.hetu.core.plugin.exchange.filesystem;

import com.esotericsoftware.kryo.io.Output;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.plugin.exchange.filesystem.storage.FileSystemExchangeStorage;
import io.hetu.core.plugin.exchange.filesystem.storage.HetuFileSystemExchangeStorage;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.hetu.core.transport.execution.buffer.SerializedPageSerde;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.exchange.ExchangeManager;
import io.prestosql.spi.exchange.ExchangeSink;
import io.prestosql.testing.TestingPagesSerdeFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.Properties;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkTest
{
    private ExchangeSink exchangeSink;
    private Slice serializedPageSlice;

    @BeforeMethod
    public void setUp() throws IOException
    {
        String baseDir = "/opt/hetu-server-1.8.0/exchange-base-dir";
        Path basePath = Paths.get(baseDir);
        File base = new File(baseDir);
        if (!base.exists()) {
            Files.createDirectories(basePath);
        }
        FileSystemExchangeConfig config = new FileSystemExchangeConfig()
                .setExchangeEncryptionEnabled(false)
                .setBaseDirectories(baseDir);
        FileSystemExchangeStorage exchangeStorage = new HetuFileSystemExchangeStorage();
        exchangeStorage.setFileSystemClient(new HetuLocalFileSystemClient(new LocalConfig(new Properties()), basePath));
        ExchangeManager exchangeManager = new FileSystemExchangeManager(
                exchangeStorage,
                new FileSystemExchangeStats(),
                config);
        exchangeSink = exchangeManager.createSink(new FileSystemExchangeSinkInstanceHandle(
                new FileSystemExchangeSinkHandle(0, Optional.empty(), false),
                config.getBaseDirectories().get(0),
                10), false);
        PagesSerde serde = new TestingPagesSerdeFactory().createPagesSerde();
        BlockBuilder expectedBlockBuilder = VARCHAR.createBlockBuilder(null, 5);
        VARCHAR.writeString(expectedBlockBuilder, "alice");
        VARCHAR.writeString(expectedBlockBuilder, "bob");
        VARCHAR.writeString(expectedBlockBuilder, "charlie");
        VARCHAR.writeString(expectedBlockBuilder, "dave");
        Block expectedBlock = expectedBlockBuilder.build();

        Page expectedPage = new Page(expectedBlock, expectedBlock, expectedBlock);
        SerializedPage page = serde.serialize(expectedPage);

        int sizeRequired = calculateSerializedPageSizeInBytes(page);
        Output output = new Output(sizeRequired);
        SerializedPageSerde.serialize(output, page);
        serializedPageSlice = Slices.wrappedBuffer(output.getBuffer());
    }

    @AfterMethod
    public void tearDown()
    {
    }

    @Test
    public void testIsBlocked()
    {
    }

    private int calculateSerializedPageSizeInBytes(SerializedPage page)
    {
        int sizeInBytes = Integer.BYTES     // positionCount
                + Byte.BYTES                // pageCodecMarkers
                + Integer.BYTES             // uncompressedSizeInBytes
                + Integer.BYTES             // slice length
                + page.getSlice().length(); // slice data
        if (page.getPageMetadata().size() != 0) {
            String pageProperties = page.getPageMetadata().toString();
            byte[] propertiesByte = pageProperties
                    .replaceAll(",", System.lineSeparator())
                    .substring(1, pageProperties.length() - 1)
                    .getBytes(UTF_8);
            sizeInBytes += Integer.BYTES + propertiesByte.length;
        }
        else {
            sizeInBytes += Integer.BYTES;
        }
        return sizeInBytes;
    }

    @Test
    public void testAdd()
    {
        requireNonNull(exchangeSink, "exchangeSink is null");
        exchangeSink.add(0, serializedPageSlice);
        exchangeSink.finish();
    }

    @Test
    public void testAbort()
    {
        requireNonNull(exchangeSink, "exchangeSink is null");
        exchangeSink.add(0, serializedPageSlice);
        exchangeSink.abort();
    }
}
