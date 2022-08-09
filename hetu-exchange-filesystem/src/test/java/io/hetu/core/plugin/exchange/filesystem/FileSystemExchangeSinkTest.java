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

import com.esotericsoftware.kryo.Kryo;
import io.airlift.slice.Slice;
import io.hetu.core.plugin.exchange.filesystem.local.LocalFileSystemExchangeStorage;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
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
import static java.util.Objects.requireNonNull;

public class FileSystemExchangeSinkTest
{
    private ExchangeSink exchangeSink;
    private SerializedPage page;
    private final Kryo kryo = new Kryo();

    @BeforeMethod
    public void setUp() throws IOException
    {
        kryo.register(SerializedPage.class);
        kryo.register(Slice.class);
        kryo.register(byte[].class);
        kryo.register(Properties.class);
        String baseDir = "file:///opt/hetu-server-1.8.0-SNAPSHOT/exchange-base-dir";
        Path basePath = Paths.get(baseDir);
        File base = new File(baseDir);
        if (!base.exists()) {
            Files.createDirectories(basePath);
        }
        FileSystemExchangeConfig config = new FileSystemExchangeConfig()
                .setExchangeEncryptionEnabled(false)
                .setBaseDirectories(baseDir);
        ExchangeManager exchangeManager = new FileSystemExchangeManager(
                new LocalFileSystemExchangeStorage(),
                new FileSystemExchangeStats(),
                config);
        exchangeSink = exchangeManager.createSink(new FileSystemExchangeSinkInstanceHandle(
                new FileSystemExchangeSinkHandle(0, Optional.empty()),
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
        page = serde.serialize(expectedPage);
    }

    @AfterMethod
    public void tearDown()
    {
    }

    @Test
    public void testIsBlocked()
    {
    }

    @Test
    public void testAdd()
    {
        requireNonNull(exchangeSink, "exchangeSink is null");
//        Slice pageSlice = SerDeInSliceUtils.serialize(page, kryo);
        Slice pageSlice = null;
        exchangeSink.add(0, pageSlice);
        exchangeSink.finish();
    }

    @Test
    public void testAbort()
    {
        requireNonNull(exchangeSink, "exchangeSink is null");
//        Slice pageSlice = SerDeInSliceUtils.serialize(page, kryo);
        Slice pageSlice = null;
        exchangeSink.add(0, pageSlice);
        exchangeSink.abort();
    }
}
