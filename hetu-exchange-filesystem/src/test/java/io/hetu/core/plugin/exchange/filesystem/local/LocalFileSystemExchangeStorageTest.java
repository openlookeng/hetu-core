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
package io.hetu.core.plugin.exchange.filesystem.local;

import io.airlift.log.Logger;
import io.hetu.core.plugin.exchange.filesystem.ExchangeStorageWriter;
import io.hetu.core.plugin.exchange.filesystem.FileSystemExchangeStorage;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class LocalFileSystemExchangeStorageTest
{
    private static final Logger LOG = Logger.get(LocalFileSystemExchangeStorageTest.class);
    private final FileSystemExchangeStorage exchangeStorage = new LocalFileSystemExchangeStorage();

    @Test
    public void testCreateDirectories()
    {
        final String path = "file:///opt/hetu-server-1.8.0-SNAPSHOT/exchange-base-dir/task1.1.0";
        try {
            URI uri = URI.create(path);
            File dir = Paths.get(uri).toFile();
            assertFalse(dir.exists());
            exchangeStorage.createDirectories(URI.create(path));
            assertTrue(dir.exists());
        }
        catch (IOException e) {
            LOG.info(e.getMessage());
        }
    }

    @Test
    public void testCreateExchangeReader()
    {
    }

    @Test
    public void testCreateExchangeWriter()
    {
        final String filePath = "file:///opt/hetu-server-1.8.0-SNAPSHOT/exchange-base-dir/task1.1.0/0001.data";
        URI uri = URI.create(filePath);
        ExchangeStorageWriter exchangeWriter = exchangeStorage.createExchangeWriter(uri, Optional.empty());
        assertNotNull(exchangeWriter);
    }

    @Test
    public void testCreateEmptyFile()
    {
        final String filePath = "file:///opt/hetu-server-1.8.0-SNAPSHOT/exchange-base-dir/task1.1.0/commit";
        URI uri = URI.create(filePath);
        File file = Paths.get(uri).toFile();
        assertFalse(file.exists());
        exchangeStorage.createEmptyFile(uri);
        assertTrue(file.exists());
    }

    @Test
    public void testDeleteRecursively()
    {
        URI uri = URI.create("file:///opt/hetu-server-1.8.0-SNAPSHOT/exchange-base-dir/task1.1.0");
        final List<URI> directories = new ArrayList<>(Collections.singletonList(uri));
        File dir = Paths.get(uri).toFile();
        File[] files = dir.listFiles();
        assertTrue(requireNonNull(files).length > 0);
        exchangeStorage.deleteRecursively(directories);
        files = dir.listFiles();
        assertTrue(files == null || files.length == 0);
    }

    @Test
    public void testListFilesRecursively()
    {
    }
}
