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
package io.hetu.core.heuristicindex;

import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexWriter;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static io.hetu.core.heuristicindex.IndexCommandUtils.loadDataSourceProperties;
import static io.hetu.core.heuristicindex.IndexCommandUtils.loadIndexProperties;
import static io.hetu.core.heuristicindex.IndexCommandUtils.loadIndexStore;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@PrepareForTest({LoggerFactory.class, IndexCommandUtils.class})
@PowerMockIgnore("javax.management.*")
@Test(singleThreaded = true)
public class TestIndexCommand
        extends PowerMockTestCase
{
    @Test
    public void testCallWithEmptyConfigDirectory()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();
            File tempFile = testFolder.newFile();
            assertTrue(tempFile.delete());

            String[] args = {"--config=" + tempFile.getAbsolutePath(), "--table=catalog.schema.table",
                    "--column=column", "--type=bloom", "create"};

            assertRuntimeException(args);
        }
    }

    @Test
    public void testCallWithNoIndexType()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();

            mockStatic(IndexCommandUtils.class);
            when(IndexCommandUtils.loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexProperties(anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));

            String[] args = {"--config=" + testFolder.getRoot().getAbsolutePath(), "--table=catalog.schema.table", "--column=column", "create"};

            assertRuntimeException(args);
        }
    }

    @Test
    public void testCreateCommand()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();

            IndexFactory factory = mock(IndexFactory.class);
            IndexWriter writer = mock(IndexWriter.class);
            when(factory.getIndexWriter(any(), any(), any(), any())).thenReturn(writer);

            mockStatic(IndexCommandUtils.class);
            when(IndexCommandUtils.loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexProperties(anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));
            when(IndexCommandUtils.getIndexFactory()).thenReturn(factory);

            String[] args = {"--config=" + testFolder.getRoot().getAbsolutePath(), "--table=catalog.schema.table", "--column=column", "--type=bloom", "create"};
            IndexCommand.main(args);

            verify(writer, times(1)).createIndex(any(), any(), any(), any(), eq(true), eq(false));
        }
    }

    @Test
    public void testDeleteCommand()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();

            IndexFactory factory = mock(IndexFactory.class);
            IndexClient client = mock(IndexClient.class);
            when(factory.getIndexClient(any(), any())).thenReturn(client);

            mockStatic(IndexCommandUtils.class);
            when(IndexCommandUtils.loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexProperties(anyString())).thenReturn(new Properties());
            when(IndexCommandUtils.loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));
            when(IndexCommandUtils.getIndexFactory()).thenReturn(factory);

            String[] args = {"--config=" + testFolder.getRoot().getAbsolutePath(), "--table=catalog.schema.table", "--column=column", "--type=bloom", "delete"};
            IndexCommand.main(args);

            verify(client, times(1)).deleteIndex(any(), any());
        }
    }

    @Test
    public void testLoadIndexWriterFromConfigFile()
            throws IOException
    {
        IndexFactory factory = new HeuristicIndexFactory();

        Properties dsProps = new Properties();
        Path root = Paths.get("/tmp");
        dsProps.setProperty("connector.name", "empty");

        Properties config = new Properties();
        config.setProperty(IndexConstants.INDEXSTORE_URI_KEY, "/tmp");
        config.setProperty(IndexConstants.INDEXSTORE_FILESYSTEM_PROFILE_KEY, "test-fs-config");
        config.setProperty(IndexConstants.INDEX_KEYS_PREFIX + "bloom.key", "value");

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File conf = folder.newFile("config.properties");
            try (OutputStream os = new FileOutputStream(conf)) {
                config.store(os, "Index Writer UT");
            }

            // catalog conf file
            File catalogFolder = folder.newFolder("catalog");
            File catalogConf = new File(catalogFolder, "test.properties");
            try (OutputStream os = new FileOutputStream(catalogConf)) {
                dsProps.store(os, "Index Writer UT");
            }

            // filesystem conf file
            // Strong coupling with filesystem client, change when modifying filesystem client profile
            File filesystemFolder = folder.newFolder("filesystem");
            File testFsConf = new File(filesystemFolder, "test-fs-config.properties");
            try (OutputStream os = new FileOutputStream(testFsConf)) {
                os.write("fs.client.type=local".getBytes());
                os.flush();
            }

            Properties dsPropsRead = loadDataSourceProperties("test.random.stuff", folder.getRoot().getCanonicalPath());
            Properties ixPropsRead = loadIndexProperties(folder.getRoot().getCanonicalPath());
            IndexCommandUtils.IndexStore is = loadIndexStore(folder.getRoot().getCanonicalPath());

            assertNotNull(factory.getIndexWriter(dsPropsRead, ixPropsRead, is.getFs(), is.getRoot()));
        }
    }

    private void assertRuntimeException(String[] args)
    {
        IndexCommand myCommand = new IndexCommand();
        CommandLine commandLine = new CommandLine(myCommand);
        commandLine.parseArgs(args);
        assertThrows(RuntimeException.class, myCommand::call);
    }
}
