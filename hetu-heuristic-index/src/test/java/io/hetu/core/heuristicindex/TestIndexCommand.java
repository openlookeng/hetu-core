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
import io.hetu.core.heuristicindex.util.IndexCommandUtils;
import io.hetu.core.heuristicindex.util.IndexConstants;
import io.prestosql.spi.heuristicindex.IndexClient;
import io.prestosql.spi.heuristicindex.IndexFactory;
import io.prestosql.spi.heuristicindex.IndexWriter;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static io.hetu.core.heuristicindex.util.IndexCommandUtils.loadDataSourceProperties;
import static io.hetu.core.heuristicindex.util.IndexCommandUtils.loadIndexStore;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@PrepareForTest({IndexCommandUtils.class, IndexRecordManager.class})
@PowerMockIgnore("javax.management.*")
@Test(singleThreaded = true)
public class TestIndexCommand
        extends PowerMockTestCase
{
    @Test(expectedExceptions = RuntimeException.class)
    public void testCallWithEmptyConfigDirectory()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();
            File tempFile = testFolder.newFile();
            assertTrue(tempFile.delete());

            IndexCommand indexCommand = new IndexCommand(tempFile.getAbsolutePath(), "abc", "catalog.schema.table", new String[] {"column"}, null,
                    "bloom", null, true, false, null);

            indexCommand.createIndex();
        }
    }

    @Test(expectedExceptions = RuntimeException.class)
    public void testCallWithNoIndexType()
            throws IOException
    {
        try (TempFolder testFolder = new TempFolder()) {
            testFolder.create();

            mockStatic(IndexCommandUtils.class);
            when(loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));

            IndexCommand indexCommand = new IndexCommand(testFolder.getRoot().getAbsolutePath(), "abc", "catalog.schema.table", new String[] {"column"}, null,
                    null, null, true, false, null);

            indexCommand.createIndex();
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
            mockStatic(IndexRecordManager.class);
            when(IndexRecordManager.readAllIndexRecords(any(), any())).thenReturn(null);
            mockStatic(IndexCommandUtils.class);
            when(loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));
            when(IndexCommandUtils.getIndexFactory()).thenReturn(factory);

            IndexCommand indexCommand = new IndexCommand(testFolder.getRoot().getAbsolutePath(), "abc", "catalog.schema.table", new String[] {"column"}, null,
                    "bloom", null, false, false, null);
            indexCommand.createIndex();

            verify(writer, times(1)).createIndex(any(), any(), any(), any(), eq(false));
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
            mockStatic(IndexRecordManager.class);
            when(IndexRecordManager.lookUpIndexRecord(any(), any(), anyString())).thenReturn(new IndexRecordManager.IndexRecord(null, null, null, null, null, null));
            mockStatic(IndexCommandUtils.class);
            when(loadDataSourceProperties(anyString(), anyString())).thenReturn(new Properties());
            when(loadIndexStore(anyString())).thenReturn(new IndexCommandUtils.IndexStore(null, null));
            when(IndexCommandUtils.getIndexFactory()).thenReturn(factory);

            IndexCommand indexCommand = new IndexCommand(testFolder.getRoot().getAbsolutePath(), "abc", "catalog.schema.table", new String[] {"column"}, null,
                    "bloom", null, false, false, null);
            indexCommand.deleteIndex();

            verify(client, times(1)).deleteIndex(any(), any(), any());
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

        Properties ixProps = new Properties();
        ixProps.setProperty("bloom.fpp", "0.01");

        Properties config = new Properties();
        config.setProperty(IndexConstants.INDEXSTORE_URI_KEY, "/tmp");
        config.setProperty(IndexConstants.INDEXSTORE_FILESYSTEM_PROFILE_KEY, "test-fs-config");

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
            IndexCommandUtils.IndexStore is = loadIndexStore(folder.getRoot().getCanonicalPath());

            assertNotNull(factory.getIndexWriter(dsPropsRead, ixProps, is.getFs(), is.getRoot()));
        }
    }
}
