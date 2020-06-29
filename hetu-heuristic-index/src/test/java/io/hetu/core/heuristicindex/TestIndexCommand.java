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

import io.prestosql.spi.filesystem.TempFolder;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.testng.PowerMockTestCase;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static org.powermock.api.mockito.PowerMockito.when;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@PrepareForTest({LoggerFactory.class, IndexFactory.class, IndexWriter.class, IndexClient.class})
public class TestIndexCommand
        extends PowerMockTestCase
{
    private TempFolder hetuRoot = new TempFolder();

    @BeforeMethod
    public void setup()
            throws IOException
    {
        hetuRoot.create();
    }

    @AfterMethod
    public void tearDown()
    {
        hetuRoot.close();
    }

    @Test
    public void testCallWithEmptyConfigDirectory()
            throws IOException
    {
        File tempFile = hetuRoot.newFile();
        assertTrue(tempFile.delete());

        String[] args = {"--config=" + tempFile.getAbsolutePath(), "--plugins=/", "--table=catalog.schema.table",
                "--column=column", "--type=bloom", "create"};

        assertRuntimeExecption(args);
    }

    @Test
    public void testCallWithEmptyPluginDirectory()
            throws IOException
    {
        File tempFile = hetuRoot.newFile();
        assertTrue(tempFile.delete());

        String[] args = {"--config=" + hetuRoot.getRoot().getAbsolutePath(), "--plugins=" +
                tempFile.getAbsolutePath(), "--table=catalog.schema.table", "--column=column", "--type=bloom", "create"};

        assertRuntimeExecption(args);
    }

    @Test
    public void testCallWithNoIndexType()
            throws IOException
    {
        // bypass loading plugins because we don't have one during unit test
        IndexFactory factory = mock(IndexFactory.class);
        when(factory.loadPlugins(new String[]{})).thenReturn(factory);

        mockStatic(IndexFactory.class);
        when(IndexFactory.getInstance()).thenReturn(factory);

        String[] args = {"--config=" + hetuRoot.getRoot().getAbsolutePath(), "--plugins=/",
                "--table=catalog.schema.table", "--column=column", "create"};

        assertRuntimeExecption(args);
    }

    @Test
    public void testCreateCommand()
            throws IOException
    {
        // bypass loading plugins because we don't have one during unit test
        IndexFactory factory = mock(IndexFactory.class);
        when(factory.loadPlugins(any())).thenReturn(factory);

        mockStatic(IndexFactory.class);
        when(IndexFactory.getInstance()).thenReturn(factory);

        IndexWriter writer = mock(IndexWriter.class);
        when(factory.getIndexWriter(anyString(), anyString())).thenReturn(writer);

        String[] args = {"--config=" + hetuRoot.getRoot().getAbsolutePath(), "--plugins=/",
                "--table=catalog.schema.table", "--column=column", "--type=bloom", "create"};
        IndexCommand.main(args);

        verify(writer, times(1)).createIndex(any(), any(), any(), any(), eq(true), eq(false));
    }

    @Test
    public void testDeleteCommand()
            throws IOException
    {
        // bypass loading plugins because we don't have one during unit test
        IndexFactory factory = mock(IndexFactory.class);
        when(factory.loadPlugins(any())).thenReturn(factory);

        mockStatic(IndexFactory.class);
        when(IndexFactory.getInstance()).thenReturn(factory);

        IndexClient client = mock(IndexClient.class);
        when(factory.getIndexClient(anyString())).thenReturn(client);

        String[] args = {"--config=" + hetuRoot.getRoot().getAbsolutePath(), "--plugins=/",
                "--table=catalog.schema.table", "--column=column", "--type=bloom", "delete"};
        IndexCommand.main(args);

        verify(client, times(1)).deleteIndex(any(), any());
    }

    private void assertRuntimeExecption(String[] args)
    {
        IndexCommand myCommand = new IndexCommand();
        CommandLine commandLine = new CommandLine(myCommand);
        commandLine.parseArgs(args);
        assertThrows(RuntimeException.class, myCommand::call);
    }
}
