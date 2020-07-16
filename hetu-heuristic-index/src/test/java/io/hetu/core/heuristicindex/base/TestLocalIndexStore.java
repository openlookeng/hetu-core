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
package io.hetu.core.heuristicindex.base;

import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalIndexStore
{
    TempFolder tFolder;
    File testFile;

    @BeforeTest
    public void prepare() throws IOException
    {
        tFolder = new TempFolder();
        tFolder.create();
        testFile = tFolder.newFile();
    }

    public void assertFileContentEquals(File testFile, String content) throws IOException
    {
        InputStreamReader reader = new InputStreamReader(new FileInputStream(testFile), StandardCharsets.UTF_8);
        try (BufferedReader bReader = new BufferedReader(reader)) {
            assertEquals(bReader.readLine(), content);
        }
    }

    @Test
    public void testWriteSimple() throws IOException
    {
        IndexStore writer = new LocalIndexStore();
        String content = "test";

        // First make sure no file is there
        if (testFile.exists()) {
            assertTrue(testFile.delete());
        }

        writer.write(content, testFile.getPath(), false);

        assertFileContentEquals(testFile, content);
    }

    @Test
    public void testWriteOverwrite() throws IOException
    {
        testWriteSimple();

        IndexStore writer = new LocalIndexStore();
        String content = "testOverwrite";

        writer.write(content, testFile.getPath(), true);

        assertFileContentEquals(new File(testFile.getPath()), content);
    }

    @Test
    public void testWriteNonExistentFile() throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = new File(folder.getRoot(), "test");
            File testFileNested = new File(testFile, "nested");

            IndexStore writer = new LocalIndexStore();
            String content = "test";
            writer.write(content, testFileNested.getCanonicalPath(), false);
            assertFileContentEquals(testFileNested, content);
        }
    }

    @Test
    public void testOutputStreamWrite() throws IOException
    {
        IndexStore writer = new LocalIndexStore();

        String content = "testOutputStream";

        if (testFile.exists()) {
            assertTrue(testFile.delete());
        }

        writer.write(content, writer.getOutputStream(testFile.getPath(), false));

        assertFileContentEquals(testFile, content);
    }

    @Test
    public void testOutputStreamWriteOverwrite() throws IOException
    {
        testOutputStreamWrite();

        IndexStore writer = new LocalIndexStore();
        String content = "test outputstream overwrite";
        writer.write(content, writer.getOutputStream(testFile.getPath(), true));

        assertFileContentEquals(new File(testFile.getPath()), content);
    }

    @Test
    public void testDelete() throws IOException
    {
        IndexStore writer = new LocalIndexStore();
        writer.write("test delete", testFile.getPath(), true);
        writer.delete(testFile.getPath());

        assertFalse(testFile.exists());
        assertEquals(testFile.exists(), writer.exists(testFile.getPath()));
    }

    @Test
    public void testDeleteNonExistingFile() throws IOException
    {
        IndexStore is = new LocalIndexStore();

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();
            assertTrue(testFile.delete());

            assertFalse(is.delete(testFile.getCanonicalPath()), "delete should fail because the file doesn't exist");
        }
    }

    @Test
    public void testDeleteFolder() throws IOException
    {
        IndexStore writer = new LocalIndexStore();
        String testFolderPath = tFolder.getRoot().getPath();
        try (TempFolder anotherTmpFolder = new TempFolder()) {
            anotherTmpFolder.create();

            writer.write("test delete folder11", tFolder.newFile().getPath(), true);
            writer.write("test delete folder2", tFolder.newFile().getPath(), true);
            writer.write("test delete folder2", anotherTmpFolder.newFile().getPath(), true);
            writer.delete(testFolderPath);

            assertFalse(tFolder.getRoot().exists());
        }
    }

    @Test
    public void testRename() throws IOException
    {
        IndexStore is = new LocalIndexStore();
        String content = "test rename";
        is.write(content, testFile.getPath(), true);
        assertTrue(is.exists(testFile.getPath()));

        String newPath = testFile.getPath().concat(".renamed");
        is.renameTo(testFile.getPath(), newPath);

        assertTrue(is.exists(newPath));
        assertFileContentEquals(new File(newPath), content);
    }

    @Test
    public void testRenameWrongPath() throws IOException
    {
        IndexStore is = new LocalIndexStore();
        assertFalse(is.renameTo("", ""), "File renaming should return false because the URI is empty");

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();
            assertTrue(testFile.delete());
            assertFalse(is.renameTo(testFile.getCanonicalPath(), "test"), "File renaming should return false because the file doesn't exist");
        }
    }

    @Test
    public void testLastModifiedTimeSimple() throws IOException, InterruptedException
    {
        IndexStore is = new LocalIndexStore();

        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File testFile = folder.newFile();

            long prev = is.getLastModifiedTime(testFile.getCanonicalPath());
            try (FileOutputStream fo = new FileOutputStream(testFile)) {
                // Some system have the last modified time precision to be 1 second instead of milliseconds
                // Sleep for 1 second for last modified time to be different
                Thread.sleep(1100);
                fo.write(123);
                fo.flush();
            }
            assertTrue(is.getLastModifiedTime(testFile.getCanonicalPath()) > prev);

            assertTrue(testFile.setLastModified(prev));
            assertTrue(prev == is.getLastModifiedTime(testFile.getCanonicalPath()));
        }
    }

    @AfterTest
    public void cleanUp()
    {
        assertTrue(testFile.delete());
        tFolder.close();
    }
}
