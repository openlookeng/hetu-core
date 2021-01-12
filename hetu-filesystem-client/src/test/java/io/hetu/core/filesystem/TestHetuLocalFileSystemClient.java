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
package io.hetu.core.filesystem;

import io.hetu.core.common.filesystem.TempFolder;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.filesystem.SupportedFileAttributes;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test class for HetuLocalFileSystemClient
 *
 * @since 2020-03-31
 */
public class TestHetuLocalFileSystemClient
{
    private static final Path OUTSIDE_WORKSPACE_PATH = Paths.get("/dir/out/side/of/workspace");
    private Path nonExistingPath;
    HetuFileSystemClient fs;
    TempFolder tFolder;

    @BeforeClass
    public void prepare()
            throws IOException
    {
        tFolder = new TempFolder();
        tFolder.create();
        nonExistingPath = Paths.get(tFolder.getRoot().getAbsolutePath(), "/path/to/a/non/existing/file");
        fs = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), tFolder.getRoot().toPath());
    }

    @Test
    public void testCreateDirectory()
            throws IOException
    {
        Path path = Paths.get(tFolder.getRoot().getAbsolutePath() + "/test-dir");
        fs.createDirectory(path);
        assertTrue(path.toFile().exists());
        assertTrue(path.toFile().isDirectory());
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testCreateDirectoryNotFound()
            throws IOException
    {
        Path path = Paths.get(tFolder.getRoot().getAbsolutePath() + "/test-fail/fail-dir");
        fs.createDirectory(path);
    }

    @Test(expectedExceptions = FileAlreadyExistsException.class)
    public void testCreateDirectoryAlreadyExists()
            throws IOException
    {
        Path path = Paths.get(tFolder.getRoot().getAbsolutePath() + "/test-dir-dup");
        fs.createDirectory(path);
        fs.createDirectory(path);
    }

    @Test
    public void testCreateDirectories()
            throws IOException
    {
        Path path = Paths.get(tFolder.getRoot().getAbsolutePath() + "/dir-parent/dir-children");
        fs.createDirectories(path);
        fs.createDirectories(path);
        assertTrue(fs.isDirectory(path));
    }

    @Test
    public void testDelete()
            throws IOException
    {
        File fileToDelete = tFolder.newFile("testDelete");
        fs.delete(fileToDelete.toPath());
        assertFalse(fs.exists(fileToDelete.toPath()));
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testDeleteNoSuchFile()
            throws IOException
    {
        assertFalse(fs.exists(nonExistingPath));
        fs.delete(nonExistingPath);
    }

    @Test(expectedExceptions = DirectoryNotEmptyException.class)
    public void testDeleteNonEmptyDir()
            throws IOException
    {
        Path subFolder = Paths.get(tFolder.getRoot().getAbsolutePath() + "/sub2");
        Path aFile = Paths.get(tFolder.getRoot().getAbsolutePath() + "/sub2/aFile");
        Files.createDirectories(subFolder);
        Files.createFile(aFile);
        fs.delete(subFolder);
    }

    @Test
    public void testDeleteIfExists()
            throws IOException
    {
        Path path = Paths.get(tFolder.getRoot().getAbsolutePath() + "/not-exist-file");
        assertFalse(fs.deleteIfExists(path));
        File fileToDelete = tFolder.newFile("testDeleteIfExists");
        assertTrue(fs.deleteIfExists(fileToDelete.toPath()));
    }

    @Test(expectedExceptions = DirectoryNotEmptyException.class)
    public void testDeleteIfExistsNonEmptyDir()
            throws IOException
    {
        Path subFolder = Paths.get(tFolder.getRoot().getAbsolutePath() + "/sub");
        Path aFile = Paths.get(tFolder.getRoot().getAbsolutePath() + "/sub/aFile");
        Files.createDirectories(subFolder);
        Files.createFile(aFile);
        fs.deleteIfExists(subFolder);
    }

    @Test
    public void testDeleteRecursively()
            throws IOException
    {
        Path newFolder = Paths.get(tFolder.getRoot().getAbsolutePath() + "/layer1");
        Path subFolder = Paths.get(tFolder.getRoot().getAbsolutePath() + "/layer1/layer2");
        Path aFile = Paths.get(tFolder.getRoot().getAbsolutePath() + "/layer1/file1");
        fs.createDirectories(subFolder);
        Files.createFile(aFile);
        boolean isPlainDeleteFailed = false;
        try {
            fs.delete(newFolder);
        }
        catch (DirectoryNotEmptyException ex) {
            isPlainDeleteFailed = true;
        }
        assertTrue(isPlainDeleteFailed);
        assertTrue(newFolder.toFile().exists());
        fs.deleteRecursively(newFolder);
        assertFalse(newFolder.toFile().exists());
    }

    @Test
    public void testMove()
            throws IOException
    {
        File fileToMove = tFolder.newFile("to-move");
        Path src = fileToMove.toPath();
        tFolder.newFolder("destination");
        Path target = Paths.get(tFolder.getRoot().toString() + "/destination/moved");
        fs.move(src, target);
        assertFalse(src.toFile().exists());
        assertTrue(target.toFile().exists());
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testMoveFailure()
            throws IOException
    {
        fs.move(nonExistingPath, tFolder.getRoot().toPath());
    }

    @Test
    public void testSimpleReadWrite()
            throws IOException
    {
        String content = "test";
        File testFile = tFolder.newFile();
        // First make sure no file is there
        if (testFile.exists()) {
            assertTrue(testFile.delete());
        }
        OutputStream os = fs.newOutputStream(testFile.toPath());
        os.write(content.getBytes());
        os.close();
        InputStream is = fs.newInputStream(testFile.toPath());
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        assertEquals(br.readLine(), content);
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testReadNoSuchFile()
            throws IOException
    {
        fs.newInputStream(nonExistingPath);
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testWriteParentDirNotExist()
            throws IOException
    {
        fs.newOutputStream(nonExistingPath);
    }

    @Test(expectedExceptions = FileAlreadyExistsException.class)
    public void testWriteDuplicate()
            throws IOException
    {
        Path path = tFolder.getRoot().toPath().resolve("testfileDup");
        OutputStream os = fs.newOutputStream(path);
        os.write("foo".getBytes());
        os.close();
        OutputStream os2 = fs.newOutputStream(path, CREATE_NEW);
        os2.write("bar".getBytes());
        os2.close();
    }

    @Test
    public void testIsDirectory()
    {
        assertTrue(fs.isDirectory(tFolder.getRoot().toPath()));
        assertFalse(fs.isDirectory(nonExistingPath));
    }

    @Test
    public void testGetProperties()
            throws IOException
    {
        Object resModifiedTime = fs.getAttribute(tFolder.getRoot().toPath(), "lastModifiedTime");
        Object resSize = fs.getAttribute(tFolder.getRoot().toPath(), "size");
        assertTrue(resModifiedTime instanceof Long);
        assertTrue(resSize instanceof Long);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetPropertiesInvalidProperty()
            throws IOException
    {
        fs.getAttribute(tFolder.getRoot().toPath(), "notSupportedAttr#");
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testGetPropertiesFailureNotExist()
            throws IOException
    {
        fs.getAttribute(nonExistingPath, "size");
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedCreateDirectories()
            throws IOException
    {
        fs.createDirectories(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedCreateDirectory()
            throws IOException
    {
        fs.createDirectory(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedDelete()
            throws IOException
    {
        fs.delete(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedDeleteIfExists()
            throws IOException
    {
        fs.deleteIfExists(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedDeleteRecursively()
            throws IOException
    {
        fs.deleteRecursively(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedMove()
            throws IOException
    {
        fs.move(OUTSIDE_WORKSPACE_PATH, OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedNewInputStream()
            throws IOException
    {
        fs.newInputStream(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedNewOutputStream()
            throws IOException
    {
        fs.newOutputStream(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedGetAttribute()
            throws IOException
    {
        fs.getAttribute(OUTSIDE_WORKSPACE_PATH, SupportedFileAttributes.SIZE);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedList()
            throws IOException
    {
        fs.list(OUTSIDE_WORKSPACE_PATH);
    }

    @Test(expectedExceptions = AccessDeniedException.class)
    public void testAccessDeniedWalk()
            throws IOException
    {
        fs.walk(OUTSIDE_WORKSPACE_PATH);
    }

    @AfterTest
    public void tearDown()
    {
        tFolder.close();
    }
}
