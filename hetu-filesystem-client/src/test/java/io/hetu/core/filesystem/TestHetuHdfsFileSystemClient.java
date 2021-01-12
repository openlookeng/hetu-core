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

import io.hetu.core.filesystem.utils.DockerizedHive;
import io.prestosql.spi.filesystem.SupportedFileAttributes;
import org.apache.hadoop.conf.Configuration;
import org.testng.SkipException;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test for HetuHdfsFileSystemClient
 *
 * @since 2020-04-01
 */
public class TestHetuHdfsFileSystemClient
{
    private static final Path NON_EXISTING_PATH = Paths.get("/tmp/test-hetuhdfs/path/to/a/non/existing/file");
    private static final Path OUTSIDE_WORKSPACE_PATH = Paths.get("/dir/out/side/of/workspace");
    private final String rootPath = "/tmp/test-hetuhdfs";
    private HetuHdfsFileSystemClient fs;

    @BeforeClass
    public void prepare()
            throws IOException
    {
        // Will skip the test if exception occurs in this process
        Configuration config = null;
        int trialCount = 0;

        while (config == null && trialCount < 3) {
            config = DockerizedHive.getContainerConfig(this.getClass().getName());
            trialCount++;
        }

        if (config == null) {
            throw new SkipException("Docker environment for this test not setup");
        }

        fs = new HetuHdfsFileSystemClient(new HdfsConfig(config), Paths.get(rootPath));

        if (fs.getHdfs().exists(new org.apache.hadoop.fs.Path(rootPath))) {
            fs.getHdfs().delete(new org.apache.hadoop.fs.Path(rootPath), true);
        }
        fs.getHdfs().mkdirs(new org.apache.hadoop.fs.Path(rootPath));
    }

    @Test
    public void testCreateDirectory()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/testCreateDirectory");
        fs.createDirectory(path);
        assertTrue(fs.getHdfs().exists(HetuHdfsFileSystemClient.toHdfsPath(path)));
        assertTrue(fs.isDirectory(path));
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testCreateDirectoryNotFound()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/test-fail/fail-dir");
        fs.createDirectory(path);
    }

    @Test(expectedExceptions = FileAlreadyExistsException.class)
    public void testCreateDirectoryAlreadyExists()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/testCreateDirectoryFail");
        fs.createDirectory(path);
        fs.createDirectory(path);
    }

    @Test
    void testCreateDirectories()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/dir-parent/dir-children");
        fs.createDirectories(path);
        fs.createDirectories(path);
        assertTrue(fs.isDirectory(path));
    }

    @Test
    public void testDelete()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/file-to-delete");
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(path));
        assertTrue(fs.exists(path));
        fs.delete(path);
        assertFalse(fs.exists(path));
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testDeleteNoSuchFile()
            throws IOException
    {
        assertFalse(fs.exists(NON_EXISTING_PATH));
        fs.delete(NON_EXISTING_PATH);
    }

    @Test(expectedExceptions = DirectoryNotEmptyException.class)
    public void testDeleteNonEmptyDir()
            throws IOException
    {
        Path sub = Paths.get(rootPath + "/sub");
        Path aFile = Paths.get(rootPath + "/sub/aFile");
        fs.createDirectories(sub);
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(aFile));
        fs.delete(sub);
    }

    @Test
    public void testDeleteIfExists()
            throws IOException
    {
        assertFalse(fs.deleteIfExists(NON_EXISTING_PATH));
        Path path2 = Paths.get(rootPath + "/testDeleteIfExists");
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(path2));
        assertTrue(fs.deleteIfExists(path2));
    }

    @Test(expectedExceptions = DirectoryNotEmptyException.class)
    public void testDeleteIfExistsNonEmptyDir()
            throws IOException
    {
        Path sub = Paths.get(rootPath + "/sub2");
        Path aFile = Paths.get(rootPath + "/sub2/aFile");
        fs.createDirectories(sub);
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(aFile));
        fs.deleteIfExists(sub);
    }

    @Test
    public void testDeleteRecursively()
            throws IOException
    {
        Path newFolder = Paths.get(rootPath + "/layer1");
        Path subFolder = Paths.get(rootPath + "/layer1/layer2");
        Path aFile = Paths.get(rootPath + "/layer1/file1");
        fs.createDirectories(subFolder);
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(aFile));
        boolean isPlainDeleteFailed = false;
        // delete() should not delete recursively and throw an Exception
        try {
            fs.delete(newFolder);
        }
        catch (DirectoryNotEmptyException ex) {
            isPlainDeleteFailed = true;
        }
        assertTrue(isPlainDeleteFailed);
        assertTrue(fs.exists(newFolder));
        // deleteRecursively() should be able to delete recursively
        fs.deleteRecursively(newFolder);
        assertFalse(fs.exists(newFolder));
    }

    @Test
    public void testMove()
            throws IOException
    {
        Path src = Paths.get(rootPath + "/to-move");
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(src));
        assertTrue(fs.exists(src));
        fs.createDirectories(Paths.get(rootPath + "/destination"));
        Path target = Paths.get(rootPath + "/destination/moved");
        fs.move(src, target);
        assertFalse(fs.exists(src));
        assertTrue(fs.exists(target));
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testMoveFailure()
            throws IOException
    {
        fs.move(NON_EXISTING_PATH, Paths.get(rootPath));
    }

    @Test
    public void testSimpleReadWrite()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/testfile");
        assertFalse(fs.exists(path));
        String content = "test content";
        OutputStream os = fs.newOutputStream(path);
        os.write(content.getBytes());
        os.close();
        assertTrue(fs.exists(path));
        InputStream is = fs.newInputStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        assertEquals(br.readLine(), content);
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testReadNoSuchFile()
            throws IOException
    {
        fs.newInputStream(NON_EXISTING_PATH);
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testWriteParentDirNotExist()
            throws IOException
    {
        fs.newOutputStream(NON_EXISTING_PATH);
    }

    @Test(expectedExceptions = FileAlreadyExistsException.class)
    public void testWriteDuplicate()
            throws IOException
    {
        Path path = Paths.get(rootPath + "/testfileDup");
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
        assertTrue(fs.isDirectory(Paths.get(rootPath)));
        assertFalse(fs.isDirectory(NON_EXISTING_PATH));
    }

    @Test
    public void testGetProperties()
            throws IOException
    {
        Object resModifiedTime = fs.getAttribute(Paths.get(rootPath), "lastModifiedTime");
        Object resSize = fs.getAttribute(Paths.get(rootPath), "size");
        assertTrue(resModifiedTime instanceof Long);
        assertTrue(resSize instanceof Long);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void testGetPropertiesInvalidProperty()
            throws IOException
    {
        fs.getAttribute(Paths.get(rootPath), "notSupportedAttr#");
    }

    @Test(expectedExceptions = NoSuchFileException.class)
    public void testGetPropertiesFailureNotExist()
            throws IOException
    {
        fs.getAttribute(NON_EXISTING_PATH, "size");
    }

    @Test
    public void testListAndWalk()
            throws IOException
    {
        Path listRoot = Paths.get(rootPath + "/listRoot");
        Path fileInRoot = Paths.get(rootPath + "/listRoot/fileInRoot");
        Path folderInRoot = Paths.get(rootPath + "/listRoot/subFolder");
        Path fileInSubfolder = Paths.get(rootPath + "/listRoot/subFolder/fileInSubfolder");
        fs.createDirectories(folderInRoot);
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(fileInRoot));
        fs.getHdfs().create(HetuHdfsFileSystemClient.toHdfsPath(fileInSubfolder));

        Stream<Path> lsRoot = fs.list(listRoot);
        Stream<Path> lsSub = fs.list(folderInRoot);
        assertStreamContentEquals(lsRoot, Stream.of(fileInRoot, folderInRoot));
        assertStreamContentEquals(lsSub, Stream.of(fileInSubfolder));

        Stream<Path> wkRoot = fs.walk(listRoot);
        Stream<Path> wkSub = fs.walk(folderInRoot);
        assertStreamContentEquals(wkRoot, Stream.of(listRoot, fileInRoot, folderInRoot, fileInSubfolder));
        assertStreamContentEquals(wkSub, Stream.of(folderInRoot, fileInSubfolder));
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
            throws IOException
    {
        fs.getHdfs().delete(new org.apache.hadoop.fs.Path(rootPath), true);
    }

    /**
     * Compare the content of two streams and assert same regardless of order.
     */
    private void assertStreamContentEquals(Stream<Path> s1, Stream<Path> s2)
    {
        List<Path> l1 = s1.collect(Collectors.toList());
        List<Path> l2 = s2.collect(Collectors.toList());
        Collections.sort(l1);
        Collections.sort(l2);
        assertEquals(l1, l2);
    }
}
