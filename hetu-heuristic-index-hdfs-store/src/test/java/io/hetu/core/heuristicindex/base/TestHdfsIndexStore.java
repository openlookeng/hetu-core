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

import io.hetu.core.heuristicindex.hdfs.HdfsIndexStore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@PrepareForTest(UserGroupInformation.class)
public class TestHdfsIndexStore
{
    public static final String CONFIG_DIR = "src/test/resources/config";

    private String testFileLocation = "/tmp/orcFileReaderTest" + new Random().nextInt(1000) + ".test";
    private HdfsIndexStore is;
    private FileSystem fs;

    @BeforeClass
    public void setup() throws IOException
    {
        Configuration conf = new Configuration();
        is = new HdfsIndexStore(null, conf, null);
        fs = is.getFs();
    }

    @AfterClass
    public void cleanUp() throws IOException
    {
        if (is != null) {
            is.close();
        }
    }

    public void assertTestFileContentEquals(String content) throws IOException
    {
        Path path = new Path(testFileLocation);
        BufferedInputStream bstream = new BufferedInputStream(fs.open(path));
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(bstream, StandardCharsets.UTF_8))) {
            // Notice: this only check the first line
            assertEquals(reader.readLine(), content);
        }
    }

    @Test
    public void testWriteSimple() throws IOException
    {
        String content = "test write simple";

        // Make sure no file exists
        is.delete(testFileLocation);
        is.write(content, testFileLocation, false);

        assertTestFileContentEquals(content);
    }

    @Test
    public void testWriteOverwrite() throws IOException
    {
        testWriteSimple();
        String content = "testOverwrite";

        is.write(content, testFileLocation, true);
        assertTestFileContentEquals(content);
    }

    @Test
    public void testOutputStreamWrite() throws IOException
    {
        String content = "testOutputStream";
        is.delete(testFileLocation);
        try (OutputStream os = is.getOutputStream(testFileLocation, false)) {
            is.write(content, os);
        }

        assertTestFileContentEquals(content);
    }

    @Test
    public void testOutputStreamWriteOverwrite() throws IOException
    {
        testOutputStreamWrite();
        String content = "test outputstream overwrite";
        try (OutputStream os = is.getOutputStream(testFileLocation, true)) {
            is.write(content, os);
        }
        assertTestFileContentEquals(content);
    }

    @Test
    public void testRead() throws IOException
    {
        is.delete(testFileLocation);
        String content = "read test";
        is.write(content, testFileLocation, true);
        StringBuilder read = new StringBuilder();
        try (InputStream in = is.read(testFileLocation)) {
            while (in.available() > 0) {
                read.append((char) in.read());
            }
        }
        assertEquals(content, read.toString());
    }

    @Test
    public void testDelete() throws IOException
    {
        is.write("test delete", testFileLocation, true);

        Path path = new Path(testFileLocation);
        assertTrue(fs.exists(path));
        is.delete(testFileLocation);
        assertFalse(fs.exists(path));
        assertEquals(fs.exists(path), is.exists(testFileLocation));
    }

    @Test
    public void testDeleteFolder() throws IOException
    {
        String testFolderLoation = "/tmp/orcFileReaderTest" + new Random().nextInt(1000);
        fs.mkdirs(new Path(testFolderLoation + "/testfolder/"));
        is.write("test delte folder", testFolderLoation + "/testFile1.test", true);
        is.write("test delete folder 2", testFolderLoation + "/testfolder/testFile2.test", true);
        is.delete(testFolderLoation);

        assertFalse(fs.exists(new Path(testFolderLoation)));
    }

    @Test
    public void testRename() throws IOException
    {
        String content = "test rename";
        is.write(content, testFileLocation, true);
        assertTrue(is.exists(testFileLocation));

        String newPath = testFileLocation.concat(".renamed");
        is.renameTo(testFileLocation, newPath);
        testFileLocation = newPath;

        assertTrue(is.exists(testFileLocation));
        assertTestFileContentEquals(content);
    }

    @Test
    public void testListChildren() throws IOException
    {
        fs.createNewFile(new Path("/tmp/UT/test1"));
        fs.createNewFile(new Path("/tmp/UT/test2"));
        is.listChildren("/tmp/UT", true).forEach(System.out::println);
    }

    @Test
    public void testLastModified() throws IOException, InterruptedException
    {
        is.delete(testFileLocation);
        is.write("Ut test", testFileLocation, true);
        long prev = is.getLastModifiedTime(testFileLocation);
        Thread.sleep(1000);
        try (OutputStream os = is.getOutputStream(testFileLocation, true)) {
            is.write(" appended", os);
        }

        long newTime = is.getLastModifiedTime(testFileLocation);
        assertTrue(newTime > prev);
    }

    @Test
    public void testFileSystemObjGeneration() throws IOException
    {
        Properties properties = new Properties();
        // Adjust the container properties to what HdfsIndexStore needs because properties from the container
        // is in hive.properties file, but IndexStore reads in from config.properties
        properties.setProperty("hdfs.config.resources",
                CONFIG_DIR + "/core-site.xml," + CONFIG_DIR + "/hdfs-site.xml");
        properties.setProperty("hdfs.authentication.type", "NONE");

        HdfsIndexStore store = new HdfsIndexStore(properties, null, null);

        assertNotNull(store.getFs());
    }

    @Test
    public void testKerberosAuthFlow() throws Exception
    {
        Properties props = new Properties();
        try (InputStream is = new FileInputStream(CONFIG_DIR + "/config.properties")) {
            props.load(is);
        }
        // Indexstore properties starts with hetu.filter.indexstore in config.properties, we wanna extract it first
        String indexStorePropertyPrefix = "hetu.filter.indexstore";
        Properties indexStoreProps = new Properties();
        props.stringPropertyNames().forEach(property -> {
            if (property.startsWith(indexStorePropertyPrefix)) {
                indexStoreProps.setProperty(
                        property.substring(property.indexOf(indexStorePropertyPrefix) + indexStorePropertyPrefix.length() + 1),
                        props.getProperty(property));
            }
        });

        HdfsIndexStore store = new HdfsIndexStore();
        store.setProperties(indexStoreProps);

        store.getFs();
    }
}
