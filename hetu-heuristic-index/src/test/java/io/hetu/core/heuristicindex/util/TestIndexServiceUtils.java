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
package io.hetu.core.heuristicindex.util;

import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static io.hetu.core.heuristicindex.util.IndexServiceUtils.getPropertiesSubset;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIndexServiceUtils
{
    private static final HetuFileSystemClient LOCAL_FS_CLIENT = new HetuLocalFileSystemClient(
            new LocalConfig(new Properties()), Paths.get("/tmp"));

    @Test
    public void testGetPath()
    {
        String[] input1 = new String[] {"abc", "efg_s", "f5%3132d", "dfs_s"};
        String expected1 = "efg_s";
        checkStringEquals(IndexServiceUtils.getPath(input1, "_s"), expected1);

        String[] input2 = new String[] {"random", "字符"};
        String expected2 = "字符";
        checkStringEquals(IndexServiceUtils.getPath(input2, "符"), expected2);

        assertNull(IndexServiceUtils.getPath(input2, "字字符"));
    }

    @Test
    public void testFormatPathAsFolder()
    {
        String testStr1 = "random";
        String expected1 = "random" + File.separator;
        assertTrue(IndexServiceUtils.formatPathAsFolder(testStr1).equals(expected1));

        String testStr2 = "random" + File.separator;
        String expected2 = "random" + File.separator;
        assertTrue(IndexServiceUtils.formatPathAsFolder(testStr2).equals(expected2));
    }

    @Test
    public void testIsFileExisting() throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File tmpFile = folder.newFile();
            assertTrue(tmpFile.delete());
            assertThrows(IllegalArgumentException.class, () -> IndexServiceUtils.isFileExisting(tmpFile));
        }
    }

    @Test
    public void testLoadProperties() throws IOException
    {
        Properties props = new Properties();
        props.setProperty("connector.name", "hive-hadoop2");
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            File temp = folder.newFile();
            try (OutputStream os = new FileOutputStream(temp)) {
                props.store(os, "test");
            }

            Properties properties = IndexServiceUtils.loadProperties(temp);
            assertEquals("hive-hadoop2", properties.getProperty("connector.name"));
        }
    }

    @Test
    public void testGetPathReturnNull()
    {
        String[] inputPath = {"/root/hetu"};
        String suffix = "e_hetu";
        String path = IndexServiceUtils.getPath(inputPath, suffix);
        assertNull(path);
    }

    @Test
    public void testGetPathWithValidValue()
    {
        String[] inputPath = {"/root/hetu", "/root/lookeng"};
        String suffix = "lookeng";
        String path = IndexServiceUtils.getPath(inputPath, suffix);
        assertEquals(inputPath[1], path);
    }

    @Test
    public void testValidGetTableParts()
    {
        String[] parts = IndexServiceUtils.getTableParts("catalog.schema.table");
        assertEquals(3, parts.length);
        assertEquals("catalog", parts[0]);
        assertEquals("schema", parts[1]);
        assertEquals("table", parts[2]);
    }

    @Test
    public void testArchive()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            Path tarPath = Paths.get(folder.getRoot().getAbsolutePath(), IndexConstants.LAST_MODIFIED_FILE_PREFIX + "123456.tar");
            folder.newFile("100000.bloom");
            folder.newFile("200000.bloom");
            IndexServiceUtils.writeToHdfs(LOCAL_FS_CLIENT, LOCAL_FS_CLIENT, folder.getRoot().toPath(), tarPath);

            assertTrue(Files.exists(tarPath));
            Set<String> filesInTar = new HashSet<>();
            try (TarArchiveInputStream i = new TarArchiveInputStream(Files.newInputStream(tarPath))) {
                ArchiveEntry entry;
                while ((entry = i.getNextEntry()) != null) {
                    if (!i.canReadEntryData(entry)) {
                        throw new FileSystemException("Unable to read archive entry: " + entry.toString());
                    }

                    String filename = entry.getName();
                    filesInTar.add(filename);
                }
            }
            assertEquals(filesInTar.size(), 2);
            assertTrue(filesInTar.contains("100000.bloom"));
            assertTrue(filesInTar.contains("200000.bloom"));
        }
    }

    @DataProvider(name = "invalidTableNames")
    public static Object[][] invalidTableNames()
    {
        return new Object[][] {{"dc.catalog.schema.table", false}, {"schema.table", false}, {".schema.table", false}, {" .schema.table", false},
                {"table", false}, {"catalog..table", false}, {"catalog.schema.", false}};
    }

    @Test(expectedExceptions = IllegalArgumentException.class, dataProvider = "invalidTableNames")
    public void testInvalidGetTableParts(String tableName, Boolean expected)
    {
        IndexServiceUtils.getTableParts(tableName);
    }

    private void checkStringEquals(String input, String expected)
    {
        if (!input.equals(expected)) {
            throw new AssertionError("String not matched between input: " + input + " and expected: " + expected);
        }
    }

    @Test
    public void testGetPropertiesSubset()
    {
        Properties input = new Properties();
        input.put("key1", "value1");
        input.put("\"key2\"", "\"value2\"");
        input.put("'key3'", "'value3'");
        input.put("key4", 4);
        input.put("bloom.key5", "value5");
        input.put("\"bloom.key6\"", "\"value6\"");
        input.put("'bloom.key7'", "'value7'");
        input.put("bloom.key8", 8);

        Properties expectedOutput = new Properties();
        expectedOutput.put("key5", "value5");
        expectedOutput.put("key6", "value6");
        expectedOutput.put("key7", "value7");
        expectedOutput.put("key8", "8");

        Properties output = getPropertiesSubset(input, "bloom.");
        assertEquals(expectedOutput, output);
    }
}
