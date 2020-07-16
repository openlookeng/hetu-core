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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

public class TestIndexServiceUtils
{
    @Test
    public void testGetPath()
    {
        String[] input1 = new String[]{"abc", "efg_s", "f5%3132d", "dfs_s"};
        String expected1 = "efg_s";
        checkStringEquals(IndexServiceUtils.getPath(input1, "_s"), expected1);

        String[] input2 = new String[]{"random", "character"};
        String expected2 = "character";
        checkStringEquals(IndexServiceUtils.getPath(input2, "cter"), expected2);

        assertNull(IndexServiceUtils.getPath(input2, "e_char"));
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
        String[] parts;

        parts = IndexServiceUtils.getTableParts("catalog.schema.table");
        assertEquals(3, parts.length);
        assertEquals("catalog", parts[0]);
        assertEquals("schema", parts[1]);
        assertEquals("table", parts[2]);

        parts = IndexServiceUtils.getTableParts("dc.catalog.schema.table");
        assertEquals(3, parts.length);
        assertEquals("dc.catalog", parts[0]);
        assertEquals("schema", parts[1]);
        assertEquals("table", parts[2]);
    }

    @DataProvider(name = "invalidTableNames")
    public static Object[][] invalidTableNames()
    {
        return new Object[][]{{"schema.table", false}, {".schema.table", false}, {" .schema.table", false},
                {"table", false}};
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
}
