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
import io.hetu.core.heuristicindex.base.LocalIndexStore;
import io.hetu.core.spi.heuristicindex.Index;
import io.hetu.core.spi.heuristicindex.IndexStore;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestIndexClient
{
    List<TempFolder> testFolders = new LinkedList<>();

    @Test
    public void testDeleteAllColumns() throws IOException
    {
        testDeleteSelectedColumnsHelper(new String[]{"c1", "c2", "c3"}, new String[]{"c1", "c2", "c3"});
    }

    @Test
    public void testDeleteSelectedColumns() throws IOException
    {
        testDeleteSelectedColumnsHelper(new String[]{"c1", "c2", "c3"}, new String[]{"c1", "c3"});
        testDeleteSelectedColumnsHelper(new String[]{"c1", "c2", "c3"}, new String[]{"c1"});
        testDeleteSelectedColumnsHelper(new String[]{"c1", "c2", "c3", "abc"}, new String[]{"c3"});
        testDeleteSelectedColumnsHelper(new String[]{"c1", "c2", "c3", "abc", "def"}, new String[]{"abc", "c2"});
    }

    private void testDeleteSelectedColumnsHelper(String[] columns, String[] deleted) throws IOException
    {
        String tableName = "catalog.schema.UT_test";
        assertTrue(columns.length >= deleted.length);

        String[] remained = new String[columns.length - deleted.length];
        int i = 0;
        for (String c : columns) {
            boolean found = false;
            for (String dc : deleted) {
                if (c.equals(dc)) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                remained[i++] = c;
            }
        }

        TempFolder folder = new TempFolder();
        testFolders.add(folder);
        folder.create();

        createIndexFolderStructure(folder.getRoot(), tableName, columns);
        IndexStore is = createTestIndexStore(folder.getRoot());
        Set<Index> emptyIndices = new HashSet<>();

        IndexClient client = new IndexClient(emptyIndices, is);
        client.deleteIndex(tableName, deleted);

        File tableFolder = new File(folder.getRoot(), tableName);
        if (remained.length == 0) {
            // if all columns are deleted, no folder/files should be under the table folder
            assertEquals(tableFolder.list().length, 0);
        }
        else {
            // if there are columns left, tableFolder has to have the same number of folders of the remained columns
            assertEquals(remained.length, tableFolder.list().length);
            for (String remainedColumn : remained) {
                // remained column should have at least one index file
                File columnFolder = new File(tableFolder, remainedColumn);
                assertTrue(columnFolder.list().length > 0);
            }
            for (String deletedColumn : deleted) {
                // deleted column should not exist
                File columnFolder = new File(tableName, deletedColumn);
                assertFalse(columnFolder.exists());
            }
        }
    }

    private IndexStore createTestIndexStore(File indexRootFolder) throws IOException
    {
        Properties props = new Properties();
        props.setProperty(IndexStore.ROOT_URI_KEY, indexRootFolder.getCanonicalPath());
        IndexStore is = new LocalIndexStore();
        is.setProperties(props);

        return is;
    }

    private void createIndexFolderStructure(File folder, String table, String... columns) throws IOException
    {
        File tableFolder = new File(folder, table);
        assertTrue(tableFolder.mkdir());
        for (String column : columns) {
            File columnFolder = new File(tableFolder, column);
            assertTrue(columnFolder.mkdirs());
            assertTrue(new File(columnFolder, "testIndex.index").createNewFile());
        }
    }

    @AfterTest
    public void cleanUp()
    {
        for (TempFolder folder : testFolders) {
            folder.close();
        }
    }
}
