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

import com.google.common.collect.ImmutableList;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import org.testng.annotations.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestIndexRecordManager
{
    private static final HetuFileSystemClient FILE_SYSTEM_CLIENT = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(System.getProperty("java.io.tmpdir")));

    @Test
    public void testDelete()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            indexRecordManager.addIndexRecord("1", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            indexRecordManager.addIndexRecord("2", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            assertNotNull(indexRecordManager.lookUpIndexRecord("1"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 2);

            // Delete 1
            indexRecordManager.deleteIndexRecord("1");
            assertNull(indexRecordManager.lookUpIndexRecord("1"));
            assertNotNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 1);

            // Delete 1 again
            indexRecordManager.deleteIndexRecord("1");
            assertNull(indexRecordManager.lookUpIndexRecord("1"));
            assertNotNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 1);

            // Delete 2
            indexRecordManager.deleteIndexRecord("2");
            assertNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 0);
        }
    }

    @Test
    public void testAddAndLookUp()
            throws IOException, IllegalAccessException
    {
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn", "testColumn2"}, "minmax", Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", ImmutableList.of("12"));
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", ImmutableList.of("12", "123"));
    }

    @Test
    public void testRecordEqualAndHash()
    {
        IndexRecord r1 = new IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList());
        IndexRecord r2 = new IndexRecord("testName", "testUser", "testTable", new String[] {
                "testColumn"}, "minmax", ImmutableList.of("note"));
        IndexRecord r3 = new IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn"}, "bloom", Collections.emptyList());
        IndexRecord r4 = new IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn",
                "testColumn2"}, "minmax", Collections.emptyList());
        assertEquals(r1, r1);
        assertEquals(r1, r2);
        assertNotEquals(r1, r3);
        assertNotEquals(r1, r4);

        HashSet<IndexRecord> testSet = new HashSet<>();
        testSet.add(r1);
        assertEquals(testSet.size(), 1);
        testSet.add(r2);
        assertEquals(testSet.size(), 1);
        testSet.add(r3);
        assertEquals(testSet.size(), 2);
        testSet.add(r4);
        assertEquals(testSet.size(), 3);
    }

    @Test(expectedExceptions = AssertionError.class)
    public void testAddAndLookUpDifferentNotes()
            throws IOException, IllegalAccessException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            IndexRecord expected = new IndexRecord("testName", "testUser", "testTable", new String[] {
                    "testColumn"}, "minmax", ImmutableList.of(""));
            indexRecordManager.addIndexRecord("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            IndexRecord actual = indexRecordManager.lookUpIndexRecord("testName");
            assertIndexRecordFullyEqual(actual, expected);
        }
    }

    private void testIndexRecordAddLookUpHelper(String name, String user, String table, String[] columns, String indexType, List<String> note)
            throws IOException, IllegalAccessException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            IndexRecord expected = new IndexRecord(name, user, table, columns, indexType, note);
            indexRecordManager.addIndexRecord(name, user, table, columns, indexType, note.toArray(new String[0]));

            IndexRecord actual1 = indexRecordManager.lookUpIndexRecord(name);
            assertNotNull(actual1);
            assertIndexRecordFullyEqual(actual1, expected);

            IndexRecord actual2 = indexRecordManager.lookUpIndexRecord(table, columns, indexType);
            assertNotNull(actual2);
            assertIndexRecordFullyEqual(actual2, expected);
        }
    }

    // Compare two IndexRecord objects and assert all fields are equal.
    // Unlike the equals() method of IndexRecord, this method compares ALL fields for testing.
    private void assertIndexRecordFullyEqual(IndexRecord actual, IndexRecord expected)
            throws IllegalAccessException
    {
        for (Field field : actual.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            assertEquals(field.get(actual), field.get(expected));
        }
    }
}
