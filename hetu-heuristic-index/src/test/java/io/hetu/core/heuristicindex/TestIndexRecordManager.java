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
    private static final HetuFileSystemClient FILE_SYSTEM_CLIENT = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get("/"));

    @Test
    public void testDelete()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager.addIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            IndexRecordManager.addIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "2", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            assertNotNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1"));
            assertEquals(IndexRecordManager.readAllIndexRecords(FILE_SYSTEM_CLIENT, folder.getRoot().toPath()).size(), 2);

            // Delete 1
            IndexRecordManager.deleteIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1");
            assertNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1"));
            assertNotNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "2"));
            assertEquals(IndexRecordManager.readAllIndexRecords(FILE_SYSTEM_CLIENT, folder.getRoot().toPath()).size(), 1);

            // Delete 1 again
            IndexRecordManager.deleteIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1");
            assertNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "1"));
            assertNotNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "2"));
            assertEquals(IndexRecordManager.readAllIndexRecords(FILE_SYSTEM_CLIENT, folder.getRoot().toPath()).size(), 1);

            // Delete 2
            IndexRecordManager.deleteIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "2");
            assertNull(IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "2"));
            assertEquals(IndexRecordManager.readAllIndexRecords(FILE_SYSTEM_CLIENT, folder.getRoot().toPath()).size(), 0);
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
        IndexRecordManager.IndexRecord r1 = new IndexRecordManager.IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList());
        IndexRecordManager.IndexRecord r2 = new IndexRecordManager.IndexRecord("testName", "testUser", "testTable", new String[] {
                "testColumn"}, "minmax", ImmutableList.of("note"));
        IndexRecordManager.IndexRecord r3 = new IndexRecordManager.IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn"}, "bloom", Collections.emptyList());
        IndexRecordManager.IndexRecord r4 = new IndexRecordManager.IndexRecord("testName", "testUser", "testTable", new String[] {"testColumn",
                "testColumn2"}, "minmax", Collections.emptyList());
        assertEquals(r1, r1);
        assertEquals(r1, r2);
        assertNotEquals(r1, r3);
        assertNotEquals(r1, r4);

        HashSet<IndexRecordManager.IndexRecord> testSet = new HashSet<>();
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
            IndexRecordManager.IndexRecord expected = new IndexRecordManager.IndexRecord("testName", "testUser", "testTable", new String[] {
                    "testColumn"}, "minmax", ImmutableList.of(""));
            IndexRecordManager.addIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "testName", "testUser", "testTable", new String[] {"testColumn"}, "minmax", "cp=1");
            IndexRecordManager.IndexRecord actual = IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), "testName");
            assertIndexRecordFullyEqual(actual, expected);
        }
    }

    private void testIndexRecordAddLookUpHelper(String name, String user, String table, String[] columns, String indexType, List<String> note)
            throws IOException, IllegalAccessException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager.IndexRecord expected = new IndexRecordManager.IndexRecord(name, user, table, columns, indexType, note);
            IndexRecordManager.addIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), name, user, table, columns, indexType, note.toArray(new String[0]));

            IndexRecordManager.IndexRecord actual1 = IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), name);
            assertNotNull(actual1);
            assertIndexRecordFullyEqual(actual1, expected);

            IndexRecordManager.IndexRecord actual2 = IndexRecordManager.lookUpIndexRecord(FILE_SYSTEM_CLIENT, folder.getRoot().toPath(), table, columns, indexType);
            assertNotNull(actual2);
            assertIndexRecordFullyEqual(actual2, expected);
        }
    }

    // Compare two IndexRecord objects and assert all fields are equal.
    // Unlike the equals() method of IndexRecord, this method compares ALL fields for testing.
    private void assertIndexRecordFullyEqual(IndexRecordManager.IndexRecord actual, IndexRecordManager.IndexRecord expected)
            throws IllegalAccessException
    {
        for (Field field : actual.getClass().getDeclaredFields()) {
            field.setAccessible(true);
            assertEquals(field.get(actual), field.get(expected));
        }
    }
}
