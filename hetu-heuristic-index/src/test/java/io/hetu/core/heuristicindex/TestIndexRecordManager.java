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
import io.prestosql.spi.heuristicindex.IndexRecord;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

public class TestIndexRecordManager
{
    private static final HetuFileSystemClient FILE_SYSTEM_CLIENT = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(System.getProperty("java.io.tmpdir")));

    @Test
    public void testIndexRecordExpire()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager1 = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            IndexRecordManager indexRecordManager2 = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());

            indexRecordManager1.addIndexRecord("1", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList(), Arrays.asList("cp=1"));
            List<IndexRecord> original1 = indexRecordManager1.getIndexRecords();
            List<IndexRecord> original2 = indexRecordManager2.getIndexRecords();
            assertEquals(original2.size(), 1);
            assertEquals(original1, original2);

            List<IndexRecord> beforeadd1 = indexRecordManager1.getIndexRecords();
            indexRecordManager2.addIndexRecord("2", "testUser", "testTable", new String[] {"testColumn"}, "bloom", Collections.emptyList(), Arrays.asList("cp=1"));
            List<IndexRecord> added2 = indexRecordManager2.getIndexRecords();
            assertEquals(added2.size(), 2);

            indexRecordManager2.deleteIndexRecord("2", Collections.emptyList());
            List<IndexRecord> deleted1 = indexRecordManager1.getIndexRecords();
            List<IndexRecord> deleted2 = indexRecordManager2.getIndexRecords();
            assertEquals(deleted2.size(), 1);
            assertEquals(deleted1, deleted2);

            assertSame(original1, beforeadd1);
            assertNotSame(original1, deleted1);
        }
    }

    @Test(timeOut = 30000)
    public void testConcurrentMultipleManagers()
            throws IOException, InterruptedException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();

            // 6 entries will be created by different threads in parallel, in which the first four will be deleted also in parallel
            String[] names = new String[] {"a", "b", "c", "d", "e", "f"};
            Thread[] threads = new Thread[10];

            for (int i = 0; i < 6; i++) {
                int finalI = i;
                threads[i] = new Thread(() -> {
                    try {
                        new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath())
                                .addIndexRecord(names[finalI], "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList(), Arrays.asList("cp=1"));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].start();
            }

            for (int i = 6; i < 10; i++) {
                int finalI = i;
                threads[i] = new Thread(() -> {
                    try {
                        IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
                        while (indexRecordManager.lookUpIndexRecord(names[finalI - 6]) == null) {
                            Thread.sleep(50L);
                        }
                        indexRecordManager.deleteIndexRecord(names[finalI - 6], Collections.emptyList());
                    }
                    catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            assertEquals(indexRecordManager.getIndexRecords().size(), 2);
            assertNull(indexRecordManager.lookUpIndexRecord(names[0]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[1]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[2]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[3]));
            assertNotNull(indexRecordManager.lookUpIndexRecord(names[4]));
            assertNotNull(indexRecordManager.lookUpIndexRecord(names[5]));
        }
    }

    @Test(timeOut = 20000)
    public void testConcurrentSingleManager()
            throws IOException, InterruptedException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();

            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());

            // 6 entries will be created by different threads in parallel, in which the first four will be deleted also in parallel
            String[] names = new String[] {"a", "b", "c", "d", "e", "f"};
            Thread[] threads = new Thread[10];

            for (int i = 0; i < 6; i++) {
                int finalI = i;
                threads[i] = new Thread(() -> {
                    try {
                        indexRecordManager.addIndexRecord(names[finalI], "u", "t", new String[] {"c"}, "minmax", Collections.emptyList(), Arrays.asList("cp=1"));
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].start();
            }

            for (int i = 6; i < 10; i++) {
                int finalI = i;
                threads[i] = new Thread(() -> {
                    try {
                        while (indexRecordManager.lookUpIndexRecord(names[finalI - 6]) == null) {
                            Thread.sleep(50L);
                        }
                        indexRecordManager.deleteIndexRecord(names[finalI - 6], Collections.emptyList());
                    }
                    catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                });
                threads[i].start();
            }

            for (Thread thread : threads) {
                thread.join();
            }

            assertEquals(indexRecordManager.getIndexRecords().size(), 2);
            assertNull(indexRecordManager.lookUpIndexRecord(names[0]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[1]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[2]));
            assertNull(indexRecordManager.lookUpIndexRecord(names[3]));
            assertNotNull(indexRecordManager.lookUpIndexRecord(names[4]));
            assertNotNull(indexRecordManager.lookUpIndexRecord(names[5]));
        }
    }

    @Test
    public void testDelete()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            indexRecordManager.addIndexRecord("1", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList(), Arrays.asList("cp=1"));
            indexRecordManager.addIndexRecord("2", "testUser", "testTable", new String[] {"testColumn"}, "minmax", Collections.emptyList(), Arrays.asList("cp=1"));
            assertNotNull(indexRecordManager.lookUpIndexRecord("1"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 2);

            // Delete 1
            indexRecordManager.deleteIndexRecord("1", Collections.emptyList());
            assertNull(indexRecordManager.lookUpIndexRecord("1"));
            assertNotNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 1);

            // Delete 1 again
            indexRecordManager.deleteIndexRecord("1", Collections.emptyList());
            assertNull(indexRecordManager.lookUpIndexRecord("1"));
            assertNotNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 1);

            // Delete 2
            indexRecordManager.deleteIndexRecord("2", Collections.emptyList());
            assertNull(indexRecordManager.lookUpIndexRecord("2"));
            assertEquals(indexRecordManager.getIndexRecords().size(), 0);
        }
    }

    @Test
    public void testAddAndLookUp()
            throws IOException
    {
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "MINMAX", Collections.emptyList(), Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn", "testColumn2"}, "MINMAX", Collections.emptyList(), Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "MINMAX", Collections.emptyList(), ImmutableList.of("12"));
        testIndexRecordAddLookUpHelper("testName", "testUser", "testTable", new String[] {"testColumn"}, "MINMAX", Collections.emptyList(), ImmutableList.of("12", "123"));
    }

    private void testIndexRecordAddLookUpHelper(String name, String user, String table, String[] columns, String indexType, List<String> indexProperties, List<String> partitions)
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            IndexRecordManager indexRecordManager = new IndexRecordManager(FILE_SYSTEM_CLIENT, folder.getRoot().toPath());
            IndexRecord expected = new IndexRecord(name, user, table, columns, indexType, indexProperties, partitions);
            indexRecordManager.addIndexRecord(name, user, table, columns, indexType, indexProperties, partitions);

            IndexRecord actual1 = indexRecordManager.lookUpIndexRecord(name);
            assertNotNull(actual1);
            assertEquals(actual1, expected);

            IndexRecord actual2 = indexRecordManager.lookUpIndexRecord(table, columns, indexType);
            assertNotNull(actual2);
            assertEquals(actual2, expected);
        }
    }
}
