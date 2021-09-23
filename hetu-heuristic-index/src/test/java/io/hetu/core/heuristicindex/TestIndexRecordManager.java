/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.airlift.testing.mysql.TestingMySqlServer;
import io.hetu.core.common.filesystem.TempFolder;
import io.hetu.core.filesystem.HetuLocalFileSystemClient;
import io.hetu.core.filesystem.LocalConfig;
import io.hetu.core.metastore.hetufilesystem.HetuFsMetastore;
import io.hetu.core.metastore.hetufilesystem.HetuFsMetastoreConfig;
import io.hetu.core.metastore.jdbc.JdbcHetuMetastore;
import io.hetu.core.metastore.jdbc.JdbcMetastoreConfig;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.IndexRecord;
import io.prestosql.spi.metastore.HetuMetastore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestIndexRecordManager
{
    private static final HetuFileSystemClient FILE_SYSTEM_CLIENT = new HetuLocalFileSystemClient(new LocalConfig(new Properties()), Paths.get(System.getProperty("java.io.tmpdir")));
    private HetuMetastore testMetastore1;
    private HetuMetastore testMetastore2;
    private TestingMySqlServer mysqlServer;

    @BeforeClass
    public void setup()
            throws Exception
    {
        mysqlServer = new TestingMySqlServer("test", "mysql", "metastore1", "metastore2");
        testMetastore1 = new JdbcHetuMetastore(new JdbcMetastoreConfig()
                .setDbUrl(mysqlServer.getJdbcUrl("metastore1"))
                .setDbUser(mysqlServer.getUser())
                .setDbPassword(mysqlServer.getPassword()));
        testMetastore2 = new JdbcHetuMetastore(new JdbcMetastoreConfig()
                .setDbUrl(mysqlServer.getJdbcUrl("metastore2"))
                .setDbUser(mysqlServer.getUser())
                .setDbPassword(mysqlServer.getPassword()));
    }

    @Test(timeOut = 30000)
    public void testConcurrentMultipleManagers()
            throws InterruptedException
    {
        Random random = new Random();
        String[] names = new String[] {"a", "b", "c"};
        Thread[] threads = new Thread[4];

        // create index with name[0], name[1], name[2]
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(random.nextInt(100));
                    new IndexRecordManager(testMetastore1)
                            .addIndexRecord(names[finalI], "testUser", "c.s.t", new String[] {"testColumn"}, names[finalI], 0L, Collections.emptyList(), Arrays.asList("cp=1"));
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }

        // delete index with name[0]
        threads[3] = new Thread(() -> {
            try {
                IndexRecordManager indexRecordManager = new IndexRecordManager(testMetastore1);
                while (indexRecordManager.lookUpIndexRecord(names[0]) == null) {
                    Thread.sleep(random.nextInt(100));
                }
                indexRecordManager.deleteIndexRecord(names[0], Collections.emptyList());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        threads[3].start();

        for (Thread thread : threads) {
            thread.join();
        }

        IndexRecordManager indexRecordManager = new IndexRecordManager(testMetastore1);
        assertEquals(indexRecordManager.getIndexRecords().size(), 2);
        assertNull(indexRecordManager.lookUpIndexRecord(names[0]));
        assertNotNull(indexRecordManager.lookUpIndexRecord(names[1]));
        assertNotNull(indexRecordManager.lookUpIndexRecord(names[2]));
    }

    @Test(timeOut = 20000)
    public void testConcurrentSingleManager()
            throws InterruptedException
    {
        Random random = new Random();
        IndexRecordManager indexRecordManager = new IndexRecordManager(testMetastore2);

        String[] names = new String[] {"a", "b", "c"};
        Thread[] threads = new Thread[4];

        // create index with name[0], name[1], name[2]
        for (int i = 0; i < 3; i++) {
            int finalI = i;
            threads[i] = new Thread(() -> {
                try {
                    Thread.sleep(random.nextInt(100));
                    indexRecordManager.addIndexRecord(names[finalI], "u", "c.s.t", new String[] {"c"}, names[finalI], 0L, Collections.emptyList(), Arrays.asList("cp=1"));
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads[i].start();
        }

        // delete index with name[0]
        threads[3] = new Thread(() -> {
            try {
                while (indexRecordManager.lookUpIndexRecord(names[0]) == null) {
                    Thread.sleep(random.nextInt(100));
                }
                indexRecordManager.deleteIndexRecord(names[0], Collections.emptyList());
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        threads[3].start();

        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(indexRecordManager.getIndexRecords().size(), 2);
        assertNull(indexRecordManager.lookUpIndexRecord(names[0]));
        assertNotNull(indexRecordManager.lookUpIndexRecord(names[1]));
        assertNotNull(indexRecordManager.lookUpIndexRecord(names[2]));
    }

    @Test
    public void testDelete()
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            HetuMetastore testMetaStore = new HetuFsMetastore(
                    new HetuFsMetastoreConfig().setHetuFileSystemMetastorePath(folder.getRoot().getPath()),
                    FILE_SYSTEM_CLIENT);

            IndexRecordManager indexRecordManager = new IndexRecordManager(testMetaStore);
            indexRecordManager.addIndexRecord("1", "testUser", "testCatalog.testSchema.testTable", new String[] {"testColumn"}, "minmax", 0L, Collections.emptyList(), Arrays.asList("cp=1"));
            indexRecordManager.addIndexRecord("2", "testUser", "testCatalog.testSchema.testTable", new String[] {"testColumn"}, "bloom", 0L, Collections.emptyList(), Arrays.asList("cp=1"));
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
        testIndexRecordAddLookUpHelper("testName", "testUser", "testCatalog.testSchema.testTable", new String[] {
                "test_column"}, "A", Collections.emptyList(), Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testCatalog.testSchema.testTable", new String[] {"test_column",
                "test_column2"}, "B", Collections.emptyList(), Collections.emptyList());
        testIndexRecordAddLookUpHelper("testName", "testUser", "testCatalog.testSchema.testTable", new String[] {
                "test_column"}, "C", Collections.emptyList(), ImmutableList.of("12"));
        testIndexRecordAddLookUpHelper("testName", "testUser", "testCatalog.testSchema.testTable", new String[] {
                "test_column"}, "D", Collections.emptyList(), ImmutableList.of("12", "123"));
    }

    @AfterClass
    public void cleanUp()
    {
        mysqlServer.close();
    }

    private void testIndexRecordAddLookUpHelper(String name, String user, String table, String[] columns, String indexType, List<String> indexProperties, List<String> partitions)
            throws IOException
    {
        try (TempFolder folder = new TempFolder()) {
            folder.create();
            HetuMetastore testMetaStore = new HetuFsMetastore(
                    new HetuFsMetastoreConfig().setHetuFileSystemMetastorePath(folder.getRoot().getPath()),
                    FILE_SYSTEM_CLIENT);

            IndexRecordManager indexRecordManager = new IndexRecordManager(testMetaStore);
            IndexRecord expected = new IndexRecord(name, user, table, columns, indexType, 0L, indexProperties, partitions);
            indexRecordManager.addIndexRecord(name, user, table, columns, indexType, 0L, indexProperties, partitions);

            IndexRecord actual1 = indexRecordManager.lookUpIndexRecord(name);
            assertNotNull(actual1);
            assertEquals(actual1, expected);

            IndexRecord actual2 = indexRecordManager.lookUpIndexRecord(table, columns, indexType);
            assertNotNull(actual2);
            assertEquals(actual2, expected);
        }
    }
}
