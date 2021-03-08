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

import io.prestosql.spi.HetuConstant;
import io.prestosql.spi.connector.CreateIndexMetadata;
import io.prestosql.spi.filesystem.HetuFileSystemClient;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.spi.type.Type;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.testng.Assert.assertEquals;

public class TestPartitionIndexWriter
{
    private String tableName = "testTable";
    private String columnName = "testColumn";

    @Test
    public void testAddValue()
            throws IOException
    {
        List<Pair<String, Type>> columns = new ArrayList<>();
        List<String> partitions = Collections.singletonList("partition1");
        Properties properties = new Properties();
        CreateIndexMetadata createIndexMetadata = new CreateIndexMetadata("hetu_partition_idx",
                "testTable",
                "BTREE",
                columns,
                partitions,
                properties,
                "testuser",
                CreateIndexMetadata.Level.PARTITION);
        HetuFileSystemClient fileSystemClient = Mockito.mock(HetuFileSystemClient.class);
        Properties connectorMetadata = new Properties();
        connectorMetadata.setProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION, String.valueOf(System.currentTimeMillis()));
        connectorMetadata.setProperty(HetuConstant.DATASOURCE_FILE_PATH, "hdfs://testable/testcolumn/cp=123121/file1");
        connectorMetadata.setProperty(HetuConstant.DATASOURCE_STRIPE_OFFSET, "3");
        connectorMetadata.setProperty(HetuConstant.DATASOURCE_STRIPE_LENGTH, "100");
        PartitionIndexWriter indexWriter = new PartitionIndexWriter(createIndexMetadata, fileSystemClient, Paths.get("/tmp"));
        Map<String, List<Object>> valuesMap = new HashMap<>();
        List<Object> values = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            values.add("key" + i);
        }
        valuesMap.put("testcolumne", values);
        indexWriter.addData(valuesMap, connectorMetadata);
        Map<Comparable<? extends Comparable<?>>, String> result = indexWriter.getDataMap();
        assertEquals(10, result.size());
        assertEquals(1, indexWriter.getSymbolTable().size());
    }

    @Test
    public void testAddValueMultThread()
            throws InterruptedException
    {
        List<Pair<String, Type>> columns = new ArrayList<>();
        List<String> partitions = Collections.singletonList("partition1");
        Properties properties = new Properties();
        CreateIndexMetadata createIndexMetadata = new CreateIndexMetadata("hetu_partition_idx",
                "testTable",
                "BTREE",
                columns,
                partitions,
                properties,
                "testuser",
                CreateIndexMetadata.Level.PARTITION);
        HetuFileSystemClient fileSystemClient = Mockito.mock(HetuFileSystemClient.class);
        Properties connectorMetadata1 = new Properties();
        connectorMetadata1.setProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION, String.valueOf(System.currentTimeMillis()));
        connectorMetadata1.setProperty(HetuConstant.DATASOURCE_FILE_PATH, "hdfs://testable/testcolumn/cp=123121/file1");
        connectorMetadata1.setProperty(HetuConstant.DATASOURCE_STRIPE_OFFSET, "3");
        connectorMetadata1.setProperty(HetuConstant.DATASOURCE_STRIPE_LENGTH, "100");
        Properties connectorMetadata2 = new Properties();
        connectorMetadata2.setProperty(HetuConstant.DATASOURCE_FILE_MODIFICATION, String.valueOf(System.currentTimeMillis()));
        connectorMetadata2.setProperty(HetuConstant.DATASOURCE_FILE_PATH, "hdfs://testable/testcolumn/cp=123121/file2");
        connectorMetadata2.setProperty(HetuConstant.DATASOURCE_STRIPE_OFFSET, "3");
        connectorMetadata2.setProperty(HetuConstant.DATASOURCE_STRIPE_LENGTH, "100");
        PartitionIndexWriter indexWriter = new PartitionIndexWriter(createIndexMetadata, fileSystemClient, Paths.get("/tmp"));
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        executorService.submit(new TestDriver(indexWriter, connectorMetadata1, latch));
        executorService.submit(new TestDriver(indexWriter, connectorMetadata2, latch));
        latch.await(5, TimeUnit.SECONDS);
        Map<Comparable<? extends Comparable<?>>, String> result = indexWriter.getDataMap();
        assertEquals(10, result.size());
        assertEquals(2, indexWriter.getSymbolTable().size());
    }

    private static class TestDriver
            implements Runnable
    {
        PartitionIndexWriter indexWriter;
        Properties connectorMetadata;
        CountDownLatch latch;

        public TestDriver(PartitionIndexWriter partitionIndexWriter, Properties connectorMetadata, CountDownLatch latch)
        {
            this.indexWriter = partitionIndexWriter;
            this.connectorMetadata = connectorMetadata;
            this.latch = latch;
        }

        @Override
        public void run()
        {
            Map<String, List<Object>> valuesMap = new HashMap<>();
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                values.add("key" + i);
            }
            valuesMap.put("testcolumne", values);
            try {
                indexWriter.addData(valuesMap, connectorMetadata);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            latch.countDown();
        }
    }
}
