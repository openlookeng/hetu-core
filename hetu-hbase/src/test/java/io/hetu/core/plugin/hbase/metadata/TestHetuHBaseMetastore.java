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
package io.hetu.core.plugin.hbase.metadata;

import io.hetu.core.plugin.hbase.client.TestUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHetuHBaseMetastore
{
    private TestingHetuMetastore hetuMetastore;
    private HetuHBaseMetastore metaStore;

    @BeforeClass
    public void setUp()
    {
        hetuMetastore = new TestingHetuMetastore();
        metaStore = hetuMetastore.getHetuMetastore();
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        hetuMetastore.close();
    }

    @Test
    public void testGetId()
    {
        assertEquals(metaStore.getId(), "Hetu");
    }

    @Test
    public void testOPHBaseTable()
    {
        Optional<String> serializerClassName =
                Optional.of("io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer");

        HBaseTable testHBaseTable1 =
                new HBaseTable(
                        "hbase",
                        "testTable1",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("testTable"),
                        Optional.of(""));

        metaStore.addHBaseTable(testHBaseTable1);

        HBaseTable testHBaseTable2 =
                new HBaseTable(
                        "hbase",
                        "testTable2",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("testTable2"),
                        Optional.of(""));

        metaStore.addHBaseTable(testHBaseTable2);
        // test get all tables
        Map<String, HBaseTable> hBaseTables = metaStore.getAllHBaseTables();
        assertTrue(hBaseTables.containsKey("hbase.testTable1"));
        assertTrue(hBaseTables.containsKey("hbase.testTable2"));
        // test get one of tables
        HBaseTable table = metaStore.getHBaseTable("hbase.testTable1");
        assertNotNull(table);
        assertEquals("hbase.testTable1", table.getFullTableName());
        // test rename table
        HBaseTable testHBaseTable3 =
                new HBaseTable(
                        "hbase",
                        "testTable3",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("testTable3"),
                        Optional.of(""));
        metaStore.renameHBaseTable(testHBaseTable3, "hbase.testTable1");
        hBaseTables = metaStore.getAllHBaseTables();
        assertFalse(hBaseTables.containsKey("hbase.testTable1"));
        assertTrue(hBaseTables.containsKey("hbase.testTable3"));
        // test drop table
        metaStore.dropHBaseTable(testHBaseTable3);
        hBaseTables = metaStore.getAllHBaseTables();
        assertFalse(hBaseTables.containsKey("hbase.testTable3"));
    }
}
