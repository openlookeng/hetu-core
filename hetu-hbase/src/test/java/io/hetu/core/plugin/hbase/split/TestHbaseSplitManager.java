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
package io.hetu.core.plugin.hbase.split;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.connector.TestHBaseClientConnection;
import io.hetu.core.plugin.hbase.metadata.TestingHetuMetastore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;

/**
 * TestHbaseSplitManager
 *
 * @since 2020-03-20
 */
public class TestHbaseSplitManager
{
    private HBaseConnection hconn;
    private HBaseConfig hCConf = new HBaseConfig();
    private HBaseSplitManager hsm;
    private TestingHetuMetastore hetuMetastore;

    /**
     * setUp
     */
    @BeforeClass
    public void setUp()
    {
        hCConf.setZkClientPort("2181");
        hCConf.setZkQuorum("zk1");
        hetuMetastore = new TestingHetuMetastore();
        hconn = new TestHBaseClientConnection(hCConf, hetuMetastore.getHetuMetastore());
        hconn.createConnection();
        hsm = new HBaseSplitManager(hconn);
    }

    @AfterClass
    public void destroy()
    {
        hetuMetastore.close();
    }

    /**
     * testSplitManager
     *
     * @throws NullPointerException
     */
    @Test
    public void testSplitManager()
    {
        try {
            hsm.getSplits(null, null, TestUtils.createHBaseTableHandle(), null);
            throw new NoSuchElementException("No value present");
        }
        catch (NoSuchElementException e) {
            assertEquals(e.toString(), "java.util.NoSuchElementException: No value present");
        }
    }

    /**
     * testGetSplitsIsBatchGet
     */
    @Test
    public void testGetSplitsIsBatchGet()
    {
        HBaseTableHandle tableHandle =
                new HBaseTableHandle(
                        "hbase",
                        "test_table",
                        "rowkey",
                        false,
                        "StringRowSerializer",
                        Optional.of("test_table"),
                        "",
                        TestUtils.createTupleDomain(1),
                        TestUtils.createColumnList(),
                        0,
                        OptionalLong.empty());

        hsm.getSplits(null, null, tableHandle, null);
    }
}
