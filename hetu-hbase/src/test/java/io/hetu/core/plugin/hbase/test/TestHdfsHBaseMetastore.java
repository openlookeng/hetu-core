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
package io.hetu.core.plugin.hbase.test;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.hetu.core.plugin.hbase.metadata.HdfsHBaseMetastore;
import io.hetu.core.plugin.hbase.metadata.LocalHBaseMetastore;
import org.codehaus.jettison.json.JSONObject;
import org.mockito.Mockito;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * TestHdfsHBaseMetastore
 *
 * @since 2020-03-20
 */
public class TestHdfsHBaseMetastore
{
    private HdfsHBaseMetastore hdfsHBaseMetastore;
    private HBaseConfig hCConf = new HBaseConfig();

    /**
     * setUp
     */
    @BeforeClass
    public void setUp()
    {
        hCConf.setZkClientPort("2181");
        hCConf.setZkQuorum("zk1");
        hCConf.setMetastoreType("hdfs");
        hCConf.setHdfsSitePath("./hdfs-site.xml.test");
        hCConf.setCoreSitePath("./core-site.xml.test");
        hCConf.setKerberos("KERBEROS");
        hCConf.setUserKeytabPath("./user.keytab.test");
        hCConf.setKrb5ConfPath("./krb5.conf.test");
        hCConf.setPrincipalUsername("root");
        hCConf.setMetastoreUrl("./hbasetablecatalogtmp.ini");
        TestJsonHBaseTableUtils.preFile(hCConf.getMetastoreUrl());
        hdfsHBaseMetastore = new HdfsHBaseMetastore(hCConf);
    }

    /**
     * testInit
     */
    @Test
    public void testInit()
    {
        hdfsHBaseMetastore.init();
    }

    /**
     * testAuthenticate
     *
     * @throws FileNotFoundException InvocationTargetException Exception
     */
    @Test
    public void testAuthenticate()
            throws Exception
    {
        Method method = HdfsHBaseMetastore.class.getDeclaredMethod("authenticate");
        method.setAccessible(true);
        try {
            TestJsonHBaseTableUtils.createFile("./hdfs-site.xml.test");
            method.invoke(hdfsHBaseMetastore);
        }
        catch (FileNotFoundException e) {
            assertEquals(e.getMessage(), null);
        }

        try {
            TestJsonHBaseTableUtils.createFile("./core-site.xml.test");
            method.invoke(hdfsHBaseMetastore);
        }
        catch (FileNotFoundException e) {
            assertEquals(e.getMessage(), null);
        }

        try {
            TestJsonHBaseTableUtils.createFile("./user.keytab.test");
            method.invoke(hdfsHBaseMetastore);
        }
        catch (FileNotFoundException e) {
            assertEquals(e.getMessage(), null);
        }

        try {
            TestJsonHBaseTableUtils.createFile("./krb5.conf.test");
            method.invoke(hdfsHBaseMetastore);
        }
        catch (InvocationTargetException e) {
            assertEquals(e.getMessage(), null);
        }

        TestJsonHBaseTableUtils.delFile("./hdfs-site.xml.test");
        TestJsonHBaseTableUtils.delFile("./core-site.xml.test");
        TestJsonHBaseTableUtils.delFile("./user.keytab.test");
        TestJsonHBaseTableUtils.delFile("./krb5.conf.test");
    }

    /**
     * testGetId
     */
    @Test
    public void testGetId()
    {
        assertEquals("HDFS", hdfsHBaseMetastore.getId());
    }

    /**
     * testOPHBaseTable
     */
    @Test
    public void testOPHBaseTable()
    {
        HdfsHBaseMetastore hDFSHBaseMetastore = Mockito.spy(new HdfsHBaseMetastore(hCConf));
        Optional<String> serializerClassName =
                Optional.of("io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer");
        HBaseTable hBaseTable =
                new HBaseTable(
                        "hbase",
                        "oldtable",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("oldtable"));
        Mockito.doNothing().when(hDFSHBaseMetastore).putJsonToHdfs(Mockito.anyString(), Mockito.any(JSONObject.class));
        hDFSHBaseMetastore.addHBaseTable(hBaseTable);

        HBaseTable hBaseNewTable =
                new HBaseTable(
                        "hbase",
                        "newtable",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("newtable"));

        hDFSHBaseMetastore.addHBaseTable(hBaseNewTable);
        // add an existing hBaseTable
        hDFSHBaseMetastore.addHBaseTable(hBaseNewTable);
        Map<String, HBaseTable> hbaseTables = hDFSHBaseMetastore.getAllHBaseTables();
        assertEquals(true, hbaseTables.containsKey("hbase.oldtable"));
        assertEquals(true, hbaseTables.containsKey("hbase.newtable"));

        HBaseTable hBaseOldTable = hDFSHBaseMetastore.getHBaseTable("hbase.oldtable");
        assertEquals("hbase.oldtable", hBaseOldTable.getFullTableName());

        hDFSHBaseMetastore.renameHBaseTable(hBaseNewTable, "hbase.oldtable");
        hbaseTables = hDFSHBaseMetastore.getAllHBaseTables();
        assertEquals(false, hbaseTables.containsKey("hbase.oldtable"));
        assertEquals(true, hbaseTables.containsKey("hbase.newtable"));

        hDFSHBaseMetastore.dropHBaseTable(hBaseNewTable);
        hbaseTables = hDFSHBaseMetastore.getAllHBaseTables();
        assertEquals(false, hbaseTables.containsKey("hbase.oldtable"));
        assertEquals(false, hbaseTables.containsKey("hbase.newtable"));
    }

    /**
     * testLocalHBaseMetastore
     */
    @Test
    public void testLocalHBaseMetastore()
    {
        HBaseConfig conf = new HBaseConfig();
        conf.setMetastoreUrl("LOCAL");
        conf.setMetastoreUrl("./localHBaseMetastore.ini");
        LocalHBaseMetastore localHBaseMetastore = new LocalHBaseMetastore(conf);

        Optional<String> serializerClassName =
                Optional.of("io.hetu.core.plugin.hbase.utils.serializers.StringRowSerializer");
        HBaseTable hBaseTable =
                new HBaseTable(
                        "hbase",
                        "oldtable",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        serializerClassName,
                        Optional.of(""),
                        Optional.of("oldtable"));
        localHBaseMetastore.addHBaseTable(hBaseTable);
        localHBaseMetastore.dropHBaseTable(hBaseTable);

        TestJsonHBaseTableUtils.delFile(conf.getMetastoreUrl());
    }

    /**
     * clear
     */
    @AfterClass
    public void clear()
    {
        TestJsonHBaseTableUtils.delFile(hCConf.getMetastoreUrl());
        TestJsonHBaseTableUtils.delFile("./.hbasetablecatalogtmp.ini.crc");
        TestJsonHBaseTableUtils.delFile("./hdfs-site.xml.test");
        TestJsonHBaseTableUtils.delFile("./core-site.xml.test");
        TestJsonHBaseTableUtils.delFile("./user.keytab.test");
        TestJsonHBaseTableUtils.delFile("./krb5.conf.test");
    }
}
