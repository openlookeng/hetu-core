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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.testing.mysql.TestingMySqlServer;
import io.hetu.core.metastore.jdbc.JdbcMetastoreModule;
import io.hetu.core.plugin.hbase.client.TestUtils;
import io.prestosql.plugin.base.jmx.MBeanServerModule;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.metastore.HetuMetastore;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.weakref.jmx.guice.MBeanModule;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.spi.metastore.HetuErrorCode.HETU_METASTORE_CODE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestHetuHBaseMetastore
{
    private static final String TEST_DATABASES = "hbase";
    private static final String TEST_MYSQL_USER = "user";
    private static final String TEST_MYSQL_PASSWORD = "testpass";

    private TestingMySqlServer mySqlServer;
    private HetuHBaseMetastore metaStore;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        this.mySqlServer = new TestingMySqlServer(TEST_MYSQL_USER, TEST_MYSQL_PASSWORD, TEST_DATABASES);
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hetu.metastore.db.url", mySqlServer.getJdbcUrl(TEST_DATABASES))
                .put("hetu.metastore.db.user", TEST_MYSQL_USER)
                .put("hetu.metastore.db.password", TEST_MYSQL_PASSWORD)
                .build();
        try {
            Bootstrap app = new Bootstrap(
                    new MBeanModule(),
                    new MBeanServerModule(),
                    new JdbcMetastoreModule());

            Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(properties)
                    .initialize();

            this.metaStore = new HetuHBaseMetastore(injector.getInstance(HetuMetastore.class));
            metaStore.init();
        }
        catch (Exception ex) {
            throwIfUnchecked(ex);
            throw new PrestoException(HETU_METASTORE_CODE,
                    "init hetu hbase metastore module failed.");
        }
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        mySqlServer.close();
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
                        Optional.of("testTable"));

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
                        Optional.of("testTable2"));

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
                        Optional.of("testTable3"));
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
