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
package io.hetu.core.plugin.hbase;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.client.TestingConnectorSession;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.connector.HBaseConnection;
import io.hetu.core.plugin.hbase.connector.HBaseConnector;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorId;
import io.hetu.core.plugin.hbase.connector.HBaseConnectorMetadataFactory;
import io.hetu.core.plugin.hbase.connector.HBaseTableHandle;
import io.hetu.core.plugin.hbase.connector.HBaseTransactionHandle;
import io.hetu.core.plugin.hbase.connector.TestHBaseClientConnection;
import io.hetu.core.plugin.hbase.metadata.HBaseConnectorMetadata;
import io.hetu.core.plugin.hbase.metadata.TestingHetuMetastore;
import io.hetu.core.plugin.hbase.query.HBasePageSinkProvider;
import io.hetu.core.plugin.hbase.query.HBasePageSourceProvider;
import io.hetu.core.plugin.hbase.query.HBaseRecordSetProvider;
import io.hetu.core.plugin.hbase.split.HBaseSplit;
import io.hetu.core.plugin.hbase.split.HBaseSplitManager;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.hetu.core.plugin.hbase.utils.TestSliceUtils;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.IntArrayBlock;
import io.prestosql.spi.block.LongArrayBlock;
import io.prestosql.spi.block.VariableWidthBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.RecordSet;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.transaction.IsolationLevel;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

import static io.prestosql.spi.connector.ConnectorPageSink.NOT_BLOCKED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * TestHBase
 *
 * @since 2020-03-20
 */
public class TestHBase
{
    private HBaseConnection hconn;
    private HBaseConfig hCConf = new HBaseConfig();
    private HBaseConnectorMetadata hcm;
    private ConnectorSession session;
    private SchemaTableName schemaTableName;
    private ConnectorTableHandle table;
    private HBaseConnector hConnector;
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
        table = TestUtils.createHBaseTableHandle();
        schemaTableName = new SchemaTableName("hbase", "test_table");
        hconn = new TestHBaseClientConnection(hCConf, hetuMetastore.getHetuMetastore());
        hconn.createConnection();
        session = new TestingConnectorSession("root");
        hcm = new HBaseConnectorMetadata(hconn);
        hConnector =
                new HBaseConnector(
                        new HBaseConnectorMetadataFactory(hconn, hCConf),
                        new HBaseSplitManager(hconn),
                        new HBasePageSinkProvider(hconn),
                        new HBasePageSourceProvider(new HBaseRecordSetProvider(hconn), hconn),
                        Optional.empty(),
                        null);
    }

    /**
     * clear
     */
    @AfterClass
    public void clear()
    {
        hetuMetastore.close();
    }

    /**
     * testGetHAdmin
     */
    @Test
    public void testGetHAdmin()
    {
        hconn.getHbaseAdmin();
    }

    /**
     * testHBaseErrorCode
     */
    @Test
    public void testHBaseErrorCode()
    {
        HBaseErrorCode.HBASE_TABLE_DNE.toErrorCode();
        HBaseErrorCode.HBASE_TABLE_EXISTS.toErrorCode();
        HBaseErrorCode.UNEXPECTED_HBASE_ERROR.toErrorCode();
        HBaseErrorCode.ZOOKEEPER_ERROR.toErrorCode();
        HBaseErrorCode.IO_ERROR.toErrorCode();
        HBaseErrorCode.MINI_HBASE.toErrorCode();
        HBaseErrorCode.HBASE_CREATE_ERROR.toErrorCode();
    }

    /**
     * testMetaFactoryCreate
     */
    @Test
    public void testMetaFactoryCreate()
    {
        HBaseConnectorMetadataFactory hmf = new HBaseConnectorMetadataFactory(hconn, hCConf);
        hmf.create();

        // new HBaseConnectorMetadataFactory without hbaseConfig
        HBaseConnectorMetadataFactory hmfWithoutHcon = new HBaseConnectorMetadataFactory(hconn, hCConf);
        hmfWithoutHcon.create();
    }

    /**
     * testGetTableHandle
     */
    @Test
    public void testGetTableHandle()
    {
        if (hcm.getTableHandle(session, schemaTableName) instanceof HBaseTableHandle) {
            HBaseTableHandle tableHandle = (HBaseTableHandle) hcm.getTableHandle(session, schemaTableName);
            assertEquals("hbase.test_table", tableHandle.getFullTableName());
        }
    }

    /**
     * testGetTableHandleSchemaNotExist
     */
    @Test
    public void testGetTableHandleSchemaNotExist()
    {
        SchemaTableName schemaHTableName = new SchemaTableName("fihbase", "test_table");
        assertEquals(null, hcm.getTableHandle(session, schemaHTableName));
    }

    /**
     * testGetTableHandleHtableNull
     */
    @Test
    public void testGetTableHandleHtableNull()
    {
        SchemaTableName schemaHTableName = new SchemaTableName("hbase", "test");
        assertEquals(null, hcm.getTableHandle(session, schemaHTableName));
    }

    /**
     * testGetTableMeta
     */
    @Test
    public void testGetTableMeta()
    {
        ConnectorTableMetadata ctm = hcm.getTableMetadata(session, table);
        assertEquals(5, ctm.getColumns().size());
        assertEquals("test_table", ctm.getTable().getTableName());
    }

    /**
     * testGetColumnHandles
     */
    @Test
    public void testGetColumnHandles()
    {
        Map<String, ColumnHandle> columns = hcm.getColumnHandles(session, TestUtils.createHBaseTableHandle());
        assertEquals(5, columns.size());
    }

    /**
     * testGetNullTableMeta
     *
     * @throws TableNotFoundException
     */
    @Test
    public void testGetNullTableMeta()
    {
        try {
            ConnectorTableHandle table2 = TestUtils.createHBaseTableHandle("hbase", "table1");
            HBaseConnectorMetadata hBcm = new HBaseConnectorMetadata(hconn);
            hBcm.getTableMetadata(session, table2);
        }
        catch (TableNotFoundException e) {
            assertEquals(e.getMessage(), format(("Table '%s' not found"), "hbase.table1"));
        }
    }

    /**
     * testGetNullSchemaTableMeta
     *
     * @throws TableNotFoundException
     */
    @Test
    public void testGetNullSchemaTableMeta()
    {
        try {
            ConnectorTableHandle table2 = TestUtils.createHBaseTableHandle("default", "table1");
            HBaseConnectorMetadata hBcm = new HBaseConnectorMetadata(hconn);
            hBcm.getTableMetadata(session, table2);
        }
        catch (TableNotFoundException e) {
            assertEquals(e.getMessage(), format(("Table '%s' not found"), "default.table1"));
        }
    }

    /**
     * testBeginInsert
     */
    @Test
    public void testBeginInsert()
    {
        ConnectorTableHandle table2 = TestUtils.createHBaseTableHandle("hbase", "test_table");
        ConnectorInsertTableHandle table3 = TestUtils.createHBaseTableHandle("hbase", "test_table");
        assertEquals(false, hcm.beginInsert(session, table2).toString().isEmpty());
        assertEquals(false, hcm.finishInsert(session, table3, null, null).isPresent());
    }

    /**
     * testListSchemaNames
     */
    @Test
    public void testListSchemaNames()
    {
        List<String> schema = hcm.listSchemaNames(session);
        assertEquals(3, schema.size());
        assertEquals("hbase", schema.get(0));
        assertEquals("default", schema.get(1));
        assertEquals("testSchema", schema.get(2));
    }

    /**
     * testRenameTable
     */
    @Test
    public void testRenameTable()
    {
        hcm.beginCreateTable(session, TestUtils.createConnectorTableMeta("test_table4"), Optional.empty());
        hcm.finishCreateTable(session, null, null, null);
        hcm.renameTable(
                session,
                TestUtils.createHBaseTableHandle("hbase", "test_table4"),
                new SchemaTableName("hbase", "test_table5"));
        hcm.dropTable(session, TestUtils.createHBaseTableHandle("hbase", "test_table5"));
        testListTables();
    }

    /**
     * testGetNullTabelColumnHandles
     */
    @Test
    public void testGetNullTableColumnHandles()
    {
        HBaseConnectorMetadata hBcm = new HBaseConnectorMetadata(hconn);
        try {
            hBcm.getColumnHandles(session, TestUtils.createHBaseTableHandle("hbase", "test_notexist_table"));
        }
        catch (TableNotFoundException e) {
            assertEquals(e.getMessage(), format(("Table '%s' not found"), "hbase.test_notexist_table"));
        }
    }

    /**
     * testGetColumnMeta
     */
    @Test
    public void testGetColumnMeta()
    {
        ColumnMetadata cm =
                hcm.getColumnMetadata(session, null, TestUtils.createHBaseColumnHandle("name", "name", "nick_name", 1));
        assertEquals("name", cm.getName());
        assertEquals("io.prestosql.spi.type.VarcharType", cm.getType().getClass().getName());
    }

    /**
     * testListTables
     */
    private void testListTables()
    {
        List<SchemaTableName> tables = hcm.listTables(session, Optional.of("hbase"));
        assertEquals(tables.size(), 1);
    }

    /**
     * testListTableColumns
     */
    @Test
    public void testListTableColumns()
    {
        Map<SchemaTableName, List<ColumnMetadata>> tables =
                hcm.listTableColumns(session, new SchemaTablePrefix("hbase", "test_table"));
        assertEquals(5, tables.get(new SchemaTableName("hbase", "test_table")).size());
    }

    /**
     * testHBaseConnectorId
     */
    @Test
    public void testHBaseConnectorId()
    {
        HBaseConnectorId h1 = new HBaseConnectorId();
        HBaseConnectorId h2 = new HBaseConnectorId();
        HBaseConnectorId.setConnectorId("hbase");
        HBaseConnectorId.setConnectorId("hbase");
        assertEquals(true, HBaseConnectorId.getConnectorId().equals(HBaseConnectorId.getConnectorId()));
        assertEquals(h1.hashCode(), h2.hashCode());
        assertEquals(h1.toString(), h2.toString());
    }

    /**
     * testCommitTransaction
     */
    @Test
    public void testCommitTransaction()
    {
        ConnectorTransactionHandle transactionHandle = hConnector.beginTransaction(IsolationLevel.READ_COMMITTED, true);
        hConnector.getMetadata(transactionHandle);
        hConnector.commit(transactionHandle);
    }

    /**
     * testRollbackTransaction
     */
    @Test
    public void testRollbackTransaction()
    {
        ConnectorTransactionHandle transactionHandle = hConnector.beginTransaction(IsolationLevel.READ_COMMITTED, true);
        hConnector.getMetadata(transactionHandle);
        hConnector.rollback(transactionHandle);
    }

    /**
     * testGetSRecordSet
     */
    @Test
    public void testGetSRecordSet()
    {
        List<HostAddress> hostAddressList = new ArrayList<>(1);
        Map<Integer, List<Range>> ranges = new HashMap<>();
        HBaseSplit split =
                new HBaseSplit(
                        "rowkey", TestUtils.createHBaseTableHandle(), hostAddressList, null, null, ranges, 0, false, null);

        HBaseRecordSetProvider hrsp = new HBaseRecordSetProvider(hconn);
        RecordSet rs =
                hrsp.getRecordSet(
                        new HBaseTransactionHandle(),
                        session,
                        split,
                        TestUtils.createHBaseTableHandle(),
                        hconn.getTable("hbase.test_table").getColumns());
        assertEquals(5, rs.getColumnTypes().size());
    }

    /**
     * testPageSink
     */
    @Test
    public void testPageSink()
    {
        HBasePageSinkProvider hpsp = new HBasePageSinkProvider(hconn);
        HBaseTableHandle insertHandler =
                new HBaseTableHandle(
                        "hbase",
                        "test_table",
                        0,
                        hconn.getTable("hbase.test_table").getColumns(),
                        hconn.getTable("hbase.test_table").getSerializerClassName(),
                        Optional.of("test_table"),
                        OptionalLong.empty());
        if (insertHandler instanceof ConnectorInsertTableHandle) {
            ConnectorPageSink cps =
                    hpsp.createPageSink(
                            new HBaseTransactionHandle(), session, (ConnectorInsertTableHandle) insertHandler);

            long completedBytes = cps.getCompletedBytes();
            long sysMemUsage = cps.getSystemMemoryUsage();
            long cpuNanos = cps.getValidationCpuNanos();

            assertTrue(cpuNanos >= 0);
            assertTrue(sysMemUsage >= 0);
            assertTrue(completedBytes >= 0);

            int[] offsets = {0, 4};
            Block rowkey = new VariableWidthBlock(1, TestSliceUtils.createSlice("0001"), offsets, Optional.empty());

            int[] offset2 = {0, 5};
            Block name = new VariableWidthBlock(1, TestSliceUtils.createSlice("name2"), offset2, Optional.empty());

            long[] longs = new long[1];
            longs[0] = 12;
            Block age = new LongArrayBlock(1, Optional.empty(), longs);

            int[] ints = new int[1];
            ints[0] = 17832;
            Block gender = new IntArrayBlock(1, Optional.empty(), ints);

            Block columnT = new LongArrayBlock(1, Optional.empty(), longs);

            Page page = new Page(rowkey, name, age, gender, columnT);
            assertEquals(NOT_BLOCKED, cps.appendPage(page));

            cps.abort();
        }
    }
}
