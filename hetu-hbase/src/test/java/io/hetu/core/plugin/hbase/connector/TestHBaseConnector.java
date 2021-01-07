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
package io.hetu.core.plugin.hbase.connector;

import io.hetu.core.plugin.hbase.client.TestUtils;
import io.hetu.core.plugin.hbase.client.TestingConnectorSession;
import io.hetu.core.plugin.hbase.conf.HBaseConfig;
import io.hetu.core.plugin.hbase.conf.HBaseTableProperties;
import io.hetu.core.plugin.hbase.metadata.HBaseConnectorMetadata;
import io.hetu.core.plugin.hbase.metadata.HBaseTable;
import io.hetu.core.plugin.hbase.metadata.TestingHetuMetastore;
import io.hetu.core.plugin.hbase.query.HBasePageSinkProvider;
import io.hetu.core.plugin.hbase.query.HBasePageSourceProvider;
import io.hetu.core.plugin.hbase.query.HBaseRecordSetProvider;
import io.hetu.core.plugin.hbase.split.HBaseSplitManager;
import io.hetu.core.plugin.hbase.utils.HBaseErrorCode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.Type;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimeType.TIME;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * TestHBaseConnector
 *
 * @since 2020-03-20
 */
public class TestHBaseConnector
{
    private HBaseConnection hconn;
    private HBaseConfig hCConf = new HBaseConfig();
    private HBaseConnectorMetadata hcm;
    private SchemaTableName schemaTableName;
    private HBaseConnector hConnector;
    private ConnectorSession session;
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
        schemaTableName = new SchemaTableName("hbase", "test_table");
        hconn = new TestHBaseClientConnection(hCConf, hetuMetastore.getHetuMetastore());
        hconn.getConn();
        session = new TestingConnectorSession("root");
        hcm = new HBaseConnectorMetadata(hconn);
        hConnector =
                new HBaseConnector(
                        new HBaseConnectorMetadataFactory(hconn, hCConf),
                        new HBaseSplitManager(hconn),
                        new HBasePageSinkProvider(hconn),
                        new HBasePageSourceProvider(new HBaseRecordSetProvider(hconn)),
                        Optional.empty(),
                        new HBaseTableProperties());
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
     * testDropSchema
     */
    @Test
    public void testDropSchema()
    {
        try {
            hcm.dropSchema(session, "hbase");
            throw new PrestoException(INVALID_TABLE_PROPERTY, "testDropSchema : failed");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.getMessage(),
                    format(
                            "drop schema[hbase] failed cause by there are some tables under the schema, can't drop the"
                                    + " schema before delete all table under the schema. "));
        }
    }

    /**
     * testAddColumnHtableNull
     */
    @Test
    public void testAddColumnHTableNull()
    {
        try {
            HBaseTableHandle tableHandle = TestUtils.createHBaseTableHandle("hbase", "test");
            ColumnMetadata column = new ColumnMetadata("name", VARCHAR);
            hcm.addColumn(session, tableHandle, column);
            throw new PrestoException(HBaseErrorCode.HBASE_TABLE_DNE, "testAddColumnHtableNull : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), format("addColumn fail, cause table[test] not exists."));
        }
    }

    /**
     * testAddColumnException
     */
    @Test
    public void testAddColumnException()
            throws Exception
    {
        HBaseTableHandle tableHandle = TestUtils.createHBaseTableHandle("hbase", "test_table");
        // ColumnExist
        try {
            ColumnMetadata column = new ColumnMetadata("name", VARCHAR);
            hcm.addColumn(session, tableHandle, column);
            throw new PrestoException(HBaseErrorCode.HBASE_TABLE_DNE, "testAddColumn : failed");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.getMessage(),
                    format("addColumn fail, cause the column[name] already exists in table[test_table] ."));
        }

        // TableFamilySizeIsZero
        try {
            Map<String, Object> properties = TestUtils.createProperties();
            properties.put("hbase_table_name", "hbase:table");
            properties.put("family", "f");
            properties.put("qualifier", "qualifier");
            ColumnMetadata column = new ColumnMetadata("newColumn", VARCHAR, true, "test", "false", false, properties);
            hcm.addColumn(session, tableHandle, column);
            throw new PrestoException(HBaseErrorCode.HBASE_TABLE_DNE, "testAddColumn : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), format("Table hbase:test_table does not exist any more"));
        }
    }

    /**
     * testGetAllTablesFromMetadata
     */
    @Test
    public void testGetAllTablesFromMetadata()
    {
        Map<String, List<SchemaTableName>> map = hconn.getAllTablesFromMetadata();
        assertEquals(1, map.size());
        assertEquals("hbase", map.get("hbase").get(0).getSchemaName());
        assertEquals("test_table", map.get("hbase").get(0).getTableName());
    }

    /**
     * testApplyFilter
     */
    @Test
    public void testApplyFilter()
    {
        Constraint constraint = new Constraint(TestUtils.createTupleDomain(5));
        Optional<ConstraintApplicationResult<ConnectorTableHandle>> result =
                hcm.applyFilter(session, TestUtils.createHBaseTableHandle(), constraint);
        assertEquals(true, result.isPresent());
    }

    /**
     * testHBaseColumnHandle
     */
    @Test
    public void testHBaseColumnHandle()
    {
        // test GetUpdateRowIdColumnHandle
        if (hcm.getUpdateRowIdColumnHandle(session, TestUtils.createHBaseTableHandle()) instanceof HBaseColumnHandle) {
            HBaseColumnHandle hBaseColumnHandle =
                    (HBaseColumnHandle) hcm.getUpdateRowIdColumnHandle(session, TestUtils.createHBaseTableHandle());
            assertEquals("rowkey", hBaseColumnHandle.getName());

            // test HBaseColumnHandle Equals
            assertEquals(true, hBaseColumnHandle.equals(hBaseColumnHandle));
            assertEquals(false, hBaseColumnHandle.equals(null));
            HBaseColumnHandle hbCH =
                    new HBaseColumnHandle(
                            "rowkey",
                            Optional.ofNullable("name"),
                            Optional.ofNullable("nick_name"),
                            VARCHAR,
                            0,
                            "HBase column name:nick_name. Indexed: false",
                            true);
            HBaseColumnHandle hBCH =
                    new HBaseColumnHandle(
                            "rowkey",
                            Optional.ofNullable("name"),
                            Optional.ofNullable("nick_name"),
                            VARCHAR,
                            0,
                            "HBase column name",
                            true);
            assertEquals(false, hBCH.equals(hbCH));

            // test HBaseColumnHandle compareTo
            assertEquals(0, hBaseColumnHandle.compareTo(hBaseColumnHandle));
        }
    }

    /**
     * testBeginDelete
     */
    @Test
    public void testBeginDelete()
    {
        if (hcm.beginDelete(session, TestUtils.createHBaseTableHandle()) instanceof HBaseTableHandle) {
            HBaseTableHandle tableHandle =
                    (HBaseTableHandle) hcm.beginDelete(session, TestUtils.createHBaseTableHandle());
            assertEquals("hbase", tableHandle.getSchema());
        }
    }

    /**
     * testApplyDelete
     */
    @Test
    public void testApplyDelete()
    {
        hcm.applyDelete(session, TestUtils.createHBaseTableHandle());
    }

    /**
     * testExecuteDelete
     */
    @Test
    public void testExecuteDelete()
    {
        hcm.executeDelete(session, TestUtils.createHBaseTableHandle());
    }

    /**
     * testFinishDelete
     */
    @Test
    public void testFinishDelete()
    {
        hcm.finishDelete(session, TestUtils.createHBaseTableHandle(), null);
    }

    /**
     * testSupportsMetadataDelete
     */
    @Test
    public void testSupportsMetadataDelete()
    {
        assertEquals(false, hcm.supportsMetadataDelete(session, TestUtils.createHBaseTableHandle(), null));
    }

    /**
     * testGetTableProperties
     */
    @Test
    public void testGetTableProperties()
    {
        ConnectorTableProperties properties = hcm.getTableProperties(session, TestUtils.createHBaseTableHandle());
        assertEquals(0, properties.getLocalProperties().size());
    }

    /**
     * testUsesLegacyTableLayouts
     */
    @Test
    public void testUsesLegacyTableLayouts()
    {
        assertEquals(false, hcm.usesLegacyTableLayouts());
    }

    /**
     * testGetTableHandleCatalogNull
     */
    @Test
    public void testGetTableHandleCatalogNull()
    {
        if (hcm.getTableHandle(session, schemaTableName) instanceof HBaseTableHandle) {
            HBaseTableHandle tableHandle = (HBaseTableHandle) hcm.getTableHandle(session, schemaTableName);
            assertEquals("hbase.test_table", tableHandle.getFullTableName());
        }
    }

    /**
     * testCreateSchema
     */
    @Test
    public void testCreateSchema()
    {
        HBaseConfig hCnnConf = new HBaseConfig();
        hCnnConf.setZkClientPort("2181");
        hCnnConf.setZkQuorum("zk1");
        hCnnConf.setRetryNumber(1);
        HBaseConnection hConn = new TestHBaseClientConnection(hCnnConf, null);

        try {
            hConn.createSchema("schema", null);
            throw new NullPointerException("testAuthenticate : failed");
        }
        catch (NullPointerException e) {
            assertEquals(e.toString(), "java.lang.NullPointerException");
        }
    }

    /**
     * testRenameTable
     */
    @Test
    public void testRenameTable()
    {
        // test table in different schema
        try {
            hconn.renameTable(schemaTableName, new SchemaTableName("fihbase", "test_table"));
            throw new PrestoException(NOT_SUPPORTED, "testAuthenticate : failed");
        }
        catch (PrestoException e) {
            assertEquals(
                    e.getMessage(), format("HBase does not support renaming tables to different namespaces (schemas)"));
        }

        // old table is not exist
        try {
            hconn.renameTable(
                    new SchemaTableName("hbase", "test_oldtable"), new SchemaTableName("hbase", "test_newtable"));
            throw new PrestoException(HBaseErrorCode.UNEXPECTED_HBASE_ERROR, "testAuthenticate : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), format(("Table '%s' not found"), "hbase.test_oldtable"));
        }

        // new table name is null
        try {
            hcm.createTable(session, TestUtils.createConnectorTableMeta("newTable"), false);
            hconn.renameTable(new SchemaTableName("hbase", "test_table"), new SchemaTableName("hbase", "newtable"));
            throw new PrestoException(HBaseErrorCode.HBASE_TABLE_EXISTS, "testAuthenticate : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), format(("Table %s already exists"), "hbase.newtable"));
        }

        hcm.dropTable(session, TestUtils.createHBaseTableHandle("hbase", "newtable"));
    }

    /**
     * testhBaseConnectorIdGetConnectorId
     */
    @Test
    public void testhBaseConnectorIdGetConnectorId()
    {
        HBaseConnectorId hBCnnId = new HBaseConnectorId();
        hBCnnId.setConnectorId("hbase");
        assertEquals("hbase", hBCnnId.getConnectorId());
    }

    /**
     * testHBaseConnectorIdEquals
     */
    @Test
    public void testHBaseConnectorIdEquals()
    {
        HBaseConnectorId hBCnnId = new HBaseConnectorId();
        hBCnnId.setConnectorId("hbase");
        assertEquals(true, hBCnnId.equals(hBCnnId));
        assertEquals(false, hBCnnId.equals(null));
    }

    /**
     * testHBaseConnectionDropTable
     */
    @Test
    public void testHBaseConnectionDropTable()
    {
        HBaseTable hBaseTable =
                new HBaseTable(
                        "hbase",
                        "table",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        Optional.of("StringRowSerializer"),
                        Optional.of(""),
                        Optional.of("table"),
                        Optional.of(""));
        // table is null
        try {
            hconn.dropTable(hBaseTable);
            throw new PrestoException(INVALID_TABLE_PROPERTY, "testDropSchema : failed");
        }
        catch (PrestoException e) {
            assertEquals(e.getMessage(), format(("table[%s] not exists."), "table"));
        }
    }

    /**
     * testHBaseConnectionAddNewFamily
     *
     * @throws InvocationTargetException
     */
    @Test
    public void testHBaseConnectionAddNewFamily()
            throws Exception
    {
        Method method = HBaseConnection.class.getDeclaredMethod("addNewFamily", String.class, String.class);
        method.setAccessible(true);
        try {
            method.invoke(hconn, "hBase.test_table", "newFamily");
        }
        catch (InvocationTargetException e) {
            assertEquals(e.getMessage(), null);
        }
    }

    /**
     * testHBaseConnectionHasSamePair
     *
     * @throws Exception
     */
    @Test
    public void testHBaseConnectionHasSamePair()
            throws Exception
    {
        Method method = HBaseConnection.class.getDeclaredMethod("hasSamePair", List.class, HBaseColumnHandle.class);
        method.setAccessible(true);
        method.invoke(hconn, TestUtils.createColumnList(), TestUtils.createHBaseColumnHandle("d", "f_d", "q_d", 4));
        method.invoke(hconn, TestUtils.createColumnList(), TestUtils.createHBaseColumnHandle("d", "f_e", "q_d", 4));
    }

    /**
     * testHBaseConnectionCheckFamilyExist
     *
     * @throws InvocationTargetException
     */
    @Test
    public void testHBaseConnectionCheckFamilyExist()
            throws Exception
    {
        Method method = HBaseConnection.class.getDeclaredMethod("checkFamilyExist", HBaseTable.class, String.class);
        method.setAccessible(true);
        HBaseTable hBaseTable =
                new HBaseTable(
                        "hbase",
                        "test_table",
                        TestUtils.createColumnList(),
                        "rowkey",
                        false,
                        Optional.of("StringRowSerializer"),
                        Optional.of(""),
                        Optional.of("test_table"),
                        Optional.of(""));
        try {
            method.invoke(hconn, hBaseTable, "rowkey");
        }
        catch (InvocationTargetException e) {
            assertEquals(e.getMessage(), null);
        }
    }

    /**
     * testHBaseConnectionGetColumnHandles
     *
     * @throws InvocationTargetException
     */
    @Test
    public void testHBaseConnectionGetColumnHandles()
            throws Exception
    {
        Method method =
                HBaseConnection.class.getDeclaredMethod("getColumnHandles", ConnectorTableMetadata.class, String.class);
        method.setAccessible(true);
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(new ColumnMetadata("rowkey", VARCHAR));
        columns.add(new ColumnMetadata("name", VARCHAR));
        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, columns);
        try {
            method.invoke(hconn, connectorTableMetadata, "rowkey");
        }
        catch (InvocationTargetException e) {
            assertEquals(e.getMessage(), null);
        }
    }

    /**
     * testHBaseConnectionGetColumnLocalityGroup
     *
     * @throws Exception
     */
    @Test
    public void testHBaseConnectionGetColumnLocalityGroup()
            throws Exception
    {
        Method method = HBaseConnection.class.getDeclaredMethod("getColumnLocalityGroup", String.class, Optional.class);
        method.setAccessible(true);
        Set<String> hashSset = new HashSet<>();
        hashSset.add("rowkey");
        Map<String, Set<String>> groups = new HashMap<>();
        groups.put("001", hashSset);
        method.invoke(hconn, "rowKey", Optional.of(groups));
    }

    /**
     * testHBaseConnectionCheckTypeValidate
     *
     * @throws InvocationTargetException
     */
    @Test
    public void testHBaseConnectionCheckTypeValidate()
            throws Exception
    {
        Method method = HBaseConnection.class.getDeclaredMethod("checkTypeValidate", Type.class);
        method.setAccessible(true);
        try {
            assertEquals(true, method.invoke(hconn, BOOLEAN));
            assertEquals(true, method.invoke(hconn, DOUBLE));
            assertEquals(true, method.invoke(hconn, BIGINT));
            assertEquals(true, method.invoke(hconn, INTEGER));
            assertEquals(true, method.invoke(hconn, SMALLINT));
            assertEquals(true, method.invoke(hconn, TINYINT));
            assertEquals(true, method.invoke(hconn, DATE));
            assertEquals(true, method.invoke(hconn, TIME));
            assertEquals(true, method.invoke(hconn, TIMESTAMP));
            method.invoke(hconn, REAL);
        }
        catch (InvocationTargetException e) {
            assertEquals(e.getMessage(), null);
        }
    }

    /**
     * testHBaseConnector
     */
    @Test
    public void testHBaseConnector()
    {
        hConnector.getSplitManager();
        hConnector.getPageSourceProvider();
        hConnector.getPageSinkProvider();
        hConnector.getPageSinkProvider();
        assertEquals(hConnector.getTableProperties().size(), 8);
        assertEquals(hConnector.getColumnProperties().size(), 2);
    }
}
