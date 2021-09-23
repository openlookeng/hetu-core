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
package io.hetu.core.plugin.clickhouse;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.Type;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

/**
 * ClickHouseTest
 */
public class ClickHouseClientTest
{
    private static final ConnectorSession SESSION = testSessionBuilder().build().toConnectorSession();

    private static final Logger LOGGER = Logger.get(ClickHouseClientTest.class);

    private DataBaseTest database;

    private String catalogName;

    private ClickHouseClient clickHouseClient;

    private Connection connection;

    private ClickHouseServerTest clickHouseServer;

    private ClickHouseClientTest()
    {
    }

    /**
     * setUp
     *
     * @throws SQLException SQLException
     */
    @BeforeClass
    public void setUp()
            throws SQLException
    {
        this.clickHouseServer = ClickHouseServerTest.getInstance();
        if (!clickHouseServer.isClickHouseServerAvailable()) {
            LOGGER.info("please set correct clickhouse data base info!");
            throw new SkipException("skip the test");
        }
        LOGGER.info("running TestClickHouseClient...");
        database = new DataBaseTest(clickHouseServer);
        connection = database.getConnection();
        catalogName = connection.getCatalog();
        clickHouseClient = database.getClickHouseClient();
    }

    /**
     * tearDown
     *
     * @throws SQLException SQLException
     */
    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws SQLException
    {
        if (this.clickHouseServer.isClickHouseServerAvailable()) {
            database.close();
        }
    }

    /**
     * testCreateTable
     */
    @Test(enabled = false)
    public void testCreateTable()
            throws SQLException
    {
        String tableName = "testCreateTable";
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);

        Type intType = IntegerType.INTEGER;
        ColumnMetadata columnMetadata = new ColumnMetadata("col1", intType);
        List<ColumnMetadata> columns = new ArrayList<>();
        columns.add(columnMetadata);

        ConnectorTableMetadata connectorTableMetadata = new ConnectorTableMetadata(schemaTableName, columns);
        clickHouseClient.createTable(SESSION, connectorTableMetadata, tableName);

        JdbcTableHandle tableHandle = clickHouseClient.getTableHandle(identity, schemaTableName).get();
        clickHouseClient.dropTable(identity, tableHandle);
    }

    /**
     * testListSchemas
     */
    @Test
    public void testListSchemas()
    {
        assertEquals(ImmutableSet.copyOf(clickHouseClient.listSchemas(connection)).contains("test"), true);
    }

    /**
     * testGetTables
     */
    @Test
    public void testGetTables()
    {
        List<String> expectedTables = database.getTables();
        List<String> allTables = getAllTables(getNameByUpperCaseIdentifiers(database.getSchema()));
        List<String> actualTables = getActualTables(allTables, expectedTables);
        assertEquals(actualTables, expectedTables);
    }

    @Test
    public void testRenameTable()
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());

        List<String> expectedTables = database.getTables();
        List<String> allTables = getAllTables(schemaName);
        List<String> actualTables = getActualTables(allTables, expectedTables);
        assertEquals(actualTables, expectedTables);

        String actualTable = database.getActualTable("number"); // get actual table number_*
        String tableNumber = getNameByUpperCaseIdentifiers(actualTable);
        String tableNewNumber = getNameByUpperCaseIdentifiers(actualTable.replace("number", "new_number"));

        /* rename table number_* to new_number_* */
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName newTableName = new SchemaTableName(schemaName, tableNewNumber);
        clickHouseClient.renameTable(identity, null, schemaName, tableNumber, newTableName);

        expectedTables.remove(3); //remove table "number_*"
        expectedTables.add(actualTable.replace("number", "new_number")); //add table "new_number_*"
        allTables = getAllTables(schemaName);
        actualTables = getActualTables(allTables, expectedTables);
        assertEquals(actualTables, expectedTables);

        /* rename table new_number_* to number_* */
        newTableName = new SchemaTableName(schemaName, tableNumber);
        clickHouseClient.renameTable(identity, null, schemaName, tableNewNumber, newTableName);

        expectedTables.remove(3); //remove table "new_number"
        expectedTables.add(actualTable); //add table "number"
        allTables = getAllTables(schemaName);
        actualTables = getActualTables(allTables, expectedTables);
        assertEquals(actualTables, expectedTables);
    }

    /**
     * testRenameColumn
     */
    @Test
    public void testRenameColumn()
            throws NoSuchFieldException, IllegalAccessException
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        // get actual table table_with_float_col_*
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("table_with_float_col"));

        List<String> expectedColumns = Arrays.asList("col1", "col2", "col3", "col4");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = clickHouseClient.getTableHandle(identity, schemaTableName).get();

        List<JdbcColumnHandle> columns = clickHouseClient.getColumns(SESSION, tableHandle);
        JdbcColumnHandle columnHandle = null;
        for (JdbcColumnHandle column : columns) {
            if ("col4".equalsIgnoreCase(column.getColumnName())) {
                columnHandle = column;
                break;
            }
        }

        Field field = JdbcTableHandle.class.getDeclaredField("catalogName");
        field.setAccessible(true);
        field.set(tableHandle, null);

        clickHouseClient.renameColumn(identity, tableHandle, columnHandle, "newcol4");

        expectedColumns = Arrays.asList("col1", "col2", "col3", "newcol4");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
    }

    /**
     * testAddColumn
     */
    @Test
    public void testAddColumn()
            throws NoSuchFieldException, IllegalAccessException
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        // get actual table student_*
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("student"));

        List<String> expectedColumns = Arrays.asList("id");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = clickHouseClient.getTableHandle(identity, schemaTableName).get();
        ColumnMetadata columnMetadata = new ColumnMetadata("name", VARCHAR);

        Field field = JdbcTableHandle.class.getDeclaredField("catalogName");
        field.setAccessible(true);
        field.set(tableHandle, null);
        clickHouseClient.addColumn(SESSION, tableHandle, columnMetadata);

        expectedColumns = Arrays.asList("id", "name");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
    }

    /**
     * testDropColumn
     */
    @Test
    public void testDropColumn()
            throws NoSuchFieldException, IllegalAccessException
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("example")); // get actual table example_*

        List<String> expectedColumns = Arrays.asList("text", "text_short", "value");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = clickHouseClient.getTableHandle(identity, schemaTableName).get();
        List<JdbcColumnHandle> columns = clickHouseClient.getColumns(SESSION, tableHandle);

        JdbcColumnHandle columnHandle = null;
        for (JdbcColumnHandle column : columns) {
            if ("value".equalsIgnoreCase(column.getColumnName())) {
                columnHandle = column;
                break;
            }
        }

        Field field = JdbcTableHandle.class.getDeclaredField("catalogName");
        field.setAccessible(true);
        field.set(tableHandle, null);
        clickHouseClient.dropColumn(identity, tableHandle, columnHandle);

        expectedColumns = Arrays.asList("text", "text_short");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
    }

    @Test(enabled = false)
    public void testGetColumns()
    {
    }

    private String getNameByUpperCaseIdentifiers(String name)
    {
        try {
            if (connection.getMetaData().storesUpperCaseIdentifiers()) {
                return name.toUpperCase(Locale.ENGLISH);
            }
            else {
                return name;
            }
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get metadata or storesUpperCaseIdentifiers", e);
        }
    }

    private List<String> getAllTables(String schemaName)
    {
        List<String> allTables = new ArrayList<>();

        try (ResultSet resultSet = clickHouseClient.getTables(connection, Optional.of(schemaName), Optional.empty())) {
            while (resultSet.next()) {
                String tableName = resultSet.getString("TABLE_NAME");
                allTables.add(tableName.toLowerCase(Locale.ENGLISH));
            }

            return allTables;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get tables", e);
        }
    }

    private List<String> getActualTables(List<String> allTables, List<String> expectedTables)
    {
        List<String> actualTables = new ArrayList<>();
        for (String table : expectedTables) {
            if (allTables.contains(table)) {
                actualTables.add(table);
            }
        }
        return actualTables;
    }

    private List<String> getActualColumns(String schemaName, String tableName)
    {
        try (ResultSet resultSet = connection.getMetaData().getColumns(null, schemaName, tableName, null)) {
            List<String> actualColumns = new ArrayList<>();
            while (resultSet.next()) {
                String columnName = resultSet.getString("COLUMN_NAME");
                actualColumns.add(columnName.toLowerCase(Locale.ENGLISH));
            }

            return actualColumns;
        }
        catch (SQLException e) {
            throw new RuntimeException("Failed to get metadata or columns", e);
        }
    }
}
