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
package io.hetu.core.plugin.hana;

import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

/**
 * TestHanaClient
 *
 * @since 2019-07-12
 */
public class TestHanaClient
{
    private static final ConnectorSession SESSION = testSessionBuilder().build().toConnectorSession();

    private static final Logger LOGGER = Logger.get(TestHanaClient.class);

    private TestingDatabase database;

    private String catalogName;

    private HanaClient hanaClient;

    private Connection connection;

    private TestingHanaServer hanaServer;

    private TestHanaClient()
    {
    }

    /**
     * setUp
     *
     * @throws SQLException SQLException
     */
    @BeforeClass
    public void setUp() throws SQLException
    {
        this.hanaServer = TestingHanaServer.getInstance();
        if (!hanaServer.isHanaServerAvailable()) {
            LOGGER.info("please set correct hana data base info!");
            throw new SkipException("skip the test");
        }
        LOGGER.info("running TestHanaClient...");
        database = new TestingDatabase(hanaServer);
        connection = database.getConnection();
        catalogName = connection.getCatalog();
        hanaClient = database.getHanaClient();
    }

    /**
     * tearDown
     *
     * @throws SQLException SQLException
     */
    @AfterClass(alwaysRun = true)
    public void tearDown() throws SQLException
    {
        if (this.hanaServer.isHanaServerAvailable()) {
            database.close();
        }
    }

    /**
     * testListSchemas
     */
    @Test
    public void testListSchemas()
    {
        Collection<String> expectedSchemas = Arrays.asList(getNameByUpperCaseIdentifiers(database.getSchema()));
        Collection<String> actualSchemas = hanaClient.listSchemas(connection);
        assertEquals(actualSchemas, expectedSchemas);
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

    /**
     * testRenameTable
     */
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
        hanaClient.renameTable(identity, catalogName, schemaName, tableNumber, newTableName);

        expectedTables.remove(3); //remove table "number_*"
        expectedTables.add(actualTable.replace("number", "new_number")); //add table "new_number_*"
        allTables = getAllTables(schemaName);
        actualTables = getActualTables(allTables, expectedTables);
        assertEquals(actualTables, expectedTables);

        /* rename table new_number_* to number_* */
        newTableName = new SchemaTableName(schemaName, tableNumber);
        hanaClient.renameTable(identity, catalogName, schemaName, tableNewNumber, newTableName);

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
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        // get actual table table_with_float_col_*
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("table_with_float_col"));

        List<String> expectedColumns = Arrays.asList("col1", "col2", "col3", "col4");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = hanaClient.getTableHandle(identity, schemaTableName).get();

        List<JdbcColumnHandle> columns = hanaClient.getColumns(SESSION, tableHandle);
        JdbcColumnHandle columnHandle = null;
        for (JdbcColumnHandle column : columns) {
            if ("col4".equalsIgnoreCase(column.getColumnName())) {
                columnHandle = column;
                break;
            }
        }

        hanaClient.renameColumn(identity, tableHandle, columnHandle, "newcol4");

        expectedColumns = Arrays.asList("col1", "col2", "col3", "newcol4");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
    }

    /**
     * testAddColumn
     */
    @Test
    public void testAddColumn()
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        // get actual table student_*
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("student"));

        List<String> expectedColumns = Arrays.asList("id");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = hanaClient.getTableHandle(identity, schemaTableName).get();
        ColumnMetadata columnMetadata = new ColumnMetadata("name", VARCHAR);
        hanaClient.addColumn(SESSION, tableHandle, columnMetadata);

        expectedColumns = Arrays.asList("id", "name");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
    }

    /**
     * testDropColumn
     */
    @Test
    public void testDropColumn()
    {
        String schemaName = getNameByUpperCaseIdentifiers(database.getSchema());
        String tableName = getNameByUpperCaseIdentifiers(database.getActualTable("example")); // get actual table example_*

        List<String> expectedColumns = Arrays.asList("text", "text_short", "value");
        List<String> actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);

        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        JdbcTableHandle tableHandle = hanaClient.getTableHandle(identity, schemaTableName).get();
        List<JdbcColumnHandle> columns = hanaClient.getColumns(SESSION, tableHandle);

        JdbcColumnHandle columnHandle = null;
        for (JdbcColumnHandle column : columns) {
            if ("value".equalsIgnoreCase(column.getColumnName())) {
                columnHandle = column;
                break;
            }
        }
        hanaClient.dropColumn(identity, tableHandle, columnHandle);

        expectedColumns = Arrays.asList("text", "text_short");
        actualColumns = getActualColumns(schemaName, tableName);
        assertEquals(actualColumns, expectedColumns);
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

        try (ResultSet resultSet = hanaClient.getTables(connection, Optional.of(schemaName), Optional.empty())) {
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
