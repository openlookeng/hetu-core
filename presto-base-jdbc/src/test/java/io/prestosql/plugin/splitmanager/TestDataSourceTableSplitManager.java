
/*
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
package io.prestosql.plugin.splitmanager;

import io.prestosql.plugin.jdbc.BaseJdbcClient;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcSplit;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.testing.TestingNodeManager;
import org.h2.Driver;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestDataSourceTableSplitManager
{
    private Connection connection;
    private JdbcClient jdbcClient;
    private DataSourceTableSplitManager splitManager;
    private String catalogName;
    private JdbcTableHandle tableHandle;
    private TestingNodeManager nodeManager;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        initSplitDatabase();
        nodeManager = new TestingNodeManager();
        catalogName = connection.getCatalog();
        initTable();
    }

    // calcStepEnable is false
    @Test
    public void testGetTableSplits01()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(false)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"scanNodes\":\"2\"," +
                        "\"fieldMinValue\":\"2\",\"fieldMaxValue\":\"12\"}]");

        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // fieldMinValue and fieldMaxValue is null and dataReadOnly is true
    @Test
    public void testGetTableSplits02()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 6},
                        {6, 12}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"\",\"fieldMaxValue\":\"\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 2);
        }
    }

    // fieldMinValue and fieldMaxValue is null and dataReadOnly is false
    @Test
    public void testGetTableSplits03()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 6},
                        {6, 12},
                        {12, Long.MAX_VALUE},
                        {Long.MIN_VALUE, 0}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"false\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"\",\"fieldMaxValue\":\"\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 4);
        }
    }

    // dataReadOnly is false
    @Test
    public void testGetTableSplits04()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 6},
                        {6, 12},
                        {12, Long.MAX_VALUE},
                        {Long.MIN_VALUE, 0}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"false\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 4);
        }
    }

    // fieldMinValue and filedMaxValue not consistent with table
    @Test
    public void testGetTableSplits05()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 6},
                        {6, 12}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"6\",\"fieldMaxValue\":\"2\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 2);
        }
    }

    // dataReadOnly is not exist.
    @Test
    public void testGetTableSplits06()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 6},
                        {6, 12},
                        {12, Long.MAX_VALUE},
                        {Long.MIN_VALUE, 0}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 4);
        }
    }

    // splitCount <= 0
    @Test
    public void testGetTableSplits07()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"-1\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // splitCount is not exist.
    @Test
    public void testGetTableSplits08()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // splitField is not exist.
    @Test
    public void testGetTableSplits09()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // fieldMinValue and fieldMaxValue is same value.
    @Test
    public void testGetTableSplits10()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"SAME_NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"10\",\"fieldMaxValue\":\"10\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "same_numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // read full table.
    @Test
    public void testGetTableSplits11()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NONE_NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"\",\"fieldMaxValue\":\"\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "none_numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    @Test
    public void testGetTableSplits12()
    {
        long[][] rangeArray = new long[][]
                {
                        {0, 2},
                        {2, 4},
                        {4, 5}
                };
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"FIVE_NUMBERS\"," +
                        "\"splitField\":\"value\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"3\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"5\"}]");
        tableHandle = getTableHandle(new SchemaTableName("example", "five_numbers"));
        tableHandle.setTableSplitFieldValidated(true);
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getTableSplits(JdbcIdentity.from(SESSION), tableHandle);
        List<ConnectorSplit> splits = getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, 1000)).getSplits();
        int index = 0;
        for (ConnectorSplit split : splits) {
            JdbcSplit jdbcSplit = (JdbcSplit) split;
            assertEquals(Long.parseLong(jdbcSplit.getRangeStart()), rangeArray[index][0]);
            assertEquals(Long.parseLong(jdbcSplit.getRangEnd()), rangeArray[index][1]);
            index++;
            assertFalse(index > 3);
        }
    }

    private void initSplitDatabase()
            throws SQLException
    {
        String connectionUrl;
        connectionUrl = "jdbc:h2:mem:test" + System.nanoTime() + ThreadLocalRandom.current().nextLong();
        jdbcClient = new BaseJdbcClient(
                new BaseJdbcConfig(),
                "\"",
                new DriverConnectionFactory(new Driver(), connectionUrl, Optional.empty(), Optional.empty(), new Properties()));
        connection = DriverManager.getConnection(connectionUrl);
    }

    // splitField is empty
    @Test
    public void testGetTableSplits13()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");

        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getSplits(JdbcIdentity.from(SESSION), tableHandle);
        assertEquals(splitSource.getClass(), FixedSplitSource.class);
    }

    // splitField is not number type
    @Test
    public void testGetTableSplits14()
    {
        BaseJdbcConfig config = new BaseJdbcConfig();
        config.setPushDownEnable(true)
                .setTableSplitEnable(true)
                .setTableSplitFields("[{\"catalogName\":\"" + catalogName + "\",\"schemaName\":\"EXAMPLE\",\"tableName\":\"NUMBERS\"," +
                        "\"splitField\":\"text_short\",\"calcStepEnable\":\"false\",\"dataReadOnly\":\"true\",\"splitCount\":\"2\"," +
                        "\"fieldMinValue\":\"1\",\"fieldMaxValue\":\"12\"}]");

        tableHandle = getTableHandle(new SchemaTableName("example", "numbers"));
        splitManager = new DataSourceTableSplitManager(config, jdbcClient, nodeManager);
        ConnectorSplitSource splitSource = splitManager.getSplits(JdbcIdentity.from(SESSION), tableHandle);
    }

    private void initTable()
            throws SQLException
    {
        connection.createStatement().execute("CREATE SCHEMA example");

        connection.createStatement().execute("CREATE TABLE example.numbers(text varchar primary key, text_short varchar(32), value bigint)");
        connection.createStatement().execute("INSERT INTO example.numbers(text, text_short, value) VALUES " +
                "('one', 'one', 1)," +
                "('two', 'two', 2)," +
                "('three', 'three', 3)," +
                "('ten', 'ten', 10)," +
                "('eleven', 'eleven', 11)," +
                "('twelve', 'twelve', 12)" +
                "");

        connection.createStatement().execute("CREATE TABLE example.same_numbers(text varchar primary key, text_short varchar(32), value bigint)");
        int times = 100;
        for (int i = 0; i < times; i++) {
            String text = "num" + i;
            connection.createStatement().execute("INSERT INTO example.same_numbers(text, text_short, value) VALUES " +
                    "('" + text + "', '" + text + "', 10)");
        }

        connection.createStatement().execute("CREATE TABLE example.none_numbers(text varchar primary key, text_short varchar(32), value bigint)");

        connection.createStatement().execute("CREATE TABLE example.five_numbers(text varchar primary key, text_short varchar(32), value bigint)");
        connection.createStatement().execute("INSERT INTO example.five_numbers(text, text_short, value) VALUES " +
                "('one1', 'one1', 1)," +
                "('one2', 'one2', 1)," +
                "('one3', 'one3', 1)," +
                "('two1', 'two1', 2)," +
                "('two2', 'two2', 2)," +
                "('two3', 'two3', 2)," +
                "('three1', 'three1', 3)," +
                "('three2', 'three2', 3)," +
                "('three3', 'three3', 3)," +
                "('four1', 'four1', 4)," +
                "('four2', 'four2', 4)," +
                "('four3', 'four3', 4)," +
                "('five1', 'five1', 5)," +
                "('five2', 'five2', 5)," +
                "('five3', 'five3', 5)" +
                "");
        connection.commit();
    }

    private JdbcTableHandle getTableHandle(SchemaTableName table)
    {
        return jdbcClient.getTableHandle(JdbcIdentity.from(SESSION), table)
                .orElseThrow(() -> new IllegalArgumentException("table not found: " + table));
    }
}
