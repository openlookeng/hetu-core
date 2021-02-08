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

package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ColumnMapping;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcHandleResolver;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcSplitManager;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.JdbcTypeHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.CharType;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.VarcharType;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.tests.AbstractTestSqlQueryWriter;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * TestOracleSqlQueryWriter
 *
 * @since 2019-07-08
 */

public class TestOracleSqlQueryWriter
        extends AbstractTestSqlQueryWriter
{
    private static final Logger LOGGER = Logger.get(TestOracleSqlQueryWriter.class);
    private static final ConnectorSession SESSION = testSessionBuilder().build().toConnectorSession();
    private static final String TEST = "test";
    private static final String ORDERS = "orders";
    private static final String CUSTOMER = "customer";
    private static final String LINEITEM = "lineitem";
    private static final String TIMESTAMPSTR = "timestamp";
    private static final String INSERT = "INSERT INTO test.numbers(text, text_short, value) VALUES ";

    private static final String STR_DECIMAL = "decimal";
    private static final int NUMBER_4 = 4;
    private static final int NUMBER_2 = 2;
    private static final int NUMBER_13 = 13;
    private static final JdbcTypeHandle JDBC_BOOLEAN = new JdbcTypeHandle(Types.BOOLEAN,
            Optional.of("boolean"), 1, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_SMALLINT = new JdbcTypeHandle(Types.SMALLINT,
            Optional.of("smallint"), 1, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_TINYINT = new JdbcTypeHandle(Types.TINYINT,
            Optional.of("tinyint"), 2, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_INTEGER = new JdbcTypeHandle(Types.INTEGER,
            Optional.of("integer"), 4, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_BIGINT = new JdbcTypeHandle(Types.BIGINT,
            Optional.of("bigint"), 8, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_REAL = new JdbcTypeHandle(Types.REAL,
            Optional.of("real"), 8, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DOUBLE = new JdbcTypeHandle(Types.DOUBLE,
            Optional.of("double precision"), 8, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_CHAR = new JdbcTypeHandle(Types.CHAR,
            Optional.of("char"), 10, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_VARCHAR = new JdbcTypeHandle(Types.VARCHAR,
            Optional.of("varchar"), 10, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DATE = new JdbcTypeHandle(Types.DATE,
            Optional.of("date"), 8, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_TIME = new JdbcTypeHandle(Types.TIME,
            Optional.of("time"), 4, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_TIMESTAMP = new JdbcTypeHandle(Types.TIMESTAMP,
            Optional.of(TIMESTAMPSTR), 8, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_30 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 3, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_50 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 5, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_100 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 10, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_190 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 19, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_0127 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 0, -3, Optional.empty());
    private static final JdbcTypeHandle JDBC_DECIMAL_384 = new JdbcTypeHandle(Types.DECIMAL,
            Optional.of(STR_DECIMAL), 12, -4, Optional.empty());
    private static final JdbcTypeHandle JDBC_CLOB_OR_NCLOB = new JdbcTypeHandle(OracleTypes.CLOB_OR_NCLOB,
            Optional.of("clob"), 12, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_LONG_RAW = new JdbcTypeHandle(OracleTypes.LONG_RAW,
            Optional.of("long_rwa"), 12, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_LONG = new JdbcTypeHandle(OracleTypes.LONG,
            Optional.of("long"), 1, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_TIMESTAMP_STRING = new JdbcTypeHandle(
            OracleTypes.TIMESTAMP_WITH_TIMEZONE_OR_NCLOB_OR_NVARCHAR2,
            Optional.of(TIMESTAMPSTR), 12, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_TIMESTAMP_NCLOB_STRING = new JdbcTypeHandle(
            OracleTypes.TIMESTAMP_WITH_TIMEZONE_OR_NCLOB_OR_NVARCHAR2,
            Optional.of("NCLOB"), 12, 0, Optional.empty());
    private static final JdbcTypeHandle JDBC_FLOAT = new JdbcTypeHandle(OracleTypes.NUMBER_OR_FLOAT,
            Optional.of("float"), 127, -127, Optional.empty());
    private static final JdbcTypeHandle JDBC_TIMESTAMP6_WITH_TIMEZONE = new JdbcTypeHandle(
            OracleTypes.TIMESTAMP6_WITH_TIMEZONE,
            Optional.of(TIMESTAMPSTR), 12, 0, Optional.empty());

    private OracleClient oracleClient;
    private Connection connection;
    private TestingOracleServer oracleServer;
    private ConnectorFactory connectorFactory;
    private List<String> tables = new ArrayList<>(1);

    /**
     * Create TestOracleSqlQueryWriter
     */
    protected TestOracleSqlQueryWriter()
    {
        super(new OracleSqlQueryWriter(), "oracle", TEST);
    }

    /**
     * Setup the database
     */
    @BeforeClass
    public void setup()
    {
        try {
            oracleServer = new TestingOracleServer();
            BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
            jdbcConfig.setConnectionUrl(oracleServer.getJdbcUrl());
            jdbcConfig.setConnectionUser(TEST);
            jdbcConfig.setConnectionPassword(TEST);

            Driver driver;
            try {
                driver = (Driver) Class.forName(Constants.ORACLE_JDBC_DRIVER_CLASS_NAME).getConstructor(((Class<?>[]) null)).newInstance();
            }
            catch (InstantiationException | ClassNotFoundException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new PrestoException(JDBC_ERROR, e);
            }

            ConnectionFactory connectionFactory = new DriverConnectionFactory(driver,
                    jdbcConfig.getConnectionUrl(),
                    Optional.ofNullable(jdbcConfig.getConnectionUser()),
                    Optional.ofNullable(jdbcConfig.getConnectionPassword()),
                    basicConnectionProperties(jdbcConfig));
            OracleConfig oracleConfig = new OracleConfig();
            oracleClient = new OracleClient(jdbcConfig, oracleConfig, connectionFactory);
            this.connectorFactory = new OracleJdbcConnectorFactory(oracleClient, "oracle");
            this.connection = connectionFactory.openConnection(JdbcIdentity.from(SESSION));
            createTables();
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.setup();
    }

    private void createTables()
            throws SQLException
    {
        connection.createStatement().execute(buildCreateTableSql(ORDERS,
                "(orderkey int NOT NULL primary key, "
                        + "custkey int NOT NULL, orderstatus varchar(1) NOT NULL, totalprice number(10) NOT NULL, "
                        + "orderdate date NOT NULL, orderpriority varchar(15) NOT NULL, clerk varchar(15) NOT NULL, "
                        + "shippriority int NOT NULL, \"COMMENT\" varchar(79) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql(CUSTOMER,
                "(custkey int NOT NULL primary key, "
                        + "name varchar(25) NOT NULL, address varchar(40) NOT NULL, nationkey int NOT NULL, "
                        + "phone varchar(15) NOT NULL, acctbal number(10) NOT NULL, mktsegment varchar(10) NOT NULL, "
                        + "\"COMMENT\" varchar(117) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql("supplier",
                "(suppkey int NOT NULL primary key, "
                        + "name varchar(25) NOT NULL, address varchar(40) NOT NULL, "
                        + "nationkey int NOT NULL, phone varchar(15) NOT NULL, acctbal number(10) NOT NULL, "
                        + "\"COMMENT\" varchar(101) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql("region",
                "(regionkey int NOT NULL primary key, name varchar(25) NOT NULL, "
                        + "\"COMMENT\" varchar(152) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql(LINEITEM,
                "(orderkey int NOT NULL primary key, "
                        + "partkey int NOT NULL, suppkey int NOT NULL, linenumber int NOT NULL, quantity number(10) NOT NULL,"
                        + " extendedprice number(10) NOT NULL, discount number(10) NOT NULL, tax number(10) NOT NULL, "
                        + "returnflag varchar(1) NOT NULL, linestatus varchar(1) NOT NULL, shipdate date NOT NULL, "
                        + "commitdate date NOT NULL, receiptdate date NOT NULL, shipinstruct varchar(25) NOT NULL, "
                        + "shipmode varchar(10) NOT NULL, \"COMMENT\" varchar(44) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql("nation",
                "(nationkey int NOT NULL primary key, name varchar(25) NOT NULL, regionkey int NOT NULL,"
                        + "\"COMMENT\" varchar(152) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql("part",
                "(partkey int NOT NULL primary key, name varchar(55) NOT NULL, mfgr varchar(25) NOT NULL, "
                        + "brand varchar(10) NOT NULL, TYPE varchar(25) NOT NULL, \"SIZE\" int NOT NULL, "
                        + "container varchar(10) NOT NULL,"
                        + " retailprice number(10) NOT NULL, \"COMMENT\" varchar(23) NOT NULL)"));
        connection.createStatement().execute(buildCreateTableSql("partsupp",
                "(partkey int NOT NULL primary key, suppkey int NOT NULL, availqty int NOT NULL, "
                        + "supplycost number(10) NOT NULL, \"COMMENT\" varchar(199) NOT NULL)"));

        createTablesForClient();
    }

    private void createTablesForClient()
            throws SQLException
    {
        connection.createStatement().execute("CREATE TABLE test.numbers(text varchar(20) primary key, "
                + "text_short varchar(32), value int)");
        connection.createStatement().execute(INSERT + "('one', 'one', 1)");
        connection.createStatement().execute(INSERT + "('two', 'two', 2)");
        connection.createStatement().execute(INSERT + "('three', 'three', 3)");
        connection.createStatement().execute(INSERT + "('ten', 'ten', 10)");
        connection.createStatement().execute(INSERT + "('eleven', 'eleven', 11)");
        connection.createStatement().execute(INSERT + "('twelve', 'twelve', 12)");
        connection.createStatement().execute("CREATE TABLE test.student(id varchar(20) primary key)");
        connection.createStatement().execute("CREATE TABLE test.num_ers(te_t varchar(20) primary key,"
                + " \"VA%UE\" int)");
        connection.createStatement().execute("CREATE TABLE test.table_with_float_col(col1 int primary key,"
                + " col2 int, col3 int, col4 int)");

        connection.createStatement().execute("CREATE TABLE test.number2(text varchar(20) primary key, "
                + "text_short varchar(32), value int)");
    }

    private String buildCreateTableSql(String tableName, String columnInfo)
    {
        tables.add(tableName);

        return "CREATE TABLE " + TEST + "." + tableName + " " + columnInfo;
    }

    /**
     * Clean the resources
     */
    @AfterClass(alwaysRun = true)
    public void clean()
    {
        oracleServer.close();
        super.clean();
    }

    /**
     * testMetadata
     */
    @Test
    public void testMetadata()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        assertTrue(oracleClient.getSchemaNames(identity).contains(TEST));
    }

    /**
     * testListSchema
     */
    @Test
    public void testListSchema()
    {
        assertEquals(ImmutableSet.copyOf(oracleClient.listSchemas(connection)).contains(TEST), true);
    }

    /**
     * testGetTableHandle
     */
    @Test
    public void testGetTableHandle()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        Optional<JdbcTableHandle> tableHandle = oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, Constants.NUMBERS));
        assertEquals(oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, Constants.NUMBERS)), tableHandle);
        assertEquals(oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, "dept")), Optional.empty());
        assertEquals(oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, "darren")), Optional.empty());
        assertEquals(oracleClient.getTableHandle(identity,
                new SchemaTableName("mysql", "dept")), Optional.empty());
    }

    /**
     * testGetTableNames
     */
    @Test
    public void testGetTableNames()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        assertEquals(oracleClient.getTableNames(identity, Optional.of(Constants.ORACLE)).size(), NUMBER_13);
    }

    /**
     * testGetTableHandleException
     */
    @Test
    public void testGetTableHandleException()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        assertEquals(oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, "notexist_table")), Optional.empty());
    }

    /**
     * testGetTableHandleGetConnectionException
     */
    @Test
    public void testGetTableHandleGetConnectionException()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        oracleClient.getTableHandle(identity, new SchemaTableName(Constants.ORACLE, Constants.NUMBERS));
    }

    /**
     * testRenameTableException
     */
    @Test(expectedExceptions = RuntimeException.class)
    public void testRenameTableException()
    {
        JdbcIdentity identity = JdbcIdentity.from(SESSION);
        SchemaTableName newTableName = new SchemaTableName("schema_test", "new_student");
        Optional<JdbcTableHandle> tableHandle = oracleClient.getTableHandle(identity,
                new SchemaTableName(Constants.ORACLE, Constants.NUMBERS));
        try {
            oracleClient.renameTable(identity, tableHandle.get(), newTableName);
        }
        // CHECKSTYLE:OFF:IllegalCatch
        catch (Exception e) {
            // CHECKSTYLE:ON:IllegalCatch
            throw new RuntimeException(e);
        }
        assertEquals(oracleClient.getTableNames(identity, Optional.of(Constants.ORACLE)).size(), NUMBER_13);
    }

    /**
     * testGenerateTempTableName
     */
    @Test
    public void testGenerateTempTableName()
    {
        String tmpName = oracleClient.generateTemporaryTableName();
        assertNotNull(tmpName);
    }

    /**
     * Test disabled support for LONG oracle type
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testLongTypeDisabledSupport()
    {
        oracleClient.toPrestoType(SESSION, connection, JDBC_LONG);
    }

    /**
     * testtoHetuType
     */
    @Test
    public void testToHetuType()
    {
        Optional<ColumnMapping> columnMapping = Optional.empty();
        columnMapping = oracleClient.toPrestoType(SESSION, connection, JDBC_BIGINT);

        oracleClient.toPrestoType(SESSION, connection, JDBC_SMALLINT);
        oracleClient.toPrestoType(SESSION, connection, JDBC_BOOLEAN);
        oracleClient.toPrestoType(SESSION, connection, JDBC_TINYINT);
        oracleClient.toPrestoType(SESSION, connection, JDBC_INTEGER);
        oracleClient.toPrestoType(SESSION, connection, JDBC_REAL);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DOUBLE);
        oracleClient.toPrestoType(SESSION, connection, JDBC_CHAR);
        oracleClient.toPrestoType(SESSION, connection, JDBC_VARCHAR);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DATE);
        oracleClient.toPrestoType(SESSION, connection, JDBC_TIME);
        oracleClient.toPrestoType(SESSION, connection, JDBC_TIMESTAMP);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_30);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_50);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_100);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_190);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_0127);
        oracleClient.toPrestoType(SESSION, connection, JDBC_DECIMAL_384);
        oracleClient.toPrestoType(SESSION, connection, JDBC_CLOB_OR_NCLOB);
        oracleClient.toPrestoType(SESSION, connection, JDBC_LONG_RAW);
        oracleClient.toPrestoType(SESSION, connection, JDBC_TIMESTAMP_NCLOB_STRING);
        oracleClient.toPrestoType(SESSION, connection, JDBC_FLOAT);
        // we do not support these following type for we remove oracle.sql.TIMESTAMPTZ;
        // JDBC_TIMESTAMP_STRING, JDBC_TIMESTAMP6_WITH_TIMEZONE
    }

    /**
     * testToWriteMapping
     */
    @Test
    public void testToWriteMapping()
    {
        oracleClient.toWriteMapping(SESSION, VarcharType.VARCHAR);
        oracleClient.toWriteMapping(SESSION, CharType.createCharType(NUMBER_4));
        oracleClient.toWriteMapping(SESSION, DecimalType.createDecimalType(NUMBER_4, NUMBER_2));
        oracleClient.toWriteMapping(SESSION, BooleanType.BOOLEAN);
        oracleClient.toWriteMapping(SESSION, INTEGER);
        oracleClient.toWriteMapping(SESSION, SMALLINT);
        oracleClient.toWriteMapping(SESSION, BIGINT);
        oracleClient.toWriteMapping(SESSION, TINYINT);
        oracleClient.toWriteMapping(SESSION, REAL);
        oracleClient.toWriteMapping(SESSION, DOUBLE);
        oracleClient.toWriteMapping(SESSION, VARBINARY);
        oracleClient.toWriteMapping(SESSION, TIMESTAMP);
        oracleClient.toWriteMapping(SESSION, TIMESTAMP_WITH_TIME_ZONE);
        oracleClient.toWriteMapping(SESSION, DATE);
    }

    /**
     * getConnectorFactory
     *
     * @return connection factory
     */
    @Override
    protected Optional<ConnectorFactory> getConnectorFactory()
    {
        return Optional.of(this.connectorFactory);
    }

    @Override
    protected void assertStatement(@Language("SQL") String query, String... keywords)
    {
        super.assertStatement(query, keywords);
    }

    /**
     * testLambdaExpression
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    @Override
    public void testLambdaExpression()
    {
        super.testLambdaExpression();
    }

    /**
     * testDecimalLiteralExpression
     */
    @Override
    public void testDecimalLiteralExpression()
    {
        LOGGER.info("Testing Hetu decimal literal expressions");
        assertExpression(new DecimalLiteral("12.34"), "'12.34'");
        assertExpression(new DecimalLiteral("12."), "'12.'");
        assertExpression(new DecimalLiteral("12"), "'12'");
        assertExpression(new DecimalLiteral(".34"), "'.34'");
        assertExpression(new DecimalLiteral("+12.34"), "'+12.34'");
        assertExpression(new DecimalLiteral("+12"), "'+12'");
        assertExpression(new DecimalLiteral("-12.34"), "'-12.34'");
        assertExpression(new DecimalLiteral("-12"), "'-12'");
        assertExpression(new DecimalLiteral("+.34"), "'+.34'");
        assertExpression(new DecimalLiteral("-.34"), "'-.34'");
    }

    /**
     * testSelectStatement
     */
    @Test
    public void testSelectStatement()
    {
        LOGGER.info("Testing select statement");
        @Language("SQL")
        String query = "SELECT (totalprice + 2) AS new_price FROM orders";
        assertStatement(query, "SELECT", "totalprice", "+", "2", "FROM", "orders");
    }

    /**
     * testIntermediateFunctions
     */
    @Test
    public void testIntermediateFunctions()
    {
        LOGGER.info("Testing Hetu current time in a statement");
        // current_time is converted to $literal$time with time zone
        @Language("SQL")
        String query = "SELECT current_time FROM customer";
        assertStatement(query, "SELECT", "FROM", CUSTOMER);

        // interval '29' day is converted to $literal$interval day to second
        // timestamp is converted to $literal$timestamp
        query = "SELECT * FROM orders WHERE orderdate - interval '29' day > timestamp '2012-10-31 01:00 UTC'";
        assertStatement(query, "SELECT", "FROM", ORDERS, "WHERE");
    }

    /**
     * testAggregationStatements
     */
    @Test
    public void testAggregationStatements()
    {
        LOGGER.info("Testing aggregation statements");
        String query = "SELECT * FROM " + "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM "
                + "       customer c JOIN orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey) t1 "
                + "   LEFT JOIN lineitem l ON substr(cast(t1.orderkey AS VARCHAR), 0, 2)=cast(t1.orderkey AS VARCHAR) LIMIT 20";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "GROUP BY", "LEFT JOIN",
                LINEITEM, "WHERE ROWNUM <= 20");

        query = "SELECT * FROM " + "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM"
                + " customer c join orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey HAVING orderkey>100) t1"
                + " LEFT JOIN lineitem l ON substr(cast(t1.orderkey AS VARCHAR), 0, 2)=cast(t1.orderkey AS VARCHAR) LIMIT 10";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "WHERE", ">", "100", "GROUP BY",
                "LEFT JOIN", LINEITEM, "WHERE ROWNUM <= 10");
    }

    /**
     * testExtractStatement
     */
    @Test
    public void testExtractStatement()
    {
        LOGGER.info("Testing extract statement");
        String query = "SELECT extract(YEAR FROM orderdate) AS year FROM orders LIMIT 10";
        assertStatement(query, "SELECT", "EXTRACT", "YEAR FROM orderdate", "FROM", ORDERS, "WHERE ROWNUM <= 10");
    }

    /**
     * testLambdaStatement
     */
    @Test
    public void testLambdaStatement()
    {
        LOGGER.info("Testing lambda in a statement");
        String query = "SELECT filter(split(comment, ' '), x -> length(x) > 2) FROM customer LIMIT 10";
        assertStatement(query);
    }

    /**
     * testJoinStatements
     */
    @Override
    public void testJoinStatements()
    {
        LOGGER.info("Testing join statements");
        String query = "SELECT c.name FROM customer c LEFT JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "LEFT JOIN", ORDERS, "ON",
                "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "RIGHT JOIN", ORDERS, "ON",
                "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "ON",
                "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c FULL JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "FULL JOIN", ORDERS, "ON",
                "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c JOIN orders o USING (custkey)";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "ON",
                "table0.custkey = table1.custkey_0");

        query = "SELECT c.name FROM customer c CROSS JOIN orders LIMIT 10";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "CROSS JOIN", ORDERS, "WHERE ROWNUM <= 10");

        // Predicate push down changes left join to inner join
        query = "SELECT c.name FROM customer c LEFT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "WHERE", ">", "10");

        query
                = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F'";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "RIGHT JOIN", ORDERS, "WHERE", ">", "10", "AND", "'f'");

        query
                = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F' ORDER BY cast(c.name AS VARCHAR) LIMIT 10";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "RIGHT JOIN", ORDERS, "WHERE", ">", "10", "AND", "'f'",
                "ORDER BY", "WHERE ROWNUM <= 10");

        query = "SELECT * " + "FROM (SELECT max(totalprice) AS price, o.orderkey AS orderkey "
                + "FROM customer c JOIN orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey LIMIT 10) t1 LEFT JOIN lineitem l ON t1.orderkey=l.orderkey";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "GROUP BY", "WHERE ROWNUM <= 10",
                "LEFT JOIN", LINEITEM);

        query
                = "SELECT c.name FROM customer c RIGHT JOIN orders o ON c.custkey=o.custkey WHERE o.totalprice > 10 AND o.orderstatus='F'";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "RIGHT JOIN", ORDERS, "WHERE", ">", "10", "AND", "'f'");

        query =
                "SELECT t1.custkey1, t2.custkey, t2.name FROM (SELECT c.custkey AS custkey1, o.custkey AS custkey2 FROM "
                        + "       customer c INNER JOIN orders o ON c.custkey = o.custkey) t1 "
                        + "           LEFT JOIN customer t2 ON t1.custkey1=t2.custkey LIMIT 10";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "INNER JOIN", ORDERS, "LEFT JOIN", CUSTOMER);

        query = "SELECT * FROM orders o LEFT JOIN lineitem l USING (orderkey) LEFT JOIN "
                + "   customer c using (custkey) LEFT JOIN supplier s USING (nationkey) LEFT JOIN "
                + "       partsupp ps ON ps.suppkey=s.suppkey JOIN part pt ON pt.partkey=ps.partkey LIMIT 20";
        assertStatement(query, "SELECT", "FROM", ORDERS, "LEFT JOIN", LINEITEM, "LEFT JOIN", CUSTOMER,
                "LEFT JOIN", "supplier", "LEFT JOIN", "partsupp", "INNER JOIN", "part", "WHERE ROWNUM <= 20");

        query = "SELECT c_count, count(*) AS custdist FROM " + "   (SELECT c.custkey, count(o.orderkey) FROM "
                + "       customer c LEFT OUTER JOIN orders o ON c.custkey = o.custkey AND o.comment NOT LIKE '%[WORD1]%[WORD2]%' GROUP BY c.custkey) AS c_orders(c_custkey, c_count) "
                + "           GROUP BY c_count ORDER BY custdist DESC, c_count DESC";
        assertStatement(query, "SELECT", "FROM", CUSTOMER, "LEFT JOIN", ORDERS, "NOT", "LIKE", "%", "WORD1", "%",
                "WORD2", "GROUP BY", "ORDER BY", "DESC");
    }

    @Override
    public void testCastExpression()
    {
        LOGGER.info("Testing cast expressions");
        assertExpression(new Cast(new NullLiteral(), "varchar(42)"), "CAST(null AS varchar2(42))");
        assertExpression(new Cast(new NullLiteral(), "varchar"), "CAST(null AS nclob)");
        assertExpression(new Cast(new NullLiteral(), "BIGINT"), "CAST(null AS number(19))");
        assertExpression(new Cast(new NullLiteral(), "double"), "CAST(null AS binary_double)");
        assertExpression(new Cast(new NullLiteral(), "DOUBLE"), "CAST(null AS binary_double)");
        assertExpression(new Cast(new NullLiteral(), "date"), "CAST(null AS date)");
        assertExpression(new Cast(new NullLiteral(), TIMESTAMPSTR), "CAST(null AS timestamp(3))");
        assertExpression(new Cast(new NullLiteral(), "timestamp with time zone"),
                "CAST(null AS timestamp(3) with time zone)");
    }

    @Override
    public void testFunctionCallAndTryExpression()
    {
        LOGGER.info("Testing function call and try expressions");
        FunctionCall functionCall = new FunctionCall(QualifiedName.of("strpos"),
                list(stringLiteral("b"), stringLiteral("a")));
        TryExpression tryExpression = new TryExpression(functionCall);
        assertExpression(functionCall, "strpos('b', 'a')");
        assertExpression(tryExpression, "TRY(strpos('b', 'a'))");
    }

    @Override
    public void testPredicateExpression()
    {
        LOGGER.info("Testing predicate expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        assertExpression(new InListExpression(literals), "(10, 20, 30)");
        assertExpression(new IsNullPredicate(new SymbolReference("age")), "(age IS NULL)");
        assertExpression(new IsNotNullPredicate(new SymbolReference("age")), "(age IS NOT NULL)");
        assertExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3")),
                "(1 BETWEEN 2 AND 3)");
        assertExpression(new NotExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3"))),
                "(NOT (1 BETWEEN 2 AND 3))");
        assertExpression(new ExistsPredicate(new SubqueryExpression(simpleQuery(selectList(new LongLiteral("1"))))),
                "(EXISTS (SELECT 1\n" + "\n" + "))");
    }

    @Override
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testArrayExpression()
    {
        super.testArrayExpression();
    }

    @Override
    public void testGenericLiteralExpression()
    {
        LOGGER.info("Testing Hetu generic literal expressions");
        assertExpression(new GenericLiteral("VARCHAR", "abc"), "'abc'");
        assertExpression(new GenericLiteral("BIGINT", "abc"), "abc");
        assertExpression(new GenericLiteral("DOUBLE", "abc"), "abc");
        assertExpression(new GenericLiteral("DATE", "abc"), "date 'abc'");
    }

    /**
     * testTpchSql1
     */
    @Test
    public void testTpchSql1()
    {
        LOGGER.info("Testing TPCH Sql 1");
        @Language("SQL")
        String query = "SELECT returnflag, linestatus, sum(quantity) AS sum_qty, sum(extendedprice) AS sum_base_price, sum(extendedprice * (1 - discount)) AS sum_disc_price, sum(extendedprice * (1 - discount) * (1 + tax)) AS sum_charge, avg(quantity) AS avg_qty, avg(extendedprice) AS avg_price, avg(discount) AS avg_disc, count(*) AS count_order FROM lineitem WHERE shipdate <= date '1998-09-16' GROUP BY returnflag, linestatus ORDER BY returnflag, linestatus";
        assertStatement(query, "sum", "sum", "sum", "sum", "avg", "avg", "avg", "count", "(", "*", ")", "WHERE", "\\<=", TIMESTAMPSTR, "GROUP BY", "ORDER BY");
    }

    /**
     * Oracle does not support BOOLEAN literal.
     */
    @Test(expectedExceptions = UnsupportedOperationException.class)
    public void testBooleanLiteralExpression()
    {
        LOGGER.info("Testing Hetu generic date expressions");
        assertExpression(new GenericLiteral("BOOLEAN", "abc"), "abc");
    }

    /**
     * OracleJdbcConnectorFactory
     *
     * @since 2019-10-12
     */
    private static class OracleJdbcConnectorFactory
            implements ConnectorFactory
    {
        private final JdbcClient jdbcClient;

        private final String name;

        private OracleJdbcConnectorFactory(JdbcClient jdbcClient, String name)
        {
            this.jdbcClient = jdbcClient;
            this.name = name;
        }

        @Override
        public String getName()
        {
            return this.name;
        }

        @Override
        public ConnectorHandleResolver getHandleResolver()
        {
            return new JdbcHandleResolver();
        }

        @Override
        public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
        {
            return new Connector()
            {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean isReadOnly)
                {
                    return new ConnectorTransactionHandle()
                    {
                    };
                }

                @Override
                public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
                {
                    return new JdbcMetadata(jdbcClient, false);
                }

                @Override
                public ConnectorSplitManager getSplitManager()
                {
                    return new JdbcSplitManager(jdbcClient);
                }

                @Override
                public ConnectorRecordSetProvider getRecordSetProvider()
                {
                    return new JdbcRecordSetProvider(jdbcClient);
                }
            };
        }
    }
}
