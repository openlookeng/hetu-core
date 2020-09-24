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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.hetu.core.plugin.hana.rewrite.HanaSqlQueryWriter;
import io.hetu.core.plugin.hana.rewrite.UdfFunctionRewriteConstants;
import io.prestosql.plugin.jdbc.BaseJdbcConfig;
import io.prestosql.plugin.jdbc.ConnectionFactory;
import io.prestosql.plugin.jdbc.DriverConnectionFactory;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcHandleResolver;
import io.prestosql.plugin.jdbc.JdbcMetadata;
import io.prestosql.plugin.jdbc.JdbcRecordSetProvider;
import io.prestosql.plugin.jdbc.JdbcSplitManager;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorRecordSetProvider;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.DateTimeEncoding;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.builder.functioncall.ConfigFunctionParser;
import io.prestosql.sql.builder.functioncall.FunctionCallArgsPackage;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.AtTimeZone;
import io.prestosql.sql.tree.BetweenPredicate;
import io.prestosql.sql.tree.BinaryLiteral;
import io.prestosql.sql.tree.BindExpression;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CharLiteral;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.CurrentPath;
import io.prestosql.sql.tree.CurrentTime;
import io.prestosql.sql.tree.CurrentUser;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.GroupingOperation;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InListExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.IntervalLiteral;
import io.prestosql.sql.tree.IsNotNullPredicate;
import io.prestosql.sql.tree.IsNullPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeLocation;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.SingleColumn;
import io.prestosql.sql.tree.StringLiteral;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.TimeLiteral;
import io.prestosql.sql.tree.TimestampLiteral;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.tree.Window;
import io.prestosql.tests.AbstractTestSqlQueryWriter;
import io.prestosql.util.DateTimeUtils;
import org.codehaus.plexus.util.StringUtils;
import org.intellij.lang.annotations.Language;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.hetu.core.plugin.hana.TestHanaSqlUtil.getHandledSql;
import static io.prestosql.plugin.jdbc.DriverConnectionFactory.basicConnectionProperties;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.query;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.table;
import static io.prestosql.sql.QueryUtil.values;
import static io.prestosql.sql.tree.ArithmeticUnaryExpression.negative;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.LESS_THAN;
import static org.testng.Assert.assertEquals;

/**
 * This is testing HanaSqlQueryWriter
 *
 * @since 2019-09-25
 */
public class TestHanaSqlQueryWriter
        extends AbstractTestSqlQueryWriter
{
    private static final Logger LOGGER = Logger.get(TestHanaSqlQueryWriter.class);

    private Connection connection;

    private TestingHanaServer testingHanaServer;

    private ConnectorFactory connectorFactory;

    private List<String> tables = new ArrayList<>();

    private HanaConfig hanaConfig = new HanaConfig();

    /**
     * Create TestHanaSqlQueryWriter
     */
    protected TestHanaSqlQueryWriter()
    {
        super(new HanaSqlQueryWriter(new HanaConfig()), "hana", "datahub");
    }

    //tools for test

    protected void assertExpression(Node expression, String expected, UnsupportedOperationException expectedExp)
    {
        assertExpression(expression, expected, Optional.empty(), expectedExp);
    }

    protected void assertExpression(Node expression, String expected, Optional<List<Expression>> params, UnsupportedOperationException expectedExp)
    {
        try {
            assertExpression(expression, expected, params);
        }
        catch (Exception rtmExp) {
            if ((expectedExp != null) && (rtmExp instanceof UnsupportedOperationException)) {
                assertEquals(rtmExp.getMessage(), expectedExp.getMessage());
                return;
            }
            throw rtmExp;
        }
    }

    protected void assertStatement(@Language("SQL") String query, AssertionError assertionError, String... keywords)
    {
        try {
            assertStatement(query, keywords);
        }
        catch (Error er) {
            if ((assertionError != null) && (er instanceof AssertionError)) {
                return;
            }
            throw er;
        }
    }

    /**
     * Setup the database
     */
    @BeforeClass
    public void setup()
    {
        this.testingHanaServer = TestingHanaServer.getInstance();
        if (!this.testingHanaServer.isHanaServerAvailable()) {
            LOGGER.info("please set correct hana data base info!");
            throw new SkipException("skip the test");
        }
        LOGGER.info("running TestHanaSqlQueryWriter...");
        try {
            createTables(this.testingHanaServer);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.setup();
    }

    /**
     * Clean the resources
     */
    @AfterClass(alwaysRun = true)
    public void clean()
    {
        try {
            if (this.testingHanaServer.isHanaServerAvailable()) {
                if (!testingHanaServer.isTpchLoaded()) {
                    for (String table : tables) {
                        String sql = "DROP TABLE " + testingHanaServer.getSchema() + "." + table;
                        connection.createStatement().execute(sql);
                    }
                }

                TestingHanaServer.shutDown();
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        super.clean();
    }

    private void createTables(TestingHanaServer hanaServer) throws SQLException
    {
        BaseJdbcConfig jdbcConfig = new BaseJdbcConfig();
        HanaConfig hanaConfig = new HanaConfig();

        jdbcConfig.setConnectionUrl(hanaServer.getJdbcUrl());
        jdbcConfig.setConnectionUser(hanaServer.getUser());
        jdbcConfig.setConnectionPassword(hanaServer.getPassword());

        hanaConfig.setTableTypes("TABLE,VIEW");
        hanaConfig.setSchemaPattern(hanaServer.getSchema());

        Driver driver = null;
        try {
            driver = (Driver) Class.forName(HanaConstants.SAP_HANA_JDBC_DRIVER_CLASS_NAME).getConstructor(((Class<?>[]) null)).newInstance();
        }
        catch (InstantiationException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (IllegalAccessException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (InvocationTargetException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }
        catch (NoSuchMethodException e) {
            throw new PrestoException(JDBC_ERROR, e);
        }

        ConnectionFactory connectionFactory = new DriverConnectionFactory(driver, jdbcConfig.getConnectionUrl(),
                Optional.ofNullable(jdbcConfig.getUserCredentialName()),
                Optional.ofNullable(jdbcConfig.getPasswordCredentialName()), basicConnectionProperties(jdbcConfig));

        HanaClient hanaClient = new HanaClient(jdbcConfig, hanaConfig, connectionFactory);
        connectorFactory = new HanaJdbcConnectorFactory(hanaClient, "hana");
        connection = DriverManager.getConnection(hanaServer.getJdbcUrl(), hanaServer.getUser(), hanaServer.getPassword());
        if (!hanaServer.isTpchLoaded()) {
            connection.createStatement().execute(buildCreateTableSql("orders", "(orderkey bigint NOT NULL, custkey bigint NOT NULL, orderstatus varchar(1) NOT NULL, totalprice DOUBLE NOT NULL, orderdate date NOT NULL, orderpriority varchar(15) NOT NULL, clerk varchar(15) NOT NULL, shippriority integer NOT NULL, COMMENT varchar(79) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("customer", "(custkey bigint NOT NULL, name varchar(25) NOT NULL, address varchar(40) NOT NULL, nationkey bigint NOT NULL, phone varchar(15) NOT NULL, acctbal DOUBLE NOT NULL, mktsegment varchar(10) NOT NULL, COMMENT varchar(117) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("supplier", "(suppkey bigint NOT NULL, name varchar(25) NOT NULL, address varchar(40) NOT NULL, nationkey bigint NOT NULL, phone varchar(15) NOT NULL, acctbal DOUBLE NOT NULL, COMMENT varchar(101) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("region", "(regionkey bigint NOT NULL, name varchar(25) NOT NULL, COMMENT varchar(152) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("lineitem", "(orderkey bigint NOT NULL, partkey bigint NOT NULL, suppkey bigint NOT NULL, linenumber integer NOT NULL, quantity DOUBLE NOT NULL, extendedprice DOUBLE NOT NULL, discount DOUBLE NOT NULL, tax DOUBLE NOT NULL, returnflag varchar(1) NOT NULL, linestatus varchar(1) NOT NULL, shipdate date NOT NULL, commitdate date NOT NULL, receiptdate date NOT NULL, shipinstruct varchar(25) NOT NULL, shipmode varchar(10) NOT NULL, COMMENT varchar(44) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("nation", "(nationkey bigint NOT NULL, name varchar(25) NOT NULL, regionkey bigint NOT NULL, COMMENT varchar(152) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("part", "(partkey bigint NOT NULL, name varchar(55) NOT NULL, mfgr varchar(25) NOT NULL, brand varchar(10) NOT NULL, TYPE varchar(25) NOT NULL, SIZE integer NOT NULL, container varchar(10) NOT NULL, retailprice DOUBLE NOT NULL, COMMENT varchar(23) NOT NULL)"));
            connection.createStatement().execute(buildCreateTableSql("partsupp", "(partkey bigint NOT NULL, suppkey bigint NOT NULL, availqty integer NOT NULL, supplycost DOUBLE NOT NULL, COMMENT varchar(199) NOT NULL)"));
        }
    }

    private String buildCreateTableSql(String tableName, String columnInfo)
    {
        String newTableName = TestingHanaServer.getActualTable(tableName);
        tables.add(newTableName);

        return "CREATE TABLE " + testingHanaServer.getSchema() + "." + newTableName + " " + columnInfo;
    }

    @Override
    protected void assertStatement(@Language("SQL") String query, String... keywords)
    {
        String newQuery = getHandledSql(query);
        super.assertStatement(newQuery, keywords);
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

    @Test
    public void testCast()
    {
        assertExpression(new Cast(new NullLiteral(), "date", false), "CAST(null AS date)", new UnsupportedOperationException("Hana Connector does not support try_cast"));
        assertExpression(new Cast(new NullLiteral(), "date", true), "CAST(null AS date)", new UnsupportedOperationException("Hana Connector does not support try_cast"));
    }

    @Test
    public void testQuantifiedComparisonExpression()
    {
        LOGGER.info("Testing comparison expressions");
        assertExpression(new QuantifiedComparisonExpression(
                        LESS_THAN,
                        QuantifiedComparisonExpression.Quantifier.ANY,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(new SingleColumn(identifier("col2"))), table(QualifiedName.of("table1"))))),
                "(col1 < ANY (SELECT col2\n" +
                        "FROM\n" +
                        "  table1\n" +
                        "))");
        assertExpression(new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.EQUAL,
                        QuantifiedComparisonExpression.Quantifier.ALL,
                        identifier("col1"),
                        new SubqueryExpression(query(values(row(longLiteral("1")), row(longLiteral("2")))))),
                "(col1 = ALL ( VALUES \n" + "  ROW (1)\n" + ", ROW (2)\n" + "))",
                new UnsupportedOperationException("Hana Connector does not support row"));
        assertExpression(new QuantifiedComparisonExpression(
                        ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL,
                        QuantifiedComparisonExpression.Quantifier.SOME,
                        identifier("col1"),
                        new SubqueryExpression(simpleQuery(selectList(longLiteral("10"))))),
                "(col1 >= SOME (SELECT 10\n" +
                        "\n" +
                        "))");
    }

    @Test
    public void testComparisonExpression()
    {
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.EQUAL, new SymbolReference("a"), new StringLiteral("hello")), "(a = 'hello')");
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.NOT_EQUAL, new SymbolReference("a"), new StringLiteral("hello")), "(a <> 'hello')");

        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN, new SymbolReference("a"), new StringLiteral("hello")), "(a < 'hello')");
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.LESS_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")), "(a <= 'hello')");
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN, new SymbolReference("a"), new StringLiteral("hello")), "(a > 'hello')");
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN_OR_EQUAL, new SymbolReference("a"), new StringLiteral("hello")), "(a >= 'hello')");
        assertExpression(new ComparisonExpression(ComparisonExpression.Operator.IS_DISTINCT_FROM, new SymbolReference("a"), new StringLiteral("hello")), "NA", new UnsupportedOperationException("Hana Connector does not support comparison operator IS DISTINCT FROM"));
    }

    @Test
    public void testWindowFunction()
    {
        // Hetu SQL grammar functions window.html
        String tableCustomer = TestingHanaServer.getActualTable("customer");
        String tableLineitem = TestingHanaServer.getActualTable("lineitem");
        LOGGER.info("Testing window function in a statement");
        @Language("SQL")
        String query = "select quantity , max(quantity) " +
                "over(order by returnflag) as ranking" +
                " from " + tableLineitem + " limit 100";

        assertStatement(query, "SELECT", "MAX", "over", "order", "by",
                "from", "lineitem", "LIMIT");
        query = "select quantity , max(quantity) over(partition by linestatus " +
                "order by returnflag desc nulls first rows 2 preceding) as ranking from " +
                tableLineitem + " order by quantity limit 100";
        assertStatement(query, "SELECT", "MAX", "over", "partition", "BY", "order", "by",
                "returnflag", "DESC", "NULLS", "FIRST", "ROWS", "2", "PRECEDING", "lineitem", "order", "BY", "quantity", "LIMIT");

        query = "select quantity , max(quantity) over(partition by linestatus order by " +
                "returnflag desc nulls first rows between 2 preceding and 1 following) as ranking " +
                " from " + tableLineitem + " limit 100";
        assertStatement(query, "SELECT", "MAX", "over", "partition", "BY", "order", "by",
                "returnflag", "DESC", "NULLS", "FIRST", "ROWS", "between", "2", "PRECEDING", "and", "1", "following", "lineitem", "LIMIT");

        query = "select rank() over(partition by name order by acctbal) as ranking, " +
                " sum(quantity) over(partition by linestatus order by returnflag rows 2 preceding) as ranking2 " +
                "from " + tableLineitem + ", " + tableCustomer + " limit 100";
        assertStatement(query, "SELECT", "sum", "quantity", "OVER", "PARTITION", "ORDER", "returnflag", "ROWS", "2",
                "PRECEDING", "rank", "over", "partition", "by", "name", "ORDER", "by", "acctbal", "ASC", "NULLS", "LAST",
                "lineitem", "CROSS", "JOIN ", "customer", "LIMIT");

        // Hana(grammar) Connector does not support function rank with rows, only aggregation support this! Assert error
        query = "select rank() over(partition by name order by acctbal rows 2 preceding) as ranking from " + tableCustomer + " limit 10";
        assertStatement(query, new AssertionError(), "SELECT", "rank", "over", "partition", "by", "ranking", "sum", "over", "partition", "BY",
                "order", "by", "returnflag", "DESC", "NULLS", "FIRST", "ROWS", "2", "PRECEDING", "lineitem", "customer", "LIMIT");

        query = "select rank() over(partition by name order by acctbal) as ranking from " + tableCustomer + " limit 10";
        assertStatement(query, new AssertionError(), "SELECT", "rank", "over", "partition", "by", "customer", "LIMIT");
    }

    @Test
    public void testGroupByWithComplexGroupingOperations()
    {
        // Hetu SQL grammar select#group-by-clause
        LOGGER.info("Testing Group By  Clause with Complex Grouping Operations");
        String tableCustomer = TestingHanaServer.getActualTable("customer");
        String tableOrders = TestingHanaServer.getActualTable("orders");
        @Language("SQL")
        String query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY rollup(name, address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "name", "address", "name", "()");

        query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY GROUPING SETS (name, address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "((", "address", "name", "))");

        query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY cube(name, address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "((", "address", "name", "))");

        query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY all cube(name, address), rollup(name, address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "((", "address", "name", "))");

        query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY name, rollup(name, address)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "((", "address", "name", "))");

        // group by with having clause
        query = "SELECT name, address, sum(acctbal) FROM " + tableCustomer + " GROUP BY name, rollup(name, address) having sum(acctbal) > 1000 order by sum(acctbal)";
        assertStatement(query, "SELECT", "sum", "acctbal", "GROUP", "BY", "GROUPING", "SETS", "((", "address", "name", "))");

        // group by clause with window function
        query = "select name, acctbal, sum(acctbal) over(partition by name order by name rows 2 preceding) as ranking, sum(acctbal) from " + tableCustomer + " group by cube(name, acctbal) having sum(acctbal) > 1000 order by name";
        assertStatement(query, "SELECT", "sum", "acctbal", "partition", "by", "ORDER BY", "ROWS", "2", "PRECEDING", "customer", "GROUP", "BY", "GROUPING", "SETS", "((", "name", "acctbal", "name", "()))", "where", "sum", ">", "1E3", "ORDER BY");

        // join with group by complex grouping operations
        query = "select o.custkey, sum(o.totalprice) from " + tableOrders + " o, " + tableCustomer + " c where c.custkey = o.custkey group by rollup( o.custkey, o.totalprice)";
        assertStatement(query, "SELECT", "sum", "totalprice", "orders", "INNER JOIN", "customer", "GROUP", "BY", "GROUPING", "SETS", ",", ",", ",");
    }

    @Test
    public void testJoinStatements()
    {
        LOGGER.info("Testing join statements");
        @Language("SQL") String query = "SELECT c.name FROM customer c LEFT JOIN orders o ON c.custkey=o.custkey";
        assertStatement(query, "SELECT", "FROM", "customer", "LEFT JOIN", "orders", "ON", "table0.custkey = table1.custkey_0");
    }

    @Test
    public void testAggregationStatements()
    {
        LOGGER.info("Testing aggregation statements");
        String tableCustomer = TestingHanaServer.getActualTable("customer");
        String tableOrders = TestingHanaServer.getActualTable("orders");
        String tableLineitem = TestingHanaServer.getActualTable("lineitem");

        /*@Language("SQL") String query = "SELECT * FROM " +
          "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
          "       customer c JOIN orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey) t1 " +
          "   LEFT JOIN lineitem l ON substr(cast(t1.orderkey AS VARCHAR), 0, 2)=cast(t1.orderkey AS VARCHAR) LIMIT 20";*/
        @Language("SQL") String query = "SELECT * FROM " +
                "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
                "       " + tableCustomer + " c JOIN " + tableOrders + " o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey) t1 " +
                "   LEFT JOIN " + tableLineitem + " l ON substr(cast(t1.orderkey AS VARCHAR), 0, 2)=cast(t1.orderkey AS VARCHAR) LIMIT 20";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "GROUP BY", "LEFT JOIN", "lineitem", "LIMIT 20");

        /*query = "SELECT * FROM " + "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
          "       customer c join orders o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey HAVING orderkey>100) t1 "
          +
          "   LEFT JOIN lineitem l ON substr(cast(t1.orderkey  AS VARCHAR), 0, 2)=cast(t1.orderkey  AS VARCHAR) LIMIT 10"; */
        query = "SELECT * FROM " +
                "   (SELECT max(totalprice) AS price, o.orderkey AS orderkey FROM " +
                "       " + tableCustomer + " c join " + tableOrders + " o ON c.custkey=o.custkey GROUP BY orderpriority, orderkey HAVING orderkey>100) t1 " +
                "   LEFT JOIN " + tableLineitem + " l ON substr(cast(t1.orderkey  AS VARCHAR), 0, 2)=cast(t1.orderkey  AS VARCHAR) LIMIT 10";
        assertStatement(query, "SELECT", "FROM", "customer", "INNER JOIN", "orders", "WHERE", ">", "100", "GROUP BY", "LEFT JOIN", "lineitem", "LIMIT 10");
    }

    @Test
    public void testTpchSql3()
    {
        LOGGER.info("Testing TPCH Sql 3");
        // @Language("SQL") String query = "SELECT l.orderkey, sum(l.extendedprice * (1 - l.discount)) AS revenue, o.orderdate, o.shippriority FROM customer c, orders o, lineitem l WHERE c.mktsegment = 'BUILDING' and c.custkey = o.custkey and l.orderkey = o.orderkey and o.orderdate < date '1995-03-22' and l.shipdate > date '1995-03-22' GROUP BY l.orderkey, o.orderdate, o.shippriority ORDER BY revenue desc, o.orderdate LIMIT 10";
        @Language("SQL") String query = "SELECT l.orderkey, sum(l.extendedprice * (1 - l.discount)) AS revenue, o.orderdate, o.shippriority FROM " +
                TestingHanaServer.getActualTable("customer") + " c, " +
                TestingHanaServer.getActualTable("orders") + " o, " +
                TestingHanaServer.getActualTable("lineitem") + " l " +
                "WHERE c.mktsegment = 'BUILDING' and c.custkey = o.custkey and l.orderkey = o.orderkey and o.orderdate < date '1995-03-22' and l.shipdate > date '1995-03-22' GROUP BY l.orderkey, o.orderdate, o.shippriority ORDER BY revenue desc, o.orderdate LIMIT 10";
        assertStatement(query, "sum", "FROM", "customer", "INNER JOIN", "orders", "INNER JOIN", "lineitem", "GROUP BY", "ORDER BY", "desc");
    }

    @Test
    public void testTpchSql5()
    {
        LOGGER.info("Testing TPCH Sql 5");
        // @Language("SQL") String query = "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) AS revenue FROM customer c, orders o, lineitem l, supplier s, nation n, region r WHERE c.custkey = o.custkey and l.orderkey = o.orderkey and l.suppkey = s.suppkey and c.nationkey = s.nationkey and s.nationkey = n.nationkey and n.regionkey = r.regionkey and r.name = 'AFRICA' and o.orderdate >= date '1993-01-01' and o.orderdate < date '1994-01-01' GROUP BY n.name ORDER BY revenue desc";
        @Language("SQL") String query = "SELECT n.name, sum(l.extendedprice * (1 - l.discount)) AS revenue FROM " +
                TestingHanaServer.getActualTable("customer") + " c, " +
                TestingHanaServer.getActualTable("orders") + " o, " +
                TestingHanaServer.getActualTable("lineitem") + " l, " +
                TestingHanaServer.getActualTable("supplier") + " s, " +
                TestingHanaServer.getActualTable("nation") + " n, " +
                TestingHanaServer.getActualTable("region") + " r " +
                "WHERE c.custkey = o.custkey and l.orderkey = o.orderkey and l.suppkey = s.suppkey and c.nationkey = s.nationkey and s.nationkey = n.nationkey and n.regionkey = r.regionkey and r.name = 'AFRICA' and o.orderdate >= date '1993-01-01' and o.orderdate < date '1994-01-01' GROUP BY n.name ORDER BY revenue desc";
        assertStatement(query, "sum", "FROM", "customer", "INNER JOIN", "orders", "INNER JOIN", "lineitem", "INNER JOIN", "supplier", "INNER JOIN", "nation", "INNER JOIN", "region", "GROUP BY", "ORDER BY", "desc");
    }

    private static class HanaJdbcConnectorFactory
            implements ConnectorFactory
    {
        private final JdbcClient jdbcClient;

        private final String name;

        private HanaJdbcConnectorFactory(JdbcClient jdbcClient, String name)
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
            return new Connector() {
                @Override
                public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
                {
                    return new ConnectorTransactionHandle() {
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

    @Override
    @Test
    public void testFunctionCallAndTryExpression()
    {
        LOGGER.info("Testing function call and try expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        FunctionCall functionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("test"),
                Optional.empty(),
                Optional.of(new InPredicate(new SymbolReference("age"), array(literals))),
                Optional.empty(),
                true, literals);
        TryExpression tryExpression = new TryExpression(functionCall);
        assertExpression(functionCall, "test(DISTINCT 10, 20, 30) FILTER (WHERE (age IN ARRAY[10,20,30]))", new UnsupportedOperationException("Hana Connector does not support filter"));
        assertExpression(tryExpression, "TRY(test(DISTINCT 10, 20, 30) FILTER (WHERE (age IN ARRAY[10,20,30])))", new UnsupportedOperationException("Hana Connector does not support filter"));
    }

    @Override
    @Test
    public void testMiscellaneousExpression()
    {
        LOGGER.info("Testing HeTu miscellaneous expressions");
        assertExpression(new GroupingOperation(Optional.empty(), ImmutableList.of(QualifiedName.of("a"), QualifiedName.of("b"))), "GROUPING (a, b)");
        assertExpression(new DereferenceExpression(new SymbolReference("b"), identifier("x")), "b.x", new UnsupportedOperationException("Hana Connector does not support dereference expression"));
        assertExpression(new Window(ImmutableList.of(new SymbolReference("a")), Optional.empty(), Optional.empty()), "(PARTITION BY a)");
    }

    @Override
    @Test
    public void testMiscellaneousLiteralExpression()
    {
        LOGGER.info("Testing HeTu miscellaneous literal expressions");

        //time literal invoke by HanaSqlQueryWriter.timeLiteral(just for functional coverage)
        assertExpression(new TimeLiteral("12:10:59"), "time'12:10:59'");
        assertExpression(new TimeLiteral("03:04:05"), "time'03:04:05'");

        //time literal first handle by optimizer and end invoke by HanaSqlQueryWriter.functionCall(realword implement)
        long epochTime = DateTimeUtils.parseTimeLiteral("12:12:59.999");
        List<Expression> timefunCallParamliterals = list(longLiteral(String.valueOf(epochTime)));
        FunctionCall timeFunctionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("$literal$time"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true, timefunCallParamliterals);
        assertExpression(timeFunctionCall, "time'12:12:59.999'");

        //timestamp literal invoke by HanaSqlQueryWriter.timestampLiteral(just for functional coverage)
        assertExpression(new TimestampLiteral("2011-05-10 23:12:59.999"), "timestamp'2011-05-10 23:12:59.999'");

        //parseTimestampLiteral will encodeing with HeTu epoch time ms.
        long hetuTimeStampWchicagoTZ = DateTimeUtils.parseTimestampLiteral("2011-05-10 10:12:59.999 America/Chicago");
        long epochTimeStampWchicagoTZ = DateTimeEncoding.unpackMillisUtc(hetuTimeStampWchicagoTZ);
        LOGGER.info("America/Chicago zoneKey：" + DateTimeEncoding.unpackZoneKey(hetuTimeStampWchicagoTZ));
        assertEquals(TimeZoneKey.getTimeZoneKey("America/Chicago"), DateTimeEncoding.unpackZoneKey(hetuTimeStampWchicagoTZ));

        List<Expression> timeStampWtzfunCallParamliterals = list(longLiteral(String.valueOf(epochTimeStampWchicagoTZ)));
        FunctionCall timestampWtzFunctionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("$literal$timestamp"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true, timeStampWtzfunCallParamliterals);
        assertExpression(timestampWtzFunctionCall, "timestamp'2011-05-10 15:12:59.999'");

        //timestamp literal first handle by optimizer and end invoke by HanaSqlQueryWriter.functionCall(realword implement)
        long epochTimeStampWutcTZ = DateTimeUtils.parseTimestampLiteral("2011-05-10 23:12:59.999");
        List<Expression> timeStampfunCallParamliterals = list(longLiteral(String.valueOf(epochTimeStampWutcTZ)));
        FunctionCall timestampFunctionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("$literal$timestamp"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true, timeStampfunCallParamliterals);
        assertExpression(timestampFunctionCall, "timestamp'2011-05-10 23:12:59.999'");
        assertExpression(new IntervalLiteral("33", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.empty()), "INTERVAL '33' DAY", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("33", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.of(IntervalLiteral.IntervalField.SECOND)), "INTERVAL '33' DAY TO SECOND", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new CharLiteral("abc"), "CHAR 'abc'");
    }

    @Override
    @Test
    public void testPredicateExpression()
    {
        LOGGER.info("Testing predicate expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        assertExpression(new InPredicate(new SymbolReference("age"), array(literals)), "(age IN ARRAY(10, 20, 30))");
        assertExpression(new InListExpression(literals), "(10, 20, 30)");
        assertExpression(new IsNullPredicate(new SymbolReference("age")), "(age IS NULL)");
        assertExpression(new IsNotNullPredicate(new SymbolReference("age")), "(age IS NOT NULL)");
        assertExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3")), "(1 BETWEEN 2 AND 3)");
        assertExpression(new NotExpression(new BetweenPredicate(longLiteral("1"), longLiteral("2"), longLiteral("3"))), "(NOT (1 BETWEEN 2 AND 3))");
    }

    @Test
    public void testArithmeticBinary()
    {
        LOGGER.info("Testing ArithmeticBinary expressions");
        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, negative(longLiteral("23")), longLiteral("2")), "(-23 + 2)");
        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.SUBTRACT, longLiteral("233"), longLiteral("2")), longLiteral("3")), "((233 - 2) - 3)");
        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE, new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.DIVIDE, longLiteral("1"), longLiteral("233")), longLiteral("3")), "((1 / 233) / 3)");
        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, longLiteral("1"), new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, longLiteral("2"), longLiteral("233"))), "(1 + (2 * 233))");
        assertExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MODULUS, longLiteral("233"), new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.MULTIPLY, longLiteral("2"), longLiteral("3"))), "MOD(233, (2 * 3))");
    }

    @Override
    @Test
    public void testArrayExpression()
    {
        LOGGER.info("Testing ArrayConstructor expressions");
        assertExpression(array(list()), "ARRAY()");
        assertExpression(array(list(longLiteral("1"), longLiteral("233"))), "ARRAY(1, 233)");
        assertExpression(array(list(doubleLiteral("1.0"), doubleLiteral("233.5"))), "ARRAY(1E0, 2.335E2)");
        assertExpression(array(list(stringLiteral("hi233"))), "ARRAY('hi233')");
        assertExpression(array(list(stringLiteral("hi233"), stringLiteral("hello world"))), "ARRAY('hi233', 'hello world')");
    }

    @Test
    public void testSubscriptExpression()
    {
        assertExpression(new SubscriptExpression(array(list(longLiteral("1"), longLiteral("233"))), longLiteral("1")), "MEMBER_AT(ARRAY(1, 233), 1)");
    }

    @Override
    @Test
    public void testAtTimeZoneExpression()
    {
        LOGGER.info("Testing at timezone expression");
        assertExpression(new AtTimeZone(stringLiteral("2012-10-31 01:00 UTC"), stringLiteral("Asia/Shanghai")), "'2012-10-31 01:00 UTC' AT TIME ZONE 'Asia/Shanghai'", new UnsupportedOperationException("Hana Connector does not support at time zone"));
    }

    @Test
    public void testBinaryLiteral()
    {
        LOGGER.info("Testing binary literal expressions");
        assertExpression(new BinaryLiteral(""), "X''");
        assertExpression(new BinaryLiteral("abcdef1234567890ABCDEF"), "X'ABCDEF1234567890ABCDEF'");
    }

    @Test
    public void testCurrentPathExpression()
    {
        LOGGER.info("Testing HeTu current path expression");
        assertExpression(new CurrentPath(new NodeLocation(0, 0)), "CURRENT_PATH", new UnsupportedOperationException("Hana Connector does not support current path"));
    }

    @Test
    public void testCurrentTimestampExpression()
    {
        LOGGER.info("Testing HeTu current timestamp expression");
        assertExpression(new CurrentTime(CurrentTime.Function.TIME, 2), "current_time(2)", new UnsupportedOperationException("Hana Connector does not support current time"));
    }

    @Test
    public void testCurrentUserExpression()
    {
        LOGGER.info("Testing HeTu current user expression");
        assertExpression(new CurrentUser(new NodeLocation(0, 0)), "CURRENT_USER", new UnsupportedOperationException("Hana Connector does not support current user"));
    }

    @Test
    public void testDereferenceExpression()
    {
        LOGGER.info("Testing Dereference Expression expression");
        assertExpression(new DereferenceExpression(new SymbolReference("b"), identifier("x")), "b.x", new UnsupportedOperationException("Hana Connector does not support dereference expression"));
    }

    @Test
    public void testExistsAndSubqueryExpression()
    {
        LOGGER.info("Testing exists expression");
        // TODO test sub query Expression independently
        assertExpression(new SubqueryExpression(simpleQuery(selectList(new LongLiteral("1")))), "(SELECT 1\n" + "\n" + ")");
        assertExpression(new ExistsPredicate(new SubqueryExpression(simpleQuery(selectList(new LongLiteral("1"))))), "(EXISTS (SELECT 1\n" + "\n" + "))");
    }

    @Override
    @Test
    public void testIfExpression()
    {
        LOGGER.info("Testing if and nullif expressions");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("1"), longLiteral("0")), "CASE WHEN true THEN 1 ELSE 0 END");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("3"), new NullLiteral()), "CASE WHEN true THEN 3 ELSE null END");
        assertExpression(new IfExpression(new BooleanLiteral("false"), new NullLiteral(), longLiteral("4")), "CASE WHEN false THEN null ELSE 4 END");
        assertExpression(new IfExpression(new BooleanLiteral("false"), new NullLiteral(), new NullLiteral()), "CASE WHEN false THEN null ELSE null END");
        assertExpression(new IfExpression(new BooleanLiteral("true"), longLiteral("3"), null), "CASE WHEN true THEN 3 END");
        // TODO: VERIFY THE NULLIF
    }

    @Override
    @Test
    public void testIntervalLiteralExpression()
    {
        LOGGER.info("Testing interval literal expressions");
        assertExpression(new IntervalLiteral("1234", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.YEAR), "INTERVAL '1234' YEAR", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("123-4", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.YEAR, Optional.of(IntervalLiteral.IntervalField.MONTH)), "INTERVAL '123-4' YEAR TO MONTH", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("4", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.MONTH), "INTERVAL '4' MONTH", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("12", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY), "INTERVAL '12' DAY", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("1234 23:58:53.456", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.DAY, Optional.of(IntervalLiteral.IntervalField.SECOND)), "INTERVAL '1234 23:58:53.456' DAY TO SECOND", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("12", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.HOUR), "INTERVAL '12' HOUR", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("00:59", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.HOUR, Optional.of(IntervalLiteral.IntervalField.MINUTE)), "INTERVAL '00:59' HOUR TO MINUTE", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("59", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.MINUTE), "INTERVAL '59' MINUTE", new UnsupportedOperationException("Hana Connector does not support interval literal"));
        assertExpression(new IntervalLiteral("59", IntervalLiteral.Sign.POSITIVE, IntervalLiteral.IntervalField.SECOND), "INTERVAL '59' SECOND", new UnsupportedOperationException("Hana Connector does not support interval literal"));
    }

    @Override
    @Test
    public void testLambdaExpression()
    {
        // TODO: test identifier independently
        LOGGER.info("Testing Lambda Argument Declaration Expression");
        assertExpression(new LambdaExpression(list(), identifier("x1")), "() -> x1", new UnsupportedOperationException("Hana Connector does not support lambda expression"));
        assertExpression(new LambdaExpression(list(new LambdaArgumentDeclaration(identifier("x1"))), new FunctionCall(QualifiedName.of("sin"), list(identifier("x1")))), "(x1) -> sin(x1)", new UnsupportedOperationException("Hana Connector does not support lambda argument declaration"));
        assertExpression(new LambdaExpression(list(new LambdaArgumentDeclaration(identifier("x1")), new LambdaArgumentDeclaration(identifier("y1"))), new FunctionCall(QualifiedName.of("mod"), list(identifier("x1"), identifier("y1")))), "(x1, y1) -> mod(x1, y1)", new UnsupportedOperationException("Hana Connector does not support lambda argument declaration"));
        assertExpression(new LambdaArgumentDeclaration(identifier("x1")), "", new UnsupportedOperationException("Hana Connector does not support lambda argument declaration"));
    }

    @Override
    @Test
    public void testParameterExpression()
    {
        LOGGER.info("Testing parameter expressions");
        Optional<List<Expression>> params = Optional.of(list(new SymbolReference("tpch.tiny.item"), longLiteral("1")));
        assertExpression(new Parameter(0), "tpch.tiny.item", params);
        assertExpression(new Parameter(1), "1", params);
        assertExpression(new Parameter(2), "?");
    }

    @Override
    @Test
    public void testLambdaStatement()
    {
        LOGGER.info("Testing lambda in a statement");
        @Language("SQL") String query = "SELECT filter(split(comment, ' '), x -> length(x) > 2) FROM customer LIMIT 10";
        assertStatement(query, new AssertionError("Failed to rewrite the query "), "SELECT", "filter", "split", "comment", "' '", "expr", "->", "length", ">", "2", "LIMIT 10");
    }

    @Override
    @Test
    public void testExtractStatement()
    {
        LOGGER.info("Testing extract statement");
        @Language("SQL") String queryYear1 = "SELECT extract(YEAR FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryMonth1 = "SELECT extract(MONTH FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryDay1 = "SELECT extract(DAY FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryHour1 = "SELECT extract(HOUR FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryMinute1 = "SELECT extract(MINUTE FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String querySecond1 = "SELECT extract(SECOND FROM orderdate) AS year FROM orders LIMIT 10";

        assertStatement(queryYear1, "SELECT", "year", "orderdate", "FROM", "orders", "LIMIT 10");
        assertStatement(queryMonth1, "SELECT", "month", "orderdate", "FROM", "orders", "LIMIT 10");
        assertStatement(queryDay1, "SELECT", "day", "orderdate", "FROM", "orders", "LIMIT 10");
        assertStatement(queryHour1, "SELECT", "hour", "orderdate", "FROM", "orders", "LIMIT 10");
        assertStatement(queryMinute1, "SELECT", "minute", "orderdate", "FROM", "orders", "LIMIT 10");
        assertStatement(querySecond1, "SELECT", "second", "orderdate", "FROM", "orders", "LIMIT 10");

        @Language("SQL") String queryYear2 = "select year(cast(web_rec_start_date as date)) as year from web_site order by year limit 2";
        @Language("SQL") String queryMonth2 = "select month(cast(web_rec_start_date as date)) as month from web_site order by month limit 2";
        @Language("SQL") String queryDay2 = "select day(cast(web_rec_start_date as date)) as day from web_site order by day limit 2";
        @Language("SQL") String queryHour2 = "select hour(cast(web_rec_start_date as date)) as hour from web_site order by hour limit 2";
        @Language("SQL") String queryMinute2 = "select minute(cast(web_rec_start_date as date)) as minute from web_site order by minute limit 2";
        @Language("SQL") String querySecond2 = "select second(cast(web_rec_start_date as date)) as second from web_site order by second limit 2";

        assertStatement(queryYear2, "SELECT", "year", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryMonth2, "SELECT", "month", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryDay2, "SELECT", "day", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryHour2, "SELECT", "hour", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryMinute2, "SELECT", "minute", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(querySecond2, "SELECT", "second", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");

        @Language("SQL") String queryYear11 = "SELECT extract(YEAR_OF_WEEK FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryMonth12 = "SELECT extract(DAY_OF_MONTH FROM orderdate) AS year FROM orders LIMIT 10";
        @Language("SQL") String queryYear21 = "select YEAR_OF_WEEK(cast(web_rec_start_date as date)) as year from web_site order by year limit 2";
        @Language("SQL") String queryMonth22 = "select DAY_OF_MONTH(cast(web_rec_start_date as date)) as month from web_site order by month limit 2";
        assertStatement(queryYear21, new AssertionError(), "SELECT", "YEAR_OF_WEEK", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryMonth22, new AssertionError(), "SELECT", "DAY_OF_MONTH", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryYear11, new AssertionError(), "SELECT", "YEAR_OF_WEEK", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
        assertStatement(queryMonth12, new AssertionError(), "SELECT", "DAY_OF_MONTH", "web_rec_start_date", "FROM", "web_site", "LIMIT 2");
    }
    // TODO: testFilter

    @Test
    public void testRowExpression()
    {
        assertExpression(row(longLiteral("1")), "row(1)", new UnsupportedOperationException("Hana Connector does not support row"));
        assertExpression(row(longLiteral("1"), longLiteral("1")), "row(1, 1)", new UnsupportedOperationException("Hana Connector does not support row"));
    }

    @Test
    public void testBindExpression()
    {
        assertExpression(new BindExpression(list(new StringLiteral("value")), new StringLiteral("targetFunction")), "$INTERNAL$BIND(value, targetFunction)", new UnsupportedOperationException("Hana Connector does not support bind expression"));
    }

    @Test
    public void testTryExpression()
    {
        LOGGER.info("Testing function call and try expressions");
        List<Expression> literals = list(longLiteral("10"), longLiteral("20"), longLiteral("30"));
        FunctionCall functionCall = new FunctionCall(Optional.empty(),
                QualifiedName.of("test"),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                true, literals);
        TryExpression tryExpression = new TryExpression(functionCall);
        assertExpression(functionCall, "test(DISTINCT 10, 20, 30)",
                new UnsupportedOperationException("Hana Connector does not support function call of test"));
        assertExpression(tryExpression, "TRY(test(DISTINCT 10, 20, 30))", new UnsupportedOperationException("Hana Connector does not support function call of test"));
    }

    @Test
    public void testAggregationWithOrderByExpression()
    {
        // ignore this functioncall support TODO: add new ut case
        assertEquals(true, true);
    }

    @Test
    public void testDecimalLiteralExpression()
    {
        LOGGER.info("Testing HeTu decimal literal expressions");
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

    @Test
    public void testGenericLiteralExpression()
    {
        LOGGER.info("Testing Hana Connector generic literal expressions");
        assertExpression(new GenericLiteral("VARCHAR", "abc"), "'abc'");
        assertExpression(new GenericLiteral("CHAR", "abc"), "'abc'");
        assertExpression(new GenericLiteral("BIGINT", "abc"), "abc");
        assertExpression(new GenericLiteral("SMALLINT", "abc"), "abc");
        assertExpression(new GenericLiteral("TINYINT", "abc"), "abc");
        assertExpression(new GenericLiteral("REAL", "abc"), "abc");
        assertExpression(new GenericLiteral("INTEGER", "abc"), "abc");
        assertExpression(new GenericLiteral("DOUBLE", "3141592"), "3.141592E6");
        assertExpression(new GenericLiteral("BOOLEAN", "true"), "true");
        assertExpression(new GenericLiteral("DECIMAL", "3.141592"), "'3.141592'");
        assertExpression(new GenericLiteral("DATE", "abc"), "DATE 'abc'");
    }

    @Test
    public void testVarbinaryLiteralExpression()
    {
        LOGGER.info("Testing HeTu varbinary literal expressions");
        assertExpression(new FunctionCall(QualifiedName.of("from_base64"), list(stringLiteral("c2VsZWN0"))), "73656C656374");
        assertExpression(new FunctionCall(QualifiedName.of("$literal$varbinary"),
                list(stringLiteral("73656C656374"))), "X'73656C656374'");
        assertExpression(new FunctionCall(QualifiedName.of("$literal$varbinary"),
                list(new FunctionCall(QualifiedName.of("from_base64"), list(stringLiteral("c2VsZWN0"))))), "X'73656C656374'");
    }

    @Test
    public void testArrayConstructorExpression()
    {
        LOGGER.info("Testing HeTu array constructor expressions");
        assertExpression(new FunctionCall(QualifiedName.of("array_constructor"), list()), "ARRAY()");
        assertExpression(new FunctionCall(QualifiedName.of("array_constructor"), list(longLiteral("1"), longLiteral("233"))), "ARRAY(1, 233)");
        assertExpression(new FunctionCall(QualifiedName.of("array_constructor"), list(doubleLiteral("1.0"), doubleLiteral("233.5"))), "ARRAY(1E0, 2.335E2)");
        assertExpression(new FunctionCall(QualifiedName.of("array_constructor"), list(stringLiteral("hi233"))), "ARRAY('hi233')");
        assertExpression(new FunctionCall(QualifiedName.of("array_constructor"), list(stringLiteral("hi233"), stringLiteral("hello world"))), "ARRAY('hi233', 'hello world')");
    }

    @Test
    public void testConfigFunctionCallDefault()
    {
        LOGGER.info("Testing config function call rewrite");

        Map<String, String> propertiesMap = UdfFunctionRewriteConstants.DEFAULT_VERSION_UDF_REWRITE_PATTERNS;
        // config functions
        for (Map.Entry<String, String> entry : propertiesMap.entrySet()) {
            String key = entry.getKey();
            String regex = "\\(.*\\)";
            int argsCount = StringUtils.countMatches(key, "$");
            String functionName = key.replaceAll(regex, "");
            List<String> funcNameList = new ArrayList<>(Collections.emptyList());
            funcNameList.add(functionName);
            List<Expression> argsListExp = new ArrayList<>();
            List<String> argsListStr = new ArrayList<>();
            for (int i = 0; i < argsCount; i++) {
                argsListExp.add(stringLiteral("arg" + i));
                argsListStr.add("arg" + i);
            }
            LOGGER.info(functionName + " " + argsListStr.toString());
            FunctionCallArgsPackage functionCallArgsPackage = new FunctionCallArgsPackage(new io.prestosql.spi.sql.expression.QualifiedName(funcNameList), false, argsListStr, Optional.empty(), Optional.empty(), Optional.empty());
            String propertyName = ConfigFunctionParser.baseFunctionArgsToConfigPropertyName(functionCallArgsPackage);
            String rewriteResult = ConfigFunctionParser.baseConfigPropertyValueToFunctionPushDownString(functionCallArgsPackage, propertiesMap.get(propertyName));
            LOGGER.info(rewriteResult);
            if (rewriteResult == null) {
                throw new AssertionError("found null from FunctionCallRewriteUtil");
            }
            for (int i = 0; i < argsCount; i++) {
                String argsC = "arg" + i;
                rewriteResult = rewriteResult.replace(argsC, "'" + argsC + "'");
            }
            assertExpression(new FunctionCall(QualifiedName.of(functionName), argsListExp), rewriteResult);
        }
    }

    @Test
    public void testDataAddFunctions()
    {
        LOGGER.info("Testing Data Add Function call rewrite");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("second"), longLiteral("233"), stringLiteral("date233"))), "ADD_SECONDS('date233', 233)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("minute"), longLiteral("233"), stringLiteral("date233"))), "ADD_SECONDS('date233', 233 * 60)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("hour"), longLiteral("233"), stringLiteral("date233"))), "ADD_SECONDS('date233', 233 * 3600)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("day"), longLiteral("233"), stringLiteral("date233"))), "ADD_DAYS('date233', 233)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("week"), longLiteral("233"), stringLiteral("date233"))), "ADD_DAYS('date233', 233 * 7)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("month"), longLiteral("233"), stringLiteral("date233"))), "ADD_MONTHS('date233', 233)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("quarter"), longLiteral("233"), stringLiteral("date233"))), "ADD_MONTHS('date233', 233 * 3)");
        assertExpression(new FunctionCall(QualifiedName.of("date_add"), list(stringLiteral("year"), longLiteral("233"), stringLiteral("date233"))), "ADD_YEARS('date233', 233)");
    }
}
