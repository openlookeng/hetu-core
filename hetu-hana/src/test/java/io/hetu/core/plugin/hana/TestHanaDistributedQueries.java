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
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.sql.tree.ExplainType;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueries;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.hetu.core.plugin.hana.HanaQueryRunner.createHanaQueryRunner;
import static io.hetu.core.plugin.hana.TestHanaSqlUtil.getHandledSql;
import static io.hetu.core.plugin.hana.TestingHanaServer.getActualTable;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.UnknownType.UNKNOWN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.tree.ExplainType.Type.LOGICAL;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.DELETE_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.INSERT_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

/**
 * This is testing HanaDstributedQueries constructing functions
 *
 * @since 2019-07-11
 */
public class TestHanaDistributedQueries
        extends AbstractTestQueries
{
    private static final Logger LOGGER = Logger.get(TestHanaDistributedQueries.class);

    private TestingHanaServer hanaServer = TestingHanaServer.getInstance();

    /**
     * This is testing createHanaQueryRunner constructor
     *
     */
    public TestHanaDistributedQueries()
    {
        this(TestingHanaServer.getInstance());
    }

    public TestHanaDistributedQueries(TestingHanaServer server)
    {
        super(() -> createHanaQueryRunner(server));
        this.hanaServer = server;
    }

    @AfterClass(alwaysRun = true)
    public void destroy() throws SQLException
    {
        if (this.hanaServer.isHanaServerAvailable()) {
            TestingHanaServer.shutDown();
        }
    }

    protected void assertQuery(@Language("SQL") String sql)
    {
        String newSql = getHandledSql(sql);
        super.assertQuery(newSql, sql);
    }

    /*
     * remove testcast: SELECT CAST(totalprice AS BIGINT) FROM orders
     * because of precision problem.
     * */
    @Override
    public void testCast()
    {
        assertQuery("SELECT CAST('1' AS BIGINT)");
        assertQuery("SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT CAST(orderkey AS VARCHAR) FROM orders");

        assertQuery("SELECT try_cast('1' AS BIGINT)", "SELECT CAST('1' AS BIGINT)");
        assertQuery("SELECT try_cast(totalprice AS BIGINT) FROM orders", "SELECT CAST(totalprice AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS DOUBLE) FROM orders", "SELECT CAST(orderkey AS DOUBLE) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(orderkey AS BOOLEAN) FROM orders", "SELECT CAST(orderkey AS BOOLEAN) FROM orders");

        assertQuery("SELECT try_cast('foo' AS BIGINT)", "SELECT CAST(null AS BIGINT)");
        assertQuery("SELECT try_cast(clerk AS BIGINT) FROM orders", "SELECT CAST(null AS BIGINT) FROM orders");
        assertQuery("SELECT try_cast(orderkey * orderkey AS VARCHAR) FROM orders", "SELECT CAST(orderkey * orderkey AS VARCHAR) FROM orders");
        assertQuery("SELECT try_cast(try_cast(orderkey AS VARCHAR) AS BIGINT) FROM orders", "SELECT orderkey FROM orders");
        assertQuery("SELECT try_cast(clerk AS VARCHAR) || try_cast(clerk AS VARCHAR) FROM orders", "SELECT clerk || clerk FROM orders");

        assertQuery("SELECT coalesce(try_cast('foo' AS BIGINT), 456)", "SELECT 456");
        assertQuery("SELECT coalesce(try_cast(clerk AS BIGINT), 456) FROM orders", "SELECT 456 FROM orders");

        assertQuery("SELECT CAST(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
        assertQuery("SELECT try_cast(x AS BIGINT) FROM (VALUES 1, 2, 3, NULL) t (x)", "VALUES 1, 2, 3, NULL");
    }

    /*
     * remove this testcast because of precision problem.
     * CAST(totalprice AS BIGINT)
     * */
    @Override
    public void testGroupByKeyPredicatePushdown()
    {
    }

    /*
     * remove this testcast because of precision problem.
     * CAST(totalprice * 100 AS BIGINT)
     * */
    @Override
    public void testLimitWithAggregation()
    {
    }

    @Test
    public void testAccessControl()
    {
        String ordersTable = getActualTable("orders");
        String nationTable = getActualTable("nation");
        String regionTable = getActualTable("region");

        assertAccessDenied("INSERT INTO orders SELECT * FROM orders".replace("orders", ordersTable), "Cannot insert into table .*.orders.*", privilege(ordersTable, INSERT_TABLE));
        assertAccessDenied("DELETE FROM orders".replace("orders", ordersTable), "Cannot delete from table .*.orders.*", privilege(ordersTable, DELETE_TABLE));
        assertAccessDenied("CREATE TABLE foo AS SELECT * FROM orders".replace("orders", ordersTable), "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("SELECT * FROM nation".replace("nation", nationTable), "Cannot select from columns \\[nationkey, regionkey, name, comment\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT * FROM (SELECT * FROM nation)".replace("nation", nationTable), "Cannot select from columns \\[nationkey, regionkey, name, comment\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT name FROM (SELECT * FROM nation)".replace("nation", nationTable), "Cannot select from columns \\[nationkey, regionkey, name, comment\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name FROM nation".replace("nation", nationTable), privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(name) as c FROM " + nationTable + " where comment > 'abc' GROUP BY regionkey having max(nationkey) > 10", "Cannot select from columns \\[nationkey, regionkey, name, comment\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT 1 FROM " + regionTable + ", " + nationTable + " where " + regionTable + ".regionkey = " + nationTable + ".nationkey", "Cannot select from columns \\[nationkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessDenied("SELECT count(*) FROM nation".replace("nation", nationTable), "Cannot select from columns \\[\\] in table .*.nation.*", privilege(nationTable, SELECT_COLUMN));
        assertAccessDenied("WITH t1 AS (SELECT * FROM nation) SELECT * FROM t1".replace("nation", nationTable), "Cannot select from columns \\[nationkey, regionkey, name, comment\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
        assertAccessAllowed("SELECT name AS my_alias FROM nation".replace("nation", nationTable), privilege("my_alias", SELECT_COLUMN));
        assertAccessAllowed("SELECT my_alias from (SELECT name AS my_alias FROM nation)".replace("nation", nationTable), privilege("my_alias", SELECT_COLUMN));
        assertAccessDenied("SELECT name AS my_alias FROM nation".replace("nation", nationTable), "Cannot select from columns \\[name\\] in table .*.nation.*", privilege("name", SELECT_COLUMN));
        assertAccessDenied("SELECT n1.nationkey, n2.regionkey FROM " + nationTable + " n1, " + nationTable + " n2", "Cannot select from columns \\[nationkey, regionkey\\] in table .*.nation.*", privilege("nationkey", SELECT_COLUMN));
    }

    @Test
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        String subqueryReturnedTooManyRows = "Scalar sub-query has returned multiple rows";

        assertQuery("SELECT (SELECT 1 WHERE a = 2) FROM (VALUES 1) t(a)", "SELECT null");
        assertQuery("SELECT (SELECT 2 WHERE a = 1) FROM (VALUES 1) t(a)", "SELECT 2");
        assertQueryFails(
                "SELECT (SELECT 2 FROM (VALUES 3, 4) WHERE a = 1) FROM (VALUES 1) t(a)",
                subqueryReturnedTooManyRows);

        // multiple subquery output projections
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 1 = (SELECT 1 FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);

        // correlation used in subquery output
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT n.name FROM region WHERE regionkey > n.regionkey)",
                "line .*: Given correlated subquery is not supported");

        assertQuery(
                "SELECT (SELECT 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "VALUES 2, null, null, null, null");
        // outputs plain correlated orderkey symbol which causes ambiguity with outer query orderkey symbol
        assertQueryFails(
                "SELECT (SELECT o.orderkey WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "line .*: Given correlated subquery is not supported");
        assertQueryFails(
                "SELECT (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "line .*: Given correlated subquery is not supported");
        // correlation used outside the subquery
        assertQueryFails(
                "SELECT o.orderkey, (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "line .*: Given correlated subquery is not supported");

        // correlation in predicate
        assertQuery("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey = n.regionkey)");

        // same correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.regionkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                "line .*: Given correlated subquery is not supported");

        // different correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.nationkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                "line .*: Given correlated subquery is not supported");

        // correlation used in subrelation
        assertQuery(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT regionkey * 2 FROM (SELECT regionkey FROM region r WHERE n.regionkey = r.regionkey)) > 6 " +
                        "ORDER BY 1 LIMIT 3",
                "VALUES 4, 10, 11"); // h2 didn't make it

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES 'ARGENTINA', 'ARGENTINA', 'BRAZIL', 'CANADA'"); // h2 didn't make it

        // returning null when nothing matched
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 31) t(a)",
                "VALUES null");

        String nationTable = getActualTable("nation");
        String regionTable = getActualTable("region");
        String actualQuery = "SELECT (SELECT r.name FROM " + nationTable + " n, " + regionTable + " r WHERE r.regionkey = n.regionkey AND n.nationkey = a) FROM (VALUES 1) t(a)";
        assertQuery(actualQuery,
                "VALUES 'AMERICA'");
    }

    @Test
    public void testDescribeInput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT ? FROM " + getActualTable("nation") + " WHERE nationkey = ? and name < ?")
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "unknown")
                .row(1, "bigint")
                .row(2, "varchar")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeInputNoParameters()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM " + getActualTable("nation"))
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, UNKNOWN, UNKNOWN).build();
        assertEquals(actual, expected);
    }

    @Test
    public void testDescribeInputWithAggregation()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT count(*) + ? FROM " + getActualTable("nation"))
                .build();
        MaterializedResult actual = computeActual(session, "DESCRIBE INPUT my_query");
        MaterializedResult expected = resultBuilder(session, BIGINT, VARCHAR)
                .row(0, "bigint")
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutput()
    {
        String nationTable = getActualTable("nation");
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM " + nationTable)
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), nationTable, "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), nationTable, "varchar(25)", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), nationTable, "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), nationTable, "varchar(152)", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        String nationTable = getActualTable("nation");
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM " + nationTable)
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), nationTable, "varchar(25)", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), nationTable, "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testDescribeOutputNonSelect()
    {
        String nationTable = getActualTable("nation");
        String ordersTable = getActualTable("orders");

        assertDescribeOutputRowCount("CREATE TABLE foo AS SELECT * FROM nation".replace("nation", nationTable));
        assertDescribeOutputRowCount("DELETE FROM orders".replace("orders", ordersTable));

        assertDescribeOutputEmpty("CALL foo()");
        assertDescribeOutputEmpty("SET SESSION optimize_hash_generation=false");
        assertDescribeOutputEmpty("RESET SESSION optimize_hash_generation");
        assertDescribeOutputEmpty("START TRANSACTION");
        assertDescribeOutputEmpty("COMMIT");
        assertDescribeOutputEmpty("ROLLBACK");
        assertDescribeOutputEmpty("GRANT INSERT ON foo TO bar");
        assertDescribeOutputEmpty("REVOKE INSERT ON foo FROM bar");
        assertDescribeOutputEmpty("CREATE SCHEMA foo");
        assertDescribeOutputEmpty("ALTER SCHEMA foo RENAME TO bar");
        assertDescribeOutputEmpty("DROP SCHEMA foo");
        assertDescribeOutputEmpty("CREATE TABLE foo (x bigint)");
        assertDescribeOutputEmpty("ALTER TABLE foo ADD COLUMN y bigint");
        assertDescribeOutputEmpty("ALTER TABLE foo RENAME TO bar");
        assertDescribeOutputEmpty("DROP TABLE foo");
        assertDescribeOutputEmpty("CREATE VIEW foo AS SELECT * FROM nation".replace("nation", nationTable));
        assertDescribeOutputEmpty("DROP VIEW foo");
        assertDescribeOutputEmpty("PREPARE test FROM SELECT * FROM orders".replace("orders", ordersTable));
        assertDescribeOutputEmpty("EXECUTE test");
        assertDescribeOutputEmpty("DEALLOCATE PREPARE test");
    }

    @Test
    public void testDescribeOutputOnAliasedColumnsAndExpressions()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT count(*) AS this_is_aliased, 1 + 2 FROM " + getActualTable("nation"))
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("this_is_aliased", "", "", "", "bigint", 8, true)
                .row("_col1", "", "", "", "integer", 4, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    @Test
    public void testExecuteUsingWithSubquery()
    {
        String query = "SELECT ? in (SELECT orderkey FROM " + getActualTable("orders") + ")";
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();

        assertQuery(session,
                "EXECUTE my_query USING 10",
                "SELECT 10 in (SELECT orderkey FROM orders)");
    }

    @Test
    public void testExplainExecute()
    {
        String query = "SELECT * FROM " + getActualTable("orders");
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", query)
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan(query, LOGICAL));
    }

    @Test
    public void testExplainExecuteWithUsing()
    {
        String ordersTable = getActualTable("orders");
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM " + ordersTable + " WHERE orderkey < ?")
                .build();
        MaterializedResult result = computeActual(session, "EXPLAIN (TYPE LOGICAL) EXECUTE my_query USING 7");
        assertEquals(getOnlyElement(result.getOnlyColumnAsSet()), getExplainPlan("SELECT * FROM " + ordersTable + " WHERE orderkey < 7", LOGICAL));
    }

    protected String getExplainPlan(String query, ExplainType.Type planType)
    {
        String newQuery = getNewExplainQuery(query);
        return super.getExplainPlan(newQuery, planType);
    }

    protected String getGraphvizExplainPlan(String query, ExplainType.Type planType)
    {
        String newQuery = getNewExplainQuery(query);
        return super.getGraphvizExplainPlan(newQuery, planType);
    }

    private String getNewExplainQuery(String query)
    {
        String newQuery = query;
        int beginIndex = query.indexOf("FROM");
        if (beginIndex != -1) {
            beginIndex = beginIndex + "FROM".length() + 1;
            String table = query.substring(beginIndex);
            newQuery = query.substring(0, beginIndex) + getActualTable(table);
        }

        return newQuery;
    }

    @Test
    public void testInformationSchemaFiltering()
    {
        String ordersTable = getActualTable("orders");
        String customerTable = getActualTable("customer");
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = '" + ordersTable + "' LIMIT 1",
                "SELECT '" + ordersTable + "' table_name");
        assertQuery(
                "SELECT table_name FROM information_schema.columns WHERE data_type = 'bigint' AND table_name = '" + customerTable + "' and column_name = 'custkey' LIMIT 1",
                "SELECT '" + customerTable + "' table_name");
    }

    @Test
    public void testInvalidColumn()
    {
        assertQueryFails(
                "SELECT * FROM lineitem l JOIN (SELECT orderkey_1, custkey FROM orders) o on l.orderkey = o.orderkey_1",
                "line 1:72: Column 'orderkey_1' cannot be resolved");
    }

    @Test
    public void testQuotedIdentifiers()
    {
        String expectedQuery = "SELECT \"TOTALPRICE\" \"my price\" FROM \"ORDERS\"";
        assertQuery(expectedQuery.replace("ORDERS", getActualTable("orders").toUpperCase()), expectedQuery);
    }

    @Test
    public void testShowTables()
    {
        Set<String> expectedTables = getExpectedTables();

        MaterializedResult result = computeActual("SHOW TABLES");
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));
    }

    @Test
    public void testShowTablesFrom()
    {
        Set<String> expectedTables = getExpectedTables();

        String catalog = getSession().getCatalog().get();
        String schema = getSession().getSchema().get();

        MaterializedResult result = computeActual("SHOW TABLES FROM " + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        result = computeActual("SHOW TABLES FROM " + catalog + "." + schema);
        assertTrue(result.getOnlyColumnAsSet().containsAll(expectedTables));

        assertQueryFails("SHOW TABLES FROM UNKNOWN", "line 1:1: Schema 'unknown' does not exist");
        assertQueryFails("SHOW TABLES FROM UNKNOWNCATALOG.UNKNOWNSCHEMA", "line 1:1: Catalog 'unknowncatalog' does not exist");
    }

    private Set<String> getExpectedTables()
    {
        Set<String> expectedTables = new HashSet<>();

        for (TpchTable<?> table : TpchTable.getTables()) {
            expectedTables.add(getActualTable(table.getTableName()));
        }

        return expectedTables;
    }

    @Test
    public void testShowTablesLike()
    {
        assertThat(computeActual("SHOW TABLES LIKE 'or%'").getOnlyColumnAsSet())
                .contains(getActualTable("orders"))
                .allMatch(tableName -> ((String) tableName).startsWith("or"));
    }
}
