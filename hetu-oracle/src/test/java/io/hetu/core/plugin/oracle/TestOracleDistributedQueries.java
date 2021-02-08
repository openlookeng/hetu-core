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
package io.hetu.core.plugin.oracle;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.testing.Assertions;
import io.airlift.tpch.TpchTable;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestDistributedQueries;
import io.prestosql.tests.DistributedQueryRunner;
import io.prestosql.tests.ResultWithQueryId;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.function.Supplier;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static io.hetu.core.plugin.oracle.OracleQueryRunner.createOracleQueryRunner;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.TestingSession.TESTING_CATALOG;
import static io.prestosql.tests.QueryAssertions.assertContains;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * TestOracleDistributedQueries
 *
 * @since 2019-07-08
 */
public class TestOracleDistributedQueries
        extends AbstractTestDistributedQueries
{
    private static final int NUMBER_0 = 0;
    private static final int NUMBER_2 = 2;
    private static final int NUMBER_4 = 4;
    private static final int NUMBER_5 = 5;
    private static final int NUMBER_50 = 50;
    private static final int NUMBER_100 = 100;
    private static final int NUMBER_5000 = 5000;

    private static final long LONG_NUMBER_10 = 10L;
    private static final long LONG_NUMBER_25 = 25L;

    private static final double DOU_NUMBER_33POINT3 = 33.3d;

    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is "
            + "not supported";

    private final TestingOracleServer oracleServer;

    /**
     * constructor
     */
    public TestOracleDistributedQueries()
    {
        this(new TestingOracleServer());
    }

    /**
     * constructor
     *
     * @param oracleServer oracleServer
     */
    public TestOracleDistributedQueries(TestingOracleServer oracleServer)
    {
        super(() -> createOracleQueryRunner(oracleServer, ImmutableMap.of(), TpchTable.getTables()));
        this.oracleServer = oracleServer;
    }

    /**
     * assertUntilTimeout
     *
     * @param assertion assertion
     * @param timeout timeout
     */
    private static void assertUntilTimeout(Runnable assertion, Duration timeout)
    {
        long start = System.nanoTime();
        while (!currentThread().isInterrupted()) {
            try {
                assertion.run();
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(NUMBER_50, MILLISECONDS);
        }
    }

    /**
     * testShowCreateView
     */
    public void testShowCreateView()
    {
        // view is not supported
    }

    /**
     * destroy
     */
    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        oracleServer.close();
    }

    /**
     * testLargeIn
     */
    @Override
    public void testLargeIn()
    {
        String longValues = range(NUMBER_0, NUMBER_5000)
                .mapToObj(Integer::toString)
                .collect(joining(", "));
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (" + longValues + ")");

        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (mod(1000, orderkey), " + longValues + ")");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey NOT IN (mod(1000, orderkey), " + longValues + ")");

        String arrayValues = range(NUMBER_0, NUMBER_5000)
                .mapToObj(i -> format("ARRAY[%s, %s, %s]", i, i + 1, i + NUMBER_2))
                .collect(joining(", "));
        assertQuery("SELECT ARRAY[0, 0, 0] in (ARRAY[0, 0, 0], " + arrayValues + ")", "values true");
        assertQuery("SELECT ARRAY[0, 0, 0] in (" + arrayValues + ")", "values false");
    }

    /**
     * testCommentTable
     */
    public void testCommentTable()
    {
        // commnet is not supported
    }

    /**
     * testInsertArray
     */
    public void testInsertArray()
    {
        // array is not supported
    }

    /**
     * testView
     */
    public void testView()
    {
        // view is not supported
    }

    /**
     * testViewCaseSensitivity
     */
    public void testViewCaseSensitivity()
    {
        // view is not supported
    }

    /**
     * testCompatibleTypeChangeForView
     */
    public void testCompatibleTypeChangeForView()
    {
        // view is not supported
    }

    /**
     * testCompatibleTypeChangeForView2
     */
    public void testCompatibleTypeChangeForView2()
    {
        // view is not supported
    }

    /**
     * testViewMetadata
     */
    public void testViewMetadata()
    {
        // view is not supported
    }

    /**
     * testDelete
     */
    @Override
    public void testDelete()
    {
        // delete is not supported
    }

    @Test
    public void testCreateTable()
    {
        // for now oracle do not support NCLOB(4000, 0) with jdbc type 2011
        // when create a table, the hetu type 'varchar' will be translate into oracle type 'NCLOB(4000, 0)'
        // we reconstruct the related test case in io.prestosql.tests.AbstractTestDistributedQueries.testCreateTable
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c char)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertQueryFails("CREATE TABLE test_create (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertUpdate("CREATE TABLE test_create_table_if_not_exists (a bigint, b char, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exists (d bigint, e char)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));

        // Test CREATE TABLE LIKE
        assertUpdate("CREATE TABLE test_create_original (a bigint, b double, c char)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_original"));
        assertTableColumnNames("test_create_original", "a", "b", "c");

        assertUpdate("CREATE TABLE test_create_like (LIKE test_create_original, d boolean, e char)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_like"));
        assertTableColumnNames("test_create_like", "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE test_create_original");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));

        assertUpdate("DROP TABLE test_create_like");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_like"));
    }

    /**
     * testSetSession
     */
    @Override
    public void testSetSession()
    {
        MaterializedResult result = computeActual("SET SESSION test_string = 'bar'");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("test_string", "bar"));

        result = computeActual(format("SET SESSION %s.connector_long = 999", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "999"));

        result = computeActual(format("SET SESSION %s.connector_string = 'baz'", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_string", "baz"));

        result = computeActual(format("SET SESSION %s.connector_string = 'ban' || 'ana'", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_string", "banana"));

        result = computeActual(format("SET SESSION %s.connector_long = 444", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "444"));

        result = computeActual(format("SET SESSION %s.connector_long = 111 + 111", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_long", "222"));

        result = computeActual(format("SET SESSION %s.connector_boolean = 111 < 3", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_boolean", "false"));

        result = computeActual(format("SET SESSION %s.connector_double = 11.1", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of(TESTING_CATALOG + ".connector_double", "11.1"));
    }

    /**
     * testResetSession
     */
    @Override
    public void testResetSession()
    {
        MaterializedResult result = computeActual(getSession(), "RESET SESSION test_string");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of("test_string"));

        result = computeActual(getSession(), format("RESET SESSION %s.connector_string", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of(TESTING_CATALOG + ".connector_string"));
    }

    /**
     * testCreateTableAsSelect
     */
    @Override
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) "
                + "FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");

        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderkey FROM orders ORDER BY orderkey LIMIT 10",
                "SELECT 10");

        assertCreateTableAsSelect(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT 1");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
    }

    /**
     * testExplainAnalyze
     */
    @Override
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM ("
                        + "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN ("
                        + "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b"
                        + ".orderdate");
        assertExplainAnalyze(""
                + "EXPLAIN ANALYZE SELECT *, o2.custkey" + System.lineSeparator()
                + "  IN (" + System.lineSeparator()
                + "    SELECT orderkey" + System.lineSeparator()
                + "    FROM lineitem" + System.lineSeparator()
                + "    WHERE orderkey % 5 = 0)" + System.lineSeparator()
                + "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1" + System.lineSeparator()
                + "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2" + System.lineSeparator()
                + "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN "
                + "(SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))" + System.lineSeparator()
                + "WHERE o1.orderkey" + System.lineSeparator()
                + "  IN (" + System.lineSeparator()
                + "    SELECT orderkey" + System.lineSeparator()
                + "    FROM lineitem" + System.lineSeparator()
                + "    WHERE orderkey % 4 = 0)" + System.lineSeparator()
                + "ORDER BY o1.orderkey" + System.lineSeparator()
                + "  IN (" + System.lineSeparator()
                + "    SELECT orderkey" + System.lineSeparator()
                + "    FROM lineitem" + System.lineSeparator()
                + "    WHERE orderkey % 7 = 0)");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum"
                + "(orderkey), clerk FROM orders GROUP BY clerk");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    /**
     * testExplainAnalyzeVerbose
     */
    @Override
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) "
                + "FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) "
                + "FROM orders WHERE orderkey < 0");
    }

    /**
     * testRenameTable
     */
    @Override
    public void testRenameTable()
    {
        // result is not equal : expected [123] but found [123]
    }

    /**
     * testRenameColumn
     */
    @Override
    public void testRenameColumn()
    {
        // result is not equal : expected [123] but found [123]
    }

    /**
     * testDropColumn
     */
    @Override
    public void testDropColumn()
    {
        assertUpdate("CREATE TABLE test_drop_column AS SELECT 123 x, 111 a", 1);

        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN x");
        assertQueryFails("SELECT x FROM test_drop_column", ".* Column 'x' cannot be resolved");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN a", ".* Cannot drop the only column in a table");
    }

    /**
     * testApproxSetBigint
     */
    @Override
    public void testApproxSetBigint()
    {
        // result is not equal : [1002] != [1014] expected [[1002]] but found [[1014]]
    }

    /**
     * testApproxSetBigintGroupBy
     */
    @Override
    public void testApproxSetBigintGroupBy()
    {
        // result is not equal
    }

    /**
     * testApproxSetGroupByWithOnlyNullsInOneGroup
     */
    @Override
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // result is not equal
    }

    /**
     * testApproxSetWithNulls
     */
    @Override
    public void testApproxSetWithNulls()
    {
        // result is not equal
    }

    /**
     * testCustomAdd
     */
    @Override
    public void testCustomAdd()
    {
        // execute query failed
    }

    /**
     * testCustomSum
     */
    @Override
    public void testCustomSum()
    {
        // execute query failed
    }

    /**
     * testAggregationOverUnknown
     */
    @Override
    public void testAggregationOverUnknown()
    {
        // Execute query failed
    }

    /**
     * testDescribeInput
     */
    @Override
    public void testDescribeInput()
    {
        // result is not equal
    }

    /**
     * testDescribeOutput
     */
    @Override
    public void testDescribeOutput()
    {
        // result is not equal
    }

    /**
     * testDescribeOutputNamedAndUnnamed
     */
    @Override
    public void testDescribeOutputNamedAndUnnamed()
    {
        // result is not equal
    }

    /**
     * testInformationSchemaFiltering
     */
    @Override
    public void testInformationSchemaFiltering()
    {
        assertQuery(
                "SELECT table_name FROM information_schema.tables WHERE table_name = 'orders' LIMIT 1",
                "SELECT 'orders' table_name");
    }

    /**
     * testMergeEmptyNonEmptyApproxSet
     */
    @Override
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // Unexpected parameters
    }

    /**
     * testMergeHyperLogLog
     */
    @Override
    public void testMergeHyperLogLog()
    {
        // Unexpected parameters
    }

    /**
     * testMergeHyperLogLogGroupBy
     */
    @Override
    public void testMergeHyperLogLogGroupBy()
    {
        // Unexpected parameters
    }

    /**
     * testMergeHyperLogLogGroupByWithNulls
     */
    @Override
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // Unexpected parameters
    }

    /**
     * testMergeHyperLogLogWithNulls
     */
    @Override
    public void testMergeHyperLogLogWithNulls()
    {
        // Unexpected parameters
    }

    /**
     * testP4ApproxSetBigint
     */
    @Override
    public void testP4ApproxSetBigint()
    {
        // result is not equal
    }

    /**
     * testP4ApproxSetBigintGroupBy
     */
    @Override
    public void testP4ApproxSetBigintGroupBy()
    {
        // result is not equal
    }

    /**
     * testP4ApproxSetGroupByWithNulls
     */
    @Override
    public void testP4ApproxSetGroupByWithNulls()
    {
        // result is not equal
    }

    /**
     * testP4ApproxSetGroupByWithOnlyNullsInOneGroup
     */
    @Override
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        // result is not equal
    }

    /**
     * testP4ApproxSetWithNulls
     */
    @Override
    public void testP4ApproxSetWithNulls()
    {
        // result is not equal
    }

    /**
     * testShowColumns
     */
    @Override
    public void testShowColumns()
    {
        // result is not equal
    }

    /**
     * testAddColumn
     */
    @Override
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column "
                + "'q'");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(NUMBER_2));
        assertNull(materializedRows.getMaterializedRows().get(1).getField(NUMBER_2));
        assertEquals(materializedRows.getMaterializedRows().get(NUMBER_2).getField(NUMBER_2), DOU_NUMBER_33POINT3);

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
    }

    /**
     * testInsert
     */
    @Override
    public void testInsert()
    {
        @Language("SQL")
        String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (null)", 1);
        assertUpdate("INSERT INTO test_insert (orderdate) VALUES (DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO test_insert (totalprice) VALUES (1234)", 1);

        // UNION query produces columns in the opposite order of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) "
                        + "SELECT orderkey, orderdate, totalprice FROM orders "
                        + "UNION ALL "
                        + "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");
    }

    /**
     * testDropTableIfExists
     */
    @Override
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
    }

    /**
     * testQueryLoggingCount
     */
    @Override
    public void testQueryLoggingCount()
    {
        QueryManager queryManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertUntilTimeout(
                    () -> assertEquals(
                            queryManager.getQueries().stream().map(BasicQueryInfo::getQueryId)
                                    .map(queryManager::getFullQueryInfo)
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()),
                    new Duration(1, MINUTES));

            DispatchManager dispatchManager =
                    ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount =
                    waitUntilStable(() -> dispatchManager.getStats()
                                    .getCompletedQueries()
                                    .getTotalCount(),
                            new Duration(NUMBER_5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            assertUpdate("CREATE TABLE test_query_logging_count AS SELECT 1 foo_1, 2 foo_2_4", 1);
            assertQuery("SELECT foo_1, foo_2_4 FROM test_query_logging_count", "SELECT 1, 2");
            assertUpdate("DROP TABLE test_query_logging_count");
            assertQueryFails("SELECT * FROM test_query_logging_count", ".*Table .* does not exist");
            assertUntilTimeout(
                    () -> assertEquals(dispatchManager.getStats()
                            .getCompletedQueries()
                            .getTotalCount() - beforeCompletedQueriesCount, NUMBER_4),
                    new Duration(1, MINUTES));
            assertEquals(dispatchManager.getStats()
                    .getSubmittedQueries()
                    .getTotalCount() - beforeSubmittedQueriesCount, NUMBER_4);
        });
    }

    /**
     * waitUntilStable
     *
     * @param computation computation
     * @param timeout timeout
     * @param <T> T
     * @return T
     */
    private <T> T waitUntilStable(Supplier<T> computation, Duration timeout)
    {
        T lastValue = computation.get();
        long start = System.nanoTime();
        while (!currentThread().isInterrupted() && nanosSince(start).compareTo(timeout) < 0) {
            sleepUninterruptibly(NUMBER_100, MILLISECONDS);
            T currentValue = computation.get();
            if (currentValue.equals(lastValue)) {
                return currentValue;
            }
            lastValue = currentValue;
        }
        throw new UncheckedTimeoutException();
    }

    /**
     * testLargeQuerySuccess
     */
    @Override
    public void testLargeQuerySuccess()
    {
        // execute query failed
    }

    /**
     * testShowSchemasFromOther
     */
    @Override
    public void testShowSchemasFromOther()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    /**
     * testTableSampleSystemBoundaryValues
     */
    @Override
    public void testTableSampleSystemBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    /**
     * testSymbolAliasing
     */
    @Override
    public void testSymbolAliasing()
    {
        assertUpdate("CREATE TABLE test_symbol_aliasing AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM test_symbol_aliasing", "SELECT 1, 2");
        assertUpdate("DROP TABLE test_symbol_aliasing");
    }

    /**
     * testNonQueryAccessControl
     */
    @Override
    public void testNonQueryAccessControl()
    {
        skipTestUnless(supportsViews());

        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property " + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo",
                CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*",
                privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*",
                privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders DROP COLUMN foo", "Cannot drop a column from table .*.orders.*",
                privilege("orders", DROP_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*"
                + ".orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo",
                CREATE_VIEW));

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*",
                    privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            Assertions.assertContains(e.getMessage(), "statusCode=403");
        }
    }

    /**
     * testViewAccessControl
     */
    @Override
    public void testViewAccessControl()
    {
        // view is not supported
    }

    /**
     * testWrittenStats
     */
    @Override
    public void testWrittenStats()
    {
        String sql = "CREATE TABLE test_written_stats AS SELECT * FROM nation";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId =
                distributedQueryRunner.executeWithQueryId(getSession(), sql);
        QueryInfo queryInfo =
                distributedQueryRunner.getCoordinator()
                        .getQueryManager()
                        .getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), LONG_NUMBER_25);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        sql = "INSERT INTO test_written_stats SELECT * FROM nation LIMIT 10";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        queryInfo =
                distributedQueryRunner.getCoordinator()
                        .getQueryManager()
                        .getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), LONG_NUMBER_10);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        assertUpdate("DROP TABLE test_written_stats");
    }

    /**
     * TestOracleDistributedQueries
     */
    public void testUnionWithJoin()
    {
        // execute query failed
    }

    /**
     * testUnionWithFilterNotInSelect
     */
    public void testUnionWithFilterNotInSelect()
    {
        // result is not equal
    }

    /**
     * testUnionWithAggregationAndJoin
     */
    public void testUnionWithAggregationAndJoin()
    {
        // result is not equal
    }

    /**
     * testTableQueryOrderLimit
     */
    public void testTableQueryOrderLimit()
    {
        // result is not equal
    }

    /**
     * testTableQueryInUnion
     */
    public void testTableQueryInUnion()
    {
        // result is not equal
    }

    /**
     * testTableAsSubquery
     */
    public void testTableAsSubquery()
    {
        // result is not equal
    }

    /**
     * testScalarSubquery
     */
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = " + System.lineSeparator()
                + "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT * FROM lineitem WHERE " + System.lineSeparator()
                + "(SELECT orderkey FROM orders WHERE 0=1) "
                + "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE "
                + "(SELECT min(orderkey) FROM orders)"
                + "<"
                + "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem "
                + "WHERE orderkey BETWEEN"
                + "   (SELECT avg(orderkey) FROM orders) - 10 "
                + "   AND"
                + "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        assertQuery("SELECT o1.orderkey, COUNT(*) "
                + "FROM orders o1 "
                + "INNER JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 "
                + "ON o1.orderkey "
                + "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 "
                + "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) "
                + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 "
                + "LEFT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 "
                + "ON o1.orderkey "
                + "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 "
                + "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) "
                + "FROM orders o1 RIGHT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 "
                + "ON o1.orderkey "
                + "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 "
                + "GROUP BY o1.orderkey");
        assertQuery("SELECT DISTINCT COUNT(*) "
                        + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 "
                        + "FULL JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 "
                        + "ON o1.orderkey "
                        + "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 "
                        + "GROUP BY o1.orderkey",
                "VALUES 1, 10");
    }

    /**
     * testScalarSubquery2
     */
    @Test
    public void testScalarSubquery2()
    {
        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (" + System.lineSeparator()
                        + "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");

        // cast scalar sub-query
        assertQuery("SELECT 1.0/(SELECT 1), CAST(1.0 AS REAL)/(SELECT 1), 1/(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1) AND 1 = (SELECT 1), 2.0 = (SELECT 1) WHERE 1.0 = (SELECT 1) AND 1 = "
                + "(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1), 2.0 = (SELECT 1), CAST(2.0 AS REAL) = (SELECT 1) WHERE 1.0 = (SELECT 1)");

        // coerce correlated symbols
        assertQuery("SELECT * FROM (VALUES 1) t(a) WHERE 1=(SELECT count(*) WHERE 1.0 = a)", "SELECT 1");
        assertQuery("SELECT * FROM (VALUES 1.0) t(a) WHERE 1=(SELECT count(*) WHERE 1 = a)", "SELECT 1.0");
    }

    /**
     * testRepeatedOutputs2
     */
    public void testRepeatedOutputs2()
    {
        // result is not equal
    }

    /**
     * testReferenceToWithQueryInFromClause
     */
    public void testReferenceToWithQueryInFromClause()
    {
        // result is not equal
    }

    /**
     * testQualifiedWildcardFromAlias
     */
    public void testQualifiedWildcardFromAlias()
    {
        // result is not equal
    }

    /**
     * testQualifiedWildcard
     */
    public void testQualifiedWildcard()
    {
        // result is not equal
    }

    /**
     * testMultipleWildcards
     */
    public void testMultipleWildcards()
    {
        // result is not equal
    }

    /**
     * testMultiColumnUnionAll
     */
    public void testMultiColumnUnionAll()
    {
        // result is not equal
    }

    /**
     * testMixedWildcards
     */
    public void testMixedWildcards()
    {
        // result is not equal
    }

    /**
     * testLimitPushDown
     */
    public void testLimitPushDown()
    {
        // result is not equal
    }

    /**
     * testDistinctHaving
     */
    public void testDistinctHaving()
    {
        // result is not equal
    }

    /**
     * testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere
     */
    public void testCorrelatedScalarSubqueriesWithScalarAggregationAndEqualityPredicatesInWhere()
    {
        assertQuery("SELECT (SELECT count(*) WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM nation n WHERE "
                        + "(SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) > 1");
        assertQueryFails(
                "SELECT count(*) FROM nation n WHERE "
                        + "(SELECT avg(a) FROM (SELECT count(*) FROM region r WHERE n.regionkey = r.regionkey) t(a)) "
                        + "> 1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT count(*) WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, (SELECT count(*) WHERE o.orderkey = 0) "
                        + "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey "
                        + "FROM orders o GROUP BY o.orderkey HAVING 1 = (SELECT count(*) WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o "
                        + "GROUP BY o.orderkey, (SELECT count(*) WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) "
                        + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 "
                        + "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 "
                        + "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 "
                        + "ON NOT 1 = (SELECT count(*) WHERE o1.orderkey = o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE 1 = (SELECT * FROM (SELECT (SELECT count(*) WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");
    }

    /**
     * testCorrelatedScalarSubqueriesWithScalarAggregation
     */
    public void testCorrelatedScalarSubqueriesWithScalarAggregation()
    {
        // projection
        assertQuery(
                "SELECT (SELECT round(3 * avg(i.a)) FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < "
                        + "4) "
                        + "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES null, 4, 4, 5");

        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE (SELECT avg(i.orderkey) FROM orders i "
                        + "WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0) > 100",
                "VALUES 14999");

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o "
                        + "ORDER BY "
                        + "   (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % "
                        + "10000 = 0), "
                        + "   orderkey "
                        + "LIMIT 1",
                "VALUES 1");
        // join
        assertQuery(
                "SELECT count(*) "
                        + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 "
                        + "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 "
                        + "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey AND i"
                        + ".orderkey % 10000 = 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 "
                        + "ON NOT 1 = (SELECT avg(i.orderkey) FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");
    }

    /**
     * testCorrelatedScalarSubqueriesWithScalarAggregation2
     */
    @Test
    public void testCorrelatedScalarSubqueriesWithScalarAggregation2()
    {
        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE 100 < (SELECT * "
                        + "FROM (SELECT (SELECT avg(i.orderkey) FROM orders i WHERE o.orderkey < i.orderkey AND i"
                        + ".orderkey % 10000 = 0)))",
                "VALUES 14999");

        // consecutive correlated subqueries with scalar aggregation
        assertQuery("SELECT "
                + "(SELECT avg(regionkey) "
                + " FROM nation n2"
                + " WHERE n2.nationkey = n1.nationkey),"
                + "(SELECT avg(regionkey)"
                + " FROM nation n3"
                + " WHERE n3.nationkey = n1.nationkey)"
                + "FROM nation n1");
        assertQuery("SELECT"
                + "(SELECT avg(regionkey)"
                + " FROM nation n2 "
                + " WHERE n2.nationkey = n1.nationkey),"
                + "(SELECT avg(regionkey)+1 "
                + " FROM nation n3 "
                + " WHERE n3.nationkey = n1.nationkey)"
                + "FROM nation n1");

        // count in subquery
        assertQuery("SELECT * "
                        + "FROM (VALUES (0),( 1), (2), (7)) AS v1(c1) "
                        + "WHERE v1.c1 > (SELECT count(c1) FROM (VALUES (0),( 1), (2)) AS v2(c1) WHERE v1.c1 = v2.c1)",
                "VALUES (2), (7)");
    }

    /**
     * testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols
     */
    public void testCorrelatedExistsSubqueriesWithPrunedCorrelationSymbols()
    {
        assertQuery("SELECT EXISTS(SELECT o.orderkey) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT o.orderkey)");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT o.orderkey) FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey "
                        + "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT o.orderkey)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey FROM orders o GROUP BY o.orderkey, EXISTS (SELECT o.orderkey)");

        // join
        assertQuery(
                "SELECT * FROM orders o JOIN (SELECT * FROM lineitem ORDER BY orderkey LIMIT 2) l "
                        + "ON NOT EXISTS(SELECT o.orderkey = l.orderkey)");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT o.orderkey)))",
                "VALUES 15000");
    }

    /**
     * testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere
     */
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        assertQuery("SELECT EXISTS(SELECT 1 WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT EXISTS(SELECT null WHERE o.orderkey = 1) FROM orders o");
        assertQuery("SELECT count(*) FROM orders o WHERE EXISTS(SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey)");
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE EXISTS (SELECT avg(l.orderkey) FROM lineitem l WHERE o.orderkey = l.orderkey GROUP BY "
                        + "l.linenumber)");
        assertQueryFails(
                "SELECT count(*) FROM orders o "
                        + "WHERE EXISTS (SELECT count(*) FROM lineitem l WHERE o.orderkey = l.orderkey HAVING count(*) "
                        + "> 3)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // with duplicated rows
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE a = 1) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES true, true, false, false");

        // group by
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey, EXISTS(SELECT 1 WHERE o.orderkey = 0) "
                        + "FROM orders o GROUP BY o.orderkey");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey "
                        + "FROM orders o GROUP BY o.orderkey HAVING EXISTS (SELECT 1 WHERE o.orderkey = 0)");
        assertQuery(
                "SELECT max(o.totalprice), o.orderkey "
                        + "FROM orders o GROUP BY o.orderkey, EXISTS (SELECT 1 WHERE o.orderkey = 0)");

        // join
        assertQuery(
                "SELECT count(*) "
                        + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 "
                        + "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 "
                        + "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 "
                        + "ON NOT EXISTS(SELECT 1 WHERE o1.orderkey = o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey = 0)))",
                "SELECT count(*) FROM orders o WHERE o.orderkey = 0");

        // not exists
        assertQuery(
                "SELECT count(*) FROM customer WHERE NOT EXISTS(SELECT * FROM orders WHERE orders.custkey=customer"
                        + ".custkey)",
                "VALUES 500");
    }

    /**
     * testCorrelatedExistsSubqueries
     */
    public void testCorrelatedExistsSubqueries()
    {
        // projection
        assertQuery(
                "SELECT EXISTS(SELECT 1 FROM (VALUES 1, 1, 1, 2, 2, 3, 4) i(a) WHERE i.a < o.a AND i.a < 4) "
                        + "FROM (VALUES 0, 3, 3, 5) o(a)",
                "VALUES false, true, true, true");
        assertQuery(
                "SELECT EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) "
                        + "FROM lineitem l LIMIT 1");

        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 1000 = "
                        + "0)",
                "VALUES 14999");
        assertQuery(
                "SELECT count(*) FROM lineitem l "
                        + "WHERE EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // order by
        assertQuery(
                "SELECT orderkey FROM orders o ORDER BY "
                        + "EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND i.orderkey % 10000 = 0)"
                        + "LIMIT 1",
                "VALUES 60000");
        assertQuery(
                "SELECT orderkey FROM lineitem l ORDER BY "
                        + "EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");
    }

    /**
     * testCorrelatedExistsSubqueries2
     */
    @Test
    public void testCorrelatedExistsSubqueries2()
    {
        // group by
        assertQuery(
                "SELECT max(l.quantity), l.orderkey, EXISTS(SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3) FROM "
                        + "lineitem l "
                        + "GROUP BY l.orderkey");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l "
                        + "GROUP BY l.orderkey "
                        + "HAVING EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");
        assertQuery(
                "SELECT max(l.quantity), l.orderkey FROM lineitem l "
                        + "GROUP BY l.orderkey, EXISTS (SELECT 1 WHERE l.orderkey > 0 OR l.orderkey != 3)");

        // join
        assertQuery(
                "SELECT count(*) "
                        + "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o1 "
                        + "JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o2 "
                        + "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey AND i.orderkey % 10000 "
                        + "= 0)");
        assertQueryFails(
                "SELECT count(*) FROM orders o1 LEFT JOIN orders o2 "
                        + "ON NOT EXISTS(SELECT 1 FROM orders i WHERE o1.orderkey < o2.orderkey)",
                "line .*: Correlated subquery in given context is not supported");

        // subrelation
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 FROM orders i WHERE o.orderkey < i.orderkey AND"
                        + " i.orderkey % 10000 = 0)))",
                "VALUES 14999");
        assertQuery(
                "SELECT count(*) FROM orders o "
                        + "WHERE (SELECT * FROM (SELECT EXISTS(SELECT 1 WHERE o.orderkey > 10 OR o.orderkey != 3)))",
                "VALUES 14999");
    }

    /**
     * testWithAliased
     */
    public void testWithAliased()
    {
        // result is not equal
    }

    /**
     * testWith
     */
    public void testWith()
    {
        // result is not equal
    }

    /**
     * testWildcard
     */
    public void testWildcard()
    {
        // result is not equal
    }

    /**
     * testUnnest
     */
    public void testUnnest()
    {
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a)", "SELECT 1");
        assertQuery("SELECT x[1] FROM UNNEST(ARRAY[ARRAY[1, 2, 3]]) t(x)", "SELECT 1");
        assertQuery("SELECT x[1][2] FROM UNNEST(ARRAY[ARRAY[ARRAY[1, 2, 3]]]) t(x)", "SELECT 2");
        assertQuery("SELECT x[2] FROM UNNEST(ARRAY[MAP(ARRAY[1,2], ARRAY['hello', 'hi'])]) t(x)", "SELECT 'hi'");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3])", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3]) t(a)", "SELECT * FROM VALUES (1), (2), (3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2], ARRAY[3, 4]) t(a, b)", "SELECT * FROM VALUES (1, 3), (2, 4)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES (1, 4), (2,"
                + " 5), (3, NULL)");
        assertQuery("SELECT a FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 1, 2, 3");
        assertQuery("SELECT b FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b)", "SELECT * FROM VALUES 4, 5, NULL");
        assertQuery("SELECT count(*) FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5])", "SELECT 3");
        assertQuery("SELECT a FROM UNNEST(ARRAY['kittens', 'puppies']) t(a)", "SELECT * FROM VALUES ('kittens'), "
                + "('puppies')");
        assertQuery(""
                        + "SELECT c "
                        + "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) t(a, b) "
                        + "CROSS JOIN (values (8), (9)) t2(c)",
                "SELECT * FROM VALUES 8, 8, 8, 9, 9, 9");
        assertQuery(""
                        + "SELECT a.custkey, t.e "
                        + "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a "
                        + "CROSS JOIN UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), "
                        + "(3))");
        assertQuery(""
                        + "SELECT a.custkey, t.e "
                        + "FROM (SELECT custkey, ARRAY[1, 2, 3] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a, "
                        + "UNNEST(my_array) t(e)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (1), (2), "
                        + "(3))");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1]) CROSS JOIN UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), "
                        + "(1, 1, 1)");
        assertQuery("SELECT * FROM UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1]), UNNEST(ARRAY[0, 1])",
                "SELECT * FROM VALUES (0, 0, 0), (0, 0, 1), (0, 1, 0), (0, 1, 1), (1, 0, 0), (1, 0, 1), (1, 1, 0), "
                        + "(1, 1, 1)");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', 'dog'])) t(a, b)", "SELECT * FROM VALUES "
                + "(1, 'cat'), (2, 'dog')");
        assertQuery("SELECT a, b FROM UNNEST(MAP(ARRAY[1,2], ARRAY['cat', NULL])) t(a, b)", "SELECT * FROM VALUES (1,"
                + " 'cat'), (2, NULL)");
    }

    /**
     * testUnnest2
     */
    @Test
    public void testUnnest2()
    {
        assertQuery("SELECT 1 FROM (VALUES (ARRAY[1])) AS t (a) CROSS JOIN UNNEST(a) WITH ORDINALITY", "SELECT 1");
        assertQuery("SELECT * FROM UNNEST(ARRAY[1, 2, 3]) WITH ORDINALITY", "SELECT * FROM VALUES (1, 1), (2, 2), (3,"
                + " 3)");
        assertQuery("SELECT b FROM UNNEST(ARRAY[10, 20, 30]) WITH ORDINALITY t(a, b)", "SELECT * FROM VALUES (1), (2)"
                + ", (3)");
        assertQuery("SELECT a, b, c FROM UNNEST(ARRAY[10, 20, 30], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c)", "SELECT "
                + "* FROM VALUES (10, 4, 1), (20, 5, 2), (30, NULL, 3)");
        assertQuery("SELECT a, b FROM UNNEST(ARRAY['kittens', 'puppies']) WITH ORDINALITY t(a, b)", "SELECT * FROM "
                + "VALUES ('kittens', 1), ('puppies', 2)");
        assertQuery(""
                        + "SELECT c "
                        + "FROM UNNEST(ARRAY[1, 2, 3], ARRAY[4, 5]) WITH ORDINALITY t(a, b, c) "
                        + "CROSS JOIN (values (8), (9)) t2(d)",
                "SELECT * FROM VALUES 1, 1, 2, 2, 3, 3");
        assertQuery(""
                        + "SELECT a.custkey, t.e, t.f "
                        + "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) a"
                        + " "
                        + "CROSS JOIN UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), "
                        + "(20, 2), (30, 3))");
        assertQuery(""
                        + "SELECT a.custkey, t.e, t.f "
                        + "FROM (SELECT custkey, ARRAY[10, 20, 30] AS my_array FROM orders ORDER BY orderkey LIMIT 1) "
                        + "a, "
                        + "UNNEST(my_array) WITH ORDINALITY t(e, f)",
                "SELECT * FROM (SELECT custkey FROM orders ORDER BY orderkey LIMIT 1) CROSS JOIN (VALUES (10, 1), "
                        + "(20, 2), (30, 3))");

        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
        assertQueryFails(
                "SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true",
                "line .*: UNNEST on other than the right side of CROSS JOIN is not supported");
    }

    @Test
    public void testSynonyms()
    {
        try (TestSynonym synonym = new TestSynonym(oracleServer, oracleServer.getUser() + ".test_synonym", "FOR ORDERS")) {
            assertQueryFails("SELECT orderkey FROM " + synonym.getName(), "line 1:22: Table oracle.* does not exist");
        }
    }

    /**
     * testUnionWithJoinOnNonTranslateableSymbols
     */
    public void testUnionWithJoinOnNonTranslateableSymbols()
    {
        // result is not equal
    }

    /**
     * testComplexCast
     */
    @Override
    public void testComplexCast()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .build();
        assertQuery(session, "WITH t(a, b) AS (VALUES (1, INTERVAL '1' SECOND)) "
                        + "SELECT count(DISTINCT a), CAST(max(b) AS VARCHAR) FROM t",
                "VALUES (1, '0 00:00:01.000')");
    }

    /**
     * testUnion
     */
    public void testUnion()
    {
        assertQuery("SELECT orderkey FROM orders UNION SELECT custkey FROM orders");
        assertQuery("SELECT 123 UNION DISTINCT SELECT 123 UNION ALL SELECT 123");
        assertQuery("SELECT NULL UNION SELECT NULL");
        assertQuery("SELECT 'x', 'y' UNION ALL SELECT name, name FROM nation");

        // mixed single-node vs fixed vs source-distributed
        assertQuery("SELECT orderkey FROM orders UNION ALL SELECT 123 UNION ALL (SELECT custkey FROM orders GROUP BY "
                + "custkey)");
    }

    /**
     * testUnionWithAggregation
     */
    public void testUnionWithAggregation()
    {
        assertQuery(
                "SELECT regionkey, count(*) FROM ("
                        + "   SELECT regionkey FROM nation "
                        + "   UNION ALL "
                        + "   SELECT * FROM (VALUES 2, 100) t(regionkey)) "
                        + "GROUP BY regionkey",
                "SELECT * FROM (VALUES  (0, 5), (1, 5), (2, 6), (3, 5), (4, 5), (100, 1))");

        assertQuery(
                "SELECT clerk, count(DISTINCT orderstatus) FROM ("
                        + "SELECT * FROM orders WHERE orderkey=0 "
                        + " UNION ALL "
                        + "SELECT * FROM orders WHERE orderkey<>0) "
                        + "GROUP BY clerk");
        assertQuery(
                "SELECT count(clerk) FROM ("
                        + "SELECT clerk FROM orders WHERE orderkey=0 "
                        + " UNION ALL "
                        + "SELECT clerk FROM orders WHERE orderkey<>0) "
                        + "GROUP BY clerk");
        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM ("
                        + "    SELECT sum(custkey) sc, orderkey FROM ("
                        + "        SELECT custkey,orderkey, orderkey+1 FROM orders WHERE orderkey=0"
                        + "        UNION ALL "
                        + "        SELECT custkey,orderkey,orderkey+1 FROM orders WHERE orderkey<>0) "
                        + "    GROUP BY orderkey)");

        assertQuery(
                "SELECT count(orderkey), sum(sc) FROM (" + System.lineSeparator()
                        + "    SELECT sum(custkey) sc, orderkey FROM (" + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0"
                        + System.lineSeparator() + "        UNION ALL " + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) "
                        + System.lineSeparator() + "    GROUP BY GROUPING SETS ((orderkey, orderstatus), (orderkey)))",
                "SELECT count(orderkey), sum(sc) FROM (" + System.lineSeparator()
                        + "    SELECT sum(custkey) sc, orderkey FROM (" + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0"
                        + System.lineSeparator() + "        UNION ALL " + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) "
                        + System.lineSeparator() + "    GROUP BY orderkey, orderstatus " + System.lineSeparator()
                        + "    " + System.lineSeparator()
                        + "    UNION ALL " + System.lineSeparator()
                        + "    " + System.lineSeparator()
                        + "    SELECT sum(custkey) sc, orderkey FROM (" + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey=0"
                        + System.lineSeparator() + "        UNION ALL " + System.lineSeparator()
                        + "        SELECT custkey, orderkey, orderkey+1, orderstatus FROM orders WHERE orderkey<>0) "
                        + System.lineSeparator() + "    GROUP BY orderkey)");
    }

    /**
     * testTableQuery
     */
    public void testTableQuery()
    {
        // result is not equal
    }

    /**
     * testCaseInsensitiveAliasedRelation
     */
    public void testCaseInsensitiveAliasedRelation()
    {
        // result is not equal
    }

    /**
     * assertExplainAnalyze
     *
     * @param query the query to test
     */
    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:"
                + ".*, Input:.*, Output\", but it is %s", value));
    }
}
