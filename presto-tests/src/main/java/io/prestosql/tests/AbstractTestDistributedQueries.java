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
package io.prestosql.tests;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.UncheckedTimeoutException;
import io.airlift.testing.Assertions;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.dispatcher.DispatchManager;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManager;
import io.prestosql.server.BasicQueryInfo;
import io.prestosql.spi.security.Identity;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.TestingSession;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_MEMORY;
import static io.prestosql.connector.informationschema.InformationSchemaMetadata.INFORMATION_SCHEMA;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.ADD_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.CREATE_VIEW_WITH_SELECT_COLUMNS;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.DROP_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.RENAME_TABLE;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SELECT_COLUMN;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SET_SESSION;
import static io.prestosql.testing.TestingAccessControlManager.TestingPrivilegeType.SET_USER;
import static io.prestosql.testing.TestingAccessControlManager.privilege;
import static io.prestosql.testing.TestingSession.TESTING_CATALOG;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static io.prestosql.tests.QueryAssertions.assertContains;
import static java.lang.String.format;
import static java.lang.Thread.currentThread;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestDistributedQueries
        extends AbstractTestQueries
{
    protected AbstractTestDistributedQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    protected boolean supportsViews()
    {
        return true;
    }

    protected boolean supportsArrays()
    {
        return true;
    }

    protected boolean supportsPushdown()
    {
        return false;
    }

    @Test
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

    @Test
    public void testResetSession()
    {
        MaterializedResult result = computeActual(getSession(), "RESET SESSION test_string");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of("test_string"));

        result = computeActual(getSession(), format("RESET SESSION %s.connector_string", TESTING_CATALOG));
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getResetSessionProperties(), ImmutableSet.of(TESTING_CATALOG + ".connector_string"));
    }

    @Test
    public void testCreateTable()
    {
        assertUpdate("CREATE TABLE test_create (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create"));
        assertTableColumnNames("test_create", "a", "b", "c");

        assertUpdate("DROP TABLE test_create");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertQueryFails("CREATE TABLE test_create (a bad_type)", ".* Unknown type 'bad_type' for column 'a'");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create"));

        assertUpdate("CREATE TABLE test_create_table_if_not_exists (a bigint, b varchar, c double)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("CREATE TABLE IF NOT EXISTS test_create_table_if_not_exists (d bigint, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));
        assertTableColumnNames("test_create_table_if_not_exists", "a", "b", "c");

        assertUpdate("DROP TABLE test_create_table_if_not_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_table_if_not_exists"));

        // Test CREATE TABLE LIKE
        assertUpdate("CREATE TABLE test_create_original (a bigint, b double, c varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_original"));
        assertTableColumnNames("test_create_original", "a", "b", "c");

        assertUpdate("CREATE TABLE test_create_like (LIKE test_create_original, d boolean, e varchar)");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_create_like"));
        assertTableColumnNames("test_create_like", "a", "b", "c", "d", "e");

        assertUpdate("DROP TABLE test_create_original");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_original"));

        assertUpdate("DROP TABLE test_create_like");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_create_like"));
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");

        // Some connectors support CREATE TABLE AS but not the ordinary CREATE TABLE. Let's test CTAS IF NOT EXISTS with a table that is guaranteed to exist.
        assertUpdate("CREATE TABLE IF NOT EXISTS nation AS SELECT orderkey, discount FROM lineitem", 0);
        assertTableColumnNames("nation", "nationkey", "name", "regionkey", "comment");

        assertCreateTableAsSelect(
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

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
                "test_with_data",
                "SELECT * FROM orders WITH DATA",
                "SELECT * FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT 0");

        // Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
    }

    @Test
    public void testExplainAnalyze()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk");
        assertExplainAnalyze(
                "EXPLAIN ANALYZE SELECT x + y FROM (" +
                        "   SELECT orderdate, COUNT(*) x FROM orders GROUP BY orderdate) a JOIN (" +
                        "   SELECT orderdate, COUNT(*) y FROM orders GROUP BY orderdate) b ON a.orderdate = b.orderdate");
        assertExplainAnalyze("" +
                "EXPLAIN ANALYZE SELECT *, o2.custkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 5 = 0)\n" +
                "FROM (SELECT * FROM orders WHERE custkey % 256 = 0) o1\n" +
                "JOIN (SELECT * FROM orders WHERE custkey % 256 = 0) o2\n" +
                "  ON (o1.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0)) = (o2.orderkey IN (SELECT orderkey FROM lineitem WHERE orderkey % 4 = 0))\n" +
                "WHERE o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 4 = 0)\n" +
                "ORDER BY o1.orderkey\n" +
                "  IN (\n" +
                "    SELECT orderkey\n" +
                "    FROM lineitem\n" +
                "    WHERE orderkey % 7 = 0)");
        assertExplainAnalyze("EXPLAIN ANALYZE SELECT count(*), clerk FROM orders GROUP BY clerk UNION ALL SELECT sum(orderkey), clerk FROM orders GROUP BY clerk");

        assertExplainAnalyze("EXPLAIN ANALYZE SHOW COLUMNS FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE EXPLAIN ANALYZE SELECT count(*) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW FUNCTIONS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW TABLES");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SCHEMAS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW CATALOGS");
        assertExplainAnalyze("EXPLAIN ANALYZE SHOW SESSION");
    }

    @Test
    public void testExplainAnalyzeVerbose()
    {
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT * FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders");
        assertExplainAnalyze("EXPLAIN ANALYZE VERBOSE SELECT rank() OVER (PARTITION BY orderkey ORDER BY clerk DESC) FROM orders WHERE orderkey < 0");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = "EXPLAIN ANALYZE doesn't support statement type: DropTable")
    public void testExplainAnalyzeDDL()
    {
        computeActual("EXPLAIN ANALYZE DROP TABLE orders");
    }

    private void assertExplainAnalyze(@Language("SQL") String query)
    {
        String value = (String) computeActual(query).getOnlyValue();

        assertTrue(value.matches("(?s:.*)CPU:.*, Input:.*, Output(?s:.*)"), format("Expected output to contain \"CPU:.*, Input:.*, Output\", but it is %s", value));

        // TODO: check that rendered plan is as expected, once stats are collected in a consistent way
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, query, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertCreateTableAsSelect(getSession(), table, query, expectedQuery, rowCountQuery);
    }

    protected void assertCreateTableAsSelect(Session session, String table, @Language("SQL") String query, @Language("SQL") String expectedQuery, @Language("SQL") String rowCountQuery)
    {
        assertUpdate(session, "CREATE TABLE " + table + " AS " + query, rowCountQuery);
        assertQuery(session, "SELECT * FROM " + table, expectedQuery);
        assertUpdate(session, "DROP TABLE " + table);

        assertFalse(getQueryRunner().tableExists(session, table));
    }

    @Test
    public void testRenameTable()
    {
        assertUpdate("CREATE TABLE test_rename AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename RENAME TO test_rename_new");
        MaterializedResult materializedRows = computeActual("SELECT x FROM test_rename_new");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        // provide new table name in uppercase
        assertUpdate("ALTER TABLE test_rename_new RENAME TO TEST_RENAME");
        materializedRows = computeActual("SELECT x FROM test_rename");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename");

        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_new"));
    }

    @Test
    public void testCommentTable()
    {
        assertUpdate("CREATE TABLE test_comment(id integer)");

        assertUpdate("COMMENT ON TABLE test_comment IS 'new comment'");
        MaterializedResult materializedRows = computeActual("SHOW CREATE TABLE test_comment");
        assertTrue(materializedRows.getMaterializedRows().get(0).getField(0).toString().contains("COMMENT 'new comment'"));

        assertUpdate("COMMENT ON TABLE test_comment IS ''");
        materializedRows = computeActual("SHOW CREATE TABLE test_comment");
        assertTrue(materializedRows.getMaterializedRows().get(0).getField(0).toString().contains("COMMENT ''"));

        assertUpdate("COMMENT ON TABLE test_comment IS NULL");
        materializedRows = computeActual("SHOW CREATE TABLE test_comment");
        assertFalse(materializedRows.getMaterializedRows().get(0).getField(0).toString().contains("COMMENT"));

        assertUpdate("DROP TABLE test_comment");
    }

    @Test
    public void testRenameColumn()
    {
        assertUpdate("CREATE TABLE test_rename_column AS SELECT 123 x", 1);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN x TO y");
        MaterializedResult materializedRows = computeActual("SELECT y FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("ALTER TABLE test_rename_column RENAME COLUMN y TO Z");
        materializedRows = computeActual("SELECT z FROM test_rename_column");
        assertEquals(getOnlyElement(materializedRows.getMaterializedRows()).getField(0), 123);

        assertUpdate("DROP TABLE test_rename_column");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_rename_column"));
    }

    @Test
    public void testDropColumn()
    {
        assertUpdate("CREATE TABLE test_drop_column AS SELECT 123 x, 111 a", 1);

        assertUpdate("ALTER TABLE test_drop_column DROP COLUMN x");
        assertQueryFails("SELECT x FROM test_drop_column", ".* Column 'x' cannot be resolved");

        assertQueryFails("ALTER TABLE test_drop_column DROP COLUMN a", ".* Cannot drop the only column in a table");
    }

    @Test
    public void testAddColumn()
    {
        assertUpdate("CREATE TABLE test_add_column AS SELECT 123 x", 1);
        assertUpdate("CREATE TABLE test_add_column_a AS SELECT 234 x, 111 a", 1);
        assertUpdate("CREATE TABLE test_add_column_ab AS SELECT 345 x, 222 a, 33.3E0 b", 1);

        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN x bigint", ".* Column 'x' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN X bigint", ".* Column 'X' already exists");
        assertQueryFails("ALTER TABLE test_add_column ADD COLUMN q bad_type", ".* Unknown type 'bad_type' for column 'q'");

        assertUpdate("ALTER TABLE test_add_column ADD COLUMN a bigint");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_a", 1);
        MaterializedResult materializedRows = computeActual("SELECT x, a FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);

        if (supportsPushdown()) {
            Session session1 = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .build();
            MaterializedResult materializedRows1 = computeActual(session1, "SELECT x, a FROM test_add_column ORDER BY x");
            assertEquals(materializedRows1.getMaterializedRows().get(0).getField(0), 123);
            assertNull(materializedRows1.getMaterializedRows().get(0).getField(1));
            assertEquals(materializedRows1.getMaterializedRows().get(1).getField(0), 234);
            assertEquals(materializedRows1.getMaterializedRows().get(1).getField(1), 111L);
        }
        assertUpdate("ALTER TABLE test_add_column ADD COLUMN b double");
        assertUpdate("INSERT INTO test_add_column SELECT * FROM test_add_column_ab", 1);
        materializedRows = computeActual("SELECT x, a, b FROM test_add_column ORDER BY x");
        assertEquals(materializedRows.getMaterializedRows().get(0).getField(0), 123);
        assertNull(materializedRows.getMaterializedRows().get(0).getField(1));
        assertNull(materializedRows.getMaterializedRows().get(0).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(0), 234);
        assertEquals(materializedRows.getMaterializedRows().get(1).getField(1), 111L);
        assertNull(materializedRows.getMaterializedRows().get(1).getField(2));
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(0), 345);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(1), 222L);
        assertEquals(materializedRows.getMaterializedRows().get(2).getField(2), 33.3);

        assertUpdate("DROP TABLE test_add_column");
        assertUpdate("DROP TABLE test_add_column_a");
        assertUpdate("DROP TABLE test_add_column_ab");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_a"));
        assertFalse(getQueryRunner().tableExists(getSession(), "test_add_column_ab"));
    }

    @Test
    public void testInsert()
    {
        @Language("SQL") String query = "SELECT orderdate, orderkey, totalprice FROM orders";

        assertUpdate("CREATE TABLE test_insert AS " + query + " WITH NO DATA", 0);
        assertQuery("SELECT count(*) FROM test_insert", "SELECT 0");

        assertUpdate("INSERT INTO test_insert " + query, "SELECT count(*) FROM orders");

        assertQuery("SELECT * FROM test_insert", query);

        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (-1)", 1);
        assertUpdate("INSERT INTO test_insert (orderkey) VALUES (null)", 1);
        assertUpdate("INSERT INTO test_insert (orderdate) VALUES (DATE '2001-01-01')", 1);
        assertUpdate("INSERT INTO test_insert (orderkey, orderdate) VALUES (-2, DATE '2001-01-02')", 1);
        assertUpdate("INSERT INTO test_insert (orderdate, orderkey) VALUES (DATE '2001-01-03', -3)", 1);
        assertUpdate("INSERT INTO test_insert (totalprice) VALUES (1234)", 1);

        assertQuery("SELECT * FROM test_insert", query
                + " UNION ALL SELECT null, -1, null"
                + " UNION ALL SELECT null, null, null"
                + " UNION ALL SELECT DATE '2001-01-01', null, null"
                + " UNION ALL SELECT DATE '2001-01-02', -2, null"
                + " UNION ALL SELECT DATE '2001-01-03', -3, null"
                + " UNION ALL SELECT null, null, 1234");

        // UNION query produces columns in the opposite order
        // of how they are declared in the table schema
        assertUpdate(
                "INSERT INTO test_insert (orderkey, orderdate, totalprice) " +
                        "SELECT orderkey, orderdate, totalprice FROM orders " +
                        "UNION ALL " +
                        "SELECT orderkey, orderdate, totalprice FROM orders",
                "SELECT 2 * count(*) FROM orders");

        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testInsertArray()
    {
        skipTestUnless(supportsArrays());

        assertUpdate("CREATE TABLE test_insert_array (a ARRAY<DOUBLE>, b ARRAY<BIGINT>)");

        assertUpdate("INSERT INTO test_insert_array (a) VALUES (ARRAY[null])", 1);
        assertUpdate("INSERT INTO test_insert_array (a) VALUES (ARRAY[1234])", 1);
        assertQuery("SELECT a[1] FROM test_insert_array", "VALUES (null), (1234)");

        assertQueryFails("INSERT INTO test_insert_array (b) VALUES (ARRAY[1.23E1])", "Insert query has mismatched column types: .*");

        assertUpdate("DROP TABLE test_insert_array");
    }

    @Test
    public void testDelete()
    {
        // delete half the table, then delete the rest

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE orderkey % 2 = 0", "SELECT count(*) FROM orders WHERE orderkey % 2 = 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderkey % 2 <> 0");

        assertUpdate("DELETE FROM test_delete", "SELECT count(*) FROM orders WHERE orderkey % 2 <> 0");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders LIMIT 0");

        assertUpdate("DROP TABLE test_delete");

        // delete successive parts of the table

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 100", "SELECT count(*) FROM orders WHERE custkey <= 100");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 100");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 300", "SELECT count(*) FROM orders WHERE custkey > 100 AND custkey <= 300");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 300");

        assertUpdate("DELETE FROM test_delete WHERE custkey <= 500", "SELECT count(*) FROM orders WHERE custkey > 300 AND custkey <= 500");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE custkey > 500");

        assertUpdate("DROP TABLE test_delete");

        // delete using a constant property

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate("DELETE FROM test_delete WHERE orderstatus = 'O'", "SELECT count(*) FROM orders WHERE orderstatus = 'O'");
        assertQuery("SELECT * FROM test_delete", "SELECT * FROM orders WHERE orderstatus <> 'O'");

        assertUpdate("DROP TABLE test_delete");

        // delete without matching any rows

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE rand() < 0", 0);
        assertUpdate("DELETE FROM test_delete WHERE orderkey < 0", 0);
        assertUpdate("DROP TABLE test_delete");

        // delete with a predicate that optimizes to false

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE orderkey > 5 AND orderkey < 4", 0);
        assertUpdate("DROP TABLE test_delete");

        // delete using a subquery

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertUpdate(
                "DELETE FROM test_delete WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')",
                "SELECT count(*) FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')");

        assertUpdate("DROP TABLE test_delete");

        // delete with multiple SemiJoin

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM lineitem", "SELECT count(*) FROM lineitem");

        assertUpdate(
                "DELETE FROM test_delete\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                        "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)\n",
                "SELECT count(*) FROM lineitem\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus = 'F')\n" +
                        "  AND orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 = 0)");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM lineitem\n" +
                        "WHERE orderkey IN (SELECT orderkey FROM orders WHERE orderstatus <> 'F')\n" +
                        "  OR orderkey IN (SELECT orderkey FROM orders WHERE custkey % 5 <> 0)");

        assertUpdate("DROP TABLE test_delete");

        // delete with SemiJoin null handling

        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");

        assertUpdate(
                "DELETE FROM test_delete\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n",
                "SELECT count(*) FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NULL\n");
        assertQuery(
                "SELECT * FROM test_delete",
                "SELECT * FROM orders\n" +
                        "WHERE (orderkey IN (SELECT CASE WHEN orderkey % 3 = 0 THEN NULL ELSE orderkey END FROM lineitem)) IS NOT NULL\n");

        assertUpdate("DROP TABLE test_delete");

        // delete using a scalar and EXISTS subquery
        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertUpdate("DELETE FROM test_delete WHERE orderkey = (SELECT orderkey FROM orders ORDER BY orderkey LIMIT 1)", 1);
        assertUpdate("DELETE FROM test_delete WHERE orderkey = (SELECT orderkey FROM orders WHERE false)", 0);
        assertUpdate("DELETE FROM test_delete WHERE EXISTS(SELECT 1 WHERE false)", 0);
        assertUpdate("DELETE FROM test_delete WHERE EXISTS(SELECT 1)", "SELECT count(*) - 1 FROM orders");
        assertUpdate("DROP TABLE test_delete");

        // test EXPLAIN ANALYZE with CTAS
        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT CAST(orderstatus AS VARCHAR(15)) orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        // check that INSERT works also
        assertExplainAnalyze("EXPLAIN ANALYZE INSERT INTO analyze_test SELECT clerk FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders UNION ALL SELECT clerk FROM orders");
        // check DELETE works with EXPLAIN ANALYZE
        assertExplainAnalyze("EXPLAIN ANALYZE DELETE FROM analyze_test WHERE TRUE");
        assertQuery("SELECT COUNT(*) from analyze_test", "SELECT 0");
        assertUpdate("DROP TABLE analyze_test");

        // Test DELETE access control
        assertUpdate("CREATE TABLE test_delete AS SELECT * FROM orders", "SELECT count(*) FROM orders");
        assertAccessDenied("DELETE FROM test_delete where orderkey < 12", "Cannot select from columns \\[orderkey\\] in table or view .*.test_delete.*", privilege("orderkey", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM test_delete where orderkey < 12", privilege("orderdate", SELECT_COLUMN));
        assertAccessAllowed("DELETE FROM test_delete", privilege("orders", SELECT_COLUMN));
    }

    @Test
    public void testDropTableIfExists()
    {
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
        assertUpdate("DROP TABLE IF EXISTS test_drop_if_exists");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop_if_exists"));
    }

    @Test
    public void testView()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT orderkey, orderstatus, totalprice / 2 half FROM orders";

        assertUpdate("CREATE VIEW test_view AS SELECT 123 x");
        assertUpdate("CREATE OR REPLACE VIEW test_view AS " + query);

        assertQuery("SELECT * FROM test_view", query);

        assertQuery(
                "SELECT * FROM test_view a JOIN test_view b on a.orderkey = b.orderkey",
                format("SELECT * FROM (%s) a JOIN (%s) b ON a.orderkey = b.orderkey", query, query));

        assertQuery("WITH orders AS (SELECT * FROM orders LIMIT 0) SELECT * FROM test_view", query);

        String name = format("%s.%s.test_view", getSession().getCatalog().get(), getSession().getSchema().get());
        assertQuery("SELECT * FROM " + name, query);

        assertUpdate("DROP VIEW test_view");
    }

    @Test
    public void testViewCaseSensitivity()
    {
        skipTestUnless(supportsViews());

        computeActual("CREATE VIEW test_view_uppercase AS SELECT X FROM (SELECT 123 X)");
        computeActual("CREATE VIEW test_view_mixedcase AS SELECT XyZ FROM (SELECT 456 XyZ)");
        assertQuery("SELECT * FROM test_view_uppercase", "SELECT X FROM (SELECT 123 X)");
        assertQuery("SELECT * FROM test_view_mixedcase", "SELECT XyZ FROM (SELECT 456 XyZ)");
    }

    @Test
    public void testCompatibleTypeChangeForView()
    {
        skipTestUnless(supportsViews());

        assertUpdate("CREATE TABLE test_table_1 AS SELECT 'abcdefg' a", 1);
        assertUpdate("CREATE VIEW test_view_1 AS SELECT a FROM test_table_1");

        assertQuery("SELECT * FROM test_view_1", "VALUES 'abcdefg'");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE test_table_1");
        assertUpdate("CREATE TABLE test_table_1 AS SELECT 'abc' a", 1);

        assertQuery("SELECT * FROM test_view_1", "VALUES 'abc'");

        assertUpdate("DROP VIEW test_view_1");
        assertUpdate("DROP TABLE test_table_1");
    }

    @Test
    public void testCompatibleTypeChangeForView2()
    {
        skipTestUnless(supportsViews());

        assertUpdate("CREATE TABLE test_table_2 AS SELECT BIGINT '1' v", 1);
        assertUpdate("CREATE VIEW test_view_2 AS SELECT * FROM test_table_2");

        assertQuery("SELECT * FROM test_view_2", "VALUES 1");

        // replace table with a version that's implicitly coercible to the previous one
        assertUpdate("DROP TABLE test_table_2");
        assertUpdate("CREATE TABLE test_table_2 AS SELECT INTEGER '1' v", 1);

        assertQuery("SELECT * FROM test_view_2 WHERE v = 1", "VALUES 1");

        assertUpdate("DROP VIEW test_view_2");
        assertUpdate("DROP TABLE test_table_2");
    }

    @Test
    public void testViewMetadata()
    {
        skipTestUnless(supportsViews());

        @Language("SQL") String query = "SELECT BIGINT '123' x, 'foo' y";
        assertUpdate("CREATE VIEW meta_test_view AS " + query);

        // test INFORMATION_SCHEMA.TABLES
        MaterializedResult actual = computeActual(format(
                "SELECT table_name, table_type FROM information_schema.tables WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("customer", "BASE TABLE")
                .row("lineitem", "BASE TABLE")
                .row("meta_test_view", "VIEW")
                .row("nation", "BASE TABLE")
                .row("orders", "BASE TABLE")
                .row("part", "BASE TABLE")
                .row("partsupp", "BASE TABLE")
                .row("region", "BASE TABLE")
                .row("supplier", "BASE TABLE")
                .build();

        assertContains(actual, expected);

        // test SHOW TABLES
        actual = computeActual("SHOW TABLES");

        MaterializedResult.Builder builder = resultBuilder(getSession(), actual.getTypes());
        for (MaterializedRow row : expected.getMaterializedRows()) {
            builder.row(row.getField(0));
        }
        expected = builder.build();

        assertContains(actual, expected);

        // test INFORMATION_SCHEMA.VIEWS
        actual = computeActual(format(
                "SELECT table_name, view_definition FROM information_schema.views WHERE table_schema = '%s'",
                getSession().getSchema().get()));

        expected = resultBuilder(getSession(), actual.getTypes())
                .row("meta_test_view", formatSqlText(query))
                .build();

        assertContains(actual, expected);

        // test SHOW COLUMNS
        actual = computeActual("SHOW COLUMNS FROM meta_test_view");

        expected = resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("x", "bigint", "", "")
                .row("y", "varchar(3)", "", "")
                .build();

        assertEquals(actual, expected);

        // test SHOW CREATE VIEW
        String expectedSql = formatSqlText(format(
                "CREATE VIEW %s.%s.%s AS %s",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                "meta_test_view",
                query)).trim();

        actual = computeActual("SHOW CREATE VIEW meta_test_view");

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        actual = computeActual(format("SHOW CREATE VIEW %s.%s.meta_test_view", getSession().getCatalog().get(), getSession().getSchema().get()));

        assertEquals(getOnlyElement(actual.getOnlyColumnAsSet()), expectedSql);

        assertUpdate("DROP VIEW meta_test_view");
    }

    @Test
    public void testShowCreateView()
    {
        skipTestUnless(supportsViews());
        checkState(getSession().getCatalog().isPresent(), "catalog is not set");
        checkState(getSession().getSchema().isPresent(), "schema is not set");

        String viewName = "test_show_create_view";
        assertUpdate("DROP VIEW IF EXISTS " + viewName);
        String ddl = format(
                "CREATE VIEW %s.%s.%s AS\n" +
                        "SELECT *\n" +
                        "FROM\n" +
                        "  (\n" +
                        " VALUES \n" +
                        "     ROW (1, 'one')\n" +
                        "   , ROW (2, 't')\n" +
                        ")  t (col1, col2)",
                getSession().getCatalog().get(),
                getSession().getSchema().get(),
                viewName);
        assertUpdate(ddl);

        assertEquals(computeActual("SHOW CREATE VIEW " + viewName).getOnlyValue(), ddl);

        assertUpdate("DROP VIEW " + viewName);
    }

    @Test
    public void testQueryLoggingCount()
    {
        QueryManager queryManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getQueryManager();
        executeExclusively(() -> {
            assertUntilTimeout(
                    () -> assertEquals(
                            queryManager.getQueries().stream()
                                    .map(BasicQueryInfo::getQueryId)
                                    .map(queryManager::getFullQueryInfo)
                                    .filter(info -> !info.isFinalQueryInfo())
                                    .collect(toList()),
                            ImmutableList.of()),
                    new Duration(1, MINUTES));

            // We cannot simply get the number of completed queries as soon as all the queries are completed, because this counter may not be up-to-date at that point.
            // The completed queries counter is updated in a final query info listener, which is called eventually.
            // Therefore, here we wait until the value of this counter gets stable.

            DispatchManager dispatchManager = ((DistributedQueryRunner) getQueryRunner()).getCoordinator().getDispatchManager();
            long beforeCompletedQueriesCount = waitUntilStable(() -> dispatchManager.getStats().getCompletedQueries().getTotalCount(), new Duration(5, SECONDS));
            long beforeSubmittedQueriesCount = dispatchManager.getStats().getSubmittedQueries().getTotalCount();
            assertUpdate("CREATE TABLE test_query_logging_count AS SELECT 1 foo_1, 2 foo_2_4", 1);
            assertQuery("SELECT foo_1, foo_2_4 FROM test_query_logging_count", "SELECT 1, 2");
            assertUpdate("DROP TABLE test_query_logging_count");
            assertQueryFails("SELECT * FROM test_query_logging_count", ".*Table .* does not exist");

            // TODO: Figure out a better way of synchronization
            assertUntilTimeout(
                    () -> assertEquals(dispatchManager.getStats().getCompletedQueries().getTotalCount() - beforeCompletedQueriesCount, 4),
                    new Duration(1, MINUTES));
            assertEquals(dispatchManager.getStats().getSubmittedQueries().getTotalCount() - beforeSubmittedQueriesCount, 4);
        });
    }

    private <T> T waitUntilStable(Supplier<T> computation, Duration timeout)
    {
        T lastValue = computation.get();
        long start = System.nanoTime();
        while (!currentThread().isInterrupted() && nanosSince(start).compareTo(timeout) < 0) {
            sleepUninterruptibly(100, MILLISECONDS);
            T currentValue = computation.get();
            if (currentValue.equals(lastValue)) {
                return currentValue;
            }
            lastValue = currentValue;
        }
        throw new UncheckedTimeoutException();
    }

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
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }

    @Test
    public void testLargeQuerySuccess()
    {
        assertQuery("SELECT " + Joiner.on(" AND ").join(nCopies(500, "1 = 1")), "SELECT true");
    }

    @Test
    public void testShowSchemasFromOther()
    {
        MaterializedResult result = computeActual("SHOW SCHEMAS FROM tpch");
        assertTrue(result.getOnlyColumnAsSet().containsAll(ImmutableSet.of(INFORMATION_SCHEMA, "tiny", "sf1")));
    }

    @Test
    public void testTableSampleSystemBoundaryValues()
    {
        MaterializedResult fullSample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (100)");
        MaterializedResult emptySample = computeActual("SELECT orderkey FROM orders TABLESAMPLE SYSTEM (0)");
        MaterializedResult all = computeActual("SELECT orderkey FROM orders");

        assertContains(all, fullSample);
        assertEquals(emptySample.getMaterializedRows().size(), 0);
    }

    @Test
    public void testSymbolAliasing()
    {
        assertUpdate("CREATE TABLE test_symbol_aliasing AS SELECT 1 foo_1, 2 foo_2_4", 1);
        assertQuery("SELECT foo_1, foo_2_4 FROM test_symbol_aliasing", "SELECT 1, 2");
        assertUpdate("DROP TABLE test_symbol_aliasing");
    }

    @Test
    public void testNonQueryAccessControl()
    {
        skipTestUnless(supportsViews());

        assertAccessDenied("SET SESSION " + QUERY_MAX_MEMORY + " = '10MB'",
                "Cannot set system session property " + QUERY_MAX_MEMORY,
                privilege(QUERY_MAX_MEMORY, SET_SESSION));

        assertAccessDenied("CREATE TABLE foo (pk bigint)", "Cannot create table .*.foo.*", privilege("foo", CREATE_TABLE));
        assertAccessDenied("DROP TABLE orders", "Cannot drop table .*.orders.*", privilege("orders", DROP_TABLE));
        assertAccessDenied("ALTER TABLE orders RENAME TO foo", "Cannot rename table .*.orders.* to .*.foo.*", privilege("orders", RENAME_TABLE));
        assertAccessDenied("ALTER TABLE orders ADD COLUMN foo bigint", "Cannot add a column to table .*.orders.*", privilege("orders", ADD_COLUMN));
        assertAccessDenied("ALTER TABLE orders DROP COLUMN foo", "Cannot drop a column from table .*.orders.*", privilege("orders", DROP_COLUMN));
        assertAccessDenied("ALTER TABLE orders RENAME COLUMN orderkey TO foo", "Cannot rename a column in table .*.orders.*", privilege("orders", RENAME_COLUMN));
        assertAccessDenied("CREATE VIEW foo as SELECT * FROM orders", "Cannot create view .*.foo.*", privilege("foo", CREATE_VIEW));
        // todo add DROP VIEW test... not all connectors have view support

        try {
            assertAccessDenied("SELECT 1", "Principal .* cannot become user " + getSession().getUser() + ".*", privilege(getSession().getUser(), SET_USER));
        }
        catch (AssertionError e) {
            // There is no clean exception message for authorization failure.  We simply get a 403
            Assertions.assertContains(e.getMessage(), "statusCode=403");
        }
    }

    @Test
    public void testViewAccessControl()
    {
        skipTestUnless(supportsViews());

        Session viewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // TEST COLUMN-LEVEL PRIVILEGES
        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_view_access AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table requires the view owner to have special view creation privileges for the table
        assertAccessDenied(
                "SELECT * FROM test_view_access",
                "View owner 'test_view_access_owner' cannot create view that selects from .*.orders.*",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify the view owner can select from the view even without special view creation privileges
        assertAccessAllowed(
                viewOwnerSession,
                "SELECT * FROM test_view_access",
                privilege(viewOwnerSession.getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a table does not require the session user to have SELECT privileges on the underlying table
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_view_access",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        Session nestedViewOwnerSession = TestingSession.testSessionBuilder()
                .setIdentity(new Identity("test_nested_view_access_owner", Optional.empty()))
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .build();

        // view creation permissions are only checked at query time, not at creation
        assertAccessAllowed(
                nestedViewOwnerSession,
                "CREATE VIEW test_nested_view_access AS SELECT * FROM test_view_access",
                privilege("test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view requires the view owner of the outer view to have special view creation privileges for the inner view
        assertAccessDenied(
                "SELECT * FROM test_nested_view_access",
                "View owner 'test_nested_view_access_owner' cannot create view that selects from .*.test_view_access.*",
                privilege(nestedViewOwnerSession.getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));

        // verify selecting from a view over a view does not require the session user to have SELECT privileges for the inner view
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_nested_view_access",
                privilege(getSession().getUser(), "test_view_access", SELECT_COLUMN));

        // verify that INVOKER security runs as session user
        assertAccessAllowed(
                viewOwnerSession,
                "CREATE VIEW test_invoker_view_access SECURITY INVOKER AS SELECT * FROM orders",
                privilege("orders", CREATE_VIEW_WITH_SELECT_COLUMNS));
        assertAccessAllowed(
                "SELECT * FROM test_invoker_view_access",
                privilege(viewOwnerSession.getUser(), "orders", SELECT_COLUMN));
        assertAccessDenied(
                "SELECT * FROM test_invoker_view_access",
                "Cannot select from columns \\[.*\\] in table .*.orders.*",
                privilege(getSession().getUser(), "orders", SELECT_COLUMN));

        assertAccessAllowed(nestedViewOwnerSession, "DROP VIEW test_nested_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_view_access");
        assertAccessAllowed(viewOwnerSession, "DROP VIEW test_invoker_view_access");
    }

    @Test
    public void testWrittenStats()
    {
        String sql = "CREATE TABLE test_written_stats AS SELECT * FROM nation";
        DistributedQueryRunner distributedQueryRunner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        QueryInfo queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 25L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        sql = "INSERT INTO test_written_stats SELECT * FROM nation LIMIT 10";
        resultResultWithQueryId = distributedQueryRunner.executeWithQueryId(getSession(), sql);
        queryInfo = distributedQueryRunner.getCoordinator().getQueryManager().getFullQueryInfo(resultResultWithQueryId.getQueryId());

        assertEquals(queryInfo.getQueryStats().getOutputPositions(), 1L);
        assertEquals(queryInfo.getQueryStats().getWrittenPositions(), 10L);
        assertTrue(queryInfo.getQueryStats().getLogicalWrittenDataSize().toBytes() > 0L);

        assertUpdate("DROP TABLE test_written_stats");
    }

    @Test
    public void testComplexCast()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.OPTIMIZE_DISTINCT_AGGREGATIONS, "true")
                .build();
        // This is optimized using CAST(null AS interval day to second) which may be problematic to deserialize on worker
        assertQuery(session, "WITH t(a, b) AS (VALUES (1, INTERVAL '1' SECOND)) " +
                        "SELECT count(DISTINCT a), CAST(max(b) AS VARCHAR) FROM t",
                "VALUES (1, '0 00:00:01.000')");
    }

    @Test
    public void testDisjunctPredicateWithPartitionKey()
    {
        if (supportsPushdown()) {
            assertUpdate("CREATE TABLE test_partition_predicate (id int, p1 char, p2 int) WITH (partitioned_by=ARRAY['p2'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_partition_predicate"));
            assertTableColumnNames("test_partition_predicate", "id", "p1", "p2");

            assertUpdate("INSERT INTO test_partition_predicate VALUES (1,'a',1), (2,'b',2), (3,'c',3)", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (4,'d',4), (5,'e',5), (6,'f',6)", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (7,'g',7), (8,'h',8), (9,'i',9)", 3);

            String sql = "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1='b' or p2>3) ORDER BY id";
            assertQuery(getSession(), sql,
                    "VALUES (2,'b',2), (4,'d',4), (5,'e',5), (6,'f',6), (7,'g',7), (8,'h',8), (9,'i',9)");

            Session session1 = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            assertQuery(session1, sql,
                    "VALUES (2,'b',2), (4,'d',4), (5,'e',5), (6,'f',6), (7,'g',7), (8,'h',8), (9,'i',9)");

            assertUpdate("INSERT INTO test_partition_predicate VALUES (10,'j',10)", 1);
            assertQuery(session1, sql,
                    "VALUES (2,'b',2), (4,'d',4), (5,'e',5), (6,'f',6), (7,'g',7), (8,'h',8), (9,'i',9), (10,'j',10)");

            assertUpdate("INSERT INTO test_partition_predicate VALUES (11,NULL,11), (12,NULL,NULL), (NULL,NULL,NULL)", 3);

            /* NUlls Excluded */
            MaterializedResult resultNormal = computeActual(sql);
            MaterializedResult resultPushdown = computeActual(session1, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());

            /* NUlls Included */
            sql = "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 IS NULL or p2<3) ORDER BY id, p1";
            resultPushdown = computeActual(session1, sql);
            resultNormal = computeActual(sql);
            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());

            /* Query Test with Cache */
            Session session2 = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();
            assertQuery(session2, "CACHE TABLE test_partition_predicate WHERE p2 > 0", "VALUES ('OK')");

            MaterializedResult resultCachePushdown = computeActual(session2, sql);
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());

            sql = "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 IS NULL or p2 IS NULL) ORDER BY id";

            resultNormal = computeActual(sql);
            resultPushdown = computeActual(session1, sql);
            resultCachePushdown = computeActual(session2, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());

            sql = "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 IS NOT NULL or p2 IS NULL) ORDER BY id";

            resultNormal = computeActual(sql);
            resultPushdown = computeActual(session1, sql);
            resultCachePushdown = computeActual(session2, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());

            sql = "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 > 'e' OR p2 IS NULL) ORDER BY id";

            resultNormal = computeActual(sql);
            resultPushdown = computeActual(session1, sql);
            resultCachePushdown = computeActual(session2, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
        }
    }

    @Test
    public void testLikePredicateWithPartitionKey()
    {
        if (supportsPushdown()) {
            assertUpdate("DROP TABLE IF EXISTS test_partition_predicate");
            assertUpdate("CREATE TABLE test_partition_predicate (id int, p1 varchar, p2 varchar) WITH (partitioned_by=ARRAY['p2'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_partition_predicate"));
            assertTableColumnNames("test_partition_predicate", "id", "p1", "p2");

            assertUpdate("INSERT INTO test_partition_predicate VALUES (1,'aaa','aaa'), (2,'bbb','bbb'), (3,'ccc','ccc')", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (4,'ddd','ddd'), (5,'eee','eee'), (6,'fff','fff')", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (7,'ggg','ggg'), (8,'hhh','hhh'), (9,'iii','iii')", 3);

            Session sessionPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            Session sessionCachePushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();

            assertUpdate("INSERT INTO test_partition_predicate VALUES (10,NULL,'10'), (11,NULL,NULL), (NULL,NULL,NULL)", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (13,'ab',NULL), (14,'aab',NULL), (NULL,'aaab',NULL)", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (15,'ab','aab'), (16,'aab','aab'), (NULL,'aaab','aaab')", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (18,'b',NULL), (19,'abb',NULL), (NULL,'abbb',NULL)", 3);
            assertUpdate("INSERT INTO test_partition_predicate VALUES (21,'b','b'), (22,'abb','abb'), (NULL,'abbb','abbb')", 3);

            MaterializedResult resultNormal;
            MaterializedResult resultPushdown;
            MaterializedResult resultCachePushdown;

            /* Conjuct Like */
            List<String> sqlList = Arrays.asList(
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE 'a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE 'a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE 'aa%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE 'ab%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%a' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%b' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%ab' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%aa%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p2 LIKE '%aaa%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE 'a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE 'a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE 'aa%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE 'ab%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%a' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%b' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%ab' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%a%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%aa%' ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and p1 LIKE '%aaa%' ORDER BY id");
            for (String sql : sqlList) {
                resultNormal = computeActual(sql);
                resultPushdown = computeActual(sessionPushdown, sql);
                resultCachePushdown = computeActual(sessionCachePushdown, sql);

                assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
                assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
            }

            /* disjunct Like */
            sqlList = Arrays.asList(
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'aa%'     OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'ab%'     OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a'      OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%b'      OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%ab'     OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a%'     OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aa%'    OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aaa%'   OR p1 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'aa%'     OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'ab%'     OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a'      OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%b'      OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%ab'     OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a%'     OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aa%'    OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aaa%'   OR p1 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'a%'      OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'aa%'     OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE 'ab%'     OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a'      OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%b'      OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%ab'     OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%a%'     OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aa%'    OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p2 LIKE '%aaa%'   OR p1 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'aa%'     OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'ab%'     OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a'      OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%b'      OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%ab'     OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a%'     OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aa%'    OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aaa%'   OR p2 > 'c') ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'aa%'     OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'ab%'     OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a'      OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%b'      OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%ab'     OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a%'     OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aa%'    OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aaa%'   OR p2 IS NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'a%'      OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'aa%'     OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE 'ab%'     OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a'      OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%b'      OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%ab'     OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%a%'     OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aa%'    OR p2 IS NOT NULL) ORDER BY id",
                    "SELECT id, p1, p2 FROM test_partition_predicate WHERE id > 0 and (p1 LIKE '%aaa%'   OR p2 IS NOT NULL) ORDER BY id");
            for (String sql : sqlList) {
                resultNormal = computeActual(sql);
                resultPushdown = computeActual(sessionPushdown, sql);
                resultCachePushdown = computeActual(sessionCachePushdown, sql);

                if (!resultNormal.getMaterializedRows().equals(resultPushdown.getMaterializedRows())
                        || !resultNormal.getMaterializedRows().equals(resultCachePushdown.getMaterializedRows())) {
                    System.out.println("------------------------------------------------------------------------");
                    System.out.println("Failed Query: " + sql);
                    System.out.println("------------------------------------------------------------------------");
                    System.out.println("Expected        : " + resultNormal.getMaterializedRows());
                    System.out.println("Result[Pushdown]: " + resultPushdown.getMaterializedRows());
                    System.out.println("Result[Cache]   : " + resultCachePushdown.getMaterializedRows());
                    System.out.println("------------------------------------------------------------------------");
                    resultNormal = computeActual("EXPLAIN " + sql);
                    resultPushdown = computeActual(sessionPushdown, "EXPLAIN " + sql);
                    resultCachePushdown = computeActual(sessionCachePushdown, "EXPLAIN " + sql);

                    System.out.println("Running query [" + sql + "]");
                    System.out.println("Normal Plan: " + resultNormal);
                    System.out.println("PushDown Plan: " + resultPushdown);
                    System.out.println("CacheDown Plan: " + resultCachePushdown);
                    System.out.println("------------------------------------------------------------------------");
                }
                assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
                assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
            }
        }
    }

    @Test
    public void testDecimalCachedDisjunctPushdown()
    {
        if (supportsPushdown()) {
            assertUpdate("DROP TABLE IF EXISTS test_store_sales");
            assertUpdate("create table test_store_sales\n" +
                    "(\n" +
                    "    ss_sold_time_sk           bigint,\n" +
                    "    ss_item_sk                bigint,\n" +
                    "    ss_customer_sk            bigint,\n" +
                    "    ss_cdemo_sk               bigint,\n" +
                    "    ss_hdemo_sk               bigint,\n" +
                    "    ss_addr_sk                bigint,\n" +
                    "    ss_store_sk               bigint,\n" +
                    "    ss_promo_sk               bigint,\n" +
                    "    ss_ticket_number          bigint,\n" +
                    "    ss_quantity               int,\n" +
                    "    ss_wholesale_cost         decimal(7,2),\n" +
                    "    ss_list_price             decimal(7,2),\n" +
                    "    ss_sales_price            decimal(7,2),\n" +
                    "    ss_ext_discount_amt       decimal(7,2),\n" +
                    "    ss_ext_sales_price        decimal(7,2),\n" +
                    "    ss_ext_wholesale_cost     decimal(7,2),\n" +
                    "    ss_ext_list_price         decimal(7,2),\n" +
                    "    ss_ext_tax                decimal(7,2),\n" +
                    "    ss_coupon_amt             decimal(7,2),\n" +
                    "    ss_net_paid               decimal(7,2),\n" +
                    "    ss_net_paid_inc_tax       decimal(7,2),\n" +
                    "    ss_net_profit             decimal(7,2),\n" +
                    "    ss_sold_date_sk           bigint \n" +
                    ") WITH (partitioned_by=ARRAY['ss_sold_date_sk'])");

            assertTrue(getQueryRunner().tableExists(getSession(), "test_store_sales"));

            assertUpdate("INSERT INTO test_store_sales VALUES (65495,1091,6,591617,3428,839,2,2,1,79,11.41,18.71,2.80,99.54,221.20,901.39,1478.09,6.08,99.54,121.66,127.74,-779.73,2451813)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,1478,6,591617,3428,839,2,1,1,37,63.63,101.17,41.47,46.03,1534.39,2354.31,3743.29,59.53,46.03,1488.36,1547.89,-865.95,2451813)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,1961,6,591617,3428,839,2,1,1,99,80.52,137.68,83.98,0.00,8314.02,7971.48,13630.32,0.00,0.00,8314.02,8314.02,342.54,2451814)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,470,6,591617,3428,839,2,1,1,14,57.37,76.30,6.10,0.00,85.40,803.18,1068.20,0.00,0.00,85.40,85.40,-717.78,2451814)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,404,6,591617,3428,839,2,2,1,100,25.08,36.86,0.73,0.00,73.00,2508.00,3686.00,6.57,0.00,73.00,79.57,-2435.00,NULL)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,404,6,591617,3428,839,2,2,1,100,25.08,36.86,0.73,0.00,73.00,2508.00,3686.00,6.57,0.00,73.00,79.57,-2435.00,2451815)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,2451815)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)", 1);

            /* ss_quantity: 0...5 */
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,1439,6,591617,3428,839,2,1,1,5,10.68,15.91,6.68,0.00,33.40,53.40,79.55,2.33,0.00,33.40,35.73,-20.00,2451813)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (75316,56,284,1712478,4672,266,2,2,2,4,13.46,15.34,12.57,0.00,50.28,53.84,61.36,4.02,0.00,50.28,54.30,-3.56,2451524)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (50412,1539,47,586372,697,622,1,2,3,4,95.75,146.49,43.94,0.00,175.76,383.00,585.96,5.27,0.00,175.76,181.03,-207.24,2452638)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (50412,1545,47,586372,697,622,1,1,3,4,75.67,130.15,35.14,0.00,140.56,302.68,520.60,9.83,0.00,140.56,150.39,-162.12,2452638)", 1);
            /* ss_quantity: 6...10 */
            assertUpdate("INSERT INTO test_store_sales VALUES (46712,1829,954,890396,791,633,2,3,7,9,56.13,59.49,30.93,0.00,278.37,505.17,535.41,22.26,0.00,278.37,300.63,-226.80,2452260)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (52209,665,944,578318,6045,19,1,2,8,7,18.65,27.97,20.41,0.00,142.87,130.55,195.79,1.42,0.00,142.87,144.29,12.32,2452179)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (45986,1803,480,1023064,4248,385,1,2,9,6,27.17,51.62,25.29,0.00,151.74,163.02,309.72,3.03,0.00,151.74,154.77,-11.28,2452496)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (35137,7,820,63209,3198,434,1,1,12,9,7.33,11.94,11.22,31.30,100.98,65.97,107.46,2.09,31.30,69.68,71.77,3.71,2451206)", 1);
            /* ss_quantity: 11...15 */
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,470,6,591617,3428,839,2,1,1,14,57.37,76.30,6.10,0.00,85.40,803.18,1068.20,0.00,0.00,85.40,85.40,-717.78,2451813)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (65495,1837,6,591617,3428,839,2,1,1,14,11.54,11.77,0.00,0.00,0.00,161.56,164.78,0.00,0.00,0.00,0.00,-161.56,2451813)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (53976,691,246,963544,5042,940,2,3,5,15,21.21,33.93,30.53,0.00,457.95,318.15,508.95,22.89,0.00,457.95,480.84,139.80,2451465)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (46712,1177,954,890396,791,633,2,2,7,12,47.60,75.20,65.42,0.00,785.04,571.20,902.40,47.10,0.00,785.04,832.14,213.84,2452260)", 1);
            /* ss_quantity: 16...20 */
            assertUpdate("INSERT INTO test_store_sales VALUES (50412,1719,47,586372,697,622,1,2,3,17,72.38,111.46,75.79,0.00,1288.43,1230.46,1894.82,38.65,0.00,1288.43,1327.08,57.97,2452638)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (38195,1465,814,874786,1511,83,2,1,4,17,54.26,105.80,9.52,116.52,161.84,922.42,1798.60,0.00,116.52,45.32,45.32,-877.10,2451438)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (52209,1757,944,578318,6045,19,1,3,8,20,18.95,33.92,31.88,0.00,637.60,379.00,678.40,51.00,0.00,637.60,688.60,258.60,2452179)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (60226,1377,451,71288,6925,515,2,2,10,17,88.36,133.42,53.36,0.00,907.12,1502.12,2268.14,0.00,0.00,907.12,907.12,-595.00,2451966)", 1);
            /* ss_quantity: 21...25 */
            assertUpdate("INSERT INTO test_store_sales VALUES (75316,1030,284,1712478,4672,266,2,3,2,25,74.26,89.11,35.64,0.00,891.00,1856.50,2227.75,8.91,0.00,891.00,899.91,-965.50,2451524)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (38195,397,814,874786,1511,83,2,2,4,23,2.91,3.57,3.53,32.47,81.19,66.93,82.11,0.97,32.47,48.72,49.69,-18.21,2451438)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (38195,886,814,874786,1511,83,2,2,4,21,40.39,40.39,12.11,0.00,254.31,848.19,848.19,22.88,0.00,254.31,277.19,-593.88,2451438)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (38195,908,814,874786,1511,83,2,2,4,23,53.28,77.78,1.55,21.03,35.65,1225.44,1788.94,1.16,21.03,14.62,15.78,-1210.82,2451438)", 1);
            /* ss_quantity: 26...30 */
            assertUpdate("INSERT INTO test_store_sales VALUES (75316,686,284,1712478,4672,266,2,3,2,30,2.27,3.83,1.11,0.00,33.30,68.10,114.90,0.00,0.00,33.30,33.30,-34.80,2451524)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (50412,1063,47,586372,697,622,1,2,3,27,37.04,58.89,55.35,0.00,1494.45,1000.08,1590.03,29.88,0.00,1494.45,1524.33,494.37,2452638)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (38195,272,814,874786,1511,83,2,2,4,27,26.21,44.81,19.26,0.00,520.02,707.67,1209.87,46.80,0.00,520.02,566.82,-187.65,2451438)", 1);
            assertUpdate("INSERT INTO test_store_sales VALUES (45986,775,480,1023064,4248,385,1,1,9,27,98.68,195.38,15.63,0.00,422.01,2664.36,5275.26,33.76,0.00,422.01,455.77,-2242.35,2452496)", 1);

            Session sessionPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            Session sessionCachePushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();

            MaterializedResult resultNormal;
            MaterializedResult resultPushdown;
            MaterializedResult resultCachePushdown;

            String sql = "SELECT *\n" +
                    "FROM (SELECT\n" +
                    "  avg(ss_list_price) B1_LP,\n" +
                    "  count(ss_list_price) B1_CNT,\n" +
                    "  count(DISTINCT ss_list_price) B1_CNTD\n" +
                    "FROM test_store_sales\n" +
                    "WHERE ss_quantity BETWEEN 0 AND 5\n" +
                    "  AND (ss_list_price BETWEEN 8 AND 8 + 10\n" +
                    "  OR ss_coupon_amt BETWEEN 459 AND 459 + 1000\n" +
                    "  OR ss_wholesale_cost BETWEEN 57 AND 57 + 20)) B1,\n" +
                    "  (SELECT\n" +
                    "    avg(ss_list_price) B2_LP,\n" +
                    "    count(ss_list_price) B2_CNT,\n" +
                    "    count(DISTINCT ss_list_price) B2_CNTD\n" +
                    "  FROM test_store_sales\n" +
                    "  WHERE ss_quantity BETWEEN 6 AND 10\n" +
                    "    AND (ss_list_price BETWEEN 90 AND 90 + 10\n" +
                    "    OR ss_coupon_amt BETWEEN 2323 AND 2323 + 1000\n" +
                    "    OR ss_wholesale_cost BETWEEN 31 AND 31 + 20)) B2,\n" +
                    "  (SELECT\n" +
                    "    avg(ss_list_price) B3_LP,\n" +
                    "    count(ss_list_price) B3_CNT,\n" +
                    "    count(DISTINCT ss_list_price) B3_CNTD\n" +
                    "  FROM test_store_sales\n" +
                    "  WHERE ss_quantity BETWEEN 11 AND 15\n" +
                    "    AND (ss_list_price BETWEEN 142 AND 142 + 10\n" +
                    "    OR ss_coupon_amt BETWEEN 12214 AND 12214 + 1000\n" +
                    "    OR ss_wholesale_cost BETWEEN 79 AND 79 + 20)) B3,\n" +
                    "  (SELECT\n" +
                    "    avg(ss_list_price) B4_LP,\n" +
                    "    count(ss_list_price) B4_CNT,\n" +
                    "    count(DISTINCT ss_list_price) B4_CNTD\n" +
                    "  FROM test_store_sales\n" +
                    "  WHERE ss_quantity BETWEEN 16 AND 20\n" +
                    "    AND (ss_list_price BETWEEN 135 AND 135 + 10\n" +
                    "    OR ss_coupon_amt BETWEEN 6071 AND 6071 + 1000\n" +
                    "    OR ss_wholesale_cost BETWEEN 38 AND 38 + 20)) B4,\n" +
                    "  (SELECT\n" +
                    "    avg(ss_list_price) B5_LP,\n" +
                    "    count(ss_list_price) B5_CNT,\n" +
                    "    count(DISTINCT ss_list_price) B5_CNTD\n" +
                    "  FROM test_store_sales\n" +
                    "  WHERE ss_quantity BETWEEN 21 AND 25\n" +
                    "    AND (ss_list_price BETWEEN 122 AND 122 + 10\n" +
                    "    OR ss_coupon_amt BETWEEN 836 AND 836 + 1000\n" +
                    "    OR ss_wholesale_cost BETWEEN 17 AND 17 + 20)) B5,\n" +
                    "  (SELECT\n" +
                    "    avg(ss_list_price) B6_LP,\n" +
                    "    count(ss_list_price) B6_CNT,\n" +
                    "    count(DISTINCT ss_list_price) B6_CNTD\n" +
                    "  FROM test_store_sales\n" +
                    "  WHERE ss_quantity BETWEEN 26 AND 30\n" +
                    "    AND (ss_list_price BETWEEN 154 AND 154 + 10\n" +
                    "    OR ss_coupon_amt BETWEEN 7326 AND 7326 + 1000\n" +
                    "    OR ss_wholesale_cost BETWEEN 7 AND 7 + 20)) B6\n" +
                    "LIMIT 100";

            resultNormal = computeActual(sql);
            resultPushdown = computeActual(sessionPushdown, sql);

            assertQuery("CACHE TABLE test_store_sales WHERE ss_sold_date_sk > 2451801", "VALUES('OK')");
            resultCachePushdown = computeActual(sessionCachePushdown, sql);

            if (!resultNormal.getMaterializedRows().equals(resultPushdown.getMaterializedRows())
                    || !resultNormal.getMaterializedRows().equals(resultCachePushdown.getMaterializedRows())) {
                System.out.println("------------------------------------------------------------------------");
                System.out.println("Failed Query: " + sql);
                System.out.println("------------------------------------------------------------------------");
                System.out.println("Expected        : " + resultNormal.getMaterializedRows());
                System.out.println("Result[Pushdown]: " + resultPushdown.getMaterializedRows());
                System.out.println("Result[Cache]   : " + resultCachePushdown.getMaterializedRows());
                System.out.println("------------------------------------------------------------------------");
                resultNormal = computeActual("EXPLAIN " + sql);
                resultPushdown = computeActual(sessionPushdown, "EXPLAIN " + sql);
                resultCachePushdown = computeActual(sessionCachePushdown, "EXPLAIN " + sql);

                System.out.println("Running query [" + sql + "]");
                System.out.println("Normal Plan: " + resultNormal);
                System.out.println("PushDown Plan: " + resultPushdown);
                System.out.println("CacheDown Plan: " + resultCachePushdown);
                System.out.println("------------------------------------------------------------------------");
            }
            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
        }
    }

    @Test
    public void testPushdownCacheFilterKey()
    {
        if (supportsPushdown()) {
            assertUpdate("DROP TABLE IF EXISTS test_cache_predicate");
            assertUpdate("CREATE TABLE test_cache_predicate (id int, name varchar, p1 int) WITH (partitioned_by=ARRAY['p1'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_cache_predicate"));
            assertTableColumnNames("test_cache_predicate", "id", "name", "p1");

            assertUpdate("INSERT INTO test_cache_predicate VALUES (1,'---',1), (2,'---',1), (3,'abc',1)", 3);

            Session sessionPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            Session sessionCachePushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();

            MaterializedResult resultNormal;
            MaterializedResult resultPushdown;
            MaterializedResult resultCachePushdown;

            String sql = "SELECT * FROM test_cache_predicate WHERE id=3 AND name='abc'";
            resultNormal = computeActual(sql);
            resultPushdown = computeActual(sessionPushdown, sql);

            assertQuery("CACHE TABLE test_cache_predicate WHERE p1 <> 0", "VALUES('OK')");
            resultCachePushdown = computeActual(sessionCachePushdown, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
        }
    }

    @Test
    public void testPushdownTimestampCacheFilterKey()
    {
        if (supportsPushdown()) {
            assertUpdate("DROP TABLE IF EXISTS test_cache_predicate_ts");
            assertUpdate("CREATE TABLE test_cache_predicate_ts (id int, name timestamp, p1 int) WITH (partitioned_by=ARRAY['p1'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_cache_predicate_ts"));
            assertTableColumnNames("test_cache_predicate_ts", "id", "name", "p1");

            assertUpdate("INSERT INTO test_cache_predicate_ts VALUES (1,timestamp '2019-09-09 09:09:09.909',1), (2,timestamp '2019-09-09 09:09:09.909',1), (3,timestamp '2020-02-02 02:02:02.202',1)", 3);

            Session sessionPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            Session sessionCachePushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();

            MaterializedResult resultNormal;
            MaterializedResult resultPushdown;
            MaterializedResult resultCachePushdown;

            String sql = "SELECT * FROM test_cache_predicate_ts WHERE id=3 AND name=timestamp '2020-02-02 02:02:02.202'";
            resultNormal = computeActual(sql);
            resultPushdown = computeActual(sessionPushdown, sql);

            assertQuery("CACHE TABLE test_cache_predicate_ts WHERE p1 <> 0", "VALUES('OK')");
            resultCachePushdown = computeActual(sessionCachePushdown, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
        }
    }

    @Test
    public void testPushdownVarbinary()
    {
        if (supportsPushdown()) {
            assertUpdate("DROP TABLE IF EXISTS test_pushdown_varbinary");
            assertUpdate("CREATE TABLE test_pushdown_varbinary (id int, name varbinary, p1 int) WITH (partitioned_by=ARRAY['p1'])");
            assertTrue(getQueryRunner().tableExists(getSession(), "test_pushdown_varbinary"));
            assertTableColumnNames("test_pushdown_varbinary", "id", "name", "p1");

            assertUpdate("INSERT INTO test_pushdown_varbinary VALUES (1,varbinary '2019-09-09 09:09:09.909',1), (2,varbinary '2019-09-09 09:09:09.909',1), (3,varbinary '2020-02-02 02:02:02.202',1)", 3);

            Session sessionPushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "false")
                    .build();
            Session sessionCachePushdown = Session.builder(getSession())
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_predicate_pushdown_enabled", "true")
                    .setCatalogSessionProperty(getSession().getCatalog().get(), "orc_row_data_cache_enabled", "true")
                    .build();

            MaterializedResult resultNormal;
            MaterializedResult resultPushdown;
            MaterializedResult resultCachePushdown;

            String sql = "SELECT * FROM test_pushdown_varbinary WHERE id=3 AND name=varbinary '2020-02-02 02:02:02.202'";
            resultNormal = computeActual(sql);
            resultPushdown = computeActual(sessionPushdown, sql);

            assertQuery("CACHE TABLE test_pushdown_varbinary WHERE p1 <> 0", "VALUES('OK')");
            resultCachePushdown = computeActual(sessionCachePushdown, sql);

            assertEquals(resultNormal.getMaterializedRows(), resultPushdown.getMaterializedRows());
            assertEquals(resultNormal.getMaterializedRows(), resultCachePushdown.getMaterializedRows());
        }
    }

    @Test
    public void testLargeFilterExpression()
    {
        String sql = "SELECT * FROM orders WHERE (custkey=1 AND shippriority=1)" +
                " OR (custkey=2 AND shippriority= 2)" +
                " OR (custkey=3 AND shippriority= 3)" +
                " OR (custkey=4 AND shippriority= 4)" +
                " OR (custkey=5 AND shippriority= 5)" +
                " OR (custkey=6 AND shippriority= 6)" +
                " OR (custkey=7 AND shippriority= 7)" +
                " OR (custkey=8 AND shippriority= 8)" +
                " OR (custkey=9 AND shippriority= 9)" +
                " OR (custkey=10 AND shippriority= 10)" +
                " OR (custkey=11 AND shippriority= 11)" +
                " OR (custkey=12 AND shippriority= 12)" +
                " OR (custkey=13 AND shippriority= 13)" +
                " OR (custkey=14 AND shippriority= 14)" +
                " OR (custkey=15 AND shippriority= 15)" +
                " OR (custkey=16 AND shippriority= 16)" +
                " OR (custkey=17 AND shippriority= 17)" +
                " OR (custkey=18 AND shippriority= 18)" +
                " OR (custkey=19 AND shippriority= 19)" +
                " OR (custkey=20 AND shippriority= 20)" +
                " OR (custkey=21 AND shippriority= 21)" +
                " OR (custkey=22 AND shippriority= 22)" +
                " OR (custkey=23 AND shippriority= 23)" +
                " OR (custkey=24 AND shippriority= 24)" +
                " OR (custkey=25 AND shippriority= 25)" +
                " OR (custkey=26 AND shippriority= 26)" +
                " OR (custkey=27 AND shippriority= 27)" +
                " OR (custkey=28 AND shippriority= 28)" +
                " OR (custkey=29 AND shippriority= 29)" +
                " OR (custkey=30 AND shippriority= 30)" +
                " OR (custkey=31 AND shippriority= 31)" +
                " OR (custkey=32 AND shippriority= 32)";
        MaterializedResult result = computeActual(sql);
        System.out.println("Query Output: " + result);
    }

    @Test
    public void testDefectCase()
    {
        if (!supportsViews()) {
            return;
        }
        assertUpdate("create table error(t int, u int)");

        String sql = "select *\n" +
                "from error\n" +
                "where\n" +
                "(t=51 and u=-4)\n" +
                "or (t=52 and u=-10)\t\t\n" +
                "or (t=46 and u=-2)\n" +
                "or (t=46 and u=-1)\n" +
                "or (t=43 and u=3)\n" +
                "or (t=39 and u=-7)\n" +
                "or (t=48 and u=-4)\n" +
                "or (t=42 and u=-8)\n" +
                "or (t=43 and u=-6)\n" +
                "or (t=44 and u=4)\n" +
                "or (t=37 and u=-6)\n" +
                "or (t=38 and u=-8)\n" +
                "or (t=46 and u=12)\n" +
                "or (t=39 and u=17)\n" +
                "or (t=42 and u=-9)\n" +
                "or (t=45 and u=9)\n" +
                "or (t=46 and u=3)\n" +
                "or (t=37 and u=-9)\n" +
                "or (t=47 and u=9)\n" +
                "or (t=40 and u=-6)\n" +
                "or (t=48 and u=20)\n" +
                "or (t=49 and u=12)\n" +
                "or (t=49 and u=17)\n" +
                "or (t=48 and u=1)\n" +
                "or (t=50 and u=11)\n" +
                "or (t=50 and u=22)\n" +
                "or (t=51 and u=1)\n" +
                "or (t=52 and u=-8)\n" +
                "or (t=47 and u=-2)\n" +
                "or (t=40 and u=0)\n" +
                "or (t=47 and u=-1)\n" +
                "or (t=51 and u=-5)\n" +
                "or (t=42 and u=15)\n" +
                "or (t=44 and u=-2)\n" +
                "or (t=44 and u=-1)\n" +
                "or (t=51 and u=12)\n" +
                "or (t=39 and u=-4)\n" +
                "or (t=47 and u=19)\n" +
                "or (t=42 and u=-3)\n" +
                "or (t=43 and u=-7)\n" +
                "or (t=46 and u=9)\n" +
                "or (t=37 and u=-3)\n" +
                "or (t=48 and u=14)\n" +
                "or (t=49 and u=15)\n" +
                "or (t=41 and u=-6)\n" +
                "or (t=47 and u=12)\n" +
                "or (t=45 and u=12)\n" +
                "or (t=48 and u=17)\n" +
                "or (t=50 and u=1)\n" +
                "or (t=50 and u=12)\n" +
                "or (t=40 and u=-9)\n" +
                "or (t=51 and u=19)\n" +
                "or (t=48 and u=-1)\n" +
                "or (t=52 and u=8)\n" +
                "or (t=52 and u=12)\n" +
                "or (t=51 and u=22)\n" +
                "or (t=52 and u=-3)\n" +
                "or (t=47 and u=-3)\n" +
                "or (t=45 and u=-2)\n" +
                "or (t=42 and u=0)\n" +
                "or (t=39 and u=-10)\n" +
                "or (t=40 and u=16)\n" +
                "or (t=38 and u=15)\n" +
                "or (t=45 and u=-1)\n" +
                "or (t=44 and u=3)\n" +
                "or (t=43 and u=13)\n" +
                "or (t=51 and u=21)\n" +
                "or (t=49 and u=5)\n" +
                "or (t=48 and u=11)\n" +
                "or (t=42 and u=-2)\n" +
                "or (t=42 and u=-1)\n" +
                "or (t=45 and u=2)\n" +
                "or (t=46 and u=10)\n" +
                "or (t=50 and u=2)\n" +
                "or (t=49 and u=10)\n" +
                "or (t=47 and u=3)\n" +
                "or (t=51 and u=6)\n" +
                "or (t=52 and u=1)\n" +
                "or (t=50 and u=17)\n" +
                "or (t=48 and u=5)\n" +
                "or (t=43 and u=-2)\n" +
                "or (t=43 and u=-1)\n" +
                "or (t=44 and u=0)\n" +
                "or (t=38 and u=-4)\n" +
                "or (t=49 and u=0)\n" +
                "or (t=47 and u=21)\n" +
                "or (t=46 and u=7)\n" +
                "or (t=45 and u=5)\n" +
                "or (t=48 and u=3)\n" +
                "or (t=48 and u=8)\n" +
                "or (t=50 and u=7)\n" +
                "or (t=40 and u=-2)\n" +
                "or (t=40 and u=-1)\n" +
                "or (t=47 and u=6)\n" +
                "or (t=49 and u=21)\n" +
                "or (t=51 and u=5)\n" +
                "or (t=51 and u=11)\n" +
                "or (t=50 and u=18)\n" +
                "or (t=52 and u=6)\n" +
                "or (t=40 and u=4)\n" +
                "or (t=51 and u=14)\n" +
                "or (t=52 and u=-9)\n" +
                "or (t=48 and u=-5)\n" +
                "or (t=43 and u=2)\n" +
                "or (t=39 and u=-8)\n" +
                "or (t=48 and u=-3)\n" +
                "or (t=42 and u=-7)\n" +
                "or (t=43 and u=-3)\n" +
                "or (t=44 and u=5)\n" +
                "or (t=37 and u=-7)\n" +
                "or (t=38 and u=-7)\n" +
                "or (t=46 and u=13)\n" +
                "or (t=41 and u=-2)\n" +
                "or (t=41 and u=-1)\n" +
                "or (t=45 and u=8)\n" +
                "or (t=46 and u=0)\n" +
                "or (t=47 and u=8)\n" +
                "or (t=48 and u=21)\n" +
                "or (t=40 and u=-5)\n" +
                "or (t=47 and u=5)\n" +
                "or (t=49 and u=3)\n" +
                "or (t=49 and u=16)\n" +
                "or (t=50 and u=8)\n" +
                "or (t=51 and u=0)\n" +
                "or (t=50 and u=23)\n" +
                "or (t=41 and u=15)\n" +
                "or (t=42 and u=12)\n" +
                "or (t=43 and u=-9)\n" +
                "or (t=43 and u=1)\n" +
                "or (t=38 and u=-9)\n" +
                "or (t=41 and u=17)\n" +
                "or (t=47 and u=18)\n" +
                "or (t=42 and u=-6)\n" +
                "or (t=43 and u=-8)\n" +
                "or (t=44 and u=10)\n" +
                "or (t=37 and u=-4)\n" +
                "or (t=38 and u=-6)\n" +
                "or (t=48 and u=15)\n" +
                "or (t=41 and u=-7)\n" +
                "or (t=47 and u=15)\n" +
                "or (t=45 and u=11)\n" +
                "or (t=48 and u=18)\n" +
                "or (t=49 and u=14)\n" +
                "or (t=50 and u=13)\n" +
                "or (t=40 and u=-8)\n" +
                "or (t=49 and u=19)\n" +
                "or (t=52 and u=13)\n" +
                "or (t=48 and u=0)\n" +
                "or (t=52 and u=-2)\n" +
                "or (t=52 and u=-1)\n" +
                "or (t=37 and u=15)\n" +
                "or (t=42 and u=1)\n" +
                "or (t=43 and u=12)\n" +
                "or (t=51 and u=20)\n" +
                "or (t=49 and u=4)\n" +
                "or (t=39 and u=9)\n" +
                "or (t=46 and u=11)\n" +
                "or (t=45 and u=1)\n" +
                "or (t=47 and u=17)\n" +
                "or (t=48 and u=12)\n" +
                "or (t=50 and u=3)\n" +
                "or (t=49 and u=9)\n" +
                "or (t=47 and u=2)\n" +
                "or (t=51 and u=9)\n" +
                "or (t=51 and u=13)\n" +
                "or (t=52 and u=2)\n" +
                "or (t=50 and u=14)\n" +
                "or (t=51 and u=16)\n" +
                "or (t=52 and u=15)\n" +
                "or (t=42 and u=2)\n" +
                "or (t=48 and u=6)\n" +
                "or (t=44 and u=1)\n" +
                "or (t=43 and u=11)\n" +
                "or (t=38 and u=-3)\n" +
                "or (t=49 and u=7)\n" +
                "or (t=47 and u=20)\n" +
                "or (t=46 and u=4)\n" +
                "or (t=45 and u=4)\n" +
                "or (t=44 and u=12)\n" +
                "or (t=48 and u=9)\n" +
                "or (t=50 and u=4)\n" +
                "or (t=49 and u=20)\n" +
                "or (t=41 and u=-9)\n" +
                "or (t=47 and u=1)\n" +
                "or (t=51 and u=4)\n" +
                "or (t=52 and u=7)\n" +
                "or (t=52 and u=17)\n" +
                "or (t=50 and u=19)\n" +
                "or (t=52 and u=16)\n" +
                "or (t=51 and u=-2)\n" +
                "or (t=51 and u=-1)\n" +
                "or (t=41 and u=0)\n" +
                "or (t=39 and u=-5)\n" +
                "or (t=48 and u=-2)\n" +
                "or (t=46 and u=18)\n" +
                "or (t=43 and u=-4)\n" +
                "or (t=44 and u=6)\n" +
                "or (t=37 and u=-8)\n" +
                "or (t=38 and u=-1)\n" +
                "or (t=38 and u=-2)\n" +
                "or (t=41 and u=-3)\n" +
                "or (t=46 and u=1)\n" +
                "or (t=45 and u=7)\n" +
                "or (t=47 and u=11)\n" +
                "or (t=48 and u=22)\n" +
                "or (t=49 and u=2)\n" +
                "or (t=40 and u=-4)\n" +
                "or (t=47 and u=4)\n" +
                "or (t=48 and u=2)\n" +
                "or (t=50 and u=9)\n" +
                "or (t=51 and u=3)\n" +
                "or (t=52 and u=4)\n" +
                "or (t=50 and u=20)\n" +
                "or (t=41 and u=14)\n" +
                "or (t=51 and u=-3)\n" +
                "or (t=42 and u=13)\n" +
                "or (t=51 and u=15)\n" +
                "or (t=49 and u=-2)\n" +
                "or (t=49 and u=-1)\n" +
                "or (t=43 and u=-10)\n" +
                "or (t=43 and u=0)\n" +
                "or (t=39 and u=-2)\n" +
                "or (t=39 and u=-1)\n" +
                "or (t=41 and u=16)\n" +
                "or (t=42 and u=-5)\n" +
                "or (t=43 and u=-5)\n" +
                "or (t=37 and u=-5)\n" +
                "or (t=38 and u=-5)\n" +
                "or (t=44 and u=11)\n" +
                "or (t=41 and u=-8)\n" +
                "or (t=46 and u=2)\n" +
                "or (t=45 and u=10)\n" +
                "or (t=47 and u=14)\n" +
                "or (t=48 and u=19)\n" +
                "or (t=49 and u=13)\n" +
                "or (t=40 and u=-7)\n" +
                "or (t=49 and u=18)\n" +
                "or (t=50 and u=10)\n" +
                "or (t=52 and u=9)\n" +
                "or (t=52 and u=14)\n" +
                "or (t=41 and u=9)\n" +
                "or (t=42 and u=14)\n" +
                "or (t=38 and u=13)\n" +
                "or (t=51 and u=23)\n" +
                "or (t=39 and u=-3)\n" +
                "or (t=47 and u=16)\n" +
                "or (t=38 and u=16)\n" +
                "or (t=42 and u=-4)\n" +
                "or (t=44 and u=8)\n" +
                "or (t=37 and u=-2)\n" +
                "or (t=45 and u=0)\n" +
                "or (t=46 and u=8)\n" +
                "or (t=41 and u=-5)\n" +
                "or (t=47 and u=13)\n" +
                "or (t=48 and u=13)\n" +
                "or (t=48 and u=16)\n" +
                "or (t=49 and u=8)\n" +
                "or (t=50 and u=15)\n" +
                "or (t=51 and u=8)\n" +
                "or (t=52 and u=3)\n" +
                "or (t=40 and u=9)\n" +
                "or (t=52 and u=-4)\n" +
                "or (t=42 and u=3)\n" +
                "or (t=39 and u=-9)\n" +
                "or (t=48 and u=7)\n" +
                "or (t=38 and u=14)\n" +
                "or (t=44 and u=2)\n" +
                "or (t=51 and u=18)\n" +
                "or (t=49 and u=6)\n" +
                "or (t=48 and u=10)\n" +
                "or (t=46 and u=5)\n" +
                "or (t=45 and u=3)\n" +
                "or (t=51 and u=7)\n" +
                "or (t=50 and u=5)\n" +
                "or (t=49 and u=11)\n" +
                "or (t=47 and u=0)\n" +
                "or (t=51 and u=10)\n" +
                "or (t=52 and u=0)\n" +
                "or (t=50 and u=16)\n" +
                "or (t=42 and u=9)\n" +
                "or (t=39 and u=-6)\n" +
                "or (t=48 and u=4)\n" +
                "or (t=46 and u=19)\n" +
                "or (t=44 and u=7)\n" +
                "or (t=51 and u=17)\n" +
                "or (t=52 and u=11)\n" +
                "or (t=49 and u=1)\n" +
                "or (t=41 and u=-4)\n" +
                "or (t=46 and u=6)\n" +
                "or (t=45 and u=6)\n" +
                "or (t=47 and u=10)\n" +
                "or (t=51 and u=2)\n" +
                "or (t=50 and u=6)\n" +
                "or (t=40 and u=-3)\n" +
                "or (t=47 and u=7)\n" +
                "or (t=49 and u=22)\n" +
                "or (t=52 and u=10) " +
                "or (t=52 and u=5) " +
                "or (t=50 and u=21) " +
                "or (t=52 and u=-5)";

        MaterializedResult result = computeActual(sql);
        System.out.println("Query Output: " + result);
    }

    @Test
    public void testLargeFilterExpressionWith500Between()
    {
        String sql = "SELECT * FROM orders WHERE (custkey BETWEEN 1 AND 5) " +
                " OR (custkey BETWEEN 11 AND 15) " +
                " OR (custkey BETWEEN 21 AND 25) " +
                " OR (custkey BETWEEN 31 AND 35) " +
                " OR (custkey BETWEEN 41 AND 45) " +
                " OR (custkey BETWEEN 51 AND 55) " +
                " OR (custkey BETWEEN 61 AND 65) " +
                " OR (custkey BETWEEN 71 AND 75) " +
                " OR (custkey BETWEEN 81 AND 85) " +
                " OR (custkey BETWEEN 91 AND 95) " +
                " OR (custkey BETWEEN 101 AND 105) " +
                " OR (custkey BETWEEN 111 AND 115) " +
                " OR (custkey BETWEEN 121 AND 125) " +
                " OR (custkey BETWEEN 131 AND 135) " +
                " OR (custkey BETWEEN 141 AND 145) " +
                " OR (custkey BETWEEN 151 AND 155) " +
                " OR (custkey BETWEEN 161 AND 165) " +
                " OR (custkey BETWEEN 171 AND 175) " +
                " OR (custkey BETWEEN 181 AND 185) " +
                " OR (custkey BETWEEN 191 AND 195) " +
                " OR (custkey BETWEEN 201 AND 205) " +
                " OR (custkey BETWEEN 211 AND 215) " +
                " OR (custkey BETWEEN 221 AND 225) " +
                " OR (custkey BETWEEN 231 AND 235) " +
                " OR (custkey BETWEEN 241 AND 245) " +
                " OR (custkey BETWEEN 251 AND 255) " +
                " OR (custkey BETWEEN 261 AND 265) " +
                " OR (custkey BETWEEN 271 AND 275) " +
                " OR (custkey BETWEEN 281 AND 285) " +
                " OR (custkey BETWEEN 291 AND 295) " +
                " OR (custkey BETWEEN 301 AND 305) " +
                " OR (custkey BETWEEN 311 AND 315) " +
                " OR (custkey BETWEEN 321 AND 325) " +
                " OR (custkey BETWEEN 331 AND 335) " +
                " OR (custkey BETWEEN 341 AND 345) " +
                " OR (custkey BETWEEN 351 AND 355) " +
                " OR (custkey BETWEEN 361 AND 365) " +
                " OR (custkey BETWEEN 371 AND 375) " +
                " OR (custkey BETWEEN 381 AND 385) " +
                " OR (custkey BETWEEN 391 AND 395) " +
                " OR (custkey BETWEEN 401 AND 405) " +
                " OR (custkey BETWEEN 411 AND 415) " +
                " OR (custkey BETWEEN 421 AND 425) " +
                " OR (custkey BETWEEN 431 AND 435) " +
                " OR (custkey BETWEEN 441 AND 445) " +
                " OR (custkey BETWEEN 451 AND 455) " +
                " OR (custkey BETWEEN 461 AND 465) " +
                " OR (custkey BETWEEN 471 AND 475) " +
                " OR (custkey BETWEEN 481 AND 485) " +
                " OR (custkey BETWEEN 491 AND 495) " +
                " OR (custkey BETWEEN 501 AND 505) " +
                " OR (custkey BETWEEN 511 AND 515) " +
                " OR (custkey BETWEEN 521 AND 525) " +
                " OR (custkey BETWEEN 531 AND 535) " +
                " OR (custkey BETWEEN 541 AND 545) " +
                " OR (custkey BETWEEN 551 AND 555) " +
                " OR (custkey BETWEEN 561 AND 565) " +
                " OR (custkey BETWEEN 571 AND 575) " +
                " OR (custkey BETWEEN 581 AND 585) " +
                " OR (custkey BETWEEN 591 AND 595) " +
                " OR (custkey BETWEEN 601 AND 605) " +
                " OR (custkey BETWEEN 611 AND 615) " +
                " OR (custkey BETWEEN 621 AND 625) " +
                " OR (custkey BETWEEN 631 AND 635) " +
                " OR (custkey BETWEEN 641 AND 645) " +
                " OR (custkey BETWEEN 651 AND 655) " +
                " OR (custkey BETWEEN 661 AND 665) " +
                " OR (custkey BETWEEN 671 AND 675) " +
                " OR (custkey BETWEEN 681 AND 685) " +
                " OR (custkey BETWEEN 691 AND 695) " +
                " OR (custkey BETWEEN 701 AND 705) " +
                " OR (custkey BETWEEN 711 AND 715) " +
                " OR (custkey BETWEEN 721 AND 725) " +
                " OR (custkey BETWEEN 731 AND 735) " +
                " OR (custkey BETWEEN 741 AND 745) " +
                " OR (custkey BETWEEN 751 AND 755) " +
                " OR (custkey BETWEEN 761 AND 765) " +
                " OR (custkey BETWEEN 771 AND 775) " +
                " OR (custkey BETWEEN 781 AND 785) " +
                " OR (custkey BETWEEN 791 AND 795) " +
                " OR (custkey BETWEEN 801 AND 805) " +
                " OR (custkey BETWEEN 811 AND 815) " +
                " OR (custkey BETWEEN 821 AND 825) " +
                " OR (custkey BETWEEN 831 AND 835) " +
                " OR (custkey BETWEEN 841 AND 845) " +
                " OR (custkey BETWEEN 851 AND 855) " +
                " OR (custkey BETWEEN 861 AND 865) " +
                " OR (custkey BETWEEN 871 AND 875) " +
                " OR (custkey BETWEEN 881 AND 885) " +
                " OR (custkey BETWEEN 891 AND 895) " +
                " OR (custkey BETWEEN 901 AND 905) " +
                " OR (custkey BETWEEN 911 AND 915) " +
                " OR (custkey BETWEEN 921 AND 925) " +
                " OR (custkey BETWEEN 931 AND 935) " +
                " OR (custkey BETWEEN 941 AND 945) " +
                " OR (custkey BETWEEN 951 AND 955) " +
                " OR (custkey BETWEEN 961 AND 965) " +
                " OR (custkey BETWEEN 971 AND 975) " +
                " OR (custkey BETWEEN 981 AND 985) " +
                " OR (custkey BETWEEN 991 AND 995) " +
                " OR (custkey BETWEEN 1001 AND 1005) " +
                " OR (custkey BETWEEN 1011 AND 1015) " +
                " OR (custkey BETWEEN 1021 AND 1025) " +
                " OR (custkey BETWEEN 1031 AND 1035) " +
                " OR (custkey BETWEEN 1041 AND 1045) " +
                " OR (custkey BETWEEN 1051 AND 1055) " +
                " OR (custkey BETWEEN 1061 AND 1065) " +
                " OR (custkey BETWEEN 1071 AND 1075) " +
                " OR (custkey BETWEEN 1081 AND 1085) " +
                " OR (custkey BETWEEN 1091 AND 1095) " +
                " OR (custkey BETWEEN 1101 AND 1105) " +
                " OR (custkey BETWEEN 1111 AND 1115) " +
                " OR (custkey BETWEEN 1121 AND 1125) " +
                " OR (custkey BETWEEN 1131 AND 1135) " +
                " OR (custkey BETWEEN 1141 AND 1145) " +
                " OR (custkey BETWEEN 1151 AND 1155) " +
                " OR (custkey BETWEEN 1161 AND 1165) " +
                " OR (custkey BETWEEN 1171 AND 1175) " +
                " OR (custkey BETWEEN 1181 AND 1185) " +
                " OR (custkey BETWEEN 1191 AND 1195) " +
                " OR (custkey BETWEEN 1201 AND 1205) " +
                " OR (custkey BETWEEN 1211 AND 1215) " +
                " OR (custkey BETWEEN 1221 AND 1225) " +
                " OR (custkey BETWEEN 1231 AND 1235) " +
                " OR (custkey BETWEEN 1241 AND 1245) " +
                " OR (custkey BETWEEN 1251 AND 1255) " +
                " OR (custkey BETWEEN 1261 AND 1265) " +
                " OR (custkey BETWEEN 1271 AND 1275) " +
                " OR (custkey BETWEEN 1281 AND 1285) " +
                " OR (custkey BETWEEN 1291 AND 1295) " +
                " OR (custkey BETWEEN 1301 AND 1305) " +
                " OR (custkey BETWEEN 1311 AND 1315) " +
                " OR (custkey BETWEEN 1321 AND 1325) " +
                " OR (custkey BETWEEN 1331 AND 1335) " +
                " OR (custkey BETWEEN 1341 AND 1345) " +
                " OR (custkey BETWEEN 1351 AND 1355) " +
                " OR (custkey BETWEEN 1361 AND 1365) " +
                " OR (custkey BETWEEN 1371 AND 1375) " +
                " OR (custkey BETWEEN 1381 AND 1385) " +
                " OR (custkey BETWEEN 1391 AND 1395) " +
                " OR (custkey BETWEEN 1401 AND 1405) " +
                " OR (custkey BETWEEN 1411 AND 1415) " +
                " OR (custkey BETWEEN 1421 AND 1425) " +
                " OR (custkey BETWEEN 1431 AND 1435) " +
                " OR (custkey BETWEEN 1441 AND 1445) " +
                " OR (custkey BETWEEN 1451 AND 1455) " +
                " OR (custkey BETWEEN 1461 AND 1465) " +
                " OR (custkey BETWEEN 1471 AND 1475) " +
                " OR (custkey BETWEEN 1481 AND 1485) " +
                " OR (custkey BETWEEN 1491 AND 1495) " +
                " OR (custkey BETWEEN 1501 AND 1505) " +
                " OR (custkey BETWEEN 1511 AND 1515) " +
                " OR (custkey BETWEEN 1521 AND 1525) " +
                " OR (custkey BETWEEN 1531 AND 1535) " +
                " OR (custkey BETWEEN 1541 AND 1545) " +
                " OR (custkey BETWEEN 1551 AND 1555) " +
                " OR (custkey BETWEEN 1561 AND 1565) " +
                " OR (custkey BETWEEN 1571 AND 1575) " +
                " OR (custkey BETWEEN 1581 AND 1585) " +
                " OR (custkey BETWEEN 1591 AND 1595) " +
                " OR (custkey BETWEEN 1601 AND 1605) " +
                " OR (custkey BETWEEN 1611 AND 1615) " +
                " OR (custkey BETWEEN 1621 AND 1625) " +
                " OR (custkey BETWEEN 1631 AND 1635) " +
                " OR (custkey BETWEEN 1641 AND 1645) " +
                " OR (custkey BETWEEN 1651 AND 1655) " +
                " OR (custkey BETWEEN 1661 AND 1665) " +
                " OR (custkey BETWEEN 1671 AND 1675) " +
                " OR (custkey BETWEEN 1681 AND 1685) " +
                " OR (custkey BETWEEN 1691 AND 1695) " +
                " OR (custkey BETWEEN 1701 AND 1705) " +
                " OR (custkey BETWEEN 1711 AND 1715) " +
                " OR (custkey BETWEEN 1721 AND 1725) " +
                " OR (custkey BETWEEN 1731 AND 1735) " +
                " OR (custkey BETWEEN 1741 AND 1745) " +
                " OR (custkey BETWEEN 1751 AND 1755) " +
                " OR (custkey BETWEEN 1761 AND 1765) " +
                " OR (custkey BETWEEN 1771 AND 1775) " +
                " OR (custkey BETWEEN 1781 AND 1785) " +
                " OR (custkey BETWEEN 1791 AND 1795) " +
                " OR (custkey BETWEEN 1801 AND 1805) " +
                " OR (custkey BETWEEN 1811 AND 1815) " +
                " OR (custkey BETWEEN 1821 AND 1825) " +
                " OR (custkey BETWEEN 1831 AND 1835) " +
                " OR (custkey BETWEEN 1841 AND 1845) " +
                " OR (custkey BETWEEN 1851 AND 1855) " +
                " OR (custkey BETWEEN 1861 AND 1865) " +
                " OR (custkey BETWEEN 1871 AND 1875) " +
                " OR (custkey BETWEEN 1881 AND 1885) " +
                " OR (custkey BETWEEN 1891 AND 1895) " +
                " OR (custkey BETWEEN 1901 AND 1905) " +
                " OR (custkey BETWEEN 1911 AND 1915) " +
                " OR (custkey BETWEEN 1921 AND 1925) " +
                " OR (custkey BETWEEN 1931 AND 1935) " +
                " OR (custkey BETWEEN 1941 AND 1945) " +
                " OR (custkey BETWEEN 1951 AND 1955) " +
                " OR (custkey BETWEEN 1961 AND 1965) " +
                " OR (custkey BETWEEN 1971 AND 1975) " +
                " OR (custkey BETWEEN 1981 AND 1985) " +
                " OR (custkey BETWEEN 1991 AND 1995) " +
                " OR (custkey BETWEEN 2001 AND 2005) " +
                " OR (custkey BETWEEN 2011 AND 2015) " +
                " OR (custkey BETWEEN 2021 AND 2025) " +
                " OR (custkey BETWEEN 2031 AND 2035) " +
                " OR (custkey BETWEEN 2041 AND 2045) " +
                " OR (custkey BETWEEN 2051 AND 2055) " +
                " OR (custkey BETWEEN 2061 AND 2065) " +
                " OR (custkey BETWEEN 2071 AND 2075) " +
                " OR (custkey BETWEEN 2081 AND 2085) " +
                " OR (custkey BETWEEN 2091 AND 2095) " +
                " OR (custkey BETWEEN 2101 AND 2105) " +
                " OR (custkey BETWEEN 2111 AND 2115) " +
                " OR (custkey BETWEEN 2121 AND 2125) " +
                " OR (custkey BETWEEN 2131 AND 2135) " +
                " OR (custkey BETWEEN 2141 AND 2145) " +
                " OR (custkey BETWEEN 2151 AND 2155) " +
                " OR (custkey BETWEEN 2161 AND 2165) " +
                " OR (custkey BETWEEN 2171 AND 2175) " +
                " OR (custkey BETWEEN 2181 AND 2185) " +
                " OR (custkey BETWEEN 2191 AND 2195) " +
                " OR (custkey BETWEEN 2201 AND 2205) " +
                " OR (custkey BETWEEN 2211 AND 2215) " +
                " OR (custkey BETWEEN 2221 AND 2225) " +
                " OR (custkey BETWEEN 2231 AND 2235) " +
                " OR (custkey BETWEEN 2241 AND 2245) " +
                " OR (custkey BETWEEN 2251 AND 2255) " +
                " OR (custkey BETWEEN 2261 AND 2265) " +
                " OR (custkey BETWEEN 2271 AND 2275) " +
                " OR (custkey BETWEEN 2281 AND 2285) " +
                " OR (custkey BETWEEN 2291 AND 2295) " +
                " OR (custkey BETWEEN 2301 AND 2305) " +
                " OR (custkey BETWEEN 2311 AND 2315) " +
                " OR (custkey BETWEEN 2321 AND 2325) " +
                " OR (custkey BETWEEN 2331 AND 2335) " +
                " OR (custkey BETWEEN 2341 AND 2345) " +
                " OR (custkey BETWEEN 2351 AND 2355) " +
                " OR (custkey BETWEEN 2361 AND 2365) " +
                " OR (custkey BETWEEN 2371 AND 2375) " +
                " OR (custkey BETWEEN 2381 AND 2385) " +
                " OR (custkey BETWEEN 2391 AND 2395) " +
                " OR (custkey BETWEEN 2401 AND 2405) " +
                " OR (custkey BETWEEN 2411 AND 2415) " +
                " OR (custkey BETWEEN 2421 AND 2425) " +
                " OR (custkey BETWEEN 2431 AND 2435) " +
                " OR (custkey BETWEEN 2441 AND 2445) " +
                " OR (custkey BETWEEN 2451 AND 2455) " +
                " OR (custkey BETWEEN 2461 AND 2465) " +
                " OR (custkey BETWEEN 2471 AND 2475) " +
                " OR (custkey BETWEEN 2481 AND 2485) " +
                " OR (custkey BETWEEN 2491 AND 2495) " +
                " OR (custkey BETWEEN 2501 AND 2505) " +
                " OR (custkey BETWEEN 2511 AND 2515) " +
                " OR (custkey BETWEEN 2521 AND 2525) " +
                " OR (custkey BETWEEN 2531 AND 2535) " +
                " OR (custkey BETWEEN 2541 AND 2545) " +
                " OR (custkey BETWEEN 2551 AND 2555) " +
                " OR (custkey BETWEEN 2561 AND 2565) " +
                " OR (custkey BETWEEN 2571 AND 2575) " +
                " OR (custkey BETWEEN 2581 AND 2585) " +
                " OR (custkey BETWEEN 2591 AND 2595) " +
                " OR (custkey BETWEEN 2601 AND 2605) " +
                " OR (custkey BETWEEN 2611 AND 2615) " +
                " OR (custkey BETWEEN 2621 AND 2625) " +
                " OR (custkey BETWEEN 2631 AND 2635) " +
                " OR (custkey BETWEEN 2641 AND 2645) " +
                " OR (custkey BETWEEN 2651 AND 2655) " +
                " OR (custkey BETWEEN 2661 AND 2665) " +
                " OR (custkey BETWEEN 2671 AND 2675) " +
                " OR (custkey BETWEEN 2681 AND 2685) " +
                " OR (custkey BETWEEN 2691 AND 2695) " +
                " OR (custkey BETWEEN 2701 AND 2705) " +
                " OR (custkey BETWEEN 2711 AND 2715) " +
                " OR (custkey BETWEEN 2721 AND 2725) " +
                " OR (custkey BETWEEN 2731 AND 2735) " +
                " OR (custkey BETWEEN 2741 AND 2745) " +
                " OR (custkey BETWEEN 2751 AND 2755) " +
                " OR (custkey BETWEEN 2761 AND 2765) " +
                " OR (custkey BETWEEN 2771 AND 2775) " +
                " OR (custkey BETWEEN 2781 AND 2785) " +
                " OR (custkey BETWEEN 2791 AND 2795) " +
                " OR (custkey BETWEEN 2801 AND 2805) " +
                " OR (custkey BETWEEN 2811 AND 2815) " +
                " OR (custkey BETWEEN 2821 AND 2825) " +
                " OR (custkey BETWEEN 2831 AND 2835) " +
                " OR (custkey BETWEEN 2841 AND 2845) " +
                " OR (custkey BETWEEN 2851 AND 2855) " +
                " OR (custkey BETWEEN 2861 AND 2865) " +
                " OR (custkey BETWEEN 2871 AND 2875) " +
                " OR (custkey BETWEEN 2881 AND 2885) " +
                " OR (custkey BETWEEN 2891 AND 2895) " +
                " OR (custkey BETWEEN 2901 AND 2905) " +
                " OR (custkey BETWEEN 2911 AND 2915) " +
                " OR (custkey BETWEEN 2921 AND 2925) " +
                " OR (custkey BETWEEN 2931 AND 2935) " +
                " OR (custkey BETWEEN 2941 AND 2945) " +
                " OR (custkey BETWEEN 2951 AND 2955) " +
                " OR (custkey BETWEEN 2961 AND 2965) " +
                " OR (custkey BETWEEN 2971 AND 2975) " +
                " OR (custkey BETWEEN 2981 AND 2985) " +
                " OR (custkey BETWEEN 2991 AND 2995) " +
                " OR (custkey BETWEEN 3001 AND 3005) " +
                " OR (custkey BETWEEN 3011 AND 3015) " +
                " OR (custkey BETWEEN 3021 AND 3025) " +
                " OR (custkey BETWEEN 3031 AND 3035) " +
                " OR (custkey BETWEEN 3041 AND 3045) " +
                " OR (custkey BETWEEN 3051 AND 3055) " +
                " OR (custkey BETWEEN 3061 AND 3065) " +
                " OR (custkey BETWEEN 3071 AND 3075) " +
                " OR (custkey BETWEEN 3081 AND 3085) " +
                " OR (custkey BETWEEN 3091 AND 3095) " +
                " OR (custkey BETWEEN 3101 AND 3105) " +
                " OR (custkey BETWEEN 3111 AND 3115) " +
                " OR (custkey BETWEEN 3121 AND 3125) " +
                " OR (custkey BETWEEN 3131 AND 3135) " +
                " OR (custkey BETWEEN 3141 AND 3145) " +
                " OR (custkey BETWEEN 3151 AND 3155) " +
                " OR (custkey BETWEEN 3161 AND 3165) " +
                " OR (custkey BETWEEN 3171 AND 3175) " +
                " OR (custkey BETWEEN 3181 AND 3185) " +
                " OR (custkey BETWEEN 3191 AND 3195) " +
                " OR (custkey BETWEEN 3201 AND 3205) " +
                " OR (custkey BETWEEN 3211 AND 3215) " +
                " OR (custkey BETWEEN 3221 AND 3225) " +
                " OR (custkey BETWEEN 3231 AND 3235) " +
                " OR (custkey BETWEEN 3241 AND 3245) " +
                " OR (custkey BETWEEN 3251 AND 3255) " +
                " OR (custkey BETWEEN 3261 AND 3265) " +
                " OR (custkey BETWEEN 3271 AND 3275) " +
                " OR (custkey BETWEEN 3281 AND 3285) " +
                " OR (custkey BETWEEN 3291 AND 3295) " +
                " OR (custkey BETWEEN 3301 AND 3305) " +
                " OR (custkey BETWEEN 3311 AND 3315) " +
                " OR (custkey BETWEEN 3321 AND 3325) " +
                " OR (custkey BETWEEN 3331 AND 3335) " +
                " OR (custkey BETWEEN 3341 AND 3345) " +
                " OR (custkey BETWEEN 3351 AND 3355) " +
                " OR (custkey BETWEEN 3361 AND 3365) " +
                " OR (custkey BETWEEN 3371 AND 3375) " +
                " OR (custkey BETWEEN 3381 AND 3385) " +
                " OR (custkey BETWEEN 3391 AND 3395) " +
                " OR (custkey BETWEEN 3401 AND 3405) " +
                " OR (custkey BETWEEN 3411 AND 3415) " +
                " OR (custkey BETWEEN 3421 AND 3425) " +
                " OR (custkey BETWEEN 3431 AND 3435) " +
                " OR (custkey BETWEEN 3441 AND 3445) " +
                " OR (custkey BETWEEN 3451 AND 3455) " +
                " OR (custkey BETWEEN 3461 AND 3465) " +
                " OR (custkey BETWEEN 3471 AND 3475) " +
                " OR (custkey BETWEEN 3481 AND 3485) " +
                " OR (custkey BETWEEN 3491 AND 3495) " +
                " OR (custkey BETWEEN 3501 AND 3505) " +
                " OR (custkey BETWEEN 3511 AND 3515) " +
                " OR (custkey BETWEEN 3521 AND 3525) " +
                " OR (custkey BETWEEN 3531 AND 3535) " +
                " OR (custkey BETWEEN 3541 AND 3545) " +
                " OR (custkey BETWEEN 3551 AND 3555) " +
                " OR (custkey BETWEEN 3561 AND 3565) " +
                " OR (custkey BETWEEN 3571 AND 3575) " +
                " OR (custkey BETWEEN 3581 AND 3585) " +
                " OR (custkey BETWEEN 3591 AND 3595) " +
                " OR (custkey BETWEEN 3601 AND 3605) " +
                " OR (custkey BETWEEN 3611 AND 3615) " +
                " OR (custkey BETWEEN 3621 AND 3625) " +
                " OR (custkey BETWEEN 3631 AND 3635) " +
                " OR (custkey BETWEEN 3641 AND 3645) " +
                " OR (custkey BETWEEN 3651 AND 3655) " +
                " OR (custkey BETWEEN 3661 AND 3665) " +
                " OR (custkey BETWEEN 3671 AND 3675) " +
                " OR (custkey BETWEEN 3681 AND 3685) " +
                " OR (custkey BETWEEN 3691 AND 3695) " +
                " OR (custkey BETWEEN 3701 AND 3705) " +
                " OR (custkey BETWEEN 3711 AND 3715) " +
                " OR (custkey BETWEEN 3721 AND 3725) " +
                " OR (custkey BETWEEN 3731 AND 3735) " +
                " OR (custkey BETWEEN 3741 AND 3745) " +
                " OR (custkey BETWEEN 3751 AND 3755) " +
                " OR (custkey BETWEEN 3761 AND 3765) " +
                " OR (custkey BETWEEN 3771 AND 3775) " +
                " OR (custkey BETWEEN 3781 AND 3785) " +
                " OR (custkey BETWEEN 3791 AND 3795) " +
                " OR (custkey BETWEEN 3801 AND 3805) " +
                " OR (custkey BETWEEN 3811 AND 3815) " +
                " OR (custkey BETWEEN 3821 AND 3825) " +
                " OR (custkey BETWEEN 3831 AND 3835) " +
                " OR (custkey BETWEEN 3841 AND 3845) " +
                " OR (custkey BETWEEN 3851 AND 3855) " +
                " OR (custkey BETWEEN 3861 AND 3865) " +
                " OR (custkey BETWEEN 3871 AND 3875) " +
                " OR (custkey BETWEEN 3881 AND 3885) " +
                " OR (custkey BETWEEN 3891 AND 3895) " +
                " OR (custkey BETWEEN 3901 AND 3905) " +
                " OR (custkey BETWEEN 3911 AND 3915) " +
                " OR (custkey BETWEEN 3921 AND 3925) " +
                " OR (custkey BETWEEN 3931 AND 3935) " +
                " OR (custkey BETWEEN 3941 AND 3945) " +
                " OR (custkey BETWEEN 3951 AND 3955) " +
                " OR (custkey BETWEEN 3961 AND 3965) " +
                " OR (custkey BETWEEN 3971 AND 3975) " +
                " OR (custkey BETWEEN 3981 AND 3985) " +
                " OR (custkey BETWEEN 3991 AND 3995) " +
                " OR (custkey BETWEEN 4001 AND 4005) " +
                " OR (custkey BETWEEN 4011 AND 4015) " +
                " OR (custkey BETWEEN 4021 AND 4025) " +
                " OR (custkey BETWEEN 4031 AND 4035) " +
                " OR (custkey BETWEEN 4041 AND 4045) " +
                " OR (custkey BETWEEN 4051 AND 4055) " +
                " OR (custkey BETWEEN 4061 AND 4065) " +
                " OR (custkey BETWEEN 4071 AND 4075) " +
                " OR (custkey BETWEEN 4081 AND 4085) " +
                " OR (custkey BETWEEN 4091 AND 4095) " +
                " OR (custkey BETWEEN 4101 AND 4105) " +
                " OR (custkey BETWEEN 4111 AND 4115) " +
                " OR (custkey BETWEEN 4121 AND 4125) " +
                " OR (custkey BETWEEN 4131 AND 4135) " +
                " OR (custkey BETWEEN 4141 AND 4145) " +
                " OR (custkey BETWEEN 4151 AND 4155) " +
                " OR (custkey BETWEEN 4161 AND 4165) " +
                " OR (custkey BETWEEN 4171 AND 4175) " +
                " OR (custkey BETWEEN 4181 AND 4185) " +
                " OR (custkey BETWEEN 4191 AND 4195) " +
                " OR (custkey BETWEEN 4201 AND 4205) " +
                " OR (custkey BETWEEN 4211 AND 4215) " +
                " OR (custkey BETWEEN 4221 AND 4225) " +
                " OR (custkey BETWEEN 4231 AND 4235) " +
                " OR (custkey BETWEEN 4241 AND 4245) " +
                " OR (custkey BETWEEN 4251 AND 4255) " +
                " OR (custkey BETWEEN 4261 AND 4265) " +
                " OR (custkey BETWEEN 4271 AND 4275) " +
                " OR (custkey BETWEEN 4281 AND 4285) " +
                " OR (custkey BETWEEN 4291 AND 4295) " +
                " OR (custkey BETWEEN 4301 AND 4305) " +
                " OR (custkey BETWEEN 4311 AND 4315) " +
                " OR (custkey BETWEEN 4321 AND 4325) " +
                " OR (custkey BETWEEN 4331 AND 4335) " +
                " OR (custkey BETWEEN 4341 AND 4345) " +
                " OR (custkey BETWEEN 4351 AND 4355) " +
                " OR (custkey BETWEEN 4361 AND 4365) " +
                " OR (custkey BETWEEN 4371 AND 4375) " +
                " OR (custkey BETWEEN 4381 AND 4385) " +
                " OR (custkey BETWEEN 4391 AND 4395) " +
                " OR (custkey BETWEEN 4401 AND 4405) " +
                " OR (custkey BETWEEN 4411 AND 4415) " +
                " OR (custkey BETWEEN 4421 AND 4425) " +
                " OR (custkey BETWEEN 4431 AND 4435) " +
                " OR (custkey BETWEEN 4441 AND 4445) " +
                " OR (custkey BETWEEN 4451 AND 4455) " +
                " OR (custkey BETWEEN 4461 AND 4465) " +
                " OR (custkey BETWEEN 4471 AND 4475) " +
                " OR (custkey BETWEEN 4481 AND 4485) " +
                " OR (custkey BETWEEN 4491 AND 4495) " +
                " OR (custkey BETWEEN 4501 AND 4505) " +
                " OR (custkey BETWEEN 4511 AND 4515) " +
                " OR (custkey BETWEEN 4521 AND 4525) " +
                " OR (custkey BETWEEN 4531 AND 4535) " +
                " OR (custkey BETWEEN 4541 AND 4545) " +
                " OR (custkey BETWEEN 4551 AND 4555) " +
                " OR (custkey BETWEEN 4561 AND 4565) " +
                " OR (custkey BETWEEN 4571 AND 4575) " +
                " OR (custkey BETWEEN 4581 AND 4585) " +
                " OR (custkey BETWEEN 4591 AND 4595) " +
                " OR (custkey BETWEEN 4601 AND 4605) " +
                " OR (custkey BETWEEN 4611 AND 4615) " +
                " OR (custkey BETWEEN 4621 AND 4625) " +
                " OR (custkey BETWEEN 4631 AND 4635) " +
                " OR (custkey BETWEEN 4641 AND 4645) " +
                " OR (custkey BETWEEN 4651 AND 4655) " +
                " OR (custkey BETWEEN 4661 AND 4665) " +
                " OR (custkey BETWEEN 4671 AND 4675) " +
                " OR (custkey BETWEEN 4681 AND 4685) " +
                " OR (custkey BETWEEN 4691 AND 4695) " +
                " OR (custkey BETWEEN 4701 AND 4705) " +
                " OR (custkey BETWEEN 4711 AND 4715) " +
                " OR (custkey BETWEEN 4721 AND 4725) " +
                " OR (custkey BETWEEN 4731 AND 4735) " +
                " OR (custkey BETWEEN 4741 AND 4745) " +
                " OR (custkey BETWEEN 4751 AND 4755) " +
                " OR (custkey BETWEEN 4761 AND 4765) " +
                " OR (custkey BETWEEN 4771 AND 4775) " +
                " OR (custkey BETWEEN 4781 AND 4785) " +
                " OR (custkey BETWEEN 4791 AND 4795) " +
                " OR (custkey BETWEEN 4801 AND 4805) " +
                " OR (custkey BETWEEN 4811 AND 4815) " +
                " OR (custkey BETWEEN 4821 AND 4825) " +
                " OR (custkey BETWEEN 4831 AND 4835) " +
                " OR (custkey BETWEEN 4841 AND 4845) " +
                " OR (custkey BETWEEN 4851 AND 4855) " +
                " OR (custkey BETWEEN 4861 AND 4865) " +
                " OR (custkey BETWEEN 4871 AND 4875) " +
                " OR (custkey BETWEEN 4881 AND 4885) " +
                " OR (custkey BETWEEN 4891 AND 4895) " +
                " OR (custkey BETWEEN 4901 AND 4905) " +
                " OR (custkey BETWEEN 4911 AND 4915) " +
                " OR (custkey BETWEEN 4921 AND 4925) " +
                " OR (custkey BETWEEN 4931 AND 4935) " +
                " OR (custkey BETWEEN 4941 AND 4945) " +
                " OR (custkey BETWEEN 4951 AND 4955) " +
                " OR (custkey BETWEEN 4961 AND 4965) " +
                " OR (custkey BETWEEN 4971 AND 4975) " +
                " OR (custkey BETWEEN 4981 AND 4985) " +
                " OR (custkey BETWEEN 4991 AND 4995)";
        MaterializedResult result = computeActual(sql);
        System.out.println("Query Output: " + result);
    }
}
