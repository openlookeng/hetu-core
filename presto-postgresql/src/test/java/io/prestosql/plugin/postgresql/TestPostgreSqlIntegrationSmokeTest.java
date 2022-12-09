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
package io.prestosql.plugin.postgresql;

import io.airlift.testing.postgresql.TestingPostgreSqlServer;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestPostgreSqlIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingPostgreSqlServer postgreSqlServer;
    private final TestPostgreSqlExtendServer extendServer;

    public TestPostgreSqlIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingPostgreSqlServer("testuser", "tpch"));
    }

    public TestPostgreSqlIntegrationSmokeTest(TestingPostgreSqlServer postgreSqlServer)
            throws Exception
    {
        super(() -> PostgreSqlQueryRunner.createPostgreSqlQueryRunner(postgreSqlServer, ORDERS));
        this.postgreSqlServer = postgreSqlServer;
        this.extendServer = null;
        execute("CREATE EXTENSION file_fdw");
    }

    public TestPostgreSqlIntegrationSmokeTest(QueryRunnerSupplier supplier, TestPostgreSqlExtendServer postgreSqlServer)
    {
        super(supplier);
        this.postgreSqlServer = null;
        this.extendServer = postgreSqlServer;
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws IOException
    {
        if (extendServer != null) {
            extendServer.close();
        }
        else {
            postgreSqlServer.close();
        }
    }

    @Test
    public void testDropTable()
    {
        assertUpdate("CREATE TABLE test_drop AS SELECT 123 x", 1);
        assertTrue(getQueryRunner().tableExists(getSession(), "test_drop"));

        assertUpdate("DROP TABLE test_drop");
        assertFalse(getQueryRunner().tableExists(getSession(), "test_drop"));
    }

    @Test
    public void testUpdateByOneField()
            throws SQLException
    {
        execute("DROP TABLE IF EXISTS tpch.test_update");
        execute("CREATE TABLE tpch.test_update (id serial primary key, name varchar, sex char, age int, score varchar, birthday date, salary double precision)");
        assertUpdate("INSERT INTO test_update VALUES(1, 'Bob', '1', 24, 'excellent', date'1997-09-28', 40000)", 1);
        assertUpdate("INSERT INTO test_update VALUES(2, 'Jack', '1', 25, 'good', date'1996-08-14', 35000)", 1);
        assertUpdate("INSERT INTO test_update VALUES(3, 'Rose', '2', 22, 'excellent', date'1999-07-11', 10000)", 1);

        assertQuery("SELECT * FROM test_update where id = 1", "VALUES(1, 'Bob', '1', 24, 'excellent', date'1997-09-28', 40000)");
        assertUpdate("UPDATE test_update SET name = 'Kitty', age = 26, birthday = date'1995-08-16' where id = 1", 1);
        assertQuery("SELECT * FROM test_update WHERE id = 1", "VALUES(1, 'Kitty', '1', 26, 'excellent', date'1995-08-16', 40000)");

        assertQuery("SELECT * FROM test_update WHERE score = 'good'", "VALUES(2, 'Jack', '1', 25, 'good', date'1996-08-14', 35000)");
        assertUpdate("UPDATE test_update SET name = 'Jane', sex = '2' where score = 'good'", 1);
        assertQuery("SELECT * FROM test_update WHERE score = 'good'", "VALUES(2, 'Jane', '2', 25, 'good', date'1996-08-14', 35000)");

        assertQuery("SELECT * FROM test_update WHERE salary = 10000", "VALUES(3, 'Rose', '2', 22, 'excellent', date'1999-07-11', 10000)");
        assertUpdate("UPDATE test_update SET score = 'qualified' where salary = 10000.00 ", 1);
        assertQuery("SELECT * FROM test_update WHERE salary = 10000", "VALUES(3, 'Rose', '2', 22, 'qualified', date'1999-07-11', 10000)");

        assertQuery("SELECT * FROM test_update WHERE age = 22", "VALUES(3, 'Rose', '2', 22, 'qualified', date'1999-07-11', 10000)");
        assertUpdate("UPDATE test_update SET age = age + 1, birthday = date'1998-08-16' where age = 22 ", 1);
        assertQuery("SELECT * FROM test_update WHERE age = 23", "VALUES(3, 'Rose', '2', 23, 'qualified', date'1998-08-16', 10000)");

        assertQuery("SELECT * FROM test_update WHERE birthday = date'1996-08-14'", "VALUES(2, 'Jane', '2', 25, 'good', date'1996-08-14', 35000)");
        assertUpdate("UPDATE test_update SET salary = 36000 where birthday = date'1996-08-14'", 1);
        assertQuery("SELECT * FROM test_update WHERE birthday = date'1996-08-14'", "VALUES(2, 'Jane', '2', 25, 'good', date'1996-08-14', 36000)");

        assertQuery("SELECT * FROM test_update WHERE name = 'Jane'", "VALUES(2, 'Jane', '2', 25, 'good', date'1996-08-14', 36000)");
        assertUpdate("UPDATE test_update SET score = 'bad', birthday = date'1995-10-16' where name = 'Jane'", 1);
        assertQuery("SELECT * FROM test_update WHERE name = 'Jane'", "VALUES(2, 'Jane', '2', 25, 'bad', date'1995-10-16', 36000)");

        assertUpdate("DROP TABLE test_update");
    }

    @Test
    public void testUpdateByMutiField()
            throws SQLException
    {
        execute("CREATE TABLE tpch.test_update (name varchar, id int, score varchar)");
        assertUpdate("INSERT INTO test_update VALUES('Bob', 1, 'excellent')", 1);
        assertUpdate("INSERT INTO test_update VALUES('Tim', 2, 'good')", 1);
        assertUpdate("INSERT INTO test_update VALUES('Jane', 3, 'good')", 1);
        assertUpdate("INSERT INTO test_update VALUES('Rose', 4, 'bad')", 1);
        assertUpdate("INSERT INTO test_update VALUES('Petty', 5, 'good')", 1);

        assertQuery("SELECT * FROM test_update WHERE id = 4", "VALUES('Rose', 4, 'bad')");
        assertQuery("SELECT * FROM test_update WHERE id = 5", "VALUES('Petty', 5, 'good')");
        assertUpdate("UPDATE test_update SET name = 'Kitt' WHERE id IN (4, 5) AND NAME LIKE 'P%'", 1);
        assertQuery("SELECT * FROM test_update WHERE id = 4", "VALUES('Rose', 4, 'bad')");
        assertQuery("SELECT * FROM test_update WHERE id = 5", "VALUES('Kitt', 5, 'good')");

        assertQuery("SELECT * FROM test_update WHERE id = 2", "VALUES('Tim', 2, 'good')");
        assertQuery("SELECT * FROM test_update WHERE id = 3", "VALUES('Jane', 3, 'good')");
        assertQuery("SELECT * FROM test_update WHERE id = 4", "VALUES('Rose', 4, 'bad')");
        assertUpdate("UPDATE test_update SET score = 'excellent' WHERE id BETWEEN 2 And 4 AND score != 'bad'", 2);
        assertQuery("SELECT * FROM test_update WHERE id = 2", "VALUES('Tim', 2, 'excellent')");
        assertQuery("SELECT * FROM test_update WHERE id = 3", "VALUES('Jane', 3, 'excellent')");
        assertQuery("SELECT * FROM test_update WHERE id = 4", "VALUES('Rose', 4, 'bad')");

        assertUpdate("DROP TABLE test_update");
    }

    @Test
    public void testDeleteByOneField()
            throws SQLException
    {
        execute("CREATE TABLE tpch.test_delete (id serial primary key, name varchar, sex char, age int, score varchar, birthday date, salary double precision)");
        assertUpdate("INSERT INTO test_delete VALUES(1, 'Bob', '1', 24, 'excellent', date'1997-09-28', 40000)", 1);
        assertUpdate("INSERT INTO test_delete VALUES(2, 'Jack', '1', 25, 'good', date'1996-08-14', 35000)", 1);
        assertUpdate("INSERT INTO test_delete VALUES(3, 'Jane', '2', 23, 'bad', date'1998-07-25', 15000)", 1);
        assertUpdate("INSERT INTO test_delete VALUES(4, 'Rose', '2', 22, 'excellent', date'1999-07-11', 10000)", 1);

        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(4)");
        assertUpdate("DELETE FROM test_delete WHERE name = 'Bob'", 1);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(3)");
        assertUpdate("DELETE FROM test_delete WHERE birthday = date'1996-08-14'", 1);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(2)");
        assertUpdate("DELETE FROM test_delete WHERE salary = 15000", 1);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(1)");
        assertUpdate("DROP TABLE test_delete");
    }

    @Test
    public void testDeleteByMutiField()
            throws SQLException
    {
        execute("CREATE TABLE tpch.test_delete (name varchar, id int, score varchar)");
        assertUpdate("INSERT INTO test_delete VALUES('Bob', 1, 'excellent')", 1);
        assertUpdate("INSERT INTO test_delete VALUES('Tim', 2, 'good')", 1);
        assertUpdate("INSERT INTO test_delete VALUES('Jane', 3, 'good')", 1);
        assertUpdate("INSERT INTO test_delete VALUES('Rose', 4, 'bad')", 1);
        assertUpdate("INSERT INTO test_delete VALUES('Petty', 5, 'good')", 1);

        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(5)");
        assertUpdate("DELETE FROM test_delete WHERE id BETWEEN 2 AND 4 AND name IN ('Rose', 'Petty')", 1);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(4)");
        assertUpdate("DELETE FROM test_delete WHERE name = 'Petty' AND score = 'good'", 1);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(3)");
        assertUpdate("DELETE FROM test_delete WHERE name LIKE '%o%' OR score = 'good'", 3);
        assertQuery("SELECT COUNT() FROM test_delete", "VALUES(0)");
        assertUpdate("DROP TABLE test_delete");
    }

    @Test
    public void testInsert()
            throws Exception
    {
        execute("CREATE TABLE tpch.test_insert (x bigint, y varchar(100))");
        assertUpdate("INSERT INTO test_insert VALUES (123, 'test')", 1);
        assertQuery("SELECT * FROM test_insert", "SELECT 123 x, 'test' y");
        assertUpdate("DROP TABLE test_insert");
    }

    @Test
    public void testViews()
            throws Exception
    {
        execute("CREATE OR REPLACE VIEW tpch.test_view AS SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_view"));
        assertQuery("SELECT orderkey FROM test_view", "SELECT orderkey FROM orders");
        execute("DROP VIEW IF EXISTS tpch.test_view");
    }

    @Test
    public void testMaterializedView()
            throws Exception
    {
        execute("CREATE MATERIALIZED VIEW tpch.test_mv as SELECT * FROM tpch.orders");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_mv"));
        assertQuery("SELECT orderkey FROM test_mv", "SELECT orderkey FROM orders");
        execute("DROP MATERIALIZED VIEW tpch.test_mv");
    }

    @Test
    public void testForeignTable()
            throws Exception
    {
        execute("CREATE SERVER devnull FOREIGN DATA WRAPPER file_fdw");
        execute("CREATE FOREIGN TABLE tpch.test_ft (x bigint) SERVER devnull OPTIONS (filename '/dev/null')");
        assertTrue(getQueryRunner().tableExists(getSession(), "test_ft"));
        computeActual("SELECT * FROM test_ft");
        execute("DROP FOREIGN TABLE tpch.test_ft");
        execute("DROP SERVER devnull");
    }

    @Test
    public void testTableWithNoSupportedColumns()
            throws Exception
    {
        String unsupportedDataType = "interval";
        String supportedDataType = "varchar(5)";

        try (AutoCloseable ignore1 = withTable("tpch.no_supported_columns", format("(c %s)", unsupportedDataType));
                AutoCloseable ignore2 = withTable("tpch.supported_columns", format("(good %s)", supportedDataType));
                AutoCloseable ignore3 = withTable("tpch.no_columns", "()")) {
            assertThat(computeActual("SHOW TABLES").getOnlyColumnAsSet()).contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            assertQueryFails("SELECT c FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT * FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_supported_columns", "Table 'tpch.no_supported_columns' not found");

            assertQueryFails("SELECT c FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT * FROM no_columns", "Table 'tpch.no_columns' not found");
            assertQueryFails("SELECT 'a' FROM no_columns", "Table 'tpch.no_columns' not found");

            assertQueryFails("SELECT c FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT * FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");
            assertQueryFails("SELECT 'a' FROM non_existent", ".* Table .*tpch.non_existent.* does not exist");

            assertQuery("SHOW COLUMNS FROM no_supported_columns", "SELECT 'nothing' WHERE false");
            assertQuery("SHOW COLUMNS FROM no_columns", "SELECT 'nothing' WHERE false");

            // Other tables should be visible in SHOW TABLES (the no_supported_columns might be included or might be not) and information_schema.tables
            assertThat(computeActual("SHOW TABLES").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");
            assertThat(computeActual("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch'").getOnlyColumn())
                    .contains("orders", "no_supported_columns", "supported_columns", "no_columns");

            // Other tables should be introspectable with SHOW COLUMNS and information_schema.columns
            assertQuery("SHOW COLUMNS FROM supported_columns", "VALUES ('good', 'varchar(5)', '', '')");

            // Listing columns in all tables should not fail due to tables with no columns
            computeActual("SELECT column_name FROM information_schema.columns WHERE table_schema = 'tpch'");
        }
    }

    @Test
    public void testInsertWithFailureDoesntLeaveBehindOrphanedTable()
            throws Exception
    {
        String schemaName = format("tmp_schema_%s", UUID.randomUUID().toString().replaceAll("-", ""));
        try (AutoCloseable schema = withSchema(schemaName);
                AutoCloseable table = withTable(format("%s.test_cleanup", schemaName), "(x INTEGER)")) {
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");

            execute(format("ALTER TABLE %s.test_cleanup ADD CHECK (x > 0)", schemaName));

            assertQueryFails(format("INSERT INTO %s.test_cleanup (x) VALUES (0)", schemaName), "ERROR: new row .* violates check constraint [\\s\\S]*");
            assertQuery(format("SELECT table_name FROM information_schema.tables WHERE table_schema = '%s'", schemaName), "VALUES 'test_cleanup'");
        }
    }

    @Test
    public void testDecimalPredicatePushdown()
            throws Exception
    {
        // TODO test that that predicate is actually pushed down (here we test only correctness)
        try (AutoCloseable ignoreTable = withTable("tpch.test_decimal_pushdown",
                "(short_decimal decimal(9, 3), long_decimal decimal(30, 10))")) {
            execute("INSERT INTO tpch.test_decimal_pushdown VALUES (123.321, 123456789.987654321)");

            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 124",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456790",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal <= 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal <= 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE short_decimal = 123.321",
                    "VALUES (123.321, 123456789.987654321)");
            assertQuery("SELECT * FROM tpch.test_decimal_pushdown WHERE long_decimal = 123456789.987654321",
                    "VALUES (123.321, 123456789.987654321)");
        }
    }

    @Test
    public void testCharPredicatePushdown()
            throws Exception
    {
        // TODO test that that predicate is actually pushed down (here we test only correctness)
        try (AutoCloseable ignoreTable = withTable("tpch.test_char_pushdown",
                "(char_1 char(1), char_5 char(5), char_10 char(10))")) {
            execute("INSERT INTO tpch.test_char_pushdown VALUES" +
                    "('0', '0'    , '0'         )," +
                    "('1', '12345', '1234567890')");

            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_1 = '0' AND char_5 = '0'",
                    "VALUES ('0', '0    ', '0         ')");
            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_5 = CHAR'12345' AND char_10 = '1234567890'",
                    "VALUES ('1', '12345', '1234567890')");
            assertQuery("SELECT * FROM tpch.test_char_pushdown WHERE char_10 = CHAR'0'",
                    "VALUES ('0', '0    ', '0         ')");
        }
    }

    @Test
    public void testCharTrailingSpace()
            throws Exception
    {
        execute("CREATE TABLE tpch.char_trailing_space (x char(10))");
        assertUpdate("INSERT INTO char_trailing_space VALUES ('test')", 1);

        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test'", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test  '", "VALUES 'test'");
        assertQuery("SELECT * FROM char_trailing_space WHERE x = char 'test        '", "VALUES 'test'");

        assertEquals(getQueryRunner().execute("SELECT * FROM char_trailing_space WHERE x = char ' test'").getRowCount(), 0);

        assertUpdate("DROP TABLE char_trailing_space");
    }

    @Test
    public void testInsertIntoNotNullColumn()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (date '2012-12-31')", "(?s).*null value in column \"column_b\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (date '2012-12-31', null)", "(?s).*null value in column \"column_b\" violates not-null constraint.*");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a date,\n" +
                        "   column_b date NOT NULL,\n" +
                        "   column_c bigint NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (date '2012-12-31')", "(?s).*null value in column \"column_c\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', null)", "(?s).*null value in column \"column_c\" violates not-null constraint.*");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (date '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (date '2013-01-01', date '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS DATE), 1), (CAST('2013-01-01' AS DATE), CAST('2013-01-02' AS DATE), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    private AutoCloseable withSchema(String schema)
            throws Exception
    {
        execute(format("CREATE SCHEMA %s", schema));
        return () -> {
            try {
                execute(format("DROP SCHEMA %s", schema));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private AutoCloseable withTable(String tableName, String tableDefinition)
            throws Exception
    {
        execute(format("CREATE TABLE %s%s", tableName, tableDefinition));
        return () -> {
            try {
                execute(format("DROP TABLE %s", tableName));
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void execute(String sql)
            throws SQLException
    {
        try {
            if (extendServer != null) {
                extendServer.execute(sql);
            }
            else {
                try (Connection connection = DriverManager.getConnection(postgreSqlServer.getJdbcUrl());
                        Statement statement = connection.createStatement()) {
                    statement.execute(sql);
                }
            }
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to execute statement: " + sql, e);
        }
    }
}
