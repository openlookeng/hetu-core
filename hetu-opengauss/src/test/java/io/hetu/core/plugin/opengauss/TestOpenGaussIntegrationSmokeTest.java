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

package io.hetu.core.plugin.opengauss;

import io.prestosql.plugin.postgresql.TestPostgreSqlIntegrationSmokeTest;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.sql.SQLException;

import static io.airlift.tpch.TpchTable.ORDERS;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

@Test
public class TestOpenGaussIntegrationSmokeTest
        extends TestPostgreSqlIntegrationSmokeTest
{
    public TestOpenGaussIntegrationSmokeTest()
            throws Exception
    {
        this(new TestOpenGaussServer());
    }

    public TestOpenGaussIntegrationSmokeTest(TestOpenGaussServer openGaussServer)
            throws Exception
    {
        super(() -> OpenGaussQueryRunner.createOpenGaussQueryRunner(openGaussServer, ORDERS),
                openGaussServer);
    }

    @Test
    @Override
    public void testUpdateByOneField()
            throws SQLException
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_update (\n" +
                        "id int,\n" +
                        "name varchar,\n" +
                        "sex char,\n" +
                        "age int,\n" +
                        "score varchar,\n" +
                        "birthday date,\n" +
                        "salary double precision\n" +
                        ")",
                getSession().getCatalog().get());

        assertUpdate(createTableSql);
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
    @Override
    public void testUpdateByMutiField()
            throws SQLException
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_update (\n" +
                        "name varchar,\n" +
                        "id int,\n" +
                        "score varchar\n" +
                        ")",
                getSession().getCatalog().get());

        assertUpdate(createTableSql);
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
    @Override
    public void testDeleteByOneField()
            throws SQLException
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_delete (\n" +
                        "id int,\n" +
                        "name varchar,\n" +
                        "sex char,\n" +
                        "age int,\n" +
                        "score varchar,\n" +
                        "birthday date,\n" +
                        "salary double precision\n" +
                        ")",
                getSession().getCatalog().get());
        assertUpdate(createTableSql);
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
    @Override
    public void testDeleteByMutiField()
            throws SQLException
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_delete (\n" +
                        "name varchar,\n" +
                        "id int,\n" +
                        "score varchar\n" +
                        ")",
                getSession().getCatalog().get());

        assertUpdate(createTableSql);
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
    @Override
    public void testMaterializedView()
    {
        // not supported
    }

    @Test
    @Override
    public void testTableWithNoSupportedColumns()
    {
        // not supported
    }

    @Test
    @Override
    public void testInsertWithFailureDoesntLeaveBehindOrphanedTable()
    {
        // not supported
    }

    @Test
    @Override
    public void testInsertIntoNotNullColumn()
    {
        @Language("SQL") String createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a varchar,\n" +
                        "   column_b varchar NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertUpdate(createTableSql);
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_a) VALUES (varchar '2012-12-31')", "(?s).*null value in column \"column_b\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_a, column_b) VALUES (varchar '2012-12-31', null)", "(?s).*null value in column \"column_b\" violates not-null constraint.*");

        assertUpdate("ALTER TABLE test_insert_not_null ADD COLUMN column_c BIGINT NOT NULL");

        createTableSql = format("" +
                        "CREATE TABLE %s.tpch.test_insert_not_null (\n" +
                        "   column_a varchar,\n" +
                        "   column_b varchar NOT NULL,\n" +
                        "   column_c bigint NOT NULL\n" +
                        ")",
                getSession().getCatalog().get());
        assertEquals(computeScalar("SHOW CREATE TABLE test_insert_not_null"), createTableSql);

        assertQueryFails("INSERT INTO test_insert_not_null (column_b) VALUES (varchar '2012-12-31')", "(?s).*null value in column \"column_c\" violates not-null constraint.*");
        assertQueryFails("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (varchar '2012-12-31', null)", "(?s).*null value in column \"column_c\" violates not-null constraint.*");

        assertUpdate("INSERT INTO test_insert_not_null (column_b, column_c) VALUES (varchar '2012-12-31', 1)", 1);
        assertUpdate("INSERT INTO test_insert_not_null (column_a, column_b, column_c) VALUES (varchar '2013-01-01', varchar '2013-01-02', 2)", 1);
        assertQuery(
                "SELECT * FROM test_insert_not_null",
                "VALUES (NULL, CAST('2012-12-31' AS varchar), 1), (CAST('2013-01-01' AS varchar), CAST('2013-01-02' AS varchar), 2)");

        assertUpdate("DROP TABLE test_insert_not_null");
    }

    @Override
    public void testDescribeTable()
    {
        // not supported
    }
}
