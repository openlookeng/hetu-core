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
