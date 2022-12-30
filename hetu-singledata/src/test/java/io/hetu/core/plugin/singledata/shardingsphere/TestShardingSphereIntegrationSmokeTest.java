/*
 * Copyright (C) 2018-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package io.hetu.core.plugin.singledata.shardingsphere;

import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestShardingSphereIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    private final TestingShardingSphereServer shardingSphereServer;

    public TestShardingSphereIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingShardingSphereServer());
    }

    public TestShardingSphereIntegrationSmokeTest(TestingShardingSphereServer shardingSphereServer)
    {
        super(shardingSphereServer::createShardingSphereQueryRunner);
        this.shardingSphereServer = shardingSphereServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        shardingSphereServer.close();
    }

    @Test
    public void testShowSchemas()
    {
        MaterializedResult actual = computeActual("SHOW SCHEMAS");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("information_schema")
                .row("pg_catalog")
                .row("public")
                .row("shardingsphere")
                .build();
        assertEquals(expected, actual);
    }

    @Test
    public void testShowTables()
    {
        MaterializedResult actual = computeActual("SHOW TABLES");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), VARCHAR)
                .row("table_a")
                .row("table_b")
                .build();
        assertEquals(expected, actual);
    }

    @Test
    public void testShowColumns()
    {
        MaterializedResult actual = computeActual("SHOW COLUMNS FROM table_a");
        MaterializedResult expectedNoParametrized = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("col_database", "integer", "", "")
                .row("col_table", "integer", "", "")
                .row("col_value", "varchar", "", "")
                .build();
        MaterializedResult expectedParametrized = MaterializedResult.resultBuilder(getSession(), VARCHAR, VARCHAR, VARCHAR, VARCHAR)
                .row("col_database", "integer", "", "")
                .row("col_table", "integer", "", "")
                .row("col_value", "varchar(50)", "", "")
                .build();
        assertTrue(actual.equals(expectedParametrized) || actual.equals(expectedNoParametrized));
    }

    @Test
    public void testSelectStar()
    {
        MaterializedResult actual = computeActual("SELECT * FROM table_a ORDER BY col_database");
        MaterializedResult expected = MaterializedResult.resultBuilder(getSession(), INTEGER, INTEGER, createVarcharType(50))
                .row(0, 0, "TEST")
                .row(1, 1, "TEST")
                .row(2, 2, "TEST")
                .row(3, 3, "TEST")
                .row(4, 4, "TEST")
                .row(5, 5, "TEST")
                .row(6, 6, "TEST")
                .row(7, 7, "TEST")
                .row(8, 8, "TEST")
                .row(9, 9, "TEST")
                .build();
        assertEquals(actual, expected);
    }

    @Test
    public void testLimit()
    {
        MaterializedResult result = computeActual("SELECT * FROM table_a LIMIT 5");
        assertEquals(result.getRowCount(), 5);
    }

    @Test
    public void testFilter()
    {
        assertQuery("SELECT * FROM table_a WHERE col_database = 1", "SELECT 1 col_database, 1 col_table, 'TEST' col_value");
    }

    @Test
    public void testProject()
    {
        assertQuery("SELECT col_database + col_table AS exp FROM table_a WHERE col_database = 1", "SELECT 2 exp");
    }

    @Test
    public void testJoinWithPushDown()
    {
        MaterializedResult actual = computeActual("SELECT * FROM table_a JOIN table_b ON table_a.col_database = table_b.col_database " +
                "AND table_a.col_table = table_b.col_table");
        assertEquals(actual.getRowCount(), 10);
    }

    @Test
    public void testJoinWithoutPushDown()
    {
        MaterializedResult actual = computeActual("SELECT * FROM table_a JOIN table_b ON table_a.col_database = table_b.col_database");
        assertEquals(actual.getRowCount(), 10);
    }
}
