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
package io.hetu.core.plugin.vdm;

import io.airlift.testing.mysql.TestingMySqlServer;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import static io.airlift.tpch.TpchTable.ORDERS;
import static io.hetu.core.plugin.vdm.VdmQueryRunner.createVdmQueryRunner;
import static org.testng.Assert.assertEquals;

@Test
public class TestVdmIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private final TestingMySqlServer mysqlServer;

    public TestVdmIntegrationSmokeTest()
            throws Exception
    {
        this(new TestingMySqlServer("user", "testpass", "vdm"));
    }

    public TestVdmIntegrationSmokeTest(TestingMySqlServer mysqlServer)
    {
        super(() -> createVdmQueryRunner(mysqlServer, ORDERS));
        this.mysqlServer = mysqlServer;
    }

    @AfterClass(alwaysRun = true)
    public final void destroy()
    {
        mysqlServer.close();
    }

    @Test
    public void testSchemaExists()
    {
        assertUpdate("CREATE SCHEMA test_schema0");
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().toString().contains("[test_schema0]"), true);
    }

    @Test
    public void testCreateSchema()
    {
        assertUpdate("CREATE SCHEMA test_schema1");
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().toString().contains("[test_schema1]"), true);
    }

    @Test
    public void testDropSchema()
    {
        assertUpdate("CREATE SCHEMA test_schema2");
        assertUpdate("DROP SCHEMA test_schema2");
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().toString().contains("[test_schema2]"), false);
    }

    @Test
    public void listSchemaNames()
    {
        assertUpdate("CREATE SCHEMA test_schema3");
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().size() > 0, true);
    }

    @Test
    public void renameSchema()
    {
        assertUpdate("CREATE SCHEMA test_schema4");
        assertUpdate("ALTER SCHEMA test_schema4 RENAME TO test_schema4new");
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().toString().contains("[test_schema4]"), false);
        assertEquals(getQueryRunner().execute("SHOW SCHEMAS").getMaterializedRows().toString().contains("[test_schema4new]"), true);
    }

    @Test
    public void testListViews()
    {
        assertUpdate("CREATE SCHEMA test_schema6");
        assertUpdate("CREATE OR REPLACE VIEW test_schema6.test_view AS SELECT * FROM tpch.tiny.orders");
        assertEquals(getQueryRunner().execute("SHOW TABLES from test_schema6").getMaterializedRows().size() > 0, true);
    }

    @Test
    public void testCreateView()
    {
        assertUpdate("CREATE SCHEMA test_schema7");
        assertUpdate("CREATE OR REPLACE VIEW test_schema7.test_view AS SELECT * FROM tpch.tiny.orders");
        assertQuery("SELECT * FROM test_schema7.test_view", "SELECT * FROM orders");
    }

    @Test
    public void testDropView()
    {
        assertUpdate("CREATE SCHEMA test_schema8");
        assertUpdate("CREATE OR REPLACE VIEW test_schema8.test_view AS SELECT * FROM tpch.tiny.orders");
        assertUpdate("DROP VIEW test_schema8.test_view");
        assertEquals(getQueryRunner().execute("SHOW TABLES from test_schema8").getMaterializedRows().toString().contains("[test_view]"), false);
    }

    @Override
    public void testLimit()
    {
    }

    @Override
    public void testShowTables()
    {
    }

    @Override
    public void testSelectInformationSchemaTables()
    {
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
    }

    @Override
    public void testSelectAll()
    {
    }

    @Override
    public void testAggregateSingleColumn()
    {
    }

    @Override
    public void testShowSchemas()
    {
    }

    @Override
    public void testRangePredicate()
    {
    }

    @Override
    public void testMultipleRangesPredicate()
    {
    }

    @Override
    public void testIsNullPredicate()
    {
    }

    @Override
    public void testInListPredicate()
    {
    }

    @Override
    public void testExactPredicate()
    {
    }

    @Override
    public void testDuplicatedRowCreateTable()
    {
    }

    @Override
    public void testCountAll()
    {
    }

    @Override
    public void testDescribeTable()
    {
    }

    @Override
    public void testColumnsInReverseOrder()
    {
    }

    @Override
    public void testLikePredicate()
    {
    }
}
