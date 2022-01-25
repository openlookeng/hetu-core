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
import io.prestosql.testing.MaterializedResult;
import io.prestosql.tests.AbstractTestIntegrationSmokeTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.hetu.core.plugin.hana.HanaQueryRunner.createHanaQueryRunner;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.tests.QueryAssertions.assertContains;

/**
 * This is testing HanaIntegrationSmoke related functions
 *
 * @since 2019-07-11
 */
public class TestHanaIntegrationSmokeTest
        extends AbstractTestIntegrationSmokeTest
{
    private static final Logger LOGGER = Logger.get(TestHanaIntegrationSmokeTest.class);

    private AtomicBoolean isServerOk = new AtomicBoolean(false);

    private TestingHanaServer hanaServer = TestingHanaServer.getInstance();

    /**
     * This is testing createJdbcQueryRunner constructor
     */
    public TestHanaIntegrationSmokeTest()
    {
        this(TestingHanaServer.getInstance());
    }

    public TestHanaIntegrationSmokeTest(TestingHanaServer server)
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

    @Test
    @Override
    public void testDuplicatedRowCreateTable()
    {
        String tableOrders = TestingHanaServer.getActualTable("orders");

        assertQueryFails("CREATE TABLE test (a integer, a integer)",
                "line 1:31: Column name 'a' specified more than once");
        assertQueryFails("CREATE TABLE test (a integer, orderkey integer, LIKE " + tableOrders + " INCLUDING PROPERTIES)",
                "line 1:49: Column name 'orderkey' specified more than once");

        assertQueryFails("CREATE TABLE test (a integer, A integer)",
                "line 1:31: Column name 'A' specified more than once");
        assertQueryFails("CREATE TABLE test (a integer, OrderKey integer, LIKE " + tableOrders + " INCLUDING PROPERTIES)",
                "line 1:49: Column name 'orderkey' specified more than once");
    }

    @Test
    @Override
    public void testSelectInformationSchemaColumns()
    {
        String schema = getSession().getSchema().get();
        assertQuery("SELECT table_schema FROM information_schema.columns WHERE table_schema = '" + schema + "' GROUP BY table_schema", "VALUES '" + schema + "'");
    }

    @Test
    @Override
    public void testSelectInformationSchemaTables()
    {
        String schema = getSession().getSchema().get();
        String tableOrders = TestingHanaServer.getActualTable("orders");

        assertQuery("SELECT table_name FROM information_schema.tables WHERE table_schema = '" + schema + "' AND table_name = '" + tableOrders + "'", "VALUES '" + tableOrders + "'");
    }

    @Test
    @Override
    public void testShowTables()
    {
        String actualTableName = TestingHanaServer.getActualTable("orders");
        MaterializedResult actualTables = computeActual("SHOW TABLES").toTestTypes();
        MaterializedResult expectedTables = MaterializedResult.resultBuilder(getQueryRunner().getDefaultSession(), VARCHAR)
                .row(actualTableName)
                .build();
        assertContains(actualTables, expectedTables);
    }
}
