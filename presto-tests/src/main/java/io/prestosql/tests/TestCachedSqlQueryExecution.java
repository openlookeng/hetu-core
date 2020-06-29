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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.execution.SqlQueryManager;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Connector;
import io.prestosql.spi.connector.ConnectorContext;
import io.prestosql.spi.connector.ConnectorFactory;
import io.prestosql.spi.connector.ConnectorHandleResolver;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorPageSinkProvider;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.transaction.IsolationLevel;
import io.prestosql.spi.type.CharType;
import io.prestosql.sql.SqlPath;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.Plan;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.testing.TestingHandleResolver;
import io.prestosql.testing.TestingMetadata;
import io.prestosql.testing.TestingPageSinkProvider;
import io.prestosql.testing.TestingSplitManager;
import io.prestosql.testing.TestingTransactionHandle;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.prestosql.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

public class TestCachedSqlQueryExecution
{
    private DistributedQueryRunner queryRunner;

    private static final String TEST_SQL = "SELECT COUNT(*), clerk FROM orders GROUP BY clerk";
    private static final Session TPCH_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .setIdentity(new Identity("test_current_user", Optional.empty()))
            .setPath(new SqlPath(Optional.of("testPath")))
            .setSystemProperty("legacy_timestamp", String.valueOf(true))
            .setSystemProperty("enable_execution_plan_cache", String.valueOf(true))
            .setTimeZoneKey(getTimeZoneKey("+06:09"))
            .build();
    private static final Session DEFAULT_SESSION = testSessionBuilder()
            .setCatalog("test")
            .setSchema("default")
            .setSystemProperty("legacy_timestamp", String.valueOf(true))
            .setSystemProperty("enable_execution_plan_cache", String.valueOf(true))
            .setTimeZoneKey(getTimeZoneKey("+06:09"))
            .build();

    private static final Session CACHING_DISABLED_SESSION = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .setSystemProperty("legacy_timestamp", String.valueOf(true))
            .setTimeZoneKey(getTimeZoneKey("+06:09"))
            .setSystemProperty("enable_execution_plan_cache", String.valueOf(false))
            .build();

    private static final Session DEFAULT_SESSION_WITH_CHANGED_PROPERTY = testSessionBuilder()
            .setCatalog("tpch")
            .setSchema("tiny")
            .setSystemProperty("legacy_timestamp", String.valueOf(true))
            .setTimeZoneKey(getTimeZoneKey("+06:09"))
            .setSystemProperty("enable_execution_plan_cache", String.valueOf(true))
            .setSystemProperty(SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.AUTOMATIC.toString())
            .build();

    private void setupWithExecutionPlanCacheEnabled(Session session)
            throws Exception
    {
        Map<String, String> properties = new HashMap<>();
        properties.putIfAbsent("hetu.executionplan.cache.enabled", "true");
        properties.putIfAbsent("hetu.executionplan.cache.limit", "2");
        properties.putIfAbsent("hetu.executionplan.cache.timeout", "60000");
        if (queryRunner != null) {
            queryRunner.close();
        }
        queryRunner = new DistributedQueryRunner(session, 1, properties);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
    }

    private void setupWithExecutionPlanCacheDisabled(Session session)
            throws Exception
    {
        Map<String, String> properties = new HashMap<>();
        properties.putIfAbsent("hetu.executionplan.cache.enabled", "false");

        queryRunner = new DistributedQueryRunner(session, 1, properties);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
    }

    @Test
    public void testExecutionPlanCacheEnabled()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String testSql = TEST_SQL + " ORDER BY clerk LIMIT 1";
        Plan plan1 = getPlan(testSql, manager);
        Plan plan2 = getPlan(testSql, manager);

        assertSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());

        MaterializedResult rows1 = queryRunner.execute(TPCH_SESSION, testSql);
        MaterializedResult rows2 = queryRunner.execute(TPCH_SESSION, testSql);
        assertEquals(rows1.getRowCount(), rows2.getRowCount());
        MaterializedRow row1 = Iterables.getOnlyElement(rows1);
        MaterializedRow row2 = Iterables.getOnlyElement(rows2);
        assertEquals(row1.getFieldCount(), row2.getFieldCount());
        assertEquals(row1.getFields(), row2.getFields());
    }

    @Test
    public void testExecutionPlanCacheDisabled()
            throws Exception
    {
        setupWithExecutionPlanCacheDisabled(TPCH_SESSION);
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        Plan plan1 = getPlan(TEST_SQL, manager);
        Plan plan2 = getPlan(TEST_SQL, manager);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheSessionDisabled()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(CACHING_DISABLED_SESSION); // enabled globally
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        Plan plan1 = getPlan(TEST_SQL, manager);
        Plan plan2 = getPlan(TEST_SQL, manager);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheEnabledMulti()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        Plan plan1 = getPlan(TEST_SQL, manager);
        Plan plan2 = getPlan(TEST_SQL, manager);
        Plan plan3 = getPlan(TEST_SQL, manager);
        Plan plan4 = getPlan(TEST_SQL, manager);

        assertSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
        assertSame(plan2.getStatsAndCosts(), plan3.getStatsAndCosts());
        assertSame(plan3.getStatsAndCosts(), plan4.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheEnabledMixedQueries()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String query = "EXPLAIN SELECT * FROM orders";
        Plan plan1 = getPlan(TEST_SQL, manager);
        Plan plan2 = getPlan(query, manager);
        Plan plan3 = getPlan(TEST_SQL, manager);
        Plan plan4 = getPlan(TEST_SQL, manager);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
        assertSame(plan1.getStatsAndCosts(), plan3.getStatsAndCosts());
        assertSame(plan3.getStatsAndCosts(), plan4.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheWithFunctions()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String query = "EXPLAIN SELECT * FROM orders";
        Plan plan1 = getPlan(TEST_SQL, manager);
        Plan plan2 = getPlan(query, manager);
        Plan plan3 = getPlan(TEST_SQL, manager);
        Plan plan4 = getPlan(TEST_SQL, manager);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
        assertSame(plan1.getStatsAndCosts(), plan3.getStatsAndCosts());
        assertSame(plan3.getStatsAndCosts(), plan4.getStatsAndCosts());
    }

    @Test
    public void testCreateTable()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();
        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation";
        String query2 = "SELECT * FROM nation";
        String query3 = "SELECT * FROM tpch.tiny.nation";
        String query4 = "SELECT COUNT(*) FROM tpch.tiny.orders";
        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);
        Plan plan4 = getPlan(query4, manager);
        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan4);

        assertNotSame(plan2.getStatsAndCosts(), plan3.getStatsAndCosts());
    }

    @Test
    public void testCreateTableRepeated()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders";
        String query3 = "INSERT INTO orders SELECT * FROM orders";
        String query4 = "INSERT INTO orders SELECT * FROM test.default.orders";

        Plan plan1 = getPlan(query1, manager);

        metadata.dropTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                new SchemaTableName("default", "orders")));
        Plan plan2 = getPlan(query1, manager);
        Plan plan3 = getPlan(query3, manager);
        Plan plan4 = getPlan(query4, manager);
        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts()); // Create statements should not be cached
        assertNotSame(plan3.getStatsAndCosts(), plan4.getStatsAndCosts()); // Statement does not include fully qualified name
    }

    @Test
    public void testAlterTable()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, TOTALPRICE FROM tpch.tiny.orders";
        String query2 = "INSERT INTO orders SELECT * FROM orders";
        String query3 = "SELECT * FROM orders";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);

        metadata.dropTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "orders")));

        String query4 = "CREATE TABLE orders AS SELECT * FROM tpch.tiny.orders";

        Plan plan4 = getPlan(query4, manager); // Create table again
        Plan plan5 = getPlan(query2, manager); // Insert into new table
        Plan plan6 = getPlan(query3, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);
        assertNotNull(plan6);

        assertNotSame(plan2.getStatsAndCosts(), plan5.getStatsAndCosts()); // Schema changed
        assertNotSame(plan3.getStatsAndCosts(), plan6.getStatsAndCosts()); // Schema changed
    }

    @Test
    public void testRenamedTable()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, TOTALPRICE FROM tpch.tiny.orders";
        String query2 = "INSERT INTO orders SELECT * FROM orders";
        String query3 = "SELECT * FROM orders";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);

        metadata.dropTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "orders")));

        String query4 = "CREATE TABLE nation AS SELECT * FROM tpch.tiny.nation";
        Plan plan4 = getPlan(query4, manager); // Create table again

        // different table with the same name
        metadata.renameTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "nation")),
                new SchemaTableName("default", "orders"));

        Plan plan5 = getPlan(query2, manager); // Insert into new table
        Plan plan6 = getPlan(query3, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);
        assertNotNull(plan6);

        assertNotSame(plan2.getStatsAndCosts(), plan5.getStatsAndCosts()); // Underlying table schema changed
        assertNotSame(plan3.getStatsAndCosts(), plan6.getStatsAndCosts()); // new plan should not be the same as the old plan
    }

    @Test
    public void testRenamedColumn()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, TOTALPRICE, COMMENT FROM tpch.tiny.orders";
        String query2 = "INSERT INTO orders SELECT * FROM orders";
        String query3 = "SELECT * FROM orders";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);

        metadata.renameColumn(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "orders")),
                new TestingMetadata.TestingColumnHandle("comment", 3, CharType.createCharType(256)), "comments");

        Plan plan4 = getPlan(query3, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);

        assertNotSame(plan3.getStatsAndCosts(), plan4.getStatsAndCosts()); // new plan should not be the same as the old plan
    }

    @Test
    public void testAlteredColumnType()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, TOTALPRICE, COMMENT FROM tpch.tiny.orders";
        String query2 = "INSERT INTO orders SELECT * FROM orders";
        String query3 = "SELECT ORDERKEY, TOTALPRICE FROM orders";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);

        metadata.dropTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "orders")));

        String query4 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, cast(TOTALPRICE as integer) AS TOTALPRICE, COMMENT FROM tpch.tiny.orders";

        Plan plan4 = getPlan(query4, manager);
        Plan plan5 = getPlan(query3, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);
        assertNotNull(plan5);

        assertNotSame(plan3.getStatsAndCosts(), plan5.getStatsAndCosts()); // new plan should not be the same as the old plan
    }

    @Test
    public void testQueriesWithCurrentTimeFunction()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);

        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "SELECT CURRENT_TIME";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query1, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts()); // Should not have the same plan

        MaterializedResult rows1 = queryRunner.execute(DEFAULT_SESSION, query1);
        MaterializedResult rows2 = queryRunner.execute(DEFAULT_SESSION, query1);
        assertEquals(rows1.getRowCount(), rows2.getRowCount());
        MaterializedRow row1 = Iterables.getOnlyElement(rows1);
        MaterializedRow row2 = Iterables.getOnlyElement(rows2);
        assertEquals(row1.getFieldCount(), row2.getFieldCount());
        assertNotSame(row1.getFields(), row2.getFields());
    }

    @Test
    public void testQueriesWithCurrentUserFunction()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);

        TestingMetadata metadata = new TestingMetadata();
        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "SELECT CURRENT_USER";

        Plan plan1 = getPlan(query1, manager);

        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);
        TestingMetadata metadata2 = new TestingMetadata();
        queryRunner.installPlugin(new TestPlugin(metadata2));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager2 = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        Plan plan2 = getPlan(query1, manager2);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts()); // Should not have the same plan

        MaterializedResult rows1 = queryRunner.execute(DEFAULT_SESSION, query1);
        MaterializedResult rows2 = queryRunner.execute(TPCH_SESSION, query1);
        assertEquals(rows1.getRowCount(), rows2.getRowCount());
        MaterializedRow row1 = Iterables.getOnlyElement(rows1);
        MaterializedRow row2 = Iterables.getOnlyElement(rows2);
        assertEquals(row1.getFieldCount(), row2.getFieldCount());
        assertNotSame(row1.getFields(), row2.getFields());
    }

    @Test
    public void testQueriesWithCurrentPathFunction()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);

        TestingMetadata metadata = new TestingMetadata();
        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "SELECT CURRENT_PATH";

        Plan plan1 = getPlan(query1, manager);

        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);
        TestingMetadata metadata2 = new TestingMetadata();
        queryRunner.installPlugin(new TestPlugin(metadata2));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager2 = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        Plan plan2 = getPlan(query1, manager2);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts()); // Should not have the same plan

        MaterializedResult rows1 = queryRunner.execute(DEFAULT_SESSION, query1);
        MaterializedResult rows2 = queryRunner.execute(TPCH_SESSION, query1);
        assertEquals(rows1.getRowCount(), rows2.getRowCount());
        MaterializedRow row1 = Iterables.getOnlyElement(rows1);
        MaterializedRow row2 = Iterables.getOnlyElement(rows2);
        assertEquals(row1.getFieldCount(), row2.getFieldCount());
        assertNotSame(row1.getFields(), row2.getFields());
    }

    @Test
    public void testQueriesWithFunctionsOnChangedTable()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION);
        TestingMetadata metadata = new TestingMetadata();

        queryRunner.installPlugin(new TestPlugin(metadata));
        queryRunner.createCatalog("test", "test");
        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();

        String query1 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, TOTALPRICE FROM tpch.tiny.orders";
        String query2 = "INSERT INTO orders SELECT * FROM orders";
        String query3 = "SELECT SUM(TOTALPRICE) FROM orders WHERE TOTALPRICE > 1";

        String query4 = "CREATE TABLE orders AS SELECT ORDERKEY, CUSTKEY, cast(TOTALPRICE as integer) AS TOTALPRICE, COMMENT FROM tpch.tiny.orders";

        Plan plan1 = getPlan(query1, manager);
        Plan plan2 = getPlan(query2, manager);
        Plan plan3 = getPlan(query3, manager);
        metadata.dropTable(DEFAULT_SESSION.toConnectorSession(),
                metadata.getTableHandle(DEFAULT_SESSION.toConnectorSession(),
                        new SchemaTableName("default", "orders")));
        Plan plan4 = getPlan(query4, manager);
        Plan plan5 = getPlan(query3, manager);

        assertNotNull(plan1);
        assertNotNull(plan2);
        assertNotNull(plan3);
        assertNotNull(plan4);

        assertNotSame(plan3.getStatsAndCosts(), plan5.getStatsAndCosts()); // Predicate types are different
    }

    @Test
    public void testExecutionPlanCacheNestedStatements()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String testSql = "SELECT  5, COUNT(*) FROM ( SELECT * FROM tpch.tiny.orders GROUP BY clerk, orderkey, custkey, totalprice, orderstatus, orderdate, orderpriority, shippriority, comment) WHERE TOTALPRICE > 1";
        Plan plan1 = getPlan(testSql, manager);
        Plan plan2 = getPlan(testSql, manager);

        assertSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheNestedStatementsWithCurrentTime()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String testSql = "SELECT  5, COUNT(*) FROM (SELECT CURRENT_TIME, * FROM tpch.tiny.orders GROUP BY clerk, orderkey, custkey, totalprice, orderstatus, orderdate, orderpriority, shippriority, comment) WHERE TOTALPRICE > 1";
        Plan plan1 = getPlan(testSql, manager);
        Plan plan2 = getPlan(testSql, manager);

        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());
    }

    @Test
    public void testExecutionPlanCacheModifedSessionProperty()
            throws Exception
    {
        setupWithExecutionPlanCacheEnabled(TPCH_SESSION);

        SqlQueryManager manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        String testSql = "SELECT ORDERKEY, TOTALPRICE FROM orders";
        Plan plan1 = getPlan(testSql, manager);

        setupWithExecutionPlanCacheEnabled(DEFAULT_SESSION_WITH_CHANGED_PROPERTY);
        manager = (SqlQueryManager) queryRunner.getCoordinator().getQueryManager();
        Plan plan2 = getPlan(testSql, manager);
        assertNotSame(plan1.getStatsAndCosts(), plan2.getStatsAndCosts());

        Plan plan3 = getPlan(testSql, manager);
        assertSame(plan2.getStatsAndCosts(), plan3.getStatsAndCosts());
    }

    @AfterTest(alwaysRun = true)
    private void cleanup()
    {
        queryRunner.close();
        queryRunner = null;
    }

    private Plan getPlan(String sql, SqlQueryManager manager)
    {
        ResultWithQueryId<MaterializedResult> result = queryRunner.executeWithQueryId(queryRunner.getDefaultSession(), sql);
        return manager.getQueryPlan(result.getQueryId());
    }

    private static class TestPlugin
            implements Plugin
    {
        private final TestingMetadata metadata;

        private TestPlugin(TestingMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return "test";
                }

                @Override
                public ConnectorHandleResolver getHandleResolver()
                {
                    return new TestingHandleResolver();
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    return new TestConnector(metadata);
                }
            });
        }
    }

    private static class TestConnector
            implements Connector
    {
        private final ConnectorMetadata metadata;

        private TestConnector(ConnectorMetadata metadata)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        public ConnectorTransactionHandle beginTransaction(IsolationLevel isolationLevel, boolean readOnly)
        {
            return TestingTransactionHandle.create();
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorTransactionHandle transactionHandle)
        {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager()
        {
            return new TestingSplitManager(ImmutableList.of());
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider()
        {
            return new ConnectorPageSourceProvider()
            {
                @Override
                public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns)
                {
                    return new FixedPageSource(ImmutableList.of());
                }
            };
        }

        @Override
        public ConnectorPageSinkProvider getPageSinkProvider()
        {
            return new TestingPageSinkProvider();
        }
    }
}
