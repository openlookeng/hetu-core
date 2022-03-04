/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.SystemSessionProperties;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.sql.planner.Plan;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestStarTreeQueries
        extends AbstractTestQueryFramework
{
    Session starTreeEnabledSession;
    Session starTreeDisabledSession;

    protected AbstractTestStarTreeQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @BeforeClass
    public void setUp()
    {
        starTreeEnabledSession = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "true")
                .build();
        starTreeDisabledSession = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "false")
                .build();

        //Create Empty to force create Metadata catalog and schema. To avoid concurrency issue.
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_count_all ON nation WITH (AGGREGATIONS=(count(*)), group=())");
        assertUpdate("DROP CUBE nation_count_all");
    }

    @Test
    public void testStarTreeSessionProperty()
    {
        MaterializedResult result = computeActual("SET SESSION enable_star_tree_index = true");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("enable_star_tree_index", "true"));
        result = computeActual("SET SESSION enable_star_tree_index = false");
        assertTrue((Boolean) getOnlyElement(result).getField(0));
        assertEquals(result.getSetSessionProperties(), ImmutableMap.of("enable_star_tree_index", "false"));
    }

    @Test
    public void testCreateCubeWithAllAggregations()
    {
        computeActual("CREATE TABLE nation_aggregations_test_table AS SELECT * FROM nation");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_aggregations_cube_1 ON nation_aggregations_test_table " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE nation_aggregations_cube_1 where nationkey > -1", 25);
        assertQueryFails(starTreeDisabledSession, "INSERT INTO CUBE nation_aggregations_cube_1 where 1 > 0", "Invalid predicate\\. \\(1 > 0\\)");
        assertQuery(starTreeEnabledSession,
                "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation_aggregations_test_table group by nationkey",
                starTreeDisabledSession,
                "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(starTreeEnabledSession,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation_aggregations_test_table",
                starTreeDisabledSession,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(starTreeEnabledSession,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation_aggregations_test_table group by nationkey",
                starTreeDisabledSession,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(starTreeEnabledSession,
                "SELECT avg(nationkey) from nation_aggregations_test_table group by nationkey",
                starTreeDisabledSession,
                "SELECT avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertUpdate("DROP CUBE nation_aggregations_cube_1");
        assertUpdate("DROP TABLE nation_aggregations_test_table");
    }

    @Test
    public void testAverageAggregation()
    {
        computeActual("CREATE TABLE orders_avg_aggs_table AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE orders_avg_aggs_cube ON orders_avg_aggs_table WITH (AGGREGATIONS = (avg(totalprice), sum(totalprice), count(*)), GROUP = (custKEY, ORDERkey), format= 'orc')");
        assertQuerySucceeds("INSERT INTO CUBE orders_avg_aggs_cube");

        String[] queries = new String[] {
                "SELECT ord.custkey, AVG(ord.totalprice) FROM %s ord GROUP BY ord.CUSTKEY",
                "SELECT ord.custkey, ord.orderkey, AVG(ord.totalprice) FROM %s ord GROUP BY ord.custkey, ord.orderkey",
                "SELECT ord.orderkey, AVG(ord.totalprice) FROM %s ord GROUP BY ord.orderkey",
                "SELECT ord.orderkey, AVG(ord.totalprice), SUM(ord.totalprice) FROM %s ord GROUP BY ord.orderkey"
        };

        Stream.of(queries).forEach(query -> assertQuery(
                starTreeEnabledSession,
                String.format(query, "orders_avg_aggs_table"),
                starTreeDisabledSession,
                String.format(query, "orders"),
                assertInTableScans("orders_avg_aggs_cube")));

        assertUpdate("CREATE CUBE orders_avg_aggs_cube_2 ON orders_avg_aggs_table WITH (AGGREGATIONS = (avg(orderkey)), GROUP = (orderdate, orderpriority), format= 'orc')");
        assertQuerySucceeds("INSERT INTO CUBE orders_avg_aggs_cube_2");
        queries = new String[] {
                "SELECT ord.orderdate, AVG(ord.orderkey) FROM %s ord GROUP BY ord.orderdate",
                "SELECT ord.orderpriority, AVG(ord.orderkey) FROM %s ord GROUP BY ord.orderpriority",
                "SELECT ord.orderdate, ord.orderpriority, AVG(ord.orderkey) FROM %s ord GROUP BY ord.orderdate, ord.orderpriority"
        };
        Stream.of(queries).forEach(query -> {
            MaterializedResult actual = computeActualAndAssertPlan(starTreeEnabledSession, String.format(query, "orders_avg_aggs_table"), assertInTableScans("orders_avg_aggs_cube_2"));
            //Using PrestoRunner instead of H2QueryRunner as the latter does not yield correct results with decimal values.
            MaterializedResult expected = computeActualAndAssertPlan(starTreeDisabledSession, String.format(query, "orders_avg_aggs_table"), assertInTableScans("orders_avg_aggs_table"));
            assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        });
        assertUpdate("DROP TABLE orders_avg_aggs_table");
    }

    @Test
    public void testAggregationsWithCaseInSensitiveColumnNames()
    {
        computeActual("CREATE TABLE nation_case_insensitive_test_table_1 AS SELECT * FROM nation");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_aggregations_cube_3 ON nation_case_insensitive_test_table_1 " +
                "WITH (AGGREGATIONS=(avg(NationKEY), count(Regionkey), sum(regionkey)," +
                " min(regionkey), max(REGIONkey), max(nationKey), min(Nationkey), count(*))," +
                " group=(nationKEY), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE nation_aggregations_cube_3", 25);

        assertQuery(starTreeEnabledSession,
                "SELECT min(regionkey), max(regionkey), sum(regionkey), max(nationKey), min(Nationkey) from nation_case_insensitive_test_table_1 group by nationkey",
                starTreeDisabledSession,
                "SELECT min(regionkey), max(regionkey), sum(regionkey), max(nationKey), min(Nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(starTreeEnabledSession,
                "SELECT count(Regionkey), avg(nationkey) from nation_case_insensitive_test_table_1 group by nationkey",
                starTreeDisabledSession,
                "SELECT count(Regionkey), avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(starTreeEnabledSession,
                "SELECT count(regionkey), count(*), max(nationKey), min(Nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation_case_insensitive_test_table_1 group by nationKEY",
                starTreeDisabledSession,
                "SELECT count(regionkey), count(*), max(nationKey), min(Nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(starTreeEnabledSession,
                "SELECT count(regionkey), avg(nationkey) from nation_case_insensitive_test_table_1 group by NATIONkey",
                starTreeDisabledSession,
                "SELECT count(regionkey), avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertUpdate("DROP CUBE nation_aggregations_cube_3");
        assertUpdate("DROP TABLE nation_case_insensitive_test_table_1");
    }

    @Test
    public void testAverageAggregationWithExactGroupByMatch()
    {
        computeActual("CREATE TABLE lineitem_exact_group_by_match AS SELECT * FROM lineitem");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_4 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount))," +
                " group=(orderkey), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_4", 15000);

        //single column group by
        assertQuery(starTreeEnabledSession,
                "SELECT avg(quantity) from lineitem_exact_group_by_match group by orderkey",
                starTreeDisabledSession,
                "SELECT avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_4"));

        //multi column group by
        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_5 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_5", 60113);

        assertQuery(starTreeEnabledSession,
                "SELECT avg(quantity) from lineitem_exact_group_by_match group by orderkey,partkey",
                starTreeDisabledSession,
                "SELECT avg(quantity) from lineitem group by orderkey,partkey",
                assertTableScan("lineitem_aggregations_cube_5"));

        //multiple aggregations
        assertQuery(starTreeEnabledSession,
                "SELECT avg(quantity), max(discount) from lineitem_exact_group_by_match group by orderkey,partkey",
                starTreeDisabledSession,
                "SELECT avg(quantity), max(discount) from lineitem group by orderkey,partkey",
                assertTableScan("lineitem_aggregations_cube_5"));

        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_6 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(discount), " +
                " avg(quantity), max(discount))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_6", 60113);
        assertQuery(starTreeEnabledSession,
                "SELECT avg(discount), avg(quantity) from lineitem_exact_group_by_match group by orderkey,partkey",
                starTreeDisabledSession,
                "SELECT avg(discount), avg(quantity) from lineitem group by orderkey,partkey",
                assertTableScan("lineitem_aggregations_cube_6"));

        assertUpdate("DROP CUBE lineitem_aggregations_cube_4");
        assertUpdate("DROP CUBE lineitem_aggregations_cube_5");
        assertUpdate("DROP CUBE lineitem_aggregations_cube_6");
        assertUpdate("DROP TABLE lineitem_exact_group_by_match");
    }

    @Test
    public void testAggregationsWithPartialGroupByMatch()
    {
        computeActual("CREATE TABLE lineitem_partial_group_by_match AS SELECT * FROM lineitem");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_8 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(sum(discount), " +
                " min(quantity), max(discount), max(quantity))," +
                " group=(orderkey, partkey), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_8", 60113);

        assertQuery(starTreeEnabledSession,
                "SELECT sum(discount), min(quantity) from lineitem_partial_group_by_match group by partkey",
                starTreeDisabledSession,
                "SELECT sum(discount), min(quantity) from lineitem group by partkey",
                assertTableScan("lineitem_aggregations_cube_8"));

        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_9 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount), avg(discount), max(quantity))," +
                " group=(orderkey, discount), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_9", 50470);

        assertQuery(starTreeEnabledSession,
                "SELECT avg(quantity) from lineitem_partial_group_by_match group by orderkey",
                starTreeDisabledSession,
                "SELECT avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_9"));

        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_10 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(discount), " +
                " avg(quantity), min(discount), max(discount), min(quantity))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_10", 60113);

        assertQuery(starTreeEnabledSession,
                "SELECT avg(discount), avg(quantity) from lineitem_partial_group_by_match group by orderkey",
                starTreeDisabledSession,
                "SELECT avg(discount), avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_10"));

        assertUpdate(starTreeDisabledSession, "CREATE CUBE lineitem_aggregations_cube_11 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount), avg(discount), min(discount))," +
                " group=(), format= 'orc')");
        assertUpdate(starTreeDisabledSession, "INSERT INTO CUBE lineitem_aggregations_cube_11", 1);

        assertQuery(starTreeEnabledSession,
                "SELECT avg(quantity) from lineitem_partial_group_by_match",
                starTreeDisabledSession,
                "SELECT avg(quantity) from lineitem",
                assertTableScan("lineitem_aggregations_cube_11"));

        assertUpdate("DROP CUBE lineitem_aggregations_cube_8");
        assertUpdate("DROP CUBE lineitem_aggregations_cube_9");
        assertUpdate("DROP CUBE lineitem_aggregations_cube_10");
        assertUpdate("DROP CUBE lineitem_aggregations_cube_11");
        assertUpdate("DROP TABLE lineitem_partial_group_by_match");
    }

    @Test
    public void testShowCubes()
    {
        computeActual("CREATE TABLE nation_show_cube_table_1 AS SELECT * FROM nation");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_show_cube_1 ON nation_show_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_show_cube_2 ON nation_show_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=())");
        MaterializedResult result = computeActual("SHOW CUBES FOR nation_show_cube_table_1");
        MaterializedRow matchingRow1 = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_show_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow1);
        assertTrue(matchingRow1.getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_1", "hive.tpch.nation_show_cube_table_1", "Inactive", "nationkey")));

        MaterializedRow matchingRow2 = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_show_cube_2")).findFirst().orElse(null);
        assertNotNull(matchingRow2);
        assertTrue(matchingRow2.getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_2", "hive.tpch.nation_show_cube_table_1", "Inactive", "")));

        result = computeActual("SHOW CUBES FOR nation_show_cube_table_1");
        assertEquals(result.getRowCount(), 2);

        matchingRow1 = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_show_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow1);
        assertTrue(result.getMaterializedRows().get(0).getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_1", "hive.tpch.nation_show_cube_table_1", "Inactive", "nationkey")));

        matchingRow2 = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_show_cube_2")).findFirst().orElse(null);
        assertNotNull(matchingRow2);
        assertTrue(result.getMaterializedRows().get(1).getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_2", "hive.tpch.nation_show_cube_table_1", "Inactive", "")));
        assertUpdate("DROP CUBE nation_show_cube_1");
        assertUpdate("DROP CUBE nation_show_cube_2");
        assertUpdate("DROP TABLE nation_show_cube_table_1");
    }

    @Test
    public void testShowCreateCube()
    {
        computeActual("CREATE TABLE nation_show_create_cube_table_1 AS SELECT * FROM nation");
        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_show_create_cube_1 ON nation_show_create_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey))," +
                " group=(regionkey))");
        MaterializedResult originalResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_1");
        String newQuery = originalResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertUpdate("DROP CUBE nation_show_create_cube_1");
        assertUpdate(starTreeDisabledSession, newQuery);
        MaterializedResult reWrittenResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_1");
        String reWrittenQuery = reWrittenResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertTrue(newQuery.equals(reWrittenQuery));
        assertUpdate("DROP CUBE nation_show_create_cube_1");

        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_show_create_cube_2 ON nation_show_create_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        originalResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_2");
        newQuery = originalResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertUpdate("DROP CUBE nation_show_create_cube_2");
        assertUpdate(starTreeDisabledSession, newQuery);
        reWrittenResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_2");
        reWrittenQuery = reWrittenResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertTrue(newQuery.equals(reWrittenQuery));
        assertUpdate("DROP CUBE nation_show_create_cube_2");

        assertUpdate(starTreeDisabledSession, "CREATE CUBE nation_show_create_cube_3 ON nation_show_create_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), count(distinct regionkey), avg(nationkey), max(regionkey))," +
                " group=(regionkey), format= 'orc', partitioned_by = ARRAY['regionkey']," +
                " FILTER = (nationkey >= 5 and name = 'PERU'))");
        originalResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_3");
        newQuery = originalResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertUpdate("DROP CUBE nation_show_create_cube_3");
        assertUpdate(starTreeDisabledSession, newQuery);
        reWrittenResult = computeActual("SHOW CREATE CUBE nation_show_create_cube_3");
        reWrittenQuery = reWrittenResult.getMaterializedRows().get(0).getFields().get(0).toString();
        assertTrue(newQuery.equals(reWrittenQuery));
        assertUpdate("DROP CUBE nation_show_create_cube_3");
        assertUpdate("DROP TABLE nation_show_create_cube_table_1");
    }

    @Test
    public void testPartitionedByGroupOverlap()
    {
        computeActual("CREATE TABLE nation_partition_group_overlap_table AS SELECT * FROM nation");
        assertQueryFails("CREATE CUBE nation_partition_group_overlap_cube ON nation_partition_group_overlap_table " +
                "WITH (AGGREGATIONS = (sum(nationkey), count(*), max(regionkey), avg(nationkey), count(nationkey), count(DISTINCT regionkey)), " +
                " GROUP=(nationkey), format = 'ORC', partitioned_by = ARRAY['nationkey','regionkey'])", "line 1:1: Some columns in 'nationkey,regionkey' in partitioned_by are not part of Cube.");
        assertQueryFails("CREATE CUBE nation_partition_group_overlap_cube ON nation_partition_group_overlap_table " +
                "WITH (AGGREGATIONS = (sum(nationkey), count(*), max(regionkey), avg(nationkey), count(nationkey), count(DISTINCT regionkey)), " +
                " GROUP=(regionkey), format = 'ORC', partitioned_by = ARRAY['nationkey','regionkey'])", "line 1:1: Some columns in 'nationkey,regionkey' in partitioned_by are not part of Cube.");
        assertQueryFails("CREATE CUBE nation_partition_group_overlap_cube ON nation_partition_group_overlap_table " +
                "WITH (AGGREGATIONS = (sum(nationkey), count(*), max(regionkey), avg(nationkey), count(nationkey), count(DISTINCT regionkey)), " +
                " GROUP=(regionkey), format = 'ORC', partitioned_by = ARRAY['nationkey'])", "line 1:1: Some columns in 'nationkey' in partitioned_by are not part of Cube.");
        assertQueryFails("CREATE CUBE nation_partition_group_overlap_cube ON nation_partition_group_overlap_table " +
                "WITH (AGGREGATIONS = (sum(nationkey), count(*), max(regionkey), avg(nationkey), count(nationkey), count(DISTINCT regionkey)), " +
                " GROUP=(nationkey), format = 'ORC', partitioned_by = ARRAY['regionkey'])", "line 1:1: Some columns in 'regionkey' in partitioned_by are not part of Cube.");
        assertUpdate("CREATE CUBE nation_partition_group_overlap_cube ON nation_partition_group_overlap_table " +
                "WITH (AGGREGATIONS = (sum(nationkey), count(*), max(regionkey), avg(nationkey), count(nationkey), count(DISTINCT regionkey)), " +
                " GROUP=(nationkey, regionkey), format = 'ORC', partitioned_by = ARRAY['nationkey','regionkey'])");
        assertUpdate("DROP CUBE nation_partition_group_overlap_cube");
        assertUpdate("DROP TABLE nation_partition_group_overlap_table");
    }

    @Test
    public void testInsertIntoCube()
    {
        computeActual("CREATE TABLE nation_table_cube_insert_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_insert_cube_1 ON nation_table_cube_insert_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate("INSERT INTO CUBE nation_insert_cube_1 where nationkey > 5", 19);
        assertQueryFails("INSERT INTO CUBE nation where 1 > 0", "Cube not found 'hive.tpch.nation'");
        assertQueryFails("INSERT INTO CUBE nation_insert_cube_1 where regionkey > 5", "Some columns in 'regionkey' in where clause are not part of Cube.");
        assertUpdate("DROP CUBE nation_insert_cube_1");
        assertUpdate("DROP TABLE nation_table_cube_insert_test_1");
    }

    @Test
    public void testInsertIntoCubeWithoutPredicate()
    {
        computeActual("CREATE TABLE nation_table_cube_insert_2 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_insert_2 ON nation_table_cube_insert_2 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_cube_insert_2");
        assertQuery(starTreeDisabledSession,
                "SELECT count(*) FROM nation_table_cube_insert_2 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_table_cube_insert_2"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_cube_insert_2 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_cube_insert_2"));
        assertUpdate("DROP CUBE nation_cube_insert_2");
        assertUpdate("DROP TABLE nation_table_cube_insert_2");
    }

    @Test
    public void testInsertOverwriteCube()
    {
        computeActual("CREATE TABLE orders_table_overwrite_cube AS SELECT * FROM orders");
        assertQuerySucceeds("CREATE CUBE orders_cube_test_overwrite ON orders_table_overwrite_cube WITH (AGGREGATIONS = (count(*), sum(totalprice), avg(totalprice)), GROUP = (custkey))");
        assertUpdate("INSERT INTO CUBE orders_cube_test_overwrite", 1000L);
        assertEquals(computeScalar("SELECT COUNT(*) FROM orders_cube_test_overwrite"), 1000L);

        assertQuerySucceeds("CREATE CUBE orders_partitioned_cube_test_overwrite ON orders_table_overwrite_cube WITH (AGGREGATIONS = (count(*), sum(totalprice), avg(totalprice)), GROUP = (custkey, orderstatus), PARTITIONED_BY = ARRAY['orderstatus'])");
        assertUpdate("INSERT INTO CUBE orders_partitioned_cube_test_overwrite", 2298L);
        assertQueryFails("INSERT OVERWRITE CUBE orders_partitioned_cube_test_overwrite WHERE custkey > 200", "INSERT OVERWRITE not supported on partitioned cube. Drop and recreate cube, if needed.");
        assertUpdate("DROP TABLE orders_table_overwrite_cube");
    }

    @Test
    public void testCountAggregation()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_count_agg_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_count_agg_cube_1 ON nation_table_count_agg_1 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_count_agg_cube_1 where name = 'CHINA'");
        assertQuery(starTreeDisabledSession,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_table_count_agg_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_count_agg_cube_1"));
        assertQuerySucceeds("INSERT INTO CUBE nation_count_agg_cube_1 where name = 'CANADA'");
        assertQuery(starTreeDisabledSession,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CANADA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CANADA' GROUP BY name",
                assertTableScan("nation_table_count_agg_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CANADA' OR name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CANADA' OR name = 'CHINA' GROUP BY name",
                assertTableScan("nation_count_agg_cube_1"));
        assertUpdate("DROP CUBE nation_count_agg_cube_1");
        assertUpdate("DROP TABLE nation_table_count_agg_1");
    }

    @Test
    public void testMultiColumnGroup()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_multi_column_group AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_multi_column_group ON nation_table_multi_column_group WITH (AGGREGATIONS=(count(*)), group=(name, regionkey))");
        assertQuerySucceeds("INSERT INTO CUBE nation_cube_multi_column_group where name = 'CHINA'");
        assertQuery(starTreeDisabledSession,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY name, regionkey",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name, regionkey",
                assertTableScan("nation_table_multi_column_group"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_cube_multi_column_group"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY nationkey, regionkey",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY nationkey, regionkey",
                assertTableScan("nation_table_multi_column_group"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY name, regionkey",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name, regionkey",
                assertTableScan("nation_cube_multi_column_group"));
        assertUpdate("DROP CUBE nation_cube_multi_column_group");
        assertUpdate("DROP TABLE nation_table_multi_column_group");
    }

    @Test
    public void testDuplicateDataInsert()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_duplicate_insert_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_duplicate_insert_1 ON nation_table_duplicate_insert_1 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_cube_duplicate_insert_1 where name = 'CHINA'");
        assertQueryFails("INSERT INTO CUBE nation_cube_duplicate_insert_1 where name = 'CHINA'", "Cannot allow insert. Cube already contains data for the given predicate.*");
        assertUpdate("DROP CUBE nation_cube_duplicate_insert_1");
        assertUpdate("DROP TABLE nation_table_duplicate_insert_1");
    }

    @Test
    public void testDuplicateInsertCubeWithAllData()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_duplicate_insert_2 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_duplicate_insert_2 ON nation_table_duplicate_insert_2 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_cube_duplicate_insert_2 where name = 'CHINA'");
        assertQueryFails("INSERT INTO CUBE nation_cube_duplicate_insert_2", "Cannot allow insert. Inserting entire dataset but cube already has partial data*");
        assertUpdate("DROP CUBE nation_cube_duplicate_insert_2");
        assertUpdate("DROP TABLE nation_table_duplicate_insert_2");
    }

    @Test
    public void testMultipleInsertIntoCube()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_multi_insert_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_multi_insert_cube_1 ON nation_table_multi_insert_1 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_multi_insert_cube_1 where name = 'CHINA'");
        assertQuerySucceeds("INSERT INTO CUBE nation_multi_insert_cube_1 where name = 'CANADA'");
        assertUpdate("DROP CUBE nation_multi_insert_cube_1");
        assertUpdate("DROP TABLE nation_table_multi_insert_1");
    }

    @Test
    public void testCreateCube()
    {
        computeActual("CREATE TABLE nation_table_create_cube_test_1 AS SELECT * FROM nation");
        assertQueryFails("CREATE CUBE nation ON nation " +
                "WITH (AGGREGATIONS=(count(*))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])", "line 1:1: Cube or Table 'hive.tpch.nation' already exists");
        assertQueryFails("CREATE CUBE nation_create_cube_1 ON abcd " +
                "WITH (AGGREGATIONS=(count(*), count(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])", "line 1:1: Table 'hive.tpch.abcd' does not exist");
        assertQueryFails("CREATE CUBE nation_create_cube_1 ON nation " +
                "WITH (AGGREGATIONS=(sum(distinct nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])", "line 1:1: Distinct is currently only supported for count");
        assertUpdate("CREATE CUBE nation_create_cube_1 ON nation_table_create_cube_test_1 " +
                "WITH (AGGREGATIONS=(count(*))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertQueryFails("CREATE CUBE nation_create_cube_1 ON nation_table_create_cube_test_1 " +
                "WITH (AGGREGATIONS=(count(*), count(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])", "line 1:1: Cube 'hive.tpch.nation_create_cube_1' already exists");
        assertUpdate("DROP CUBE nation_create_cube_1");
        assertUpdate("DROP TABLE nation_table_create_cube_test_1");
    }

    @Test
    public void testCreateCubeTransactional()
    {
        computeActual("CREATE TABLE nation_table_cube_create_transactional_test_1 AS SELECT * FROM nation");
        assertQueryFails("CREATE CUBE nation_create_transactional_cube_1 ON nation_table_cube_create_transactional_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), transactional=true)", "line 1:1: nation_create_transactional_cube_1 is a star-tree cube with transactional = true is not supported");

        assertQueryFails("CREATE CUBE nation_create_transactional_cube_2 ON nation_table_cube_create_transactional_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'], transactional=true)", "line 1:1: nation_create_transactional_cube_2 is a star-tree cube with transactional = true is not supported");

        assertUpdate("DROP TABLE nation_table_cube_create_transactional_test_1");
    }

    @Test
    public void testDeleteFromCube()
    {
        computeActual("CREATE TABLE nation_table_delete_from_cube_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_delete_from_cube_1 ON nation_table_delete_from_cube_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate("INSERT INTO CUBE nation_delete_from_cube_1 where nationkey > 5", 19);
        assertQueryFails("DELETE FROM nation_delete_from_cube_1 where nationkey > 5", "line 1:1: hive.tpch.nation_delete_from_cube_1 is a star-tree cube, DELETE is not supported");

        assertUpdate("DROP CUBE nation_delete_from_cube_1");
        assertUpdate("DROP TABLE nation_table_delete_from_cube_test_1");
    }

    @Test
    public void testAlterTableOnCube()
    {
        computeActual("CREATE TABLE nation_table_alter_on_cube_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_alter_on_cube_1 ON nation_table_alter_on_cube_test_1 WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey), min(regionkey), max(regionkey), max(nationkey), min(nationkey)), group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate("INSERT INTO CUBE nation_alter_on_cube_1 where nationkey > 5", 19);
        assertQueryFails("ALTER Table nation_alter_on_cube_1 RENAME TO nation_delete_from_cube_1", "line 1:1: Operation not permitted. hive.tpch.nation_alter_on_cube_1 is a Cube table, use CUBE statements instead.");
        assertQueryFails("ALTER Table nation_alter_on_cube_1 RENAME COLUMN OldName to NewName", "line 1:1: Operation not permitted. hive.tpch.nation_alter_on_cube_1 is a Cube table, use CUBE statements instead.");
        assertQueryFails("ALTER Table nation_alter_on_cube_1 DROP COLUMN columnName", "line 1:1: Operation not permitted. hive.tpch.nation_alter_on_cube_1 is a Cube table, use CUBE statements instead.");
        assertQueryFails("ALTER Table nation_alter_on_cube_1 ADD COLUMN columnName INT", "line 1:1: Operation not permitted. hive.tpch.nation_alter_on_cube_1 is a Cube table, use CUBE statements instead.");
        assertUpdate("DROP CUBE nation_alter_on_cube_1");
        assertUpdate("DROP TABLE nation_table_alter_on_cube_test_1");
    }

    @Test
    public void testUpdateCube()
    {
        computeActual("CREATE TABLE nation_table_update_cube_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_update_cube_1 ON nation_table_update_cube_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate("INSERT INTO CUBE nation_update_cube_1 where nationkey > 5", 19);
        assertQueryFails("UPDATE nation_update_cube_1 set regionkey = 10 where nationkey > 5", "line 1:1: hive.tpch.nation_update_cube_1 is a star-tree cube, UPDATE is not supported");

        assertUpdate("DROP CUBE nation_update_cube_1");
        assertUpdate("DROP TABLE nation_table_update_cube_test_1");
    }

    @Test
    public void testCubeStatusChange()
    {
        computeActual("CREATE TABLE nation_table_status_test AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_status_cube_1 ON nation_table_status_test " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        MaterializedResult result = computeActual("SHOW CUBES FOR nation_table_status_test");
        MaterializedRow matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_status_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        assertEquals(matchingRow.getField(2), "Inactive");

        assertUpdate("INSERT INTO CUBE nation_status_cube_1 where nationkey > 5", 19);
        result = computeActual("SHOW CUBES FOR nation_table_status_test");
        matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("nation_status_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        assertEquals(matchingRow.getField(2), "Active");

        assertUpdate("DROP CUBE nation_status_cube_1");
        assertUpdate("DROP TABLE nation_table_status_test");
    }

    @Test
    public void testCubePredicateTimestampType()
    {
        computeActual("CREATE TABLE timestamp_test_table (cint_1 int, cint_2 int, cint_3 int, cint_4 int, time_stamp TIMESTAMP)");
        computeActual("INSERT INTO timestamp_test_table (cint_1, cint_2, cint_3, cint_4, time_stamp) VALUES (4, 8 ,9 ,10 , timestamp '2021-03-15 15:20:00')");
        computeActual("CREATE CUBE timestamp_test_table_cube_1 ON timestamp_test_table " +
                "WITH (AGGREGATIONS=(count(*))," + " group=(time_stamp, cint_1), format= 'orc', partitioned_by = ARRAY['cint_1'])");
        computeActual("INSERT INTO CUBE timestamp_test_table_cube_1 where time_stamp = timestamp '2021-03-15 15:20:00'");
        MaterializedResult result = computeActual("SHOW CUBES FOR timestamp_test_table");
        MaterializedRow matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(1).toString().contains("timestamp_test_table_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(5).toString().contains("TIMESTAMP '2021-03-15 15:20:00.000'")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        assertEquals(matchingRow.getField(2), "Active");

        assertUpdate("DROP CUBE timestamp_test_table_cube_1");
        assertUpdate("DROP TABLE timestamp_test_table");
    }

    @Test
    public void testEmptyGroup()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_empty_group_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_empty_group_test_1 ON nation_table_empty_group_test_1 WITH (aggregations=(count(*)), group=())");
        assertQuerySucceeds("INSERT INTO CUBE nation_cube_empty_group_test_1");
        Object rowCount = computeScalar("SELECT count(*) FROM nation_cube_empty_group_test_1");
        assertEquals(rowCount, 1L);
        assertQuery(starTreeDisabledSession,
                "SELECT count(*) FROM nation_table_empty_group_test_1",
                "SELECT count(*) FROM nation",
                assertTableScan("nation_table_empty_group_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_empty_group_test_1",
                "SELECT count(*) FROM nation",
                assertTableScan("nation_cube_empty_group_test_1"));
        assertUpdate("DROP CUBE nation_cube_empty_group_test_1");
        assertUpdate("DROP TABLE nation_table_empty_group_test_1");
    }

    @Test
    public void testCreateCubeSyntax()
    {
        assertQueryFails("CREATE CUBE cube_syntax_test_1 ON nation WITH ()", "Missing property: GROUP");
        assertQueryFails("CREATE CUBE cube_syntax_test_2 ON nation WITH (AGGREGATIONS = (count(*), sum(nation_key)))", "Missing property: GROUP");
        assertQueryFails("CREATE CUBE cube_syntax_test_3 ON nation WITH (GROUP=(name))", "Missing property: AGGREGATIONS");
        assertQueryFails("CREATE CUBE cube_syntax_test_4 ON nation WITH (format = 'ORC', partitioned_by = ARRAY['region_key'], GROUP=(name))", "Missing property: AGGREGATIONS");
        assertQueryFails("CREATE CUBE cube_syntax_test_5 ON nation WITH (AGGREGATIONS = (count(*), sum(nation_key)), GROUP = (name), AGGREGATIONS = (sum(region_key)))", "Duplicate property: AGGREGATIONS");
        assertQueryFails("CREATE CUBE cube_syntax_test_6 ON nation WITH (GROUP = (country), GROUP = (name), AGGREGATIONS = (sum(region_key)))", "Duplicate property: GROUP");
    }

    @Test
    public void testInsertWithDifferentSecondInsertPredicates()
    {
        assertUpdate("CREATE CUBE partial_inserts_test_1 ON nation WITH (AGGREGATIONS= (count(*)), GROUP=(regionkey, nationkey))");
        assertUpdate("INSERT INTO CUBE partial_inserts_test_1 WHERE nationkey = 1", 1);
        assertQueryFails("INSERT INTO CUBE partial_inserts_test_1 WHERE regionkey = 1", "Where condition must only use the columns from the first insert: nationkey.");
        assertQueryFails("INSERT INTO CUBE partial_inserts_test_1 WHERE nationkey > 1 and regionkey = 1", "Where condition must only use the columns from the first insert: nationkey.");
        assertUpdate("INSERT INTO CUBE partial_inserts_test_1 WHERE nationkey = 2", 1);

        assertUpdate("CREATE CUBE partial_inserts_test_2 ON nation WITH (AGGREGATIONS= (count(*)), GROUP=(regionkey, nationkey))");
        assertUpdate("INSERT INTO CUBE partial_inserts_test_2 WHERE nationkey = 1 and regionkey = 1", 1);
        assertQueryFails("INSERT INTO CUBE partial_inserts_test_2 WHERE regionkey = 2", "Where condition must only use the columns from the first insert: nationkey, regionkey.");
        assertQueryFails("INSERT INTO CUBE partial_inserts_test_2", "Cannot allow insert. Inserting entire dataset but cube already has partial data");

        assertUpdate("DROP CUBE partial_inserts_test_1");
        assertUpdate("DROP CUBE partial_inserts_test_2");
    }

    @Test
    public void testAggregationsWithPartialData()
    {
        computeActual("CREATE TABLE nation_table_partial_data_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_partial_data_1 ON nation_table_partial_data_test_1 WITH (AGGREGATIONS=(count(*)), GROUP=(nationkey))");
        assertUpdate("INSERT INTO CUBE nation_cube_partial_data_1 WHERE nationkey = 1", 1);
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_cube_partial_data_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey >= 1",
                "SELECT count(*) FROM nation WHERE nationkey >= 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertUpdate("DROP CUBE nation_cube_partial_data_1");
        assertUpdate("CREATE CUBE nation_cube_partial_data_2 ON nation_table_partial_data_test_1 WITH (AGGREGATIONS=(count(*)), GROUP=(nationkey, regionkey))");
        assertUpdate("INSERT INTO CUBE nation_cube_partial_data_2 WHERE nationkey = 1 and regionkey = 1", 1);
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey >= 1",
                "SELECT count(*) FROM nation WHERE nationkey >= 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1 and regionkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1 and regionkey = 1",
                assertTableScan("nation_cube_partial_data_2"));
        assertUpdate("INSERT INTO CUBE nation_cube_partial_data_2 WHERE nationkey > 1 and regionkey = 2", 5);
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1 and regionkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1 and regionkey = 1",
                assertTableScan("nation_cube_partial_data_2"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1 and regionkey = 2",
                "SELECT count(*) FROM nation WHERE nationkey > 1 and regionkey = 2",
                assertTableScan("nation_cube_partial_data_2"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1 and regionkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1 and regionkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1 and regionkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1 and regionkey = 1",
                assertTableScan("nation_table_partial_data_test_1"));

        assertUpdate("DROP CUBE nation_cube_partial_data_2");
        assertUpdate("DROP TABLE nation_table_partial_data_test_1");
    }

    @Test
    public void testCubeMustNotBeUsed()
    {
        computeActual("CREATE TABLE line_item_table_test_1 AS SELECT * FROM lineitem");
        assertUpdate("CREATE CUBE line_item_cube_test_1 ON line_item_table_test_1 WITH (AGGREGATIONS=(sum(extendedprice)), GROUP=(orderkey))");
        assertQuerySucceeds("INSERT INTO CUBE line_item_cube_test_1");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey FROM orders o WHERE 100000 < (SELECT sum(extendedprice) FROM line_item_table_test_1 l WHERE l.orderkey = o.orderkey) ORDER BY custkey LIMIT 10",
                "SELECT custkey FROM orders o WHERE 100000 < (SELECT sum(extendedprice) FROM lineitem l WHERE l.orderkey = o.orderkey) ORDER BY custkey LIMIT 10",
                assertInTableScans("line_item_table_test_1"));
        assertUpdate("DROP CUBE line_item_cube_test_1");
        assertUpdate("DROP TABLE line_item_table_test_1");

        computeActual("CREATE TABLE orders_table_test_cube_not_used AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_not_used ON orders_table_test_cube_not_used WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_not_used");
        String[] joinQueries = new String[] {
                "SELECT ord.orderkey, ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY ord.orderkey, ord.orderdate"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_cube_not_used"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("orders_table_test_cube_not_used")));
        assertUpdate("DROP TABLE orders_table_test_cube_not_used");
    }

    @Test
    public void testOtherQueryTypes()
    {
        List<Session> sessions = ImmutableList.of(
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "true")
                        .setSystemProperty(SystemSessionProperties.ENABLE_EXECUTION_PLAN_CACHE, "true")
                        .build(),
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "false")
                        .setSystemProperty(SystemSessionProperties.ENABLE_EXECUTION_PLAN_CACHE, "true")
                        .build(),
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "true")
                        .setSystemProperty(SystemSessionProperties.ENABLE_EXECUTION_PLAN_CACHE, "false")
                        .build(),
                Session.builder(getSession())
                        .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "false")
                        .setSystemProperty(SystemSessionProperties.ENABLE_EXECUTION_PLAN_CACHE, "false")
                        .build());
        for (Session session : sessions) {
            assertQuery(session,
                    "WITH temp_table as(SELECT nationkey, count(*) AS count FROM nation WHERE nationkey > 10 GROUP BY nationkey) SELECT nationkey, count FROM temp_table");
            assertQuery(session,
                    "SELECT o.orderpriority, COUNT(*) FROM orders o WHERE o.orderdate >= date '1993-07-01' AND EXISTS (SELECT * FROM lineitem l WHERE l.orderkey = o.orderkey AND (l.returnflag = 'R' OR l.receiptdate > l.commitdate)) GROUP BY o.orderpriority");
            assertQuerySucceeds(session,
                    "create view count_by_shipmode_cube_test_1 as select shipmode, count(*) as count from lineitem group by shipmode");
            assertQuerySucceeds(session,
                    "DROP VIEW count_by_shipmode_cube_test_1");
            assertQuerySucceeds(session,
                    "select sum(l.extendedprice) / 7.0 as avg_yearly from lineitem l, part p where p.partkey = l.partkey and p.brand = 'Brand#33' and p.container = 'WRAP PACK' and l.quantity < (select 0.2 * avg(l2.quantity) from lineitem l2 where l2.partkey = p.partkey)");
        }
    }

    @Test
    public void testCubeRangeMerge()
    {
        computeActual("CREATE TABLE nation_table_range_merge_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_cube_range_merge_1 ON nation_table_range_merge_test_1 WITH (AGGREGATIONS=(count(*)), GROUP=(nationkey))");
        assertUpdate("INSERT INTO CUBE nation_cube_range_merge_1 WHERE nationkey = 1", 1);
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_range_merge_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_cube_range_merge_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_range_merge_test_1 WHERE nationkey BETWEEN 1 AND 2",
                "SELECT count(*) FROM nation WHERE nationkey BETWEEN 1 AND 2",
                assertTableScan("nation_table_range_merge_test_1"));
        assertUpdate("INSERT INTO CUBE nation_cube_range_merge_1 WHERE nationkey = 2", 1);
        assertQuery(starTreeEnabledSession,
                "SELECT count(*) FROM nation_table_range_merge_test_1 WHERE nationkey BETWEEN 1 AND 2",
                "SELECT count(*) FROM nation WHERE nationkey BETWEEN 1 AND 2",
                assertTableScan("nation_cube_range_merge_1"));
        assertUpdate("DROP CUBE nation_cube_range_merge_1");
        assertUpdate("DROP TABLE nation_table_range_merge_test_1");
    }

    @Test
    public void testCountDistinct()
    {
        computeActual("CREATE TABLE orders_count_distinct AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_count_distinct_cube ON orders_count_distinct WITH (AGGREGATIONS = (count(distinct custkey)), GROUP = (orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE orders_count_distinct_cube");
        assertQuery(starTreeEnabledSession,
                "SELECT count(distinct custkey) FROM orders_count_distinct WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'",
                "SELECT count(distinct custkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' AND '1992-01-10'",
                assertTableScan("orders_count_distinct"));
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, count(distinct custkey) FROM orders_count_distinct WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY orderdate ORDER BY orderdate",
                "SELECT orderdate, count(distinct custkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' AND '1992-01-10' GROUP BY orderdate ORDER BY orderdate",
                assertTableScan("orders_count_distinct_cube"));
        assertUpdate("DROP TABLE orders_count_distinct");
    }

    @Test
    public void testWithSourceFilter()
    {
        computeActual("CREATE TABLE orders_source_filter_test AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_with_source_filter ON orders_source_filter_test WITH (AGGREGATIONS = (sum(totalprice), count(distinct orderkey)), GROUP = (custkey), FILTER = (orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderstatus IN('F', 'O')))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_with_source_filter");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders_source_filter_test WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderstatus = 'F' GROUP BY custkey",
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderstatus = 'F' GROUP BY custkey",
                assertTableScan("orders_source_filter_test"));
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders_source_filter_test WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderstatus IN('F', 'O') GROUP BY custkey",
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderstatus IN('F', 'O') GROUP BY custkey",
                assertTableScan("orders_cube_with_source_filter"));
        assertUpdate("DROP TABLE orders_source_filter_test");
    }

    @Test
    public void testWithSourceFilterAndDataFilter()
    {
        computeActual("CREATE TABLE orders_table_source_data_filter AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_source_data_filter ON orders_table_source_data_filter WITH (AGGREGATIONS = (sum(totalprice), count(distinct orderkey)), GROUP = (custkey, orderpriority), FILTER = (orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_data_filter WHERE orderpriority = '1-URGENT'");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, orderpriority, sum(totalprice) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice) FROM orders WHERE orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by custkey, orderpriority",
                assertTableScan("orders_table_source_data_filter"));
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderpriority = '1-URGENT' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderpriority = '1-URGENT' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                assertTableScan("orders_cube_source_data_filter"));
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_data_filter WHERE orderpriority = '2-HIGH'");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderpriority = '2-HIGH' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderpriority = '2-HIGH' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                assertTableScan("orders_cube_source_data_filter"));
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND (orderpriority = '1-URGENT' OR orderpriority = '2-HIGH') GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderpriority IN ('1-URGENT', '2-HIGH') GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                assertTableScan("orders_cube_source_data_filter"));
        assertUpdate("DROP TABLE orders_table_source_data_filter");
    }

    @Test
    public void testSourceFilterWithVarchar()
    {
        computeActual("CREATE TABLE orders_table_source_filter_varchar AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_source_filter_varchar ON orders_table_source_filter_varchar WITH (AGGREGATIONS = (sum(totalprice), count(distinct orderkey)), GROUP = (custkey, orderdate), FILTER = (orderpriority = '1-URGENT'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_filter_varchar WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, orderdate, sum(totalprice) FROM orders_table_source_filter_varchar WHERE orderpriority = '1-URGENT' AND orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by custkey, orderdate",
                "SELECT custkey, orderdate, sum(totalprice) FROM orders WHERE orderpriority = '1-URGENT' AND orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by custkey, orderdate",
                assertTableScan("orders_cube_source_filter_varchar"));
        assertUpdate("DROP TABLE orders_table_source_filter_varchar");
    }

    @Test
    public void testSourceFilterWithCast()
    {
        computeActual("CREATE TABLE orders_table_source_filter_cast AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_source_filter_cast ON orders_table_source_filter_cast WITH (AGGREGATIONS = (count(*)), GROUP = (orderdate), FILTER = (custkey BETWEEN BIGINT '1' AND BIGINT '100'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_filter_cast WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'");
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, count(*) FROM orders_table_source_filter_cast WHERE custkey BETWEEN BIGINT '1' AND BIGINT '200' AND orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by orderdate",
                "SELECT orderdate, count(*) FROM orders WHERE custkey BETWEEN 1 AND 200 AND orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by orderdate",
                assertTableScan("orders_table_source_filter_cast"));
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, count(*) FROM orders_table_source_filter_cast WHERE custkey BETWEEN BIGINT '1' AND BIGINT '100' AND orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by orderdate",
                "SELECT orderdate, count(*) FROM orders WHERE custkey BETWEEN 1 AND 100 AND orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by orderdate",
                assertTableScan("orders_cube_source_filter_cast"));
        assertUpdate("DROP TABLE orders_table_source_filter_cast");
    }

    @Test
    public void testInsertCubeWithGreaterThanOperator()
    {
        computeActual("CREATE TABLE orders_table_insert_cube_greater_than AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_insert_cube_greater_than ON orders_table_insert_cube_greater_than WITH (AGGREGATIONS = (max(totalprice)), GROUP = (orderdate,custkey))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_insert_cube_greater_than WHERE custkey > 100");
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, max(totalprice) FROM orders_table_insert_cube_greater_than WHERE custkey = 100 GROUP BY orderdate",
                "SELECT orderdate, max(totalprice) FROM orders WHERE custkey = 100 GROUP BY orderdate",
                assertTableScan("orders_table_insert_cube_greater_than"));
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, max(totalprice) FROM orders_table_insert_cube_greater_than WHERE custkey >= 101 GROUP BY orderdate",
                "SELECT orderdate, max(totalprice) FROM orders WHERE custkey >= 101 GROUP BY orderdate",
                assertTableScan("orders_cube_insert_cube_greater_than"));
        assertUpdate("DROP TABLE orders_table_insert_cube_greater_than");
    }

    @Test
    public void testWithUnsupportedPredicate()
    {
        computeActual("CREATE TABLE orders_table_predicate_unsupported_predicate AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_predicate_unsupported_predicate ON orders_table_predicate_unsupported_predicate WITH (AGGREGATIONS=(sum(totalprice)), GROUP=(custkey, orderdate))");
        assertQueryFails("INSERT INTO CUBE orders_cube_predicate_unsupported_predicate WHERE orderdate = date '1992-01-01' OR custkey = 100", ".*Cannot support predicate.*");
        assertQueryFails("INSERT OVERWRITE CUBE orders_cube_predicate_unsupported_predicate WHERE orderdate = date '1992-01-01' OR custkey = 100", ".*Cannot support predicate.*");
        assertQueryFails("INSERT INTO CUBE orders_cube_predicate_unsupported_predicate WHERE orderdate", ".*WHERE clause must evaluate to a boolean:.*");
        assertUpdate("DROP TABLE orders_table_predicate_unsupported_predicate");
    }

    @Test
    public void testCreateCubeWithIncorrectFilterPredicate()
    {
        assertQueryFails(starTreeEnabledSession,
                "CREATE CUBE orders_cube_unsuported_filter_predicate ON orders WITH (AGGREGATIONS=(sum(totalprice)), GROUP=(custkey), FILTER = (orderdate))",
                ".*Filter property must evaluate to a boolean: actual type 'date'.*");
        assertQueryFails(starTreeEnabledSession,
                "CREATE CUBE orders_cube_unsuported_filter_predicate ON orders WITH (AGGREGATIONS=(sum(totalprice)), GROUP=(orderdate), FILTER = (custkey))",
                ".*Filter property must evaluate to a boolean: actual type 'bigint'.*");
    }

    @Test
    public void testCubeInsertWithMultipleCube()
    {
        computeActual("CREATE TABLE orders_table_multiple_cube_insert AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_mutiple_cube_insert_1 ON orders_table_multiple_cube_insert WITH (AGGREGATIONS = (max(totalprice)), GROUP = (orderdate,custkey))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_mutiple_cube_insert_1 WHERE custkey >= 100");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, max(totalprice) FROM orders_table_multiple_cube_insert WHERE custkey >= 101 GROUP BY custkey",
                "SELECT custkey, max(totalprice) FROM orders WHERE custkey >= 101 GROUP BY custkey",
                assertTableScan("orders_cube_mutiple_cube_insert_1"));
        computeActual("CREATE CUBE orders_cube_mutiple_cube_insert_2 ON orders_table_multiple_cube_insert WITH (AGGREGATIONS = (max(totalprice)), GROUP = (orderdate,custkey), FILTER = (orderkey > 1))");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE orders_cube_mutiple_cube_insert_2 WHERE custkey >= 100");
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, max(totalprice) FROM orders_table_multiple_cube_insert WHERE custkey >= 101 GROUP BY custkey",
                "SELECT custkey, max(totalprice) FROM orders WHERE custkey >= 101 GROUP BY custkey",
                assertTableScan("orders_cube_mutiple_cube_insert_1"));
        assertQuery(starTreeEnabledSession,
                "SELECT custkey, max(totalprice) FROM orders_table_multiple_cube_insert WHERE orderkey > 1 AND custkey >= 101 GROUP BY custkey",
                "SELECT custkey, max(totalprice) FROM orders WHERE orderkey > 1 AND custkey >= 101 GROUP BY custkey",
                assertTableScan("orders_cube_mutiple_cube_insert_2"));
        assertUpdate("DROP TABLE orders_table_multiple_cube_insert");
    }

    @Test
    public void testWithMultipleCubesAlmostSimilar()
    {
        computeActual("CREATE TABLE orders_table_multiple_cubes_similar AS SELECT * FROM orders");
        //cube creation order is important because the recently created cube is used for query execution in case of multi-match
        computeActual("CREATE CUBE orders_cube_similar_1 ON orders_table_multiple_cubes_similar WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (orderdate, orderstatus), FILTER = (orderpriority = '1-URGENT' AND shippriority = 1))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_similar_1 WHERE orderstatus = 'F'");
        computeActual("CREATE CUBE orders_cube_similar_2 ON orders_table_multiple_cubes_similar WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (orderdate, orderstatus), FILTER = (orderpriority = '1-URGENT'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_similar_2");
        assertQuery(starTreeEnabledSession,
                "SELECT orderdate, orderstatus, avg(totalprice) FROM orders_table_multiple_cubes_similar WHERE orderpriority = '1-URGENT' AND shippriority = 1 AND orderstatus = 'F' GROUP BY orderdate, orderstatus",
                "SELECT orderdate, orderstatus, avg(totalprice) FROM orders WHERE orderpriority = '1-URGENT' AND shippriority = 1 AND orderstatus = 'F' GROUP BY orderdate, orderstatus",
                assertTableScan("orders_cube_similar_1"));
        assertUpdate("DROP TABLE orders_table_multiple_cubes_similar");
    }

    @Test
    public void testCubeRangeVisitorAggregateComparison()
    {
        // Table creation
        assertQuerySucceeds(starTreeEnabledSession, "CREATE TABLE test_cube_range_visitor_aggregation_comparison(cint int, csmallint smallint, cbigint bigint, ctinyint tinyint)");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_aggregation_comparison VALUES " +
                "(1, smallint '1',bigint '1',tinyint '1')," +
                "(2, smallint '2',bigint '2',tinyint '2')," +
                "(3, smallint '3',bigint '3',tinyint '3')," +
                "(4, smallint '4',bigint '4',tinyint '4')," +
                "(5, smallint '5',bigint '5',tinyint '5')");

        // To ensure that tests do not disturb one another, since some queries use same types, the following are tested individually:
        // Comparison tests with cube creation, query, assert and drop
        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_1 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(ctinyint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_1 WHERE ctinyint = 3");
        MaterializedResult result1 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(ctinyint) FROM test_cube_range_visitor_aggregation_comparison WHERE ctinyint = 3 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_1"));
        assertEquals(result1.getRowCount(), 1);
        assertNotEquals(result1.getMaterializedRows().get(0).getField(0).toString(), "NULL");
        assertEquals(result1.getMaterializedRows().get(0).getField(0).toString(), "3");
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_1");

        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_2 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(ctinyint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_2 WHERE ctinyint <> 3");
        MaterializedResult result2 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(ctinyint) FROM test_cube_range_visitor_aggregation_comparison WHERE ctinyint <> 3 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_2"));
        assertEquals(result2.getRowCount(), 4);
        List<String> results2 = Stream.of(result2.getMaterializedRows().get(0).getField(0).toString(),
                result2.getMaterializedRows().get(1).getField(0).toString(),
                result2.getMaterializedRows().get(2).getField(0).toString(),
                result2.getMaterializedRows().get(3).getField(0).toString()).sorted().collect(Collectors.toList());
        assertFalse(results2.contains("NULL"));
        System.out.println(results2);
        assertEquals(Arrays.asList("1", "2", "4", "5"), results2);
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_2");

        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_3 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(csmallint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_3 WHERE csmallint >= 3");
        MaterializedResult result3 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(csmallint) FROM test_cube_range_visitor_aggregation_comparison WHERE csmallint >= 3 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_3"));
        assertEquals(result3.getRowCount(), 3);
        List<String> results3 = Stream.of(result3.getMaterializedRows().get(0).getField(0).toString(),
                result3.getMaterializedRows().get(1).getField(0).toString(),
                result3.getMaterializedRows().get(2).getField(0).toString()).sorted().collect(Collectors.toList());
        assertFalse(results3.contains("NULL"));
        assertEquals(Arrays.asList("3", "4", "5"), results3);
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_3");

        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_4 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(cbigint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_4 WHERE cbigint <= 3");
        MaterializedResult result4 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(cbigint) FROM test_cube_range_visitor_aggregation_comparison WHERE cbigint <= 3 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_4"));
        assertEquals(result4.getRowCount(), 3);
        List<String> results4 = Stream.of(result4.getMaterializedRows().get(0).getField(0).toString(),
                result4.getMaterializedRows().get(1).getField(0).toString(),
                result4.getMaterializedRows().get(2).getField(0).toString()).sorted().collect(Collectors.toList());
        assertFalse(results4.contains("NULL"));
        assertEquals(Arrays.asList("1", "2", "3"), results4);
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_4");

        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_5 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(csmallint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_5 WHERE csmallint > 3");
        MaterializedResult result5 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(csmallint) FROM test_cube_range_visitor_aggregation_comparison WHERE csmallint >= 4 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_5"));
        assertEquals(result5.getRowCount(), 2);
        List<String> results5 = Stream.of(result5.getMaterializedRows().get(0).getField(0).toString(),
                result5.getMaterializedRows().get(1).getField(0).toString()).sorted().collect(Collectors.toList());
        assertFalse(results5.contains("NULL"));
        assertEquals(Arrays.asList("4", "5"), results5);
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_5");

        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_comparison_6 ON test_cube_range_visitor_aggregation_comparison WITH (AGGREGATIONS = (count(cint), sum(cbigint)), GROUP = (csmallint, cbigint, ctinyint), PARTITIONED_BY = array['cint'])");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_comparison_6 WHERE cbigint < 3");
        MaterializedResult result6 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT sum(cbigint) FROM test_cube_range_visitor_aggregation_comparison WHERE cbigint < 3 GROUP BY csmallint, cbigint, ctinyint",
                assertTableScan("test_cube_range_visitor_comparison_6"));
        assertEquals(result6.getRowCount(), 2);
        List<String> results6 = Stream.of(result6.getMaterializedRows().get(0).getField(0).toString(),
                result6.getMaterializedRows().get(1).getField(0).toString()).sorted().collect(Collectors.toList());
        assertFalse(results6.contains("NULL"));
        assertEquals(Arrays.asList("1", "2"), results6);
        assertUpdate("DROP CUBE test_cube_range_visitor_comparison_6");

        // Drop table
        assertUpdate("DROP TABLE test_cube_range_visitor_aggregation_comparison");
    }

    @Test
    public void testCubeRangeVisitorBetweenExpression()
    {
        // Table creation
        assertQuerySucceeds(starTreeEnabledSession, "CREATE TABLE test_cube_range_visitor_between_table(cint int, csmallint smallint, cbigint bigint, ctinyint tinyint)");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_between_table VALUES(1,smallint '1',bigint '1',tinyint '1')");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_between_table VALUES(2,smallint '2',bigint '2',tinyint '2')");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_between_table VALUES(3,smallint '3',bigint '3',tinyint '3')");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_between_table VALUES(4,smallint '4',bigint '4',tinyint '4')");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO test_cube_range_visitor_between_table VALUES(5,smallint '5',bigint '5',tinyint '5')");

        // Cube creations
        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_between_1 ON test_cube_range_visitor_between_table WITH (AGGREGATIONS = (count(cint)), GROUP = (csmallint, cbigint, ctinyint))");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_between_1 WHERE ctinyint BETWEEN 2 AND 4");
        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_between_2 ON test_cube_range_visitor_between_table WITH (AGGREGATIONS = (count(cint)), GROUP = (csmallint, cbigint, ctinyint))");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_between_2 WHERE csmallint BETWEEN 2 AND 4");
        assertQuerySucceeds(starTreeEnabledSession, "CREATE CUBE test_cube_range_visitor_between_3 ON test_cube_range_visitor_between_table WITH (AGGREGATIONS = (count(cint)), GROUP = (csmallint, cbigint, ctinyint))");
        assertQuerySucceeds(starTreeEnabledSession, "INSERT INTO CUBE test_cube_range_visitor_between_3 WHERE cbigint BETWEEN 2 AND 4");

        // Querying test
        MaterializedResult result1 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT count(cint) FROM test_cube_range_visitor_between_table WHERE ctinyint BETWEEN 2 AND 4",
                assertTableScan("test_cube_range_visitor_between_1"));
        MaterializedResult result2 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT count(cint) FROM test_cube_range_visitor_between_table WHERE csmallint BETWEEN 2 AND 4",
                assertTableScan("test_cube_range_visitor_between_2"));
        MaterializedResult result3 = computeActualAndAssertPlan(starTreeEnabledSession,
                "SELECT count(cint) FROM test_cube_range_visitor_between_table WHERE cbigint BETWEEN 2 AND 4",
                assertTableScan("test_cube_range_visitor_between_3"));

        // Result assertions
        assertEquals(result1.getRowCount(), 1);
        assertEquals(result2.getRowCount(), 1);
        assertEquals(result3.getRowCount(), 1);
        assertEquals(result1.getMaterializedRows().get(0).getField(0).toString(), "3");
        assertEquals(result2.getMaterializedRows().get(0).getField(0).toString(), "3");
        assertEquals(result3.getMaterializedRows().get(0).getField(0).toString(), "3");
        assertUpdate("DROP TABLE test_cube_range_visitor_between_table");
    }

    @Test
    public void testAggregationWithVarcharPredicateRange()
    {
        computeActual("CREATE TABLE web_usage_varchar_test(dt varchar(10), browser varchar(100), impressions integer)");
        computeActual("INSERT INTO web_usage_varchar_test VALUES " +
                "('20220101', 'CHROME', 10), ('20220101', 'FIREFOX', 5), ('20220101', 'SAFARI', 8), " +
                "('20220102', 'CHROME', 15), ('20220102', 'FIREFOX', 12), ('20220102', 'SAFARI', 9)," +
                "('20220103', 'CHROME', 6), ('20220103', 'FIREFOX', 4), ('20220103', 'SAFARI', 2)");
        computeActual("CREATE CUBE web_usage_varchar_cube_test ON web_usage_varchar_test WITH (AGGREGATIONS = (sum(impressions), count(*)), GROUP = (dt))");
        computeActual("INSERT INTO CUBE web_usage_varchar_cube_test WHERE dt = '20220101'");
        MaterializedResult tableResults = computeActualAndAssertPlan(
                starTreeDisabledSession,
                "SELECT dt, sum(impressions), count(*) FROM web_usage_varchar_test WHERE dt = '20220101' GROUP BY dt",
                assertTableScan("web_usage_varchar_test"));
        MaterializedResult cubeResults = computeActualAndAssertPlan(
                starTreeEnabledSession,
                "SELECT dt, sum(impressions), count(*) FROM web_usage_varchar_test WHERE dt = '20220101' GROUP BY dt",
                assertTableScan("web_usage_varchar_cube_test"));
        assertEqualsIgnoreOrder(tableResults.getMaterializedRows(), cubeResults.getMaterializedRows());
        computeActual("INSERT INTO CUBE web_usage_varchar_cube_test WHERE dt between '20220102' and '20220103'");
        tableResults = computeActualAndAssertPlan(
                starTreeDisabledSession,
                "SELECT dt, sum(impressions), count(*) FROM web_usage_varchar_test WHERE dt between '20220101' AND '20220103' GROUP BY dt",
                assertTableScan("web_usage_varchar_test"));
        cubeResults = computeActualAndAssertPlan(
                starTreeEnabledSession,
                "SELECT dt, sum(impressions), count(*) FROM web_usage_varchar_test WHERE dt between '20220101' AND '20220103' GROUP BY dt",
                assertTableScan("web_usage_varchar_cube_test"));
        assertEqualsIgnoreOrder(tableResults.getMaterializedRows(), cubeResults.getMaterializedRows());
        assertQueryFails("INSERT INTO CUBE web_usage_varchar_cube_test WHERE dt = '20220103'", "Cannot allow insert. Cube already contains data for the given predicate.*");
        assertUpdate("DROP TABLE web_usage_varchar_test");
    }

    @Test(enabled = false, description = "Need to fix decimal comparison error")
    public void testJoinSumAggregation()
    {
        computeActual("CREATE TABLE orders_table_test_sum_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_sum_totalprice ON orders_table_test_sum_agg WITH (AGGREGATIONS = (sum(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_sum_totalprice");
        String[] joinQueries = new String[] {
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_sum_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_sum_totalprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_sum_agg");
    }

    @Test(enabled = false, description = "Need to fix decimal comparison error")
    public void testJoinSumWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_sum_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_sum_with_predicate ON orders_table_sum_with_predicate WITH (AGGREGATIONS = (sum(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_sum_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_sum_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_sum_with_predicate")));

        assertUpdate("CREATE CUBE cube_orders_sum_with_predicate_2 ON orders_table_sum_with_predicate WITH (AGGREGATIONS = (sum(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_sum_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.address, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT cust.phone, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, SUM(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_sum_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_sum_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_sum_with_predicate");
    }

    @Test
    public void testJoinAvgAggregation()
    {
        computeActual("CREATE TABLE orders_table_test_avg_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_avg_totalprice ON orders_table_test_avg_agg WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_avg_totalprice");
        String[] joinQueries = new String[] {
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_avg_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_avg_totalprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_avg_agg");
    }

    @Test
    public void testJoinAvgWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_avg_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_avg_with_predicate ON orders_table_avg_with_predicate WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_avg_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_avg_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_avg_with_predicate")));
        assertUpdate("CREATE CUBE cube_orders_avg_with_predicate_2 ON orders_table_avg_with_predicate WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_avg_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.address, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT cust.phone, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, AVG(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_avg_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_avg_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_avg_with_predicate");
    }

    @Test(enabled = false, description = "Test failing. Could be due to incorrect matching group by clause. The avg_totalprice column should be used only if group is exact match. Join condition could be causing problem here.")
    public void testJoinAvgAggregationWithPartialGroupByMatch()
    {
        computeActual("CREATE TABLE orders_table_partial_group_by_match AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE orders_cube_partial_group_by_match ON orders_table_partial_group_by_match WITH (AGGREGATIONS = (avg(totalprice)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_partial_group_by_match");
        String[] joinQueries = new String[] {
                "SELECT ord.custkey, avg(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY ord.custkey",
                "SELECT ord.custkey, avg(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey",
                "SELECT ord.orderdate, avg(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY ord.orderdate",
                "SELECT ord.orderdate, avg(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.orderdate"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_partial_group_by_match"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("orders_cube_partial_group_by_match")));
        assertQuerySucceeds("DROP TABLE orders_table_partial_group_by_match");
    }

    @Test
    public void testJoinCountAggregation()
    {
        computeActual("CREATE TABLE orders_table_test_count_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_count_totalprice ON orders_table_test_count_agg WITH (AGGREGATIONS = (count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_count_totalprice");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_count_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_count_totalprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_count_agg");
    }

    @Test
    public void testJoinCountWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_count_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_count_with_predicate ON orders_table_count_with_predicate WITH (AGGREGATIONS = (count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_count_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_count_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_count_with_predicate")));

        assertUpdate("CREATE CUBE cube_orders_count_with_predicate_2 ON orders_table_count_with_predicate WITH (AGGREGATIONS = (count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_count_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.address, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT cust.phone, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_count_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_count_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_count_with_predicate");
    }

    @Test
    public void testJoinCountDistinctAggregation()
    {
        computeActual("CREATE TABLE customer_table_count_distinct_agg AS SELECT * FROM customer");
        assertUpdate("CREATE CUBE cube_customer_mktsegment_count_distinct ON customer_table_count_distinct_agg WITH (AGGREGATIONS = (count(distinct mktsegment)), GROUP = (nationkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_customer_mktsegment_count_distinct");
        String[] joinQueries = new String[] {
                "SELECT cust.nationkey, count(distinct cust.mktsegment) FROM %s cust INNER JOIN nation nat ON cust.nationkey = nat.nationkey GROUP BY cust.nationkey",
                "SELECT cust.nationkey, count(distinct cust.mktsegment) FROM %s cust, nation nat WHERE cust.nationkey = nat.nationkey GROUP BY cust.nationkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "customer_table_count_distinct_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "customer"),
                assertInTableScans("cube_customer_mktsegment_count_distinct")));
        assertQuerySucceeds("DROP TABLE customer_table_count_distinct_agg");
    }

    @Test
    public void testJoinCountDistinctWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_count_distinct_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_count_distinct_with_predicate ON orders_table_count_distinct_with_predicate WITH (AGGREGATIONS = (count(distinct orderstatus)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_count_distinct_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT ord.custkey, count(distinct ord.orderstatus) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY ord.custkey",
                "SELECT ord.custkey, count(distinct ord.orderstatus) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_count_distinct_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_count_distinct_with_predicate")));
        assertUpdate("CREATE CUBE cube_orders_count_distinct_with_predicate_2 ON orders_table_count_distinct_with_predicate WITH (AGGREGATIONS = (count(distinct orderstatus)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_count_distinct_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT ord.custkey, count(distinct ord.orderstatus) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY ord.custkey",
                "SELECT ord.custkey, count(distinct ord.orderstatus) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_count_distinct_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_count_distinct_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_count_distinct_with_predicate");
    }

    @Test
    public void testJoinCountDistinctNotSupportedWithGroupByColumnFromOtherTable()
    {
        computeActual("CREATE TABLE customer_table_count_distinct_agg_2 AS SELECT * FROM customer");
        assertUpdate("CREATE CUBE cube_customer_mktsegment_count_distinct_2 ON customer_table_count_distinct_agg_2 WITH (AGGREGATIONS = (count(distinct mktsegment)), GROUP = (nationkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_customer_mktsegment_count_distinct_2");
        String[] joinQueries = new String[] {
                "SELECT nat.name, count(distinct cust.mktsegment) FROM %s cust INNER JOIN nation nat ON cust.nationkey = nat.nationkey GROUP BY nat.name",
                "SELECT nat.name, count(distinct cust.mktsegment) FROM %s cust, nation nat WHERE cust.nationkey = nat.nationkey GROUP BY nat.name"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "customer_table_count_distinct_agg_2"),
                starTreeDisabledSession,
                String.format(joinQuery, "customer"),
                assertInTableScans("customer_table_count_distinct_agg_2")));
        assertQuerySucceeds("DROP TABLE customer_table_count_distinct_agg_2");
    }

    @Test
    public void testJoinMinAggregation()
    {
        computeActual("CREATE TABLE orders_table_test_min_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_min_totalprice ON orders_table_test_min_agg WITH (AGGREGATIONS = (min(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_min_totalprice");
        String[] joinQueries = new String[] {
                "SELECT cust.name, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_min_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_min_totalprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_min_agg");
    }

    @Test
    public void testJoinMinWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_min_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_min_with_predicate ON orders_table_min_with_predicate WITH (AGGREGATIONS = (min(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_min_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_min_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_min_with_predicate")));

        assertUpdate("CREATE CUBE cube_orders_min_with_predicate_2 ON orders_table_min_with_predicate WITH (AGGREGATIONS = (min(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_min_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT cust.name, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.address, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT cust.phone, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, min(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_min_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000 "),
                assertInTableScans("cube_orders_min_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_min_with_predicate");
    }

    @Test
    public void testJoinMaxAggregation()
    {
        computeActual("CREATE TABLE orders_table_test_max_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_max_totalprice ON orders_table_test_max_agg WITH (AGGREGATIONS = (max(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_max_totalprice");
        String[] joinQueries = new String[] {
                "SELECT cust.name, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };

        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_max_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_max_totalprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_max_agg");
    }

    @Test
    public void testJoinMaxWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_max_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_max_with_predicate ON orders_table_max_with_predicate WITH (AGGREGATIONS = (max(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_max_with_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_max_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_max_with_predicate")));
        assertUpdate("CREATE CUBE cube_orders_max_with_predicate_2 ON orders_table_max_with_predicate WITH (AGGREGATIONS = (max(totalprice)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_max_with_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'");
        joinQueries = new String[] {
                "SELECT cust.name, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.address, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT cust.phone, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, max(ord.totalprice) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_max_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_max_with_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_max_with_predicate");
    }

    @Test(enabled = false, description = "Need to fix decimal comparison error")
    public void testJoinAllAggregationsWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_test_all_agg_with_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_all_agg_orderprice_predicate ON orders_table_test_all_agg_with_predicate WITH (AGGREGATIONS = (sum(totalprice), min(totalprice), max(totalprice), avg(totalprice), count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_all_agg_orderprice_predicate");
        //Avg comparison is failing because of the Double comparison error in MaterializedView Objects
        //Need to implement Double equals with some tolerance.
        // For example. 137233.32500000004 is same as 137233.325. But current implementation says they are not.
        String[] joinQueries = new String[] {
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.name",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.address",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY cust.phone",
                "SELECT ord.custkey, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey >= 101 GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_all_agg_with_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_all_agg_orderprice_predicate")));

        assertUpdate("CREATE CUBE cube_orders_all_agg_orderprice_predicate_2 ON orders_table_test_all_agg_with_predicate WITH (AGGREGATIONS = (sum(totalprice), min(totalprice), max(totalprice), avg(totalprice), count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_all_agg_orderprice_predicate_2 WHERE custkey BETWEEN BIGINT '100000' AND BIGINT '1000000' ");
        joinQueries = new String[] {
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.name",
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.name",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.address",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.address",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE %s GROUP BY cust.phone",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY cust.phone",
                "SELECT ord.custkey, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND %s GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_all_agg_with_predicate", "ord.custkey BETWEEN BIGINT '100000' AND BIGINT '1000000'"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders", "ord.custkey BETWEEN 100000 AND 1000000"),
                assertInTableScans("cube_orders_all_agg_orderprice_predicate_2")));
        assertQuerySucceeds("DROP TABLE orders_table_test_all_agg_with_predicate");
    }

    @Test(enabled = false, description = "Need to fix decimal comparison error")
    public void testJoinAllAggregations()
    {
        computeActual("CREATE TABLE orders_table_test_all_agg AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_all_agg_orderprice ON orders_table_test_all_agg WITH (AGGREGATIONS = (sum(totalprice), min(totalprice), max(totalprice), avg(totalprice), count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_all_agg_orderprice");
        //Avg comparison is failing because of the Double comparison error in MaterializedView Objects
        //Need to implement Double equals with some tolerance.
        // For example. 137233.32500000004 is same as 137233.325. But current implementation says they are not.
        String[] joinQueries = new String[] {
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.address, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.address",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT cust.phone, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.phone",
                "SELECT ord.custkey, sum(ord.totalprice), min(ord.totalprice), max(ord.totalprice), avg(ord.totalprice), count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.custkey"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_test_all_agg"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_all_agg_orderprice")));
        assertQuerySucceeds("DROP TABLE orders_table_test_all_agg");
    }

    @Test
    public void testJoinGroupByMultipleColumn()
    {
        computeActual("CREATE TABLE orders_table_groupby_multiple_column AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_groupby_multiple_column ON orders_table_groupby_multiple_column WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_groupby_multiple_column");
        String[] joinQueries = new String[] {
                "SELECT ord.custkey, ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY ord.custkey, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name, ord.orderdate",
                "SELECT cust.custkey, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.custkey",
                "SELECT cust.custkey, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.custkey",
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY cust.name",
                "SELECT ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey GROUP BY ord.orderdate",
                "SELECT ord.orderdate, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey GROUP BY ord.orderdate"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_groupby_multiple_column"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_groupby_multiple_column")));
        assertQuerySucceeds("DROP TABLE orders_table_groupby_multiple_column");
    }

    @Test
    public void testJoinGroupByMultipleColumnWithQueryPredicate()
    {
        computeActual("CREATE TABLE orders_table_groupby_multiple_column_query_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_groupby_multiple_column_query_predicate ON orders_table_groupby_multiple_column_query_predicate WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_groupby_multiple_column_query_predicate");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey BETWEEN 10 AND 20 GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey BETWEEN 10 AND 20 GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_groupby_multiple_column_query_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_groupby_multiple_column_query_predicate")));
        assertQuerySucceeds("DROP TABLE orders_table_groupby_multiple_column_query_predicate");
    }

    @Test
    public void testJoinGroupByMultipleColumnWithCubePredicate()
    {
        computeActual("CREATE TABLE orders_table_groupby_multiple_column_cube_predicate AS SELECT * FROM orders");
        assertUpdate("CREATE CUBE cube_orders_groupby_multiple_column_cube_predicate ON orders_table_groupby_multiple_column_cube_predicate WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE cube_orders_groupby_multiple_column_cube_predicate WHERE custkey BETWEEN 1 AND 200");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey BETWEEN 10 AND 20 GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey BETWEEN 10 AND 20 GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate",
                "SELECT cust.name, ord.orderdate, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.custkey BETWEEN 10 AND 20 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name, ord.orderdate"
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_groupby_multiple_column_cube_predicate"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("cube_orders_groupby_multiple_column_cube_predicate")));
        assertQuerySucceeds("DROP TABLE orders_table_groupby_multiple_column_cube_predicate");
    }

    @Test
    public void testJoinAggregationWithSourceFilter()
    {
        computeActual("CREATE TABLE orders_join_table_source_filter_varchar AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_join_cube_source_filter_varchar ON orders_join_table_source_filter_varchar WITH (AGGREGATIONS = (count(*)), GROUP = (custkey), FILTER = (orderpriority = '1-URGENT'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_join_cube_source_filter_varchar");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderpriority = '1-URGENT' GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.orderpriority = '1-URGENT' GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderpriority = '1-URGENT' AND cust.mktsegment = 'HOUSEHOLD' GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.orderpriority = '1-URGENT' AND cust.mktsegment = 'HOUSEHOLD' GROUP BY cust.name",
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_join_table_source_filter_varchar"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("orders_join_cube_source_filter_varchar")));
        assertUpdate("DROP TABLE orders_join_table_source_filter_varchar");
    }

    @Test
    public void testAggregationWithSourceFilterAndDataFilter()
    {
        computeActual("CREATE TABLE orders_table_source_filter_and_data_filter AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_source_filter_and_data_filter ON orders_table_source_filter_and_data_filter WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderpriority), FILTER = (orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_filter_and_data_filter WHERE orderpriority = '1-URGENT'");
        String[] joinQueries = new String[] {
                "SELECT cust.name, count(*) FROM %s ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderpriority = '1-URGENT' AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name",
                "SELECT cust.name, count(*) FROM %s ord, customer cust WHERE ord.custkey = cust.custkey AND ord.orderpriority = '1-URGENT' AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name",
        };
        Stream.of(joinQueries).forEach(joinQuery -> assertQuery(
                starTreeEnabledSession,
                String.format(joinQuery, "orders_table_source_filter_and_data_filter"),
                starTreeDisabledSession,
                String.format(joinQuery, "orders"),
                assertInTableScans("orders_cube_source_filter_and_data_filter")));
        assertUpdate("DROP TABLE orders_table_source_filter_and_data_filter");

        computeActual("CREATE TABLE orders_table_source_filter_and_data_filter_2 AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_source_filter_and_data_filter_2 ON orders_table_source_filter_and_data_filter_2 WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate), FILTER = (orderkey BETWEEN BIGINT '100' AND BIGINT '10000'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_filter_and_data_filter_2 WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'");
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_source_filter_and_data_filter_2 ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderkey BETWEEN BIGINT '100000' AND BIGINT '1000000' AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderkey BETWEEN 100000 AND 1000000 AND ord.orderdate BETWEEN '1992-01-01' AND '1992-01-10' GROUP BY cust.name",
                assertInTableScans("orders_table_source_filter_and_data_filter_2"));
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_source_filter_and_data_filter_2 ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderkey BETWEEN BIGINT '100' AND BIGINT '10000' AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderkey BETWEEN 100 AND 10000 AND ord.orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' GROUP BY cust.name",
                assertInTableScans("orders_cube_source_filter_and_data_filter_2"));
        assertUpdate("DROP TABLE orders_table_source_filter_and_data_filter_2");
    }

    @Test
    public void testPartitionedCubeIncorrectResult()
    {
        computeActual("create table table_cube_incorrect_result(cint int, csmallint smallint,cbigint bigint,ctiny tinyint,cvarchar varchar,cchar char,cboolean boolean,cdouble double,creal real,cdate date,ctimestamp timestamp,cdecimal decimal(2,1),cstring string)");
        computeActual("insert into table_cube_incorrect_result values (1,smallint '1',bigint '1',tinyint '1','a',char 'a',boolean '1',double '1.0',real '1.0',date '2021-03-11',timestamp '2021-03-11 15:20:00',1.0,'a'), " +
                "(1,smallint '1',bigint '1',tinyint '1','a',char 'a',boolean '1',double '1.0',real '1.0',date '2021-03-11',timestamp '2021-03-11 15:20:00',1.0,'a'), " +
                "(2,smallint '2',bigint '2',tinyint '2','b',char 'b',boolean '0',double '2.0',real '2.0',date '2021-03-12',timestamp '2021-03-12 15:20:00',2.0,'b'), " +
                "(3,smallint '3',bigint '3',tinyint '3','c',char 'c',boolean '1',double '3.0',real '3.0',date '2021-03-13',timestamp '2021-03-13 15:20:00',3.0,'c'), " +
                "(4,smallint '4',bigint '4',tinyint '4','d',char 'd',boolean '0',double '4.0',real '4.0',date '2021-03-14',timestamp '2021-03-14 15:20:00',4.0,'d'), " +
                "(5,smallint '5',bigint '5',tinyint '5','e',char 'e',boolean '1',double '5.0',real '5.0',date '2021-03-15',timestamp '2021-03-15 15:20:00',5.0,'e'), " +
                "(6,smallint '6',bigint '6',tinyint '6','f',char 'f',boolean '0',double '6.0',real '6.0',date '2021-03-16',timestamp '2021-03-16 15:20:00',6.0,'f'), " +
                "(7,smallint '7',bigint '7',tinyint '7','g',char 'g',boolean '1',double '7.0',real '7.0',date '2021-03-17',timestamp '2021-03-17 15:20:00',7.0,'g'), " +
                "(8,smallint '8',bigint '8',tinyint '8','h',char 'h',boolean '0',double '8.0',real '8.0',date '2021-03-19',timestamp '2021-03-19 15:20:00',8.0,'h'), " +
                "(9,smallint '9',bigint '9',tinyint '9','i',char 'i',boolean '1',double '9.0',real '9.0',date '2021-03-20',timestamp '2021-03-20 15:20:00',9.0,'i'), " +
                "(NULL,smallint '9',bigint '9',tinyint '9','j',char 'j',boolean '0',double '9.0',real '9.0',date '2021-03-21',timestamp '2021-03-21 15:20:00',9.0,'j'), " +
                "(NULL,smallint '10',bigint '10',tinyint '10','k',char 'k',boolean '1',double '10.1',real '10.1',date '2021-03-22',timestamp '2021-03-22 14:20:00',NULL,'NULL')");
        assertQuerySucceeds("create cube cube_partitioned_result ON table_cube_incorrect_result with (aggregations=(count(cbigint)),group=(cint,csmallint,cbigint,ctiny),partitioned_by = array['csmallint'])");
        assertQuerySucceeds("INSERT INTO CUBE cube_partitioned_result WHERE cbigint < 3");
        String sql = "SELECT COUNT(cbigint),cint,csmallint,cbigint,ctiny FROM table_cube_incorrect_result WHERE cbigint < 3 GROUP BY cint,csmallint,cbigint,ctiny";
        MaterializedResult expected = computeActualAndAssertPlan(starTreeDisabledSession, sql, assertInTableScans("table_cube_incorrect_result"));
        MaterializedResult actual = computeActualAndAssertPlan(starTreeEnabledSession, sql, assertInTableScans("cube_partitioned_result"));
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        assertUpdate("DROP TABLE table_cube_incorrect_result");
    }

    @Test
    public void testWithMultipleMatchingCubes()
    {
        computeActual("CREATE TABLE orders_table_matching_cubes_test AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_matching_cube_1 ON orders_table_matching_cubes_test WITH (AGGREGATIONS = (count(*)), GROUP = (custkey))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_matching_cube_1 WHERE custkey >= 100");
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_matching_cubes_test ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                assertInTableScans("orders_cube_matching_cube_1"));

        computeActual("CREATE CUBE orders_cube_matching_cube_2 ON orders_table_matching_cubes_test WITH (AGGREGATIONS = (count(*)), GROUP = (custkey), FILTER = (orderkey > 1))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_matching_cube_2 WHERE custkey >= 100");
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_matching_cubes_test ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                assertInTableScans("orders_cube_matching_cube_1"));
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_matching_cubes_test ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 and ord.orderkey > 1 GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 and ord.orderkey > 1 GROUP BY cust.name",
                assertInTableScans("orders_cube_matching_cube_2"));

        //cube creation order is important because the recently created cube is used for query execution in case of multi-match
        computeActual("CREATE CUBE orders_cube_matching_cube_3 ON orders_table_matching_cubes_test WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderstatus), FILTER = (orderpriority = '1-URGENT'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_matching_cube_3");
        computeActual("CREATE CUBE orders_cube_matching_cube_4 ON orders_table_matching_cubes_test WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderstatus), FILTER = (orderpriority = '1-URGENT'))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_matching_cube_4 WHERE orderstatus = 'F'");
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_matching_cubes_test ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderpriority = '1-URGENT' AND ord.orderstatus = 'F' GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.orderpriority = '1-URGENT' AND ord.orderstatus = 'F' GROUP BY cust.name",
                assertInTableScans("orders_cube_matching_cube_4"));

        computeActual("CREATE CUBE orders_cube_matching_cube_5 ON orders_table_matching_cubes_test WITH (AGGREGATIONS = (count(*)), GROUP = (custkey, orderdate))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_matching_cube_5 WHERE custkey >= 100");
        //orders_cube_matching_cube_1 must still be used for optimizing this query because it provides optimal performance compared to orders_cube_matching_cube_5
        assertQuery(
                starTreeEnabledSession,
                "SELECT cust.name, count(*) FROM orders_table_matching_cubes_test ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                starTreeDisabledSession,
                "SELECT cust.name, count(*) FROM orders ord INNER JOIN customer cust ON ord.custkey = cust.custkey WHERE ord.custkey >= 101 GROUP BY cust.name",
                assertInTableScans("orders_cube_matching_cube_1"));

        assertUpdate("DROP TABLE orders_table_matching_cubes_test");
    }

    @Test(enabled = false, description = "Disabled because of decimal comparison issue with MaterializedRow")
    public void testStarSchemaJoin()
    {
        //Create Star Schema Tables from TPCH
        assertQuerySucceeds("CREATE TABLE ssb_supplier(suppkey, name, address, city, nation, region, phone) AS SELECT supp.suppkey, supp.name, supp.address, substr(nat.name, 1, 3) as city, nat.name as nation, reg.name as region, supp.phone FROM supplier supp LEFT JOIN nation nat ON supp.nationkey = nat.nationkey LEFT JOIN region reg ON nat.regionkey = reg.regionkey");
        assertQuerySucceeds("CREATE TABLE ssb_customer(custkey, name, address, city, nation, region, phone, mktsegment) AS SELECT cust.custkey, cust.name, cust.address, substr(nat.name, 1, 3) as city, nat.name as nation, reg.name as region, cust.phone, cust.mktsegment FROM customer cust LEFT JOIN nation nat ON cust.nationkey = nat.nationkey LEFT JOIN region reg ON nat.regionkey = reg.regionkey");
        assertQuerySucceeds("CREATE TABLE ssb_part(partkey, name, mfgr, category, brand , color, type, size, container) AS SELECT part.partkey, part.name, part.mfgr, cast(NULL as VARCHAR), part.brand, cast(NULL AS VARCHAR), part.type, part.size, part.container FROM part part");
        assertQuerySucceeds("CREATE TABLE ssb_lineorder(orderkey, linenumber, custkey, partkey, suppkey, orderdate, orderpriotity, shippriority, quantity, extendedprice, ordtotalprice, discount, revenue, supplycost, tax, commitdate, shipmode) AS select line.orderkey as orderkey, line.linenumber, ord.custkey, line.partkey, line.suppkey, ord.orderdate, ord.orderpriority, ord.shippriority, line.quantity, line.extendedprice, ord.totalprice, line.discount, cast((line.extendedprice * (cast(1 as double) - line.discount)) as bigint) as revenue, partsupp.supplycost, line.tax, line.commitdate, line.shipmode FROM lineitem line, partsupp partsupp, orders ord WHERE line.orderkey = ord.orderkey AND line.partkey = partsupp.partkey AND line.suppkey = partsupp.suppkey");

        computeActual("CREATE CUBE lo_total_sums_cube ON ssb_lineorder WITH (AGGREGATIONS = (sum(ordtotalprice), sum(extendedprice)), GROUP = ())");
        computeActual("INSERT INTO CUBE lo_total_sums_cube");
        assertSSBQuery("SELECT sum(lo.ordtotalprice), sum(lo.extendedprice) FROM ssb_lineorder lo", "lo_total_sums_cube");

        computeActual("CREATE CUBE lo_total_price_order_date_cube ON ssb_lineorder WITH (AGGREGATIONS = (sum(ordtotalprice), sum(extendedprice)), GROUP = (orderdate))");
        computeActual("INSERT INTO CUBE lo_total_price_order_date_cube");
        assertSSBQuery("SELECT sum(lo.ordtotalprice), sum(lo.extendedprice) FROM ssb_lineorder lo GROUP BY orderdate", "lo_total_price_order_date_cube");

        computeActual("CREATE CUBE lo_total_price_order_customer_cube ON ssb_lineorder WITH (AGGREGATIONS = (sum(ordtotalprice), sum(extendedprice), sum(revenue)), GROUP = (custkey))");
        computeActual("INSERT INTO CUBE lo_total_price_order_customer_cube");
        assertSSBQuery("SELECT sum(lo.ordtotalprice), sum(lo.extendedprice), sum(lo.revenue) FROM ssb_lineorder lo GROUP BY lo.custkey", "lo_total_price_order_customer_cube");
        assertSSBQuery("SELECT cust.name, sum(lo.ordtotalprice), sum(lo.extendedprice) FROM ssb_lineorder lo INNER JOIN ssb_customer cust ON lo.custkey = cust.custkey GROUP BY cust.name", "lo_total_price_order_customer_cube");
        assertSSBQuery("SELECT cust.nation, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_customer cust ON lo.custkey = cust.custkey GROUP BY cust.nation", "lo_total_price_order_customer_cube");
        assertSSBQuery("SELECT cust.region, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_customer cust ON lo.custkey = cust.custkey GROUP BY cust.region", "lo_total_price_order_customer_cube");

        computeActual("CREATE CUBE lo_revenue_cube ON ssb_lineorder WITH (AGGREGATIONS = (SUM(revenue)), GROUP = (suppkey, custkey))");
        computeActual("INSERT INTO CUBE lo_revenue_cube");
        assertSSBQuery("SELECT cust.nation, supp.nation, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_customer cust ON lo.custkey = cust.custkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE cust.region = 'ASIA' AND supp.region = 'ASIA' GROUP BY cust.nation, supp.nation", "lo_revenue_cube");
        assertSSBQuery("SELECT cust.nation, supp.nation, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_customer cust, ssb_supplier supp WHERE lo.custkey = cust.custkey AND lo.suppkey = supp.suppkey AND cust.region = 'ASIA' AND supp.region = 'ASIA' GROUP BY cust.nation, supp.nation", "lo_revenue_cube");
        assertSSBQuery("SELECT cust.city, supp.city, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_customer cust ON lo.custkey = cust.custkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE cust.nation = 'UNITED STATES' AND supp.region = 'UNITED STATES' GROUP BY cust.city, supp.city", "lo_revenue_cube");
        assertSSBQuery("SELECT cust.city, supp.city, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_customer cust, ssb_supplier supp WHERE lo.custkey = cust.custkey AND lo.suppkey = supp.suppkey AND cust.nation = 'UNITED STATES' AND supp.region = 'UNITED STATES' GROUP BY cust.city, supp.city", "lo_revenue_cube");

        computeActual("CREATE CUBE lo_revenue_part_supp_cube ON ssb_lineorder WITH (AGGREGATIONS = (SUM(revenue)), GROUP = (partkey, suppkey))");
        computeActual("INSERT INTO CUBE lo_revenue_part_supp_cube");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_part part ON lo.partkey = part.partkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE supp.region = 'AMERICA' GROUP BY part.brand", "lo_revenue_part_supp_cube");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_part part, ssb_supplier supp WHERE lo.suppkey = supp.suppkey AND lo.partkey = part.partkey AND supp.region = 'AMERICA' GROUP BY part.brand", "lo_revenue_part_supp_cube");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_part part ON lo.partkey = part.partkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE supp.region = 'AMERICA' AND part.brand between 'MFGR#2221' and 'MFGR#2228' GROUP BY part.brand", "lo_revenue_part_supp_cube");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_part part, ssb_supplier supp WHERE lo.suppkey = supp.suppkey AND lo.partkey = part.partkey AND supp.region = 'AMERICA' AND part.brand between 'MFGR#2221' and 'MFGR#2228' GROUP BY part.brand", "lo_revenue_part_supp_cube");

        computeActual("CREATE CUBE lo_revenue_part_supp_cube_date_predicate ON ssb_lineorder WITH (AGGREGATIONS = (SUM(revenue)), GROUP = (orderdate, partkey, suppkey))");
        computeActual("INSERT INTO CUBE lo_revenue_part_supp_cube_date_predicate WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_part part ON lo.partkey = part.partkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE supp.region = 'AMERICA' AND lo.orderdate = date '1992-01-02' GROUP BY part.brand", "lo_revenue_part_supp_cube_date_predicate");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_part part, ssb_supplier supp WHERE lo.suppkey = supp.suppkey AND lo.partkey = part.partkey AND supp.region = 'AMERICA' AND lo.orderdate = date '1992-01-02' GROUP BY part.brand", "lo_revenue_part_supp_cube_date_predicate");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_part part ON lo.partkey = part.partkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE supp.region = 'AMERICA' AND part.brand between 'MFGR#2221' and 'MFGR#2228' AND lo.orderdate = date '1992-01-02' GROUP BY part.brand", "lo_revenue_part_supp_cube_date_predicate");
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo, ssb_part part, ssb_supplier supp WHERE lo.suppkey = supp.suppkey AND lo.partkey = part.partkey AND supp.region = 'AMERICA' AND part.brand between 'MFGR#2221' and 'MFGR#2228' AND lo.orderdate = date '1992-01-02' GROUP BY part.brand", "lo_revenue_part_supp_cube_date_predicate");

        //Negative tests - Cubes must not be used
        assertSSBQuery("SELECT part.brand, SUM(lo.revenue) FROM ssb_lineorder lo INNER JOIN ssb_part part ON lo.partkey = part.partkey INNER JOIN ssb_supplier supp ON lo.suppkey = supp.suppkey WHERE supp.region = 'AMERICA' AND lo.orderdate = date '1992-01-12' GROUP BY part.brand", "ssb_lineorder");

        assertUpdate("DROP TABLE IF EXISTS ssb_lineorder");
        assertUpdate("DROP TABLE IF EXISTS ssb_part");
        assertUpdate("DROP TABLE IF EXISTS ssb_customer");
        assertUpdate("DROP TABLE IF EXISTS ssb_supplier");
    }

    private void assertSSBQuery(@Language("SQL") String query, String cubeName)
    {
        MaterializedResult expectedResult = computeActualAndAssertPlan(starTreeDisabledSession, query, assertInTableScans("ssb_lineorder"));
        MaterializedResult actualResult = computeActualAndAssertPlan(starTreeEnabledSession, query, assertInTableScans(cubeName));
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResult.getMaterializedRows();
        assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + query + "\n:");
    }

    private Consumer<Plan> assertInTableScans(String tableName)
    {
        return plan ->
        {
            boolean matchFound = PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findAll()
                    .stream()
                    .map(TableScanNode.class::cast)
                    .anyMatch(node -> node.getTable().getFullyQualifiedName().endsWith(tableName));

            if (!matchFound) {
                fail("Table " + tableName + " was not used for scan");
            }
        };
    }

    private Consumer<Plan> assertTableScan(String tableName)
    {
        return plan ->
        {
            Optional<TableScanNode> tableScanNode = PlanNodeSearcher.searchFrom(plan.getRoot())
                    .where(TableScanNode.class::isInstance)
                    .findSingle()
                    .map(TableScanNode.class::cast);
            if (!tableScanNode.isPresent() || !tableScanNode.get().getTable().getFullyQualifiedName().endsWith(tableName)) {
                fail("Table " + tableName + " was not used for scan");
            }
        };
    }
}
