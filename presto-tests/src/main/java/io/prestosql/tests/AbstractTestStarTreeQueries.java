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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestStarTreeQueries
        extends AbstractTestQueryFramework
{
    Session sessionStarTree;
    Session sessionNoStarTree;

    protected AbstractTestStarTreeQueries(QueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    @BeforeClass
    public void setUp()
    {
        sessionStarTree = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "true")
                .build();
        sessionNoStarTree = Session.builder(getSession())
                .setSystemProperty(SystemSessionProperties.ENABLE_STAR_TREE_INDEX, "false")
                .build();
        //Create Empty to force create Metadata catalog and schema. To avoid concurrency issue.
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_count_all ON nation WITH (AGGREGATIONS=(count(*)), group=())");
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
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_aggregations_cube_1 ON nation_aggregations_test_table " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_1 where nationkey > -1", 25);
        assertQueryFails(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_1 where 1 > 0", "Invalid predicate\\. \\(1 > 0\\)");
        assertQuery(sessionStarTree,
                "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation_aggregations_test_table group by nationkey",
                sessionNoStarTree,
                "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(sessionStarTree,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation_aggregations_test_table",
                sessionNoStarTree,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(sessionStarTree,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation_aggregations_test_table group by nationkey",
                sessionNoStarTree,
                "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertQuery(sessionStarTree,
                "SELECT avg(nationkey) from nation_aggregations_test_table group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_test_table"));
        assertUpdate("DROP CUBE nation_aggregations_cube_1");
        assertUpdate("DROP TABLE nation_aggregations_test_table");
    }

    @Test
    public void testAverageAggregation()
    {
        computeActual("CREATE TABLE nation_avg_aggregations_table_1 AS SELECT * FROM nation");
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_aggregations_cube_2 ON nation_avg_aggregations_table_1 " +
                "WITH (AGGREGATIONS=(avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(REGIONkey))," +
                " group=(nationKEY), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_2", 25);

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey), avg(nationkey), sum(regionkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey), avg(nationkey), sum(regionkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey), sum(regionkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey), sum(regionkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey), sum(regionkey), sum(regionkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey), sum(regionkey), sum(regionkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT count(regionkey), avg(nationkey), sum(regionkey), sum(regionkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT count(regionkey), avg(nationkey), sum(regionkey), sum(regionkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT sum(regionkey), avg(nationkey), avg(nationkey),  sum(regionkey), count(regionkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT sum(regionkey), avg(nationkey), avg(nationkey),  sum(regionkey), count(regionkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT sum(regionkey), avg(nationkey), avg(nationkey), count(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT sum(regionkey), avg(nationkey), avg(nationkey), count(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertQuery(sessionStarTree,
                "SELECT avg(nationkey), min(regionkey), count(regionkey), max(regionkey), sum(regionkey) from nation_avg_aggregations_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT avg(nationkey), min(regionkey), count(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_2"));

        assertUpdate("DROP CUBE nation_aggregations_cube_2");
        assertUpdate("DROP TABLE nation_avg_aggregations_table_1");
    }

    @Test
    public void testAggregationsWithCaseInSensitiveColumnNames()
    {
        computeActual("CREATE TABLE nation_case_insensitive_test_table_1 AS SELECT * FROM nation");
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_aggregations_cube_3 ON nation_case_insensitive_test_table_1 " +
                "WITH (AGGREGATIONS=(avg(NationKEY), count(Regionkey), sum(regionkey)," +
                " min(regionkey), max(REGIONkey), max(nationKey), min(Nationkey), count(*))," +
                " group=(nationKEY), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_3", 25);

        assertQuery(sessionStarTree,
                "SELECT min(regionkey), max(regionkey), sum(regionkey), max(nationKey), min(Nationkey) from nation_case_insensitive_test_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT min(regionkey), max(regionkey), sum(regionkey), max(nationKey), min(Nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(sessionStarTree,
                "SELECT count(Regionkey), avg(nationkey) from nation_case_insensitive_test_table_1 group by nationkey",
                sessionNoStarTree,
                "SELECT count(Regionkey), avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(sessionStarTree,
                "SELECT count(regionkey), count(*), max(nationKey), min(Nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation_case_insensitive_test_table_1 group by nationKEY",
                sessionNoStarTree,
                "SELECT count(regionkey), count(*), max(nationKey), min(Nationkey), min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertQuery(sessionStarTree,
                "SELECT count(regionkey), avg(nationkey) from nation_case_insensitive_test_table_1 group by NATIONkey",
                sessionNoStarTree,
                "SELECT count(regionkey), avg(nationkey) from nation group by nationkey",
                assertTableScan("nation_aggregations_cube_3"));

        assertUpdate("DROP CUBE nation_aggregations_cube_3");
        assertUpdate("DROP TABLE nation_case_insensitive_test_table_1");
    }

    @Test
    public void testAverageAggregationWithExactGroupByMatch()
    {
        computeActual("CREATE TABLE lineitem_exact_group_by_match AS SELECT * FROM lineitem");
        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_4 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount))," +
                " group=(orderkey), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_4", 15000);

        //single column group by
        assertQuery(sessionStarTree,
                "SELECT avg(quantity) from lineitem_exact_group_by_match group by orderkey",
                sessionNoStarTree,
                "SELECT avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_4"));

        //multi column group by
        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_5 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_5", 60113);

        assertQuery(sessionStarTree,
                "SELECT avg(quantity) from lineitem_exact_group_by_match group by orderkey,partkey",
                sessionNoStarTree,
                "SELECT avg(quantity) from lineitem group by orderkey,partkey",
                assertTableScan("lineitem_aggregations_cube_5"));

        //multiple aggregations
        assertQuery(sessionStarTree,
                "SELECT avg(quantity), max(discount) from lineitem_exact_group_by_match group by orderkey,partkey",
                sessionNoStarTree,
                "SELECT avg(quantity), max(discount) from lineitem group by orderkey,partkey",
                assertTableScan("lineitem_aggregations_cube_5"));

        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_6 ON lineitem_exact_group_by_match " +
                "WITH (AGGREGATIONS=(avg(discount), " +
                " avg(quantity), max(discount))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_6", 60113);
        assertQuery(sessionStarTree,
                "SELECT avg(discount), avg(quantity) from lineitem_exact_group_by_match group by orderkey,partkey",
                sessionNoStarTree,
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
        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_8 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(sum(discount), " +
                " min(quantity), max(discount), max(quantity))," +
                " group=(orderkey, partkey), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_8", 60113);

        assertQuery(sessionStarTree,
                "SELECT sum(discount), min(quantity) from lineitem_partial_group_by_match group by partkey",
                sessionNoStarTree,
                "SELECT sum(discount), min(quantity) from lineitem group by partkey",
                assertTableScan("lineitem_aggregations_cube_8"));

        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_9 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount), avg(discount), max(quantity))," +
                " group=(orderkey,discount), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_9", 50470);

        assertQuery(sessionStarTree,
                "SELECT avg(quantity) from lineitem_partial_group_by_match group by orderkey",
                sessionNoStarTree,
                "SELECT avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_9"));

        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_10 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(discount), " +
                " avg(quantity), min(discount), max(discount), min(quantity))," +
                " group=(orderkey,partkey), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_10", 60113);

        assertQuery(sessionStarTree,
                "SELECT avg(discount), avg(quantity) from lineitem_partial_group_by_match group by orderkey",
                sessionNoStarTree,
                "SELECT avg(discount), avg(quantity) from lineitem group by orderkey",
                assertTableScan("lineitem_aggregations_cube_10"));

        assertUpdate(sessionNoStarTree, "CREATE CUBE lineitem_aggregations_cube_11 ON lineitem_partial_group_by_match " +
                "WITH (AGGREGATIONS=(avg(quantity), " +
                " min(quantity), max(discount), avg(discount), min(discount))," +
                " group=(), format= 'orc')");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE lineitem_aggregations_cube_11", 1);

        assertQuery(sessionStarTree,
                "SELECT avg(quantity) from lineitem_partial_group_by_match",
                sessionNoStarTree,
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
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_show_cube_1 ON nation_show_cube_table_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_show_cube_2 ON nation_show_cube_table_1 " +
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
        assertQuery(sessionNoStarTree,
                "SELECT count(*) FROM nation_table_cube_insert_2 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_table_cube_insert_2"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_cube_insert_2 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_cube_insert_2"));
        assertUpdate("DROP CUBE nation_cube_insert_2");
        assertUpdate("DROP TABLE nation_table_cube_insert_2");
    }

    @Test
    public void testInsertOverwriteCube()
    {
        computeActual("CREATE TABLE nation_table_cube_insert_overwrite_test_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_insert_overwrite_cube_1 ON nation_table_cube_insert_overwrite_test_1 " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate("INSERT INTO CUBE nation_insert_overwrite_cube_1 where nationkey > 5", 19);
        assertEquals(computeScalar("SELECT COUNT(*) FROM nation_insert_overwrite_cube_1"), 19L);
        assertUpdate("INSERT OVERWRITE CUBE nation_insert_overwrite_cube_1 where nationkey > 5", 19);
        assertEquals(computeScalar("SELECT COUNT(*) FROM nation_insert_overwrite_cube_1"), 19L);
        assertUpdate("DROP CUBE nation_insert_overwrite_cube_1");
        assertUpdate("DROP TABLE nation_table_cube_insert_overwrite_test_1");
    }

    @Test
    public void testCountAggregation()
    {
        assertQuerySucceeds("CREATE TABLE nation_table_count_agg_1 AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_count_agg_cube_1 ON nation_table_count_agg_1 WITH (AGGREGATIONS=(count(*)), group=(name))");
        assertQuerySucceeds("INSERT INTO CUBE nation_count_agg_cube_1 where name = 'CHINA'");
        assertQuery(sessionNoStarTree,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_table_count_agg_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_count_agg_cube_1"));
        assertQuerySucceeds("INSERT INTO CUBE nation_count_agg_cube_1 where name = 'CANADA'");
        assertQuery(sessionNoStarTree,
                "SELECT count(*) FROM nation_table_count_agg_1 WHERE name = 'CANADA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CANADA' GROUP BY name",
                assertTableScan("nation_table_count_agg_1"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionNoStarTree,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY name, regionkey",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name, regionkey",
                assertTableScan("nation_table_multi_column_group"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY name",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY name",
                assertTableScan("nation_cube_multi_column_group"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_multi_column_group WHERE name = 'CHINA' GROUP BY nationkey, regionkey",
                "SELECT count(*) FROM nation WHERE name = 'CHINA' GROUP BY nationkey, regionkey",
                assertTableScan("nation_table_multi_column_group"));
        assertQuery(sessionStarTree,
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
                "WITH (AGGREGATIONS=(count(*))," + " group=(time_stamp), format= 'orc', partitioned_by = ARRAY['cint_1'])");
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
        assertQuery(sessionNoStarTree,
                "SELECT count(*) FROM nation_table_empty_group_test_1",
                "SELECT count(*) FROM nation",
                assertTableScan("nation_table_empty_group_test_1"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_cube_partial_data_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey >= 1",
                "SELECT count(*) FROM nation WHERE nationkey >= 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertUpdate("DROP CUBE nation_cube_partial_data_1");
        assertUpdate("CREATE CUBE nation_cube_partial_data_2 ON nation_table_partial_data_test_1 WITH (AGGREGATIONS=(count(*)), GROUP=(nationkey, regionkey))");
        assertUpdate("INSERT INTO CUBE nation_cube_partial_data_2 WHERE nationkey = 1 and regionkey = 1", 1);
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey >= 1",
                "SELECT count(*) FROM nation WHERE nationkey >= 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1 and regionkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1 and regionkey = 1",
                assertTableScan("nation_cube_partial_data_2"));
        assertUpdate("INSERT INTO CUBE nation_cube_partial_data_2 WHERE nationkey > 1 and regionkey = 2", 5);
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey = 1 and regionkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1 and regionkey = 1",
                assertTableScan("nation_cube_partial_data_2"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1 and regionkey = 2",
                "SELECT count(*) FROM nation WHERE nationkey > 1 and regionkey = 2",
                assertTableScan("nation_cube_partial_data_2"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_partial_data_test_1 WHERE nationkey > 1 and regionkey > 1",
                "SELECT count(*) FROM nation WHERE nationkey > 1 and regionkey > 1",
                assertTableScan("nation_table_partial_data_test_1"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT custkey FROM orders o WHERE 100000 < (SELECT sum(extendedprice) FROM line_item_table_test_1 l WHERE l.orderkey = o.orderkey) ORDER BY custkey LIMIT 10",
                "SELECT custkey FROM orders o WHERE 100000 < (SELECT sum(extendedprice) FROM lineitem l WHERE l.orderkey = o.orderkey) ORDER BY custkey LIMIT 10",
                assertInTableScans("line_item_table_test_1"));
        assertUpdate("DROP CUBE line_item_cube_test_1");
        assertUpdate("DROP TABLE line_item_table_test_1");
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
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_range_merge_test_1 WHERE nationkey = 1",
                "SELECT count(*) FROM nation WHERE nationkey = 1",
                assertTableScan("nation_cube_range_merge_1"));
        assertQuery(sessionStarTree,
                "SELECT count(*) FROM nation_table_range_merge_test_1 WHERE nationkey BETWEEN 1 AND 2",
                "SELECT count(*) FROM nation WHERE nationkey BETWEEN 1 AND 2",
                assertTableScan("nation_table_range_merge_test_1"));
        assertUpdate("INSERT INTO CUBE nation_cube_range_merge_1 WHERE nationkey = 2", 1);
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT count(distinct custkey) FROM orders_count_distinct WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10'",
                "SELECT count(distinct custkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' AND '1992-01-10'",
                assertTableScan("orders_count_distinct"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders_source_filter_test WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderstatus = 'F' GROUP BY custkey",
                "SELECT custkey, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderstatus = 'F' GROUP BY custkey",
                assertTableScan("orders_source_filter_test"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT custkey, orderpriority, sum(totalprice) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice) FROM orders WHERE orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by custkey, orderpriority",
                assertTableScan("orders_table_source_data_filter"));
        assertQuery(sessionStarTree,
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderpriority = '1-URGENT' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderpriority = '1-URGENT' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                assertTableScan("orders_cube_source_data_filter"));
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_source_data_filter WHERE orderpriority = '2-HIGH'");
        assertQuery(sessionStarTree,
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders_table_source_data_filter WHERE orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' AND orderpriority = '2-HIGH' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                "SELECT custkey, orderpriority, sum(totalprice), count(distinct orderkey) FROM orders WHERE orderdate BETWEEN '1992-01-01' and '1992-01-10' AND orderpriority = '2-HIGH' GROUP BY custkey, orderpriority ORDER BY custkey, orderpriority",
                assertTableScan("orders_cube_source_data_filter"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT orderdate, count(*) FROM orders_table_source_filter_cast WHERE custkey BETWEEN BIGINT '1' AND BIGINT '200' AND orderdate BETWEEN date '1992-01-01' AND date '1992-01-10' group by orderdate",
                "SELECT orderdate, count(*) FROM orders WHERE custkey BETWEEN 1 AND 200 AND orderdate BETWEEN '1992-01-01' AND '1992-01-10' group by orderdate",
                assertTableScan("orders_table_source_filter_cast"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT orderdate, max(totalprice) FROM orders_table_insert_cube_greater_than WHERE custkey = 100 GROUP BY orderdate",
                "SELECT orderdate, max(totalprice) FROM orders WHERE custkey = 100 GROUP BY orderdate",
                assertTableScan("orders_table_insert_cube_greater_than"));
        assertQuery(sessionStarTree,
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
        assertQueryFails(sessionStarTree,
                "CREATE CUBE orders_cube_unsuported_filter_predicate ON orders WITH (AGGREGATIONS=(sum(totalprice)), GROUP=(custkey), FILTER = (orderdate))",
                ".*Filter property must evaluate to a boolean: actual type 'date'.*");
        assertQueryFails(sessionStarTree,
                "CREATE CUBE orders_cube_unsuported_filter_predicate ON orders WITH (AGGREGATIONS=(sum(totalprice)), GROUP=(orderdate), FILTER = (custkey))",
                ".*Filter property must evaluate to a boolean: actual type 'bigint'.*");
    }

    @Test
    public void testCubeInsertWithMultipleCube()
    {
        computeActual("CREATE TABLE orders_table_multiple_cube_insert AS SELECT * FROM orders");
        computeActual("CREATE CUBE orders_cube_mutiple_cube_insert_1 ON orders_table_multiple_cube_insert WITH (AGGREGATIONS = (max(totalprice)), GROUP = (orderdate,custkey))");
        assertQuerySucceeds("INSERT INTO CUBE orders_cube_mutiple_cube_insert_1 WHERE custkey >= 100");
        assertQuery(sessionStarTree,
                "SELECT custkey, max(totalprice) FROM orders_table_multiple_cube_insert WHERE custkey >= 101 GROUP BY custkey",
                "SELECT custkey, max(totalprice) FROM orders WHERE custkey >= 101 GROUP BY custkey",
                assertTableScan("orders_cube_mutiple_cube_insert_1"));
        computeActual("CREATE CUBE orders_cube_mutiple_cube_insert_2 ON orders_table_multiple_cube_insert WITH (AGGREGATIONS = (max(totalprice)), GROUP = (orderdate,custkey), FILTER = (orderkey > 1))");
        assertQuerySucceeds(sessionStarTree, "INSERT INTO CUBE orders_cube_mutiple_cube_insert_2 WHERE custkey >= 100");
        assertQuery(sessionStarTree,
                "SELECT custkey, max(totalprice) FROM orders_table_multiple_cube_insert WHERE custkey >= 101 GROUP BY custkey",
                "SELECT custkey, max(totalprice) FROM orders WHERE custkey >= 101 GROUP BY custkey",
                assertTableScan("orders_cube_mutiple_cube_insert_1"));
        assertQuery(sessionStarTree,
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
        assertQuery(sessionStarTree,
                "SELECT orderdate, orderstatus, avg(totalprice) FROM orders_table_multiple_cubes_similar WHERE orderpriority = '1-URGENT' AND shippriority = 1 AND orderstatus = 'F' GROUP BY orderdate, orderstatus",
                "SELECT orderdate, orderstatus, avg(totalprice) FROM orders WHERE orderpriority = '1-URGENT' AND shippriority = 1 AND orderstatus = 'F' GROUP BY orderdate, orderstatus",
                assertTableScan("orders_cube_similar_1"));
        assertUpdate("DROP TABLE orders_table_multiple_cubes_similar");
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
