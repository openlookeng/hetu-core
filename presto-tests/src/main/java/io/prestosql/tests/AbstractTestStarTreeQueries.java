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
    public void testAggregations()
    {
        assertUpdate(sessionNoStarTree, "CREATE CUBE nation_aggregations_cube_1 ON nation " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        assertUpdate(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_1 where nationkey > -1", 25);
        assertQueryFails(sessionNoStarTree, "INSERT INTO CUBE nation_aggregations_cube_1 where 1 > 0", "Invalid predicate\\. \\(1 > 0\\)");
        assertQuery(sessionStarTree, "SELECT min(regionkey), max(regionkey), sum(regionkey) from nation group by nationkey");
        assertQuery(sessionStarTree, "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation");
        assertQuery(sessionStarTree, "SELECT COUNT(distinct nationkey), count(distinct regionkey) from nation group by nationkey");
        assertQuery(sessionStarTree, "SELECT avg(nationkey) from nation group by nationkey");
        assertUpdate("DROP CUBE nation_aggregations_cube_1");
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
        MaterializedRow matchingRow1 = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_show_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow1);
        assertTrue(matchingRow1.getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_1", "hive.tpch.nation_show_cube_table_1", "Inactive", "nationkey")));

        MaterializedRow matchingRow2 = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_show_cube_2")).findFirst().orElse(null);
        assertNotNull(matchingRow2);
        assertTrue(matchingRow2.getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_2", "hive.tpch.nation_show_cube_table_1", "Inactive", "")));

        result = computeActual("SHOW CUBES FOR nation_show_cube_table_1");
        assertEquals(result.getRowCount(), 2);

        matchingRow1 = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_show_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow1);
        assertTrue(result.getMaterializedRows().get(0).getFields().containsAll(ImmutableList.of("hive.tpch.nation_show_cube_1", "hive.tpch.nation_show_cube_table_1", "Inactive", "nationkey")));

        matchingRow2 = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_show_cube_2")).findFirst().orElse(null);
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
        assertQueryFails("INSERT INTO CUBE nation_insert_cube_1 where regionkey > 5", "All columns in where clause must be part Cube group\\.");
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
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])", "line 1:1: Table 'hive.tpch.nation' already exists");
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
    public void testCubeStatusChange()
    {
        computeActual("CREATE TABLE nation_table_status_test AS SELECT * FROM nation");
        assertUpdate("CREATE CUBE nation_status_cube_1 ON nation_table_status_test " +
                "WITH (AGGREGATIONS=(count(*), COUNT(distinct nationkey), count(distinct regionkey), avg(nationkey), count(regionkey), sum(regionkey)," +
                " min(regionkey), max(regionkey), max(nationkey), min(nationkey))," +
                " group=(nationkey), format= 'orc', partitioned_by = ARRAY['nationkey'])");
        MaterializedResult result = computeActual("SHOW CUBES FOR nation_table_status_test");
        MaterializedRow matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_status_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        assertEquals(matchingRow.getField(2), "Inactive");

        assertUpdate("INSERT INTO CUBE nation_status_cube_1 where nationkey > 5", 19);
        result = computeActual("SHOW CUBES FOR nation_table_status_test");
        matchingRow = result.getMaterializedRows().stream().filter(row -> row.getField(0).toString().contains("nation_status_cube_1")).findFirst().orElse(null);
        assertNotNull(matchingRow);
        assertEquals(matchingRow.getField(2), "Active");

        assertUpdate("DROP CUBE nation_status_cube_1");
        assertUpdate("DROP TABLE nation_table_status_test");
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
