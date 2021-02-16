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
package io.prestosql.plugin.hive;

import io.prestosql.execution.SplitCacheMap;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcCache
        extends AbstractTestQueryFramework
{
    public TestOrcCache()
    {
        super(() -> HiveQueryRunner.createQueryRunner());
    }

    @BeforeClass
    public void setUp()
    {
        assertUpdate("CREATE TABLE employee(id INTEGER, name VARCHAR, dob DATE, perf DOUBLE) WITH (partitioned_by = ARRAY['name', 'dob', 'perf'], format = 'ORC')");
        assertUpdate("INSERT INTO employee VALUES(0, 'Alice', DATE '1995-10-09', DOUBLE '8.0')", 1);
        assertUpdate("INSERT INTO employee VALUES(1, 'Bob', DATE '1995-11-14', DOUBLE '9.5')", 1);
        assertUpdate("INSERT INTO employee VALUES(2, 'Sheldon', DATE '1978-08-29', DOUBLE '10.0')", 1);
        assertUpdate("INSERT INTO employee VALUES(3, 'Winters', DATE '1918-01-21', DOUBLE '9.2')", 1);
        assertUpdate("INSERT INTO employee VALUES(4, 'Lenard', DATE '1980-06-24', DOUBLE '8.8')", 1);
        assertUpdate("INSERT INTO employee VALUES(5, 'Raj', DATE '1979-05-09', DOUBLE '8.2')", 1);
        assertUpdate("INSERT INTO employee VALUES(6, 'Trump', DATE '1945-08-15', DOUBLE '2.5')", 1);

        assertUpdate("" +
                "CREATE TABLE all_types(" +
                "id INTEGER," +
                "_boolean BOOLEAN," +
                "_tinyint TINYINT," +
                "_smallint SMALLINT," +
                "_integer INTEGER," +
                "_bigint BIGINT," +
                "_float FLOAT," +
                "_double DOUBLE," +
                "_date DATE," +
                "_timestamp TIMESTAMP," +
                "_varchar VARCHAR) " +
                "WITH (partitioned_by = ARRAY['_boolean', '_tinyint', '_smallint', '_integer', '_bigint', '_float', '_double', '_date', '_timestamp', '_varchar'], " +
                "format = 'ORC')");
        assertUpdate("INSERT INTO all_types VALUES(0, true, TINYINT '0', SMALLINT '0', 0, BIGINT '0', FLOAT '2E-1', DOUBLE '0.2', DATE '1995-10-09', TIMESTAMP '1995-10-09 00:00:00', 'jiahao')", 1);
        assertUpdate("INSERT INTO all_types VALUES(1, false, TINYINT '1', SMALLINT '1', 1, BIGINT '1', FLOAT '5E-2', DOUBLE '0.5', DATE '1995-11-14', TIMESTAMP '1995-11-14 00:00:00', 'han')", 1);

        assertUpdate("CREATE TABLE test_drop_cache_1(id INTEGER, p1 INTEGER, p2 INTEGER) WITH (partitioned_by = ARRAY['p1', 'p2'], format = 'ORC')");
        assertUpdate("CREATE TABLE test_drop_cache_2(id INTEGER, p3 INTEGER) WITH (partitioned_by = ARRAY['p3'], format = 'ORC')");

        assertUpdate("CREATE TABLE test_drop_cache_3(id INTEGER, p1 INTEGER, p2 INTEGER) WITH (partitioned_by = ARRAY['p1', 'p2'], format = 'ORC')");
        assertUpdate("CREATE TABLE test_drop_cache_4(id INTEGER, p3 INTEGER) WITH (partitioned_by = ARRAY['p3'], format = 'ORC')");
    }

    @Test
    public void testCacheTableOnAllColumnTypes()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE all_types WHERE _boolean = true");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_boolean = true)"));
        assertQueryOrdered("SELECT id, _boolean FROM all_types WHERE _boolean = true", "VALUES (0, true)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _tinyint = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_tinyint = 0)"));
        assertQueryOrdered("SELECT id, _tinyint FROM all_types WHERE _tinyint = 0", "VALUES (0, 0)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _smallint = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_smallint = 0)"));
        assertQueryOrdered("SELECT id, _smallint FROM all_types WHERE _smallint = 0", "VALUES (0, 0)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _integer = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_integer = 0)"));
        assertQueryOrdered("SELECT id, _integer FROM all_types WHERE _integer = 0", "VALUES (0, 0)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _bigint = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_bigint = 0)"));
        assertQueryOrdered("SELECT id, _bigint FROM all_types WHERE _bigint = 0", "VALUES (0, 0)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _float > FLOAT '6e-2'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_float > FLOAT '6e-2')"));
        assertQueryOrdered("SELECT id, _float FROM all_types WHERE _float > FLOAT '6e-2'", "VALUES (0, 2E-1)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _double = DOUBLE '0.2'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_double = DOUBLE '0.2')"));
        assertQueryOrdered("SELECT id, _double FROM all_types WHERE _double = DOUBLE '0.2'", "VALUES (0, 0.2)");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _date = DATE '1995-10-09'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_date = DATE '1995-10-09')"));
        assertQueryOrdered("SELECT id, _date FROM all_types WHERE _date = DATE '1995-10-09'", "VALUES (0, '1995-10-09')");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _timestamp = TIMESTAMP '1995-10-09 00:00:00'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_timestamp = TIMESTAMP '1995-10-09 00:00:00')"));
        assertQueryOrdered("SELECT id, _timestamp FROM all_types WHERE _timestamp = TIMESTAMP '1995-10-09 00:00:00'", "VALUES (0, '1995-10-09 00:00:00')");

        assertQuerySucceeds("CACHE TABLE all_types WHERE _varchar = 'jiahao'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.all_types").showPredicates().contains("(_varchar = 'jiahao')"));
        assertQueryOrdered("SELECT id, _varchar FROM all_types WHERE _varchar = 'jiahao'", "VALUES (0, 'jiahao')");
    }

    @Test
    public void testCacheTableOnNonPartitionedColumn()
    {
        assertQueryFails("CACHE TABLE employee WHERE id <= 3", ".*?Column 'id' is not cacheable");
    }

    @Test
    public void testCacheTableWithLikePredicate()
    {
        assertQueryFails("CACHE TABLE employee WHERE name LIKE 'S%'", ".*?LIKE predicate is not supported.");
    }

    @Test
    public void testCacheTableWithOrOperator()
    {
        assertQueryFails("CACHE TABLE employee WHERE dob > DATE '1985-01-01' OR perf < DOUBLE '9.0'", ".*?OR operator is not supported");
    }

    @Test
    public void testCacheTableWithIsNullPredicate()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE employee WHERE dob IS NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(dob IS NULL)"));

        assertQuerySucceeds("CACHE TABLE employee WHERE name IS NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(name IS NULL)"));

        assertQuerySucceeds("CACHE TABLE employee WHERE perf IS NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(perf IS NULL)"));
    }

    @Test
    public void testCacheTableWithIsNotNullPredicate()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE employee WHERE dob IS NOT NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(dob IS NOT NULL)"));

        assertQuerySucceeds("CACHE TABLE employee WHERE name IS NOT NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(name IS NOT NULL)"));

        assertQuerySucceeds("CACHE TABLE employee WHERE perf IS NOT NULL");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(perf IS NOT NULL)"));
    }

    @Test
    public void testCacheTableWithComplexPredicate()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE employee WHERE dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01' AND perf < DOUBLE '9.0'");
        assertQueryOrdered(
                "SELECT * FROM employee WHERE dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01' AND perf < DOUBLE '9.0' ORDER BY id",
                "VALUES (0, 'Alice', '1995-10-09', 8.0), (4, 'Lenard', '1980-06-24', 8.8)");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("((dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01') AND (perf < DOUBLE '9.0'))"));

        assertQuerySucceeds("CACHE TABLE employee WHERE dob < DATE '1980-01-01' AND perf < DOUBLE '8.0'");
        assertQueryOrdered(
                "SELECT * FROM employee WHERE dob < DATE '1980-01-01' AND perf < DOUBLE '8.0' ORDER BY id",
                "VALUES (6, 'Trump', '1945-08-15', 2.5)");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("((dob < DATE '1980-01-01') AND (perf < DOUBLE '8.0'))"));
    }

    @Test
    public void testCacheNonOrcTable()
    {
        // PARQUET
        assertUpdate("CREATE TABLE parquet_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'PARQUET')");
        assertQueryFails("CACHE TABLE parquet_table WHERE par = 0", ".*?Table 'hive.tpch.parquet_table' cannot be cached");
        assertUpdate("DROP TABLE parquet_table");

        // AVRO
        assertUpdate("CREATE TABLE avro_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'AVRO')");
        assertQueryFails("CACHE TABLE avro_table WHERE par = 0", ".*?Table 'hive.tpch.avro_table' cannot be cached");
        assertUpdate("DROP TABLE avro_table");

        // RCBINARY
        assertUpdate("CREATE TABLE rcbinary_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'RCBINARY')");
        assertQueryFails("CACHE TABLE rcbinary_table WHERE par = 0", ".*?Table 'hive.tpch.rcbinary_table' cannot be cached");
        assertUpdate("DROP TABLE rcbinary_table");

        // RCTEXT
        assertUpdate("CREATE TABLE rctext_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'RCTEXT')");
        assertQueryFails("CACHE TABLE rctext_table WHERE par = 0", ".*?Table 'hive.tpch.rctext_table' cannot be cached");
        assertUpdate("DROP TABLE rctext_table");

        // SEQUENCEFILE
        assertUpdate("CREATE TABLE sequencefile_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'SEQUENCEFILE')");
        assertQueryFails("CACHE TABLE sequencefile_table WHERE par = 0", ".*?Table 'hive.tpch.sequencefile_table' cannot be cached");
        assertUpdate("DROP TABLE sequencefile_table");

        // JSON
        assertUpdate("CREATE TABLE json_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'JSON')");
        assertQueryFails("CACHE TABLE json_table WHERE par = 0", ".*?Table 'hive.tpch.json_table' cannot be cached");
        assertUpdate("DROP TABLE json_table");

        // TEXTFILE
        assertUpdate("CREATE TABLE textfile_table(id BIGINT, par BIGINT) WITH (partitioned_by = ARRAY['par'], format = 'TEXTFILE')");
        assertQueryFails("CACHE TABLE textfile_table WHERE par = 0", ".*?Table 'hive.tpch.textfile_table' cannot be cached");
        assertUpdate("DROP TABLE textfile_table");

        // CSV
        assertUpdate("CREATE TABLE csv_table(id VARCHAR, par VARCHAR) WITH (partitioned_by = ARRAY['par'], format = 'CSV')");
        assertQueryFails("CACHE TABLE csv_table WHERE par = ''", ".*?Table 'hive.tpch.csv_table' cannot be cached");
        assertUpdate("DROP TABLE csv_table");
    }

    @Test
    public void testDropCacheOnMultipleTables()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE test_drop_cache_1 WHERE p1 = 1");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_1").showPredicates().contains("(p1 = 1)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_2 WHERE p3 = 3");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_2").showPredicates().contains("(p3 = 3)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_2 WHERE p3 = 4");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_2").showPredicates().contains("(p3 = 4)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_1 WHERE p2 = 2");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_1").showPredicates().contains("(p2 = 2)"));

        assertQuerySucceeds("DROP CACHE test_drop_cache_1");
        assertEquals(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_1"), null);

        assertQuerySucceeds("DROP CACHE test_drop_cache_2");
        assertEquals(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_2"), null);
    }

    @Test
    public void testDropCacheWithPredicates()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE test_drop_cache_3 WHERE p1 = 1");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_3").showPredicates().contains("(p1 = 1)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_4 WHERE p3 = 3");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_4").showPredicates().contains("(p3 = 3)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_4 WHERE p3 = 4");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_4").showPredicates().contains("(p3 = 4)"));

        assertQuerySucceeds("CACHE TABLE test_drop_cache_3 WHERE p2 = 2");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_3").showPredicates().contains("(p2 = 2)"));

        assertQuerySucceeds("DROP CACHE test_drop_cache_3 WHERE p1 = 1");
        assertFalse(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_3").showPredicates().contains("(p1 = 1)"));
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_3").showPredicates().contains("(p2 = 2)"));

        assertQuerySucceeds("DROP CACHE test_drop_cache_4 WHERE p3 = 4");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_4").showPredicates().contains("(p3 = 3)"));
        assertFalse(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache_4").showPredicates().contains("(p3 = 4)"));
    }

    @Test
    public void testDropCacheOnNonExistTable()
    {
        assertQueryFails("DROP CACHE test_drop_cache_no WHERE par = 0", ".*?Cache for table 'hive.tpch.test_drop_cache_no' does not exist");
        assertQueryFails("DROP CACHE test_drop_cache_no", ".*?Cache for table 'hive.tpch.test_drop_cache_no' does not exist");
    }

    @Test
    public void testDropCacheWithNonExistPredicates()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertUpdate("CREATE TABLE test_drop_cache(id INTEGER, par INTEGER) WITH (partitioned_by = ARRAY['par'], format = 'ORC')");

        assertQuerySucceeds("CACHE TABLE test_drop_cache WHERE par = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_drop_cache").showPredicates().contains("(par = 0)"));

        assertQueryFails("DROP CACHE test_drop_cache WHERE par = 1", ".*?Cache predicate '\\(par = 1\\)' does not exist");
    }

    @Test
    public void testShowCacheOnNonExistTable()
    {
        assertQueryFails("SHOW CACHE test_show_no_cache", ".*?Cache for table 'hive.tpch.test_show_no_cache' does not exist");
    }

    @Test
    public void testShowCacheAfterTableDeleted()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertUpdate("CREATE TABLE test_show_cache(id INTEGER, par INTEGER) WITH (partitioned_by = ARRAY['par'], format = 'ORC')");
        assertQuerySucceeds("CACHE TABLE test_show_cache WHERE par = 0");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.test_show_cache").showPredicates().contains("(par = 0)"));

        assertUpdate("DROP TABLE test_show_cache");
        assertQueryFails("SHOW CACHE test_show_cache", ".*?Cache for table 'hive.tpch.test_show_cache' does not exist");
    }

    @Test
    public void testShowCache()
    {
        SplitCacheMap splitCacheMap = SplitCacheMap.getInstance();

        assertQuerySucceeds("CACHE TABLE employee WHERE dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01')"));

        assertQuerySucceeds("CACHE TABLE employee WHERE perf > DOUBLE '9.0'");
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(dob BETWEEN DATE '1980-01-01' AND DATE '2000-01-01')"));
        assertTrue(splitCacheMap.tableCacheInfoMap().get("hive.tpch.employee").showPredicates().contains("(perf > DOUBLE '9.0')"));
    }
}
