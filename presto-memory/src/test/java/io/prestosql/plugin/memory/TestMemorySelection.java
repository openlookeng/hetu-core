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

package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMemorySelection
        extends AbstractTestQueryFramework
{
    private static final String maxLocalPartSize = "64MB";

    public TestMemorySelection()
    {
        super(() -> MemoryQueryRunner.createQueryRunner(2, ImmutableMap.of("task.writer-count", "8"), ImmutableMap.of("memory.max-logical-part-size", maxLocalPartSize), true));
    }

    @AfterMethod
    public void dropAllTables()
    {
        MaterializedResult tables = computeActual("SHOW TABLES");
        for (MaterializedRow row : tables.getMaterializedRows()) {
            assertQuerySucceeds("DROP TABLE IF EXISTS " + row.getField(0));
        }
    }

    @Test
    public void testNullSelect()
    {
        getQueryRunner().execute("CREATE TABLE test_null_noidx AS SELECT * FROM tpcds.tiny.customer");
        getQueryRunner().execute("CREATE TABLE test_null_sort WITH (sorted_by=ARRAY['c_birth_year']) AS SELECT * FROM tpcds.tiny.customer");
        getQueryRunner().execute("CREATE TABLE test_null_idx WITH (index_columns=ARRAY['c_birth_year']) AS SELECT * FROM tpcds.tiny.customer");

        Object expectedCount = getSingleResult("SELECT count(*) FROM tpcds.tiny.customer where c_birth_year is null");
        Object noidxCount = getSingleResult("SELECT count(*) FROM test_null_noidx where c_birth_year is null");
        Object sortCount = getSingleResult("SELECT count(*) FROM test_null_sort where c_birth_year is null");
        Object idxCount = getSingleResult("SELECT count(*) FROM test_null_idx where c_birth_year is null");

        assertTrue((long) expectedCount > 0, "expected null count is 0, testcase should be updated");
        assertEquals(noidxCount, expectedCount);
        assertEquals(sortCount, expectedCount);
        assertEquals(idxCount, expectedCount);
    }

    @Test
    public void testSortedBySelect()
    {
        assertUpdate("CREATE TABLE test_sort_select WITH (sorted_by=ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", "SELECT count(*) FROM nation");

        assertQuery("SELECT * FROM test_sort_select ORDER BY nationkey", "SELECT * FROM nation ORDER BY nationkey");

        assertQuery("SELECT * FROM test_sort_select WHERE nationkey = 3", "SELECT * FROM nation WHERE nationkey = 3");

        assertQueryResult("INSERT INTO test_sort_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_sort_select", 50L);

        assertQuery("SELECT * FROM test_sort_select WHERE nationkey = 3",
                "SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3");

        assertQueryResult("INSERT INTO test_sort_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_sort_select", 75L);

        assertQuery("SELECT * FROM test_sort_select WHERE nationkey = 3",
                "SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3");
    }

    @Test
    public void testIndexColumnsSelect()
    {
        assertUpdate("CREATE TABLE test_index_select WITH (index_columns=ARRAY['nationkey']) AS SELECT * FROM tpch.tiny.nation", "SELECT count(*) FROM nation");

        assertQuery("SELECT * FROM test_index_select ORDER BY nationkey", "SELECT * FROM nation ORDER BY nationkey");

        assertQuery("SELECT * FROM test_index_select WHERE nationkey = 3", "SELECT * FROM nation WHERE nationkey = 3");

        assertQueryResult("INSERT INTO test_index_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_index_select", 50L);

        assertQuery("SELECT * FROM test_index_select WHERE nationkey = 3",
                "SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3");

        assertQueryResult("INSERT INTO test_index_select SELECT * FROM tpch.tiny.nation", 25L);

        assertQueryResult("SELECT count(*) FROM test_index_select", 75L);

        assertQuery("SELECT * FROM test_index_select WHERE nationkey = 3",
                "SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3 UNION ALL SELECT * FROM nation WHERE nationkey = 3");
    }

    @DataProvider(name = "memConIndexSingleOperators")
    public Object[][] memConIndexSingleOperators()
    {
        return new Object[][] {{"=", 1}, {">", 1}, {"<", 1}, {">=", 1}, {"<=", 1}, {"IN", 1}, {"BETWEEN", 2}};
    }

    @Test(dataProvider = "memConIndexSingleOperators")
    public void testIndexSingleOperators(String queryOperator, int testKeys)
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get custkeys
        List<Long> custkeys = new ArrayList<>();
        custkeys.add(1000L);
        custkeys.add(5000L);
        custkeys.add(7000L);

        String predicateQuery = "SELECT count(*) FROM test_indexOperations WHERE custkey ";

        if (queryOperator.toLowerCase(Locale.ROOT).contains("in")) {
            predicateQuery += queryOperator + " (";
            int custkeySize = custkeys.size();
            for (int i = 0; i < custkeySize; i++) {
                predicateQuery += " " + custkeys.get(i);
                if (i < custkeySize - 1) {
                    predicateQuery += ", ";
                }
            }
            predicateQuery += ")";
        }
        else if (queryOperator.toLowerCase(Locale.ROOT).contains("between")) {
            predicateQuery += queryOperator + " ";
            long minkey = Math.min(custkeys.get(0), custkeys.get(1));
            long maxkey = Math.max(custkeys.get(0), custkeys.get(1));
            predicateQuery += minkey + " AND " + maxkey;
        }
        else {
            predicateQuery += queryOperator + " " + custkeys.get(0);
        }

        // get total number of rows
        long totalRows = assertQuerySucceedsGetInputRows("SELECT count(*) FROM test_indexOperations");

        // apply one predicate and get input rows read, this time since predicate is on sort column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        System.out.println(predicateQuery);

        assertTrue(totalRows > inputRowCountOnePredicate, "totalRows=" + totalRows + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);
    }

    @Test(dataProvider = "memConIndexSingleOperators")
    public void testIndexSingleOperatorsVerifyResults(String queryOperator, int testKeys)
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get custkeys
        List<Long> custkeys = new ArrayList<>();
        for (int i = 0; i < testKeys; i++) {
            custkeys.add((long) getSingleResult("SELECT custkey FROM test_indexOperations limit 1"));
        }

        String predicateQuery = "SELECT custkey FROM test_indexOperations WHERE custkey ";

        if (queryOperator.toLowerCase(Locale.ROOT).contains("in")) {
            predicateQuery += queryOperator + " (";
            int custkeySize = custkeys.size();
            for (int i = 0; i < custkeySize; i++) {
                predicateQuery += " " + custkeys.get(i);
                if (i < custkeySize - 1) {
                    predicateQuery += ", ";
                }
            }
            predicateQuery += ")";
        }
        else if (queryOperator.toLowerCase(Locale.ROOT).contains("between")) {
            predicateQuery += queryOperator + " ";
            long minkey = Math.min(custkeys.get(0), custkeys.get(1));
            long maxkey = Math.max(custkeys.get(0), custkeys.get(1));
            predicateQuery += minkey + " AND " + maxkey;
        }
        else {
            predicateQuery += queryOperator + " " + custkeys.get(0);
        }

        MaterializedResult result1 = computeActual(predicateQuery.replace("test_indexOperations", "tpch.tiny.orders"));

        MaterializedResult result2 = computeActual(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        System.out.println(predicateQuery);

        ArrayList<String> data1 = new ArrayList<>();
        ArrayList<String> data2 = new ArrayList<>();
        for (MaterializedRow item1 : result1.getMaterializedRows()) {
            data1.add(item1.toString());
        }
        for (MaterializedRow item2 : result2.getMaterializedRows()) {
            data2.add(item2.toString());
        }
        Collections.sort(data1);
        Collections.sort(data2);
        System.out.println(data1.size());
        System.out.println();
        System.out.println(data2.size());

        assertEquals(data1, data2);
    }

    @DataProvider(name = "memConIndexSingleColMultiOperators")
    public Object[][] memConIndexSingleColMultiOperators()
    {
        return new Object[][] {
                {"custkey < # OR custkey > #"}, {"custkey > # AND custkey < #"},
                {"custkey <= # OR custkey >= #"}, {"custkey = # OR custkey IN (#)"},
                {"custkey = # AND custkey NOT IN (#)"}, {"custkey <> # AND custkey IN (#)"},
                {"custkey BETWEEN # AND # OR custkey = #"}, {"custkey BETWEEN # AND # OR custkey IN (#)"},
                {"custkey BETWEEN # AND # OR custkey BETWEEN # AND #"}};
    }

    @Test(dataProvider = "memConIndexSingleColMultiOperators")
    public void testIndexSingleColMultiOperators(String queryOperator)
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get how many test keys there are
        long testKeys = queryOperator.chars().filter(c -> c == '#').count();

        // get custkeys
        List<Long> custkeys = new ArrayList<>();
        custkeys.add(1000L);
        custkeys.add(5000L);
        custkeys.add(7000L);
        custkeys.add(9000L);

        for (Long key : custkeys) {
            queryOperator = queryOperator.replaceFirst("#", String.valueOf(key));
        }

        String predicateQuery = "SELECT count(distinct custkey) FROM test_indexOperations WHERE " + queryOperator;

        // get total number rows
        long totalRows = assertQuerySucceedsGetInputRows("SELECT count(*) FROM test_indexOperations");

        // apply one predicate and get input rows read, this time since predicate is on sort column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        assertTrue(totalRows > inputRowCountOnePredicate, "totalRows=" + totalRows + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);
    }

    @Test(dataProvider = "memConIndexSingleColMultiOperators")
    public void testIndexSingleColMultiOperatorsVerifyResults(String queryOperator)
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get how many test keys there are
        long testKeys = queryOperator.chars().filter(c -> c == '#').count();

        // get custkeys
        List<Object> results = getResults("SELECT * from (SELECT custkey FROM test_indexOperations LIMIT " + testKeys + ") ORDER BY custkey ASC");
        for (Object key : results) {
            queryOperator = queryOperator.replaceFirst("#", String.valueOf((long) key));
        }

        String predicateQuery = "SELECT count(distinct custkey) FROM test_indexOperations WHERE " + queryOperator;

        MaterializedResult result1 = computeActual(predicateQuery.replace("test_indexOperations", "tpch.tiny.orders"));

        MaterializedResult result2 = computeActual(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        System.out.println(predicateQuery);

        ArrayList<String> data1 = new ArrayList<>();
        ArrayList<String> data2 = new ArrayList<>();
        for (MaterializedRow item1 : result1.getMaterializedRows()) {
            data1.add(item1.toString());
        }
        for (MaterializedRow item2 : result2.getMaterializedRows()) {
            data2.add(item2.toString());
        }
        Collections.sort(data1);
        Collections.sort(data2);
        System.out.println(data1.size());
        System.out.println();
        System.out.println(data2.size());

        assertEquals(data1, data2);
    }

    @Test
    public void testSortByInputRowCount()
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_sorting WITH (sorted_by=ARRAY['orderkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get one of the orderkey
        Object val = getSingleResult("SELECT orderkey FROM test_sorting limit 1");
        long orderkey = (long) val;

        // get total number of rows
        long totalRows = (long) getSingleResult("SELECT count(*) FROM test_sorting");

        // apply predicate and get input rows read, this time since predicate is on sorted column, rows should be reduced
        long inputRowCountAfter = assertQueryResultGetInputRows("SELECT count(*) FROM test_sorting WHERE orderkey = " + orderkey, 1L);

        assertTrue(totalRows > inputRowCountAfter, "totalRows=" + totalRows + " inputRowCountAfter=" + inputRowCountAfter);

        // apply another predicate and get input rows read
        // the predicate is on sort column, but its outside the value range so minmax index should reduce rows further
        long inputRowCountInvalidValue = assertQueryResultGetInputRows("SELECT count(*) FROM test_sorting WHERE orderkey = 1000000000", 0L);

        assertTrue(inputRowCountAfter > inputRowCountInvalidValue, "inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);

        System.out.println("totalRows=" + totalRows + " inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);
    }

    @Test
    public void testIndexInputRowCount()
    {
        assertQuerySucceeds("CREATE TABLE test_index WITH (index_columns=ARRAY['orderkey'], async_processing=false) AS SELECT * FROM tpch.sf1.orders");

        // get one of the orderkey
        Object val = getSingleResult("SELECT orderkey FROM test_index limit 1");
        long orderkey = (long) val;

        // get total number of rows
        long totalRows = (long) getSingleResult("SELECT count(*) FROM test_index");

        // apply predicate and get input rows read, this time since predicate is on index column, rows should be reduced
        long inputRowCountAfter = assertQueryResultGetInputRows("SELECT count(*) FROM test_index WHERE orderkey = " + orderkey, 1L);

        assertTrue(totalRows > inputRowCountAfter, "totalRows=" + totalRows + " inputRowCountAfter=" + inputRowCountAfter);

        // apply another predicate and get input rows read
        // the predicate is on index column, but its outside the value range so minmax index should reduce rows further
        long inputRowCountInvalidValue = assertQueryResultGetInputRows("SELECT count(*) FROM test_index WHERE orderkey = 1000000000", 0L);

        assertTrue(inputRowCountAfter > inputRowCountInvalidValue, "inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);

        System.out.println("totalRows=" + totalRows + " inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);
    }

    @Test
    public void testSortAndIndexInputRowCount()
    {
        assertQuerySucceeds("CREATE TABLE test_sortindex WITH (sorted_by=ARRAY['custkey'], index_columns=ARRAY['orderkey'], async_processing=false) AS SELECT * FROM tpch.sf1.orders");

        // get one of the orderkey
        Object val = getSingleResult("SELECT orderkey FROM test_sortindex limit 1");
        long orderkey = (long) val;

        // get total input rows, this should read all the rows
        long totalRows = (long) getSingleResult("SELECT count(*) FROM test_sortindex");

        // apply one predicate and get input rows read, this time since predicate is on index column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(
                "SELECT count(*) FROM test_sortindex WHERE orderkey = " + orderkey);

        assertTrue(totalRows > inputRowCountOnePredicate, "totalRows=" + totalRows + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);

        // get one of the custkeys for the orderkey
        Object val2 = getSingleResult("SELECT custkey FROM test_sortindex WHERE orderkey = " + orderkey);
        long custkey = (long) val2;

        // apply two predicates and get input rows read, this time since predicates are on both sort column and index column, rows should be reduced further
        long inputRowCountTwoPredicates = assertQuerySucceedsGetInputRows(
                String.format("SELECT count(*) FROM test_sortindex WHERE custkey = %s and orderKey = %s", custkey, orderkey));

        assertTrue(inputRowCountOnePredicate > inputRowCountTwoPredicates, "inputRowCountOnePredicate=" + inputRowCountOnePredicate + " inputRowCountTwoPredicates=" + inputRowCountTwoPredicates);
    }

    @Test
    public void testFilteringBenchmark()
            throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_filter_bench WITH (sorted_by=ARRAY['custkey'], index_columns=ARRAY['orderkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");

        // get one of the custkey
        Object val = getSingleResult("SELECT custkey FROM test_filter_bench limit 1");
        long custkey = (long) val;

        // get one of the orderkeys for the custkey
        Object val2 = getSingleResult("SELECT orderkey FROM test_filter_bench WHERE custkey = " + custkey);
        long orderKey = (long) val2;

        Thread[] threads = new Thread[25];

        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(() ->
                    assertQuerySucceedsGetInputRows(String.format("SELECT count(*) FROM test_filter_bench WHERE custkey = %s and orderKey = %s", custkey, orderKey)));
        }

        long before = System.currentTimeMillis();
        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        System.out.println(Double.valueOf(System.currentTimeMillis() - before) / threads.length);
    }

    private void assertQueryResult(@Language("SQL") String sql, Object... expected)
    {
        MaterializedResult rows = computeActual(sql);
        assertEquals(rows.getRowCount(), expected.length);

        for (int i = 0; i < expected.length; i++) {
            MaterializedRow materializedRow = rows.getMaterializedRows().get(i);
            int fieldCount = materializedRow.getFieldCount();
            assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
            Object value = materializedRow.getField(0);
            assertEquals(value, expected[i]);
            assertTrue(materializedRow.getFieldCount() == 1);
        }
    }

    private long assertQueryResultGetInputRows(@Language("SQL") String sql, Object... expected)
    {
        assertQueryResult(sql, expected);

        return getInputRowsOfLastQueryExecution(sql);
    }

    private long assertQuerySucceedsGetInputRows(@Language("SQL") String sql, Object... expected)
    {
        assertQuerySucceeds(sql);

        return getInputRowsOfLastQueryExecution(sql);
    }

    private long getInputRowsOfLastQueryExecution(@Language("SQL") String sql)
    {
        String inputRowsSql = "select sum(raw_input_rows) from system.runtime.tasks where query_id in (select query_id from system.runtime.queries where query='" + sql + "' order by created desc limit 1)";

        MaterializedResult rows = computeActual(inputRowsSql);

        assertEquals(rows.getRowCount(), 1);

        MaterializedRow materializedRow = rows.getMaterializedRows().get(0);
        int fieldCount = materializedRow.getFieldCount();
        assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
        Object value = materializedRow.getField(0);

        return (long) value;
    }

    private Object getSingleResult(@Language("SQL") String sql)
    {
        MaterializedResult rows = computeActual(sql);
        assertTrue(rows.getRowCount() > 0);

        MaterializedRow materializedRow = rows.getMaterializedRows().get(0);
        int fieldCount = materializedRow.getFieldCount();
        assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
        Object value = materializedRow.getField(0);

        return value;
    }

    private List<Object> getResults(@Language("SQL") String sql)
    {
        MaterializedResult rows = computeActual(sql);

        List<Object> values = new ArrayList<>();

        for (MaterializedRow materializedRow : rows.getMaterializedRows()) {
            int fieldCount = materializedRow.getFieldCount();
            assertTrue(fieldCount == 1, format("Expected only one column, but got '%d'", fieldCount));
            values.add(materializedRow.getField(0));
        }

        return values;
    }

    @Test
    public void testPartitionedTableSelection()
    {
        assertQuerySucceeds("CREATE TABLE test_nation WITH(async_processing=false) AS SELECT * FROM tpch.tiny.nation");

        Pair<Integer, MaterializedResult> resultPairWithoutPartition = getSplitAndMaterializedResult("select * from test_nation");
        int splitsWithoutPatition = resultPairWithoutPartition.getFirst();
        MaterializedResult resultWithoutPartition = resultPairWithoutPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_nation");

        assertQuerySucceeds("CREATE TABLE test_nation2 WITH (partitioned_by=array['regionkey'], async_processing=false) AS SELECT * FROM tpch.tiny.nation");
        Pair<Integer, MaterializedResult> resultPairWithPartition = getSplitAndMaterializedResult("select * from test_nation2");
        int splitsWithPatition = resultPairWithPartition.getFirst();
        MaterializedResult resultWithPartition = resultPairWithPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_nation2");

        assertTrue(splitsWithPatition > splitsWithoutPatition, "The splits with partition should be more since the data is partitioned.");
        assertTrue(verifyEqualResults(resultWithoutPartition, resultWithPartition), "The results should be equal");
    }

    @Test
    public void testPartitionedTableColumnTypes()
    {
        // test string column type
        assertQuerySucceeds("CREATE TABLE test_orders WITH(async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        Pair<Integer, MaterializedResult> resultPairWithoutPartition = getSplitAndMaterializedResult("select * from test_orders where clerk = 'Clerk#000000828'");
        MaterializedResult resultWithoutPartition = resultPairWithoutPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders");

        assertQuerySucceeds("CREATE TABLE test_orders2 WITH (partitioned_by=array['clerk'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        Pair<Integer, MaterializedResult> resultPairWithPartition = getSplitAndMaterializedResult("select * from test_orders2 where clerk = 'Clerk#000000828'");
        MaterializedResult resultWithPartition = resultPairWithPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders2");

        assertTrue(verifyEqualResults(resultWithoutPartition, resultWithPartition), "The results should be equal");

        // test bigint column type
        assertQuerySucceeds("CREATE TABLE test_orders WITH(async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithoutPartition = getSplitAndMaterializedResult("select * from test_orders where orderkey=34085");
        resultWithoutPartition = resultPairWithoutPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders");

        assertQuerySucceeds("CREATE TABLE test_orders2 WITH (partitioned_by=array['orderkey'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithPartition = getSplitAndMaterializedResult("select * from test_orders2 where orderkey=34085");
        resultWithPartition = resultPairWithPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders2");

        assertTrue(verifyEqualResults(resultWithoutPartition, resultWithPartition), "The results should be equal");

        // test decimal column type
        assertQuerySucceeds("CREATE TABLE test_orders WITH(async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithoutPartition = getSplitAndMaterializedResult("select * from test_orders where totalprice < 100000");
        resultWithoutPartition = resultPairWithoutPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders");

        assertQuerySucceeds("CREATE TABLE test_orders2 WITH (partitioned_by=array['totalprice'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithPartition = getSplitAndMaterializedResult("select * from test_orders2 where totalprice < 100000");
        resultWithPartition = resultPairWithPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders2");

        assertTrue(verifyEqualResults(resultWithoutPartition, resultWithPartition), "The results should be equal");

        // test date column type
        assertQuerySucceeds("CREATE TABLE test_orders WITH(async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithoutPartition = getSplitAndMaterializedResult("select * from test_orders where orderdate=date'1993-06-11'");
        resultWithoutPartition = resultPairWithoutPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders");

        assertQuerySucceeds("CREATE TABLE test_orders2 WITH (partitioned_by=array['orderdate'], async_processing=false) AS SELECT * FROM tpch.tiny.orders");
        resultPairWithPartition = getSplitAndMaterializedResult("select * from test_orders2 where orderdate=date'1993-06-11'");
        resultWithPartition = resultPairWithPartition.getSecond();
        assertQuerySucceeds("DROP TABLE test_orders2");

        assertTrue(verifyEqualResults(resultWithoutPartition, resultWithPartition), "The results should be equal");
    }

    Pair<Integer, MaterializedResult> getSplitAndMaterializedResult(String testerQuery)
    {
        // Select the entry with specifics
        MaterializedResult queryResult = computeActual(testerQuery);

        String doublyQuotedQuery = testerQuery.replaceAll("'", "''");

        // Get queries executed and query ID to find the task with sum of splits
        String splits = "select sum(splits) from system.runtime.tasks where query_id in " +
                "(select query_id from system.runtime.queries " +
                "where query='" + doublyQuotedQuery + "' order by created desc limit 1)";

        MaterializedResult rows = computeActual(splits);

        assertEquals(rows.getRowCount(), 1);

        MaterializedRow materializedRow = rows.getMaterializedRows().get(0);
        int fieldCount = materializedRow.getFieldCount();
        assertEquals(fieldCount, 1,
                "Expected only one column, but got '%d', fiedlCount: " + fieldCount);
        Object value = materializedRow.getField(0);

        return new Pair<>((int) (long) value, queryResult);
    }

    static boolean verifyEqualResults(MaterializedResult result1, MaterializedResult result2)
    {
        ArrayList<String> data1 = new ArrayList<>();
        ArrayList<String> data2 = new ArrayList<>();
        for (MaterializedRow item1 : result1.getMaterializedRows()) {
            data1.add(item1.toString());
        }
        for (MaterializedRow item2 : result2.getMaterializedRows()) {
            data2.add(item2.toString());
        }
        Collections.sort(data1);
        Collections.sort(data2);
        return data1.equals(data2);
    }
}
