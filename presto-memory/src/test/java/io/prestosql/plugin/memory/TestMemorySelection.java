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
package io.prestosql.plugin.memory;

import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
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
    long processingWait = 15000;

    public TestMemorySelection()
    {
        super(MemoryQueryRunner::createQueryRunner);
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
    public void testIndexSingleOperators(String queryOperator, int testKeys) throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey']) AS SELECT * FROM tpch.tiny.orders");

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

        // apply one predicate and get input rows read, this should read all the rows
        long inputRowCountBefore = assertQuerySucceedsGetInputRows(predicateQuery);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

        // apply one predicate and get input rows read, this time since predicate is on sort column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        System.out.println(predicateQuery);

        assertTrue(inputRowCountBefore > inputRowCountOnePredicate, "inputRowCountBefore=" + inputRowCountBefore + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);
    }

    @Test(dataProvider = "memConIndexSingleOperators")
    public void testIndexSingleOperatorsVerifyResults(String queryOperator, int testKeys) throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey']) AS SELECT * FROM tpch.tiny.orders");

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

        MaterializedResult result1 = computeActual(predicateQuery);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

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
    public void testIndexSingleColMultiOperators(String queryOperator) throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey']) AS SELECT * FROM tpch.tiny.orders");

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

        // apply one predicate and get input rows read, this should read all the rows
        long inputRowCountBefore = assertQuerySucceedsGetInputRows(predicateQuery);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

        // apply one predicate and get input rows read, this time since predicate is on sort column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(predicateQuery);

        // Drop table for next use
        assertQuerySucceeds("DROP TABLE test_indexOperations");

        assertTrue(inputRowCountBefore > inputRowCountOnePredicate, "inputRowCountBefore=" + inputRowCountBefore + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);
    }

    @Test(dataProvider = "memConIndexSingleColMultiOperators")
    public void testIndexSingleColMultiOperatorsVerifyResults(String queryOperator) throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_indexOperations WITH (sorted_by=ARRAY['custkey']) AS SELECT * FROM tpch.tiny.orders");

        // get how many test keys there are
        long testKeys = queryOperator.chars().filter(c -> c == '#').count();

        // get custkeys
        List<Object> results = getResults("SELECT * from (SELECT custkey FROM test_indexOperations LIMIT " + testKeys + ") ORDER BY custkey ASC");
        for (Object key : results) {
            queryOperator = queryOperator.replaceFirst("#", String.valueOf((long) key));
        }

        String predicateQuery = "SELECT count(distinct custkey) FROM test_indexOperations WHERE " + queryOperator;

        MaterializedResult result1 = computeActual(predicateQuery);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

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
    public void testSortByInputRowCount() throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_sorting WITH (sorted_by=ARRAY['orderkey']) AS SELECT * FROM tpch.tiny.orders");

        // get one of the orderkey
        Object val = getSingleResult("SELECT orderkey FROM test_index limit 1");
        long orderkey = (long) val;

        // apply predicate and get input rows read, this should read all the rows
        long inputRowCountBefore = assertQueryResultGetInputRows("SELECT count(*) FROM test_sorting WHERE orderkey = " + orderkey, 1L);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

        // apply predicate and get input rows read, this time since predicate is on sorted column, rows should be reduced
        long inputRowCountAfter = assertQueryResultGetInputRows("SELECT count(*) FROM test_sorting WHERE orderkey = " + orderkey, 1L);

        assertTrue(inputRowCountBefore > inputRowCountAfter, "inputRowCountBefore=" + inputRowCountBefore + " inputRowCountAfter=" + inputRowCountAfter);

        // apply another predicate and get input rows read
        // the predicate is on sort column, but its outside the value range so minmax index should reduce rows further
        long inputRowCountInvalidValue = assertQueryResultGetInputRows("SELECT count(*) FROM test_sorting WHERE orderkey = 1000000000", 0L);

        assertTrue(inputRowCountAfter > inputRowCountInvalidValue, "inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);

        System.out.println("inputRowCountBefore=" + inputRowCountBefore + " inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);
    }

    @Test
    public void testIndexInputRowCount() throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_index WITH (index_columns=ARRAY['orderkey']) AS SELECT * FROM tpch.tiny.orders");

        // get one of the orderkey
        Object val = getSingleResult("SELECT orderkey FROM test_index limit 1");
        long orderkey = (long) val;

        // apply predicate and get input rows read, this should read all the rows
        long inputRowCountBefore = assertQueryResultGetInputRows("SELECT count(*) FROM test_index WHERE orderkey = " + orderkey, 1L);

        // wait for indexing to complete
        Thread.sleep(processingWait);

        // apply predicate and get input rows read, this time since predicate is on index column, rows should be reduced
        long inputRowCountAfter = assertQueryResultGetInputRows("SELECT count(*) FROM test_index WHERE orderkey = " + orderkey, 1L);

        assertTrue(inputRowCountBefore > inputRowCountAfter, "inputRowCountBefore=" + inputRowCountBefore + " inputRowCountAfter=" + inputRowCountAfter);

        // apply another predicate and get input rows read
        // the predicate is on index column, but its outside the value range so minmax index should reduce rows further
        long inputRowCountInvalidValue = assertQueryResultGetInputRows("SELECT count(*) FROM test_index WHERE orderkey = 1000000000", 0L);

        assertTrue(inputRowCountAfter > inputRowCountInvalidValue, "inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);

        System.out.println("inputRowCountBefore=" + inputRowCountBefore + " inputRowCountAfter=" + inputRowCountAfter + " inputRowCountInvalidValue=" + inputRowCountInvalidValue);
    }

    @Test
    public void testSortAndIndexInputRowCount() throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_sortindex WITH (sorted_by=ARRAY['custkey'], index_columns=ARRAY['orderkey']) AS SELECT * FROM tpch.tiny.orders");

        // get one of the custkey
        Object val = getSingleResult("SELECT custkey FROM test_sortindex limit 1");
        long custkey = (long) val;

        // apply one predicate and get input rows read, this should read all the rows
        long inputRowCountBefore = assertQuerySucceedsGetInputRows(
                "SELECT count(*) FROM test_sortindex WHERE custkey = " + custkey);

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

        // apply one predicate and get input rows read, this time since predicate is on sort column, rows should be reduced
        long inputRowCountOnePredicate = assertQuerySucceedsGetInputRows(
                "SELECT count(*) FROM test_sortindex WHERE custkey = " + custkey);

        assertTrue(inputRowCountBefore > inputRowCountOnePredicate, "inputRowCountBefore=" + inputRowCountBefore + " inputRowCountOnePredicate=" + inputRowCountOnePredicate);

        // get one of the orderkeys for the custkey
        Object val2 = getSingleResult("SELECT orderkey FROM test_sortindex WHERE custkey = " + custkey);
        long orderKey = (long) val2;

        // apply two predicates and get input rows read, this time since predicates are on both sort column and index column, rows should be reduced further
        long inputRowCountTwoPredicates = assertQuerySucceedsGetInputRows(
                String.format("SELECT count(*) FROM test_sortindex WHERE custkey = %s and orderKey = %s", custkey, orderKey));

        assertTrue(inputRowCountOnePredicate > inputRowCountTwoPredicates, "inputRowCountOnePredicate=" + inputRowCountOnePredicate + " inputRowCountTwoPredicates=" + inputRowCountTwoPredicates);
    }

    @Test
    public void testFilteringBenchmark() throws InterruptedException
    {
        assertQuerySucceeds("CREATE TABLE test_filter_bench WITH (sorted_by=ARRAY['custkey'], index_columns=ARRAY['orderkey']) AS SELECT * FROM tpch.tiny.orders");

        // get one of the custkey
        Object val = getSingleResult("SELECT custkey FROM test_filter_bench limit 1");
        long custkey = (long) val;

        // get one of the orderkeys for the custkey
        Object val2 = getSingleResult("SELECT orderkey FROM test_filter_bench WHERE custkey = " + custkey);
        long orderKey = (long) val2;

        // wait for sorting and indexing to complete
        Thread.sleep(processingWait);

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
}
