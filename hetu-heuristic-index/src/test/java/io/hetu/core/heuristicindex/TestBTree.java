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
package io.hetu.core.heuristicindex;

import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestBTree
        extends TestIndexResources
{
    @Test
    public void testBtreeIndexOnPartitionedColumnCreateAndDelete()
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WITH (level=partition) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName);
    }

    @Test
    public void testBtreeIndexOnNonePartitionedColumnCreateAndDelete()
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WITH (level=partition) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName);
    }

    @Test
    public void testBtreeIndexHasKeyWhereDelete()
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WITH (level=partition) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName + " WHERE key2 = 11");
    }

    @Test
    public void testBtreeIndexInvalidKeyWhereDelete()
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WITH (level=partition) WHERE key2 = 11");
        try {
            assertQuerySucceeds("DROP INDEX " + indexName + " WHERE key2 = 10");
        }
        catch (AssertionError e) {
            assertTrue(e.getCause().toString().contains("line 1:1: Index '" + indexName + "' does not contain partitions: [key2=10]"));
        }
    }

    @Test
    public void testBtreeIndexCreationWherePartitionedColumn()
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WITH (level=partition) WHERE key2 = 11");

        String testerQuery = "SELECT * FROM " + tableName + " WHERE key2 = 11";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsLoadingIndex, splitsIndexLoaded);
        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal.");
    }

    @Test
    public void testBtreeIndexCreationWhereNonPartitionedColumn()
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQueryFails("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                        " (key2) WITH (level=partition) WHERE key1 = 1",
                "line 1:18: Heuristic index creation is only supported for predicates on partition columns");
    }

    @Test
    public void testBtreeIndexTransactional()
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createBtreeTableTransact1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WITH (level=partition) WHERE key2 = 12");

        String testerQuery = "SELECT * FROM " + tableName + " WHERE key2 = 12";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsLoadingIndex, splitsIndexLoaded);
        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal.");
    }

    @Test
    public void testBtreeIndexEmptyTableCreation()
    {
        String tableName = getNewTableName();
        assertQuerySucceeds("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT)" +
                " WITH (partitioned_by = ARRAY['key2'])");

        String indexName = getNewIndexName();
        assertQueryFails("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                        " (key2) WITH (level=partition) WHERE key1 = 1",
                "line 1:18: Heuristic index creation is only supported for predicates on partition columns");
    }

    // Tests the index type handles the cases of table having NULL values without intermediate errors.
    @Test(dataProvider = "btreeTable1Operators")
    public void testBtreeIndexOperators(String condition)
    {
        String tableName = getNewTableName();
        tableName = createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        try {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                    " (key2) WITH (level=partition) WHERE " + condition);
        }
        catch (AssertionError e) {
            assertNotEquals(condition, "key2 = 11");
            assertTrue(e.getCause().toString().contains("line 1:1: Unsupported WHERE expression." +
                    " Only in-predicate/equality-expressions are supported e.g. partition=1 or partition=2/partition in (1,2)"));
        }
    }
}
