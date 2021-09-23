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
package io.hetu.core.heuristicindex;

import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHindexBTreeIndex
        extends TestIndexResources
{
    @Test
    public void testBtreeIndexOnPartitionedColumnCreateAndDelete()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName);
    }

    @Test
    public void testBtreeIndexOnNonePartitionedColumnCreateAndDelete()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName);
    }

    @Test
    public void testBtreeIndexHasKeyWhereDelete()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WHERE key2 = 11");
        assertQuerySucceeds("DROP INDEX " + indexName + " WHERE key2 = 11");
    }

    @Test
    public void testBtreeIndexInvalidKeyWhereDelete()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key1) WHERE key2 = 11");
        try {
            assertQuerySucceeds("DROP INDEX " + indexName + " WHERE key2 = 10");
            throw new AssertionError("Expected drop index query to fail.");
        }
        catch (AssertionError e) {
            assertTrue(e.getCause().toString().contains("line 1:1: Index '" + indexName + "' does not contain partitions: [key2=10]"));
            return;
        }
    }

    @Test
    public void testBtreeIndexCreationWherePartitionedColumn()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WHERE key2 = 11");

        String testerQuery = "SELECT * FROM " + tableName + " WHERE key2 = 11";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsLoadingIndex, splitsIndexLoaded);
        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal.");
    }

    @Test
    public void testBtreeIndexCreationWhereNonPartitionedColumn()
    {
        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        assertQueryFails("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                        " (key2) WHERE key1 = 1",
                "line 1:18: Heuristic index creation is only supported for predicates on partition columns");
    }

    @Test
    public void testBtreeIndexTransactional()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTableTransact1(tableName);

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                " (key2) WHERE key2 = 12");

        String testerQuery = "SELECT * FROM " + tableName + " WHERE key2 = 12";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
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
                        " (key2) WHERE key1 = 1",
                "line 1:18: Heuristic index creation is only supported for predicates on partition columns");
    }

    // Tests the index type handles the cases of table having NULL values without intermediate errors.
    @Test(dataProvider = "btreeTable1Operators")
    public void testBtreeIndexOperators(String condition)
    {
        System.out.println("Running testBtreeIndexOperators[condition: " + condition + "]");

        String tableName = getNewTableName();
        createBtreeTable1(tableName);

        String indexName = getNewIndexName();
        try {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING btree ON " + tableName +
                    " (key2) WHERE " + condition);
        }
        catch (AssertionError e) {
            assertNotEquals(condition, "key2 = 11");
            assertTrue(e.getCause().toString().contains("line 1:1: Unsupported WHERE expression." +
                    " Only in-predicate/equality-expressions are supported e.g. partition=1 or partition=2/partition in (1,2)"));
        }
    }

    @Test
    public void testBtreeIndexMultiPartitionedColumn()
            throws Exception
    {
        String tableName = getNewTableName();
        createBtreeTableMultiPart1(tableName);

        String indexName1 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING btree ON " + tableName +
                " (key1) WHERE key3 = 222");

        String testerQuery1 = "SELECT * FROM " + tableName + " WHERE key1 = 2";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex1 = getSplitAndMaterializedResult(testerQuery1);
        int splitsLoadingIndex1 = resultPairLoadingIndex1.getFirst();
        MaterializedResult resultLoadingIndex1 = resultPairLoadingIndex1.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded1 = runTwiceGetSplitAndMaterializedResult(testerQuery1);
        int splitsIndexLoaded1 = resultPairIndexLoaded1.getFirst();
        MaterializedResult resultIndexLoaded1 = resultPairIndexLoaded1.getSecond();

        assertEquals(splitsLoadingIndex1, splitsIndexLoaded1);
        assertTrue(verifyEqualResults(resultLoadingIndex1, resultIndexLoaded1), "The results should be equal.");

        // Create second index and do query again on different keys

        String indexName2 = getNewIndexName();
        assertQueryFails("CREATE INDEX " + indexName2 + " USING btree ON " + tableName +
                " (key2) WHERE key5 = 22222", "Creating index on key5 is not supported as it's not first-level partition");

        String testerQuery2 = "SELECT * FROM " + tableName + " WHERE key2 = 22";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex2 = getSplitAndMaterializedResult(testerQuery2);
        int splitsLoadingIndex2 = resultPairLoadingIndex2.getFirst();
        MaterializedResult resultLoadingIndex2 = resultPairLoadingIndex2.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded2 = runTwiceGetSplitAndMaterializedResult(testerQuery2);
        int splitsIndexLoaded2 = resultPairIndexLoaded2.getFirst();
        MaterializedResult resultIndexLoaded2 = resultPairIndexLoaded2.getSecond();

        assertEquals(splitsLoadingIndex2, splitsIndexLoaded2);
        assertTrue(verifyEqualResults(resultLoadingIndex2, resultIndexLoaded2), "The results should be equal.");
    }
}
