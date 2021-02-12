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

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.ParsingException;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHindex
        extends AbstractTestQueryFramework
{
    @SuppressWarnings("unused")
    public TestHindex()
    {
        super(HindexQueryRunner::createQueryRunner);
    }

    @DataProvider(name = "splitsWithIndexAndData")
    public Object[][] splitsWithIndexAndData()
    {
        return new Object[][] {
                {"bitmap", "bigint"}, {"bloom", "bigint"}, {"minmax", "bigint"}, {"btree", "bigint"},
                {"bitmap", "boolean"}, {"bloom", "boolean"}, {"minmax", "boolean"}, {"btree", "boolean"},
                {"bitmap", "int"}, {"bloom", "int"}, {"minmax", "int"}, {"btree", "int"},
                {"bitmap", "smallint"}, {"bloom", "smallint"}, {"minmax", "smallint"}, {"btree", "smallint"},
                {"bitmap", "tinyint"}, {"bloom", "tinyint"}, {"minmax", "tinyint"}, {"btree", "tinyint"}};
    }

    // Tests to see the difference in number of splits used without and with the usage of index with specified data.
    @Test(dataProvider = "splitsWithIndexAndData")
    public void testSplitsWithIndexAndData(String indexType, String dataType)
            throws InterruptedException
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        String[] testerData = createTableDataTypeWithQuery(tableName, dataType);
        tableName = testerData[0];
        String testerQuery = testerData[1];

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create index
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            if (dataType.toLowerCase(Locale.ROOT).equals("boolean")) {
                assertQueryFails("CREATE INDEX " + indexName + " USING " +
                                indexType + " ON " + tableName + " (data_col2) WITH (level='table')",
                        "Index creation on boolean column is not supported");
            }
            else {
                assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + tableName + " (data_col2) WITH (level='table')");
            }
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (data_col2)");
        }

        // Get splits and result
        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(1000);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsBeforeIndex, splitsLoadingIndex,
                "The splits prior to index loaded should be the same:" +
                        " Splits1: " + splitsBeforeIndex +
                        " Splits2: " + splitsLoadingIndex +
                        " Splits3: " + splitsIndexLoaded);
        if (indexType.toLowerCase(Locale.ROOT).equals("bitmap") || dataType.toLowerCase(Locale.ROOT).equals("boolean")) {
            assertEquals(splitsBeforeIndex, splitsIndexLoaded,
                    "The splits with index loaded should be the same as splits before index:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }
        else {
            assertTrue(splitsBeforeIndex > splitsIndexLoaded,
                    "The splits with index loaded should be lower than splits before index:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }

        assertTrue(verifyEqualResults(resultBeforeIndex, resultIndexLoaded), "The results should be equal.");
    }

    @DataProvider(name = "tableData1")
    public Object[][] tableData1()
    {
        return new Object[][] {{"bitmap", "id", "2"}, {"bloom", "id", "2"}, {"minmax", "id", "2"}, {"btree", "id", "2"}};
    }

    // Tests data consistency and splits for which table data is changed after index creation.
    @Test(dataProvider = "tableData1")
    public void testDataConsistencyWithAdditionChange(String indexType, String queryVariable, String queryValue)
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createTable1(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        int splitsBeforeIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Create index to use for testing splits
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ")");
        }

        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'new1'), (8, 'new2')");

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertTrue(splitsBeforeIndex < splitsLoadingIndex,
                "The splits with loading index should be more:" +
                        " Splits1: " + splitsBeforeIndex +
                        " Splits2: " + splitsLoadingIndex);
        if (indexType.toLowerCase(Locale.ROOT).equals("bitmap")) {
            assertEquals(splitsLoadingIndex, splitsIndexLoaded,
                    "The splits with loading index should be equal to splits index loaded for bitmap index:" +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
            assertTrue(splitsBeforeIndex < splitsIndexLoaded,
                    "The splits with before index should be the lowest:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }
        else {
            assertTrue(splitsLoadingIndex > splitsIndexLoaded,
                    "The splits with loading index should be more than the splits with index loaded:" +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
            assertTrue(splitsBeforeIndex > splitsIndexLoaded,
                    "The splits with index loaded should be lowest:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal.");
    }

    @DataProvider(name = "tableData2")
    public Object[][] tableData2()
    {
        return new Object[][] {{"bitmap", "key", "2"}, {"bloom", "key", "2"}, {"minmax", "key", "2"}, {"btree", "key", "2"}};
    }

    // Tests data consistency when data is deleted after index is created.
    @Test(dataProvider = "tableData2")
    public void testDataConsistencyWithDataDeletionChange(String indexType, String queryVariable, String queryValue)
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createTable2(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        int splitsBeforeIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Create index to use for testing splits
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ")");
        }

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertQuerySucceeds("DELETE FROM " + tableName);

        Pair<Integer, MaterializedResult> resultPairDeletingData = getSplitAndMaterializedResult(testerQuery);
        int splitsDeletingData = resultPairDeletingData.getFirst();
        MaterializedResult resultDeletingData = resultPairDeletingData.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairDataDeleted = getSplitAndMaterializedResult(testerQuery);
        int splitsDataDeleted = resultPairDataDeleted.getFirst();
        MaterializedResult resultDataDeleted = resultPairDataDeleted.getSecond();

        assertEquals(splitsBeforeIndex, splitsLoadingIndex);
        if (indexType.toLowerCase(Locale.ROOT).equals("bitmap")) {
            assertEquals(splitsLoadingIndex, splitsIndexLoaded);
            assertEquals(splitsBeforeIndex, splitsIndexLoaded);
        }
        else {
            assertTrue(splitsLoadingIndex > splitsIndexLoaded);
            assertTrue(splitsBeforeIndex > splitsIndexLoaded);
        }

        assertTrue(splitsBeforeIndex > splitsDeletingData);
        assertEquals(splitsDeletingData, splitsDataDeleted);
        assertTrue(splitsBeforeIndex > splitsDataDeleted);
        assertTrue(splitsLoadingIndex > splitsDeletingData);
        assertTrue(splitsLoadingIndex > splitsDataDeleted);
        if (indexType.toLowerCase(Locale.ROOT).equals("minmax") || indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertEquals(splitsIndexLoaded, splitsDeletingData);
            assertEquals(splitsIndexLoaded, splitsDataDeleted);
        }
        else {
            assertTrue(splitsIndexLoaded > splitsDeletingData);
            assertTrue(splitsIndexLoaded > splitsDataDeleted);
        }

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal.");
        assertTrue(verifyEqualResults(resultDeletingData, resultDataDeleted), "The results should not be equal.");
    }

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
    public void testBtreeIndexMultiPartitionedColumn()
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createBtreeTableMultiPart1(tableName);

        String indexName1 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING btree ON " + tableName +
                " (key1) WITH (level=partition) WHERE key3 = 222");

        String testerQuery1 = "SELECT * FROM " + tableName + " WHERE key1 = 2";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex1 = getSplitAndMaterializedResult(testerQuery1);
        int splitsLoadingIndex1 = resultPairLoadingIndex1.getFirst();
        MaterializedResult resultLoadingIndex1 = resultPairLoadingIndex1.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded1 = getSplitAndMaterializedResult(testerQuery1);
        int splitsIndexLoaded1 = resultPairIndexLoaded1.getFirst();
        MaterializedResult resultIndexLoaded1 = resultPairIndexLoaded1.getSecond();

        assertEquals(splitsLoadingIndex1, splitsIndexLoaded1);
        assertTrue(verifyEqualResults(resultLoadingIndex1, resultIndexLoaded1), "The results should be equal.");

        // Create second index and do query again on different keys

        String indexName2 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING btree ON " + tableName +
                " (key2) WITH (level=partition) WHERE key5 = 22222");

        String testerQuery2 = "SELECT * FROM " + tableName + " WHERE key2 = 22";

        Pair<Integer, MaterializedResult> resultPairLoadingIndex2 = getSplitAndMaterializedResult(testerQuery2);
        int splitsLoadingIndex2 = resultPairLoadingIndex2.getFirst();
        MaterializedResult resultLoadingIndex2 = resultPairLoadingIndex2.getSecond();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded2 = getSplitAndMaterializedResult(testerQuery2);
        int splitsIndexLoaded2 = resultPairIndexLoaded2.getFirst();
        MaterializedResult resultIndexLoaded2 = resultPairIndexLoaded2.getSecond();

        assertEquals(splitsLoadingIndex2, splitsIndexLoaded2);
        assertTrue(verifyEqualResults(resultLoadingIndex2, resultIndexLoaded2), "The results should be equal.");
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

    @DataProvider(name = "btreeTable1Operators")
    public Object[][] btreeTable1Operators()
    {
        return new Object[][] {{"key2 = 11"}, {"key2 > 12"}, {"key2 >= 12"}, {"key2 < 15"}, {"key2 <= 12"},
                {"key2 BETWEEN 12 AND 15"}, {"key2 IN (11, 12)"}, {"key2 = 11 or 12"}};
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

    @DataProvider(name = "nullDataHandling")
    public Object[][] nullDataHandling()
    {
        return new Object[][] {{"bitmap", "col1", "111"}, {"bitmap", "col2", "222"}, {"bitmap", "col3", "33"},
                {"bloom", "col1", "111"}, {"bloom", "col2", "222"}, {"bloom", "col3", "33"},
                {"minmax", "col1", "111"}, {"minmax", "col2", "222"}, {"minmax", "col3", "33"},
                {"btree", "col1", "111"}, {"btree", "col2", "222"}, {"btree", "col3", "33"}};
    }

    // Tests the index type handles the cases of table having NULL values without intermediate errors.
    @Test(dataProvider = "nullDataHandling")
    public void testNullDataHandling(String indexType, String queryVariable, String queryValue)
            throws InterruptedException
    {
        String tableName = getNewTableName();
        tableName = createTableNullData(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create index to use for testing splits
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ")");
        }

        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(1000);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsBeforeIndex, splitsLoadingIndex,
                "The splits prior to index loaded should be the same:" +
                        " Splits1: " + splitsBeforeIndex +
                        " Splits2: " + splitsLoadingIndex +
                        " Splits3: " + splitsIndexLoaded);
        if (indexType.toLowerCase(Locale.ROOT).equals("bloom") || indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertTrue(splitsBeforeIndex > splitsIndexLoaded,
                    "Splits of index loaded should be lower due to filtering for bloom or btree:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }
        else {
            assertEquals(splitsBeforeIndex, splitsIndexLoaded,
                    "Splits of index loaded should be the same as before index is loaded:" +
                            " Splits1: " + splitsBeforeIndex +
                            " Splits2: " + splitsLoadingIndex +
                            " Splits3: " + splitsIndexLoaded);
        }

        assertTrue(verifyEqualResults(resultBeforeIndex, resultIndexLoaded), "The results should be equal.");
    }

    // Tests the case of creating all four types of index
    @Test
    public void testIndexAllFourTypesTogether()
            throws InterruptedException
    {
        String tableName = getNewTableName();
        String testerQuery = "SELECT * FROM " + tableName + " WHERE id = 2";
        tableName = createTable1(tableName);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create indices
        String indexName1 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING btree ON " + tableName + " (id) WITH (level='table')");
        String indexName2 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING bitmap ON " + tableName + " (id)");
        String indexName3 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName3 + " USING bloom ON " + tableName + " (id)");
        String indexName4 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName4 + " USING minmax ON " + tableName + " (id)");

        // Get splits and result
        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(1000);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsBeforeIndex, splitsLoadingIndex);
        assertTrue(splitsLoadingIndex > splitsIndexLoaded);
        assertTrue(splitsBeforeIndex > splitsIndexLoaded);

        assertTrue(verifyEqualResults(resultBeforeIndex, resultIndexLoaded), "The results should be equal.");

        assertQuerySucceeds("DROP INDEX " + indexName1);
        assertQuerySucceeds("DROP INDEX " + indexName2);
        assertQuerySucceeds("DROP INDEX " + indexName3);
        assertQuerySucceeds("DROP INDEX " + indexName4);
    }

    // Tests the case of deleting index when generating an index. Result is that splits and info are same.
    @Test(dataProvider = "indexTypes")
    public void testIndexDeletionBeforeSplitsAffected(String indexType)
            throws InterruptedException
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        String testerQuery = "SELECT * FROM " + tableName + " WHERE id = 2";
        tableName = createTable1(tableName);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create index
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id) WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id)");
        }

        // Drop index before changes
        assertQuerySucceeds("DROP INDEX " + indexName);

        // Get splits and result
        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(1000);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = getSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertEquals(splitsBeforeIndex, splitsLoadingIndex);
        assertEquals(splitsLoadingIndex, splitsIndexLoaded);
        assertEquals(splitsBeforeIndex, splitsIndexLoaded);

        assertTrue(verifyEqualResults(resultBeforeIndex, resultIndexLoaded), "The results should be equal.");
    }

    // Tests the case of creating index with if not exists.
    @Test(dataProvider = "indexTypes")
    public void testIndexIfNotExistsCreation(String indexType)
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        tableName = createTable1(tableName);

        // Create index
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX IF NOT EXISTS " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id) WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX IF NOT EXISTS " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id)");
        }

        // Validate if created
        String testerQuery = "SHOW INDEX";
        Pair<Integer, MaterializedResult> resultPairIndexCreation = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation = resultPairIndexCreation.getSecond();
        boolean indexExists = false;
        for (MaterializedRow item : resultIndexCreation.getMaterializedRows()) {
            if (item.getField(0).toString().equals(indexName)) {
                indexExists = true;
                // Assert that the shown contains catalog, schema and table names:
                assertEquals(item.getField(2).toString(), tableName);
                break;
            }
        }
        assertTrue(indexExists);

        // Show index with if exists
        testerQuery = "SHOW INDEX IF EXISTS " + indexName;
        Pair<Integer, MaterializedResult> resultPairIndexCreation2 = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation2 = resultPairIndexCreation2.getSecond();
        indexExists = false;
        for (MaterializedRow item : resultIndexCreation2.getMaterializedRows()) {
            if (item.getField(0).toString().equals(indexName)) {
                indexExists = true;
                // Assert that the shown contains catalog, schema and table names:
                assertEquals(item.getField(2).toString(), tableName);
                break;
            }
        }
        assertTrue(indexExists);
    }

    // Tests the case of asserting that the uncreated new index does not exist
    @Test
    public void testIndexNotCreatedNotExist()
    {
        String indexName = getNewIndexName();
        createTable1(getNewTableName());

        // Validate if created
        String testerQuery = "SHOW INDEX";
        Pair<Integer, MaterializedResult> resultPairIndexCreation = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation = resultPairIndexCreation.getSecond();
        boolean indexExists = false;
        for (MaterializedRow item : resultIndexCreation.getMaterializedRows()) {
            if (item.getField(0).toString().equals(indexName)) {
                indexExists = true;
                break;
            }
        }
        assertFalse(indexExists);

        // Show index with if exists
        testerQuery = "SHOW INDEX IF EXISTS " + indexName;
        Pair<Integer, MaterializedResult> resultPairIndexCreation2 = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation2 = resultPairIndexCreation2.getSecond();
        int size = resultIndexCreation2.getRowCount();
        assertEquals(size, 0);
    }

    // Tests the case of failing to delete index because the name of index is wrong.
    // Wrong name cases are:
    // using catalog name, wrong catalog name, schema name, wrong schema name, table name, wrong table name, column name, wrong column name
    @Test(dataProvider = "indexTypes")
    public void testIndexDeletionWithWrongNames(String indexType)
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        tableName = createTable1(tableName);

        // Create index
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id) WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id)");
        }
        assertQueryFails("DROP INDEX hive", "line 1:1: Index 'hive' does not exist");
        assertQueryFails("DROP INDEX HDFS", "line 1:1: Index 'hdfs' does not exist");
        assertQueryFails("DROP INDEX test", "line 1:1: Index 'test' does not exist");
        assertQueryFails("DROP INDEX wrongtest", "line 1:1: Index 'wrongtest' does not exist");
        String[] table = tableName.split("\\.");
        assertQueryFails("DROP INDEX " + table[2], "line 1:1: Index '" + table[2] + "' does not exist");
        assertQueryFails("DROP INDEX " + table[2] + "uncreated",
                "line 1:1: Index '" + table[2] + "uncreated' does not exist");
        assertQueryFails("DROP INDEX id", "line 1:1: Index 'id' does not exist");
        assertQueryFails("DROP INDEX wrongcolumn", "line 1:1: Index 'wrongcolumn' does not exist");
    }

    // Tests the case of failing to delete index because index is already deleted
    @Test(dataProvider = "indexTypes")
    public void testIndexDuplicateDeletion(String indexType)
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        tableName = createTable1(tableName);

        // Create index
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id) WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                    indexType + " ON " + tableName + " (id)");
        }

        assertQuerySucceeds("DROP INDEX " + indexName);
        assertQueryFails("DROP INDEX " + indexName,
                "line 1:1: Index '" + indexName + "' does not exist");
    }

    @DataProvider(name = "tableData3")
    public Object[][] tableData3()
    {
        return new Object[][] {{"bitmap", "id"}, {"bloom", "id"}, {"minmax", "id"}, {"btree", "id"}};
    }

    // Tests the case where more than one index is created on the same table and same column (Error case).
    @Test(dataProvider = "tableData3")
    public void testMultipleSameIndexCreation(String indexType, String queryVariable)
    {
        String tableName = getNewTableName();
        tableName = createTable1(tableName);

        String indexName1 = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING " +
                    indexType + " ON " + tableName + " (" + queryVariable + ")");
        }

        String indexName2 = getNewIndexName();
        try {
            if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
                assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING " +
                        indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')");
            }
            else {
                assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING " +
                        indexType + " ON " + tableName + " (" + queryVariable + ")");
            }
        }
        catch (AssertionError except) {
            // Catch error
            assertTrue(except.getCause().toString().contains("line 1:1: Index with same (table,column,indexType) already exists"));
        }
    }

    // Tests the case where the table at which the index is trying to be created is empty (Error case).
    @Test(dataProvider = "tableData3")
    public void testEmptyTableIndexCreation(String indexType, String queryVariable)
            throws IllegalStateException
    {
        String tableName = getNewTableName();
        tableName = createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + tableName + " (" + queryVariable + ") WITH (level='table')",
                    "The table is empty. No index will be created.");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + tableName + " (" + queryVariable + ")",
                    "The table is empty. No index will be created.");
        }
    }

    // Tests the case where index is trying to be created without catalog name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithoutCatalogCreation(String indexType, String queryVariable)
            throws IllegalStateException
    {
        String tableName = getNewTableName();
        String wrongTableName = tableName.substring(5);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "The table is empty. No index will be created.");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "The table is empty. No index will be created.");
        }
    }

    // Tests the case where index is trying to be created with a wrong catalog name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithWrongCatalogCreation(String indexType, String queryVariable)
            throws PrestoException
    {
        String tableName = getNewTableName();
        String wrongTableName = "system." + tableName.substring(5);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:1: CREATE INDEX is not supported in catalog 'system'");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:1: CREATE INDEX is not supported in catalog 'system'");
        }
    }

    // Tests the case where index is trying to be created with a wrong catalog name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexErrorCatalogCreation(String indexType, String queryVariable)
            throws PrestoException
    {
        String tableName = getNewTableName();
        String wrongTableName = "nonexisting." + tableName.substring(5);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:1: CREATE INDEX is not supported in catalog 'nonexisting'");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:1: CREATE INDEX is not supported in catalog 'nonexisting'");
        }
    }

    // Tests the case where index is trying to be created without schema name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithoutSchemaCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String tableName = getNewTableName();
        String wrongTableName = "hive." + tableName.substring(10);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:16: Schema hive does not exist");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:16: Schema hive does not exist");
        }
    }

    // Tests the case where index is trying to be created with a wrong schema name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithWrongSchemaCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String tableName = getNewTableName();
        String wrongTableName = "hive.nonexisting." + tableName.substring(10);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:16: Schema nonexisting does not exist");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:16: Schema nonexisting does not exist");
        }
    }

    // Tests the case where index is trying to be created without table name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithoutTableCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String wrongTableName = "hive.test";

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:16: Schema hive does not exist");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:16: Schema hive does not exist");
        }
    }

    // Tests the case where index is trying to be created with a wrong table name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithWrongTableCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String tableName = getNewTableName();
        String wrongTableName = "hive.test.nonexisting" + tableName.substring(10);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ") WITH (level='table')",
                    "line 1:16: Table " + wrongTableName + " does not exist");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                    "line 1:16: Table " + wrongTableName + " does not exist");
        }
    }

    // Tests the case where index is trying to be created without column name (Error case).
    @Test(dataProvider = "indexTypes")
    public void testIndexWithoutColumnCreation(String indexType)
    {
        String tableName = getNewTableName();
        tableName = createTable1(tableName);
        // Error of "line 1:115: mismatched input ')'. Expecting: <identifier>" exists
        // But code style does not allow ) to exist inside a string without having ( before it.

        String indexName = getNewIndexName();
        try {
            if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
                assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType +
                        " ON " + tableName + " () WITH (level='table')");
            }
            else {
                assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType +
                        " ON " + tableName + " ()");
            }
        }
        catch (AssertionError e) {
            assertTrue(e.getCause().toString().contains("mismatched input ')'"));
        }
    }

    // Tests the case where index is trying to be created with a wrong column name (Error case).
    @Test(dataProvider = "indexTypes")
    public void testIndexWithWrongColumnCreation(String indexType)
            throws SemanticException
    {
        String tableName = getNewTableName();
        tableName = createTable1(tableName);

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + tableName + " (wrong_column) WITH (level='table')",
                    "line 1:8: Column 'wrong_column' cannot be resolved");
        }
        else {
            assertQueryFails("CREATE INDEX " + indexName + " USING " +
                            indexType + " ON " + tableName + " (wrong_column)",
                    "line 1:8: Column 'wrong_column' cannot be resolved");
        }
    }

    // Tests the case where index is trying to be created with a wrong filter name (Error case).
    @Test
    public void testIndexWithWrongFilterCreation()
            throws ParsingException
    {
        String tableName = getNewTableName();
        tableName = createTable1(tableName);

        String indexName = getNewIndexName();
        assertQueryFails("CREATE INDEX " + indexName + " USING wrong_filter ON " + tableName + " (id)",
                "line 1:59: mismatched input 'wrong_filter'. Expecting: '.', 'USING'");
    }

    // Tests the case of creating index where variable name is capitalized. Expected to pass regardless of variable case.
    @Test(dataProvider = "indexTypes")
    public void testIndexWithCapitalColumnNameCreation(String indexType)
    {
        String tableName = getNewTableName();
        assertQuerySucceeds("CREATE TABLE " + tableName + " (P1 INTEGER, P2 VARCHAR(10))");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");

        String indexName = getNewIndexName();
        if (indexType.toLowerCase(Locale.ROOT).equals("btree")) {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType +
                    " ON " + tableName + " (P1) WITH (level='table')");
        }
        else {
            assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType + " ON " + tableName + " (P1)");
        }
    }

    @DataProvider(name = "indexTypes")
    public Object[][] indexTypes()
    {
        return new Object[][] {{"BITMAP"}, {"BLOOM"}, {"MINMAX"}, {"BTREE"}};
    }

    private String safeCreateTable(String query, String tableName)
    {
        String newName = tableName;
        int max = 1;
        for (int i = 0; i < 10; i++) {
            try {
                assertQuerySucceeds(query);
                return newName;
            }
            catch (AssertionError e) {
                max *= 10;
                newName = tableName + ((int) (Math.random() * max));
                if (newName.length() >= 128) {
                    throw new RuntimeException("Cannot Create Table: " + tableName + " due to " + e.getCause().toString());
                }
                query = query.replaceFirst(tableName, newName);
            }
        }
        assertQuerySucceeds(query);
        return newName;
    }

    private String createEmptyTable(String tableName)
    {
        return safeCreateTable("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))", tableName);
    }

    private String createTable1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");
        return tableName;
    }

    private String createTable2(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key BIGINT, status VARCHAR(7), price DECIMAL(5,2), date DATE)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 'SUCCESS', 101.12, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'FAILURE', 101.12, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, 'SUCCESS', 252.36, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'FAILURE', 101.12, DATE '2021-01-03')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 'SUCCESS', 101.12, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 'FAILURE', 252.36, DATE '2021-02-23')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(6, 'SUCCESS', 252.36, DATE '2021-01-03')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'SUCCESS', 101.12, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(8, 'FAILURE', 252.36, DATE '2021-02-23')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(9, 'FAILURE', 101.12, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'FAILURE', 252.36, DATE '2021-01-03')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 'SUCCESS', 101.12, DATE '2021-02-23')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 'FAILURE', 252.36, DATE '2021-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(6, 'FAILURE', 252.36, DATE '2021-01-03')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'SUCCESS', 101.12, DATE '2021-01-03')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(8, 'SUCCESS', 101.12, DATE '2021-01-03')");
        return tableName;
    }

    private String createTableNullData(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName + " (col1 INTEGER, col2 BIGINT, col3 TINYINT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(111, NULL, NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, BIGINT '222', NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, NULL, TINYINT '33')");
        return tableName;
    }

    private String createBtreeTable1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT)" +
                " WITH (partitioned_by = ARRAY['key2'])", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
        return tableName;
    }

    private String createBtreeTableTransact1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT)" +
                " WITH (transactional = true, partitioned_by = ARRAY['key2'])", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
        return tableName;
    }

    private String createBtreeTableMultiPart1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT, key3 INT, key4 INT, key5 INT)" +
                " WITH (partitioned_by = ARRAY['key3', 'key4', 'key5'])", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName +
                " VALUES(1, 11, 111, 1111, 11111), (2, 22, 222, 2222, 22222), (3, 33, 333, 3333, 33333)," +
                " (2, 22, 222, 2222, 22222), (5, 55, 555, 5555, 55555), (1, 11, 111, 1111, 11111)," +
                " (6, 66, 666, 6666, 66666), (7, 77, 777, 7777, 77777), (2, 22, 222, 2222, 22222), (2, 22, 222, 2222, 22222)");
        return tableName;
    }

    private String[] createTableDataTypeWithQuery(String tableName, String dataType)
            throws InterruptedException
    {
        String query = "SELECT * FROM " + tableName + " WHERE data_col2=";

        switch (dataType.toLowerCase(Locale.ROOT)) {
            case "bigint":
                tableName = createTableBigInt(tableName);
                query += "5675354";
                break;
            case "boolean":
                tableName = createTableBoolean(tableName);
                query = query.substring(0, query.length() - 1);
                break;
            case "char":
                tableName = createTableChar(tableName);
                query += "'z'";
                break;
            case "date":
                tableName = createTableDate(tableName);
                query += "'2021-12-21'";
                break;
            case "decimal":
                tableName = createTableDecimal(tableName);
                query += "DECIMAL '21.21'";
                break;
            case "double":
                tableName = createTableDouble(tableName);
                query += "DOUBLE '21.21'";
                break;
            case "int":
                tableName = createTableInt(tableName);
                query += "606";
                break;
            case "real":
                tableName = createTableReal(tableName);
                query += "21.21";
                break;
            case "smallint":
                tableName = createTableSmallInt(tableName);
                query += "32767";
                break;
            case "string":
                tableName = createTableString(tableName);
                query += "";
                break;
            case "timestamp":
                tableName = createTableTimestamp(tableName);
                query += "";
                break;
            case "tinyint":
                tableName = createTableTinyInt(tableName);
                query += "0";
                break;
            case "varbinary":
                tableName = createTableVarBinary(tableName);
                query += "AAA";
                break;
            case "varchar":
                tableName = createTableVarChar(tableName);
                query += "'tester'";
                break;
            default:
                throw new InterruptedException("Not supported data type.");
        }
        String[] result = {tableName, query};
        return result;
    }

    private String createTableBigInt(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 BIGINT, data_col2 BIGINT, data_col3 BIGINT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 1, 1)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, 2, 2)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 3, 3)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 4, 4)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 5, 5)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2818475983711351641, 2818475983711351641, 2818475983711351641)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(9214215753243532641, 9214215753243532641, 9214215753243532641)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5675354, 5675354, 5675354)");
        return tableName;
    }

    private String createTableBoolean(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 BOOLEAN, data_col2 BOOLEAN, data_col3 BOOLEAN)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '0', BOOLEAN '0', BOOLEAN '0')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '0', BOOLEAN '0', BOOLEAN '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '0', BOOLEAN '1', BOOLEAN '0')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '0', BOOLEAN '1', BOOLEAN '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '1', BOOLEAN '0', BOOLEAN '0')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '1', BOOLEAN '0', BOOLEAN '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '1', BOOLEAN '1', BOOLEAN '0')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN '1', BOOLEAN '1', BOOLEAN '1')");
        return tableName;
    }

    private String createTableChar(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 CHAR(1), data_col2 CHAR(1), data_col3 CHAR(1))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('a', 'a', 'a')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('b', 'b', 'b')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('c', 'c', 'c')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('d', 'd', 'd')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('e', 'e', 'e')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('z', 'z', 'z')");
        return tableName;
    }

    private String createTableDate(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 DATE, data_col2 DATE, data_col3 DATE)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'1997-01-01', DATE'1997-01-01', DATE'1997-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'2021-02-09', DATE'2021-02-09', DATE'2021-02-09')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'2020-03-08', DATE'2020-03-08', DATE'2020-03-08')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'2022-04-07', DATE'2022-04-07', DATE'2022-04-07')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'1899-05-06', DATE'1997-05-06', DATE'1899-05-06')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE'2021-12-21', DATE'2021-12-21', DATE'2021-12-21')");
        return tableName;
    }

    private String createTableDecimal(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 DECIMAL(4,2), data_col2 DECIMAL(4,2), data_col3 DECIMAL(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '11.11', DECIMAL '11.11', DECIMAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '21.21', DECIMAL '21.21', DECIMAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '12.34', DECIMAL '43.21', DECIMAL '88.34')");
        return tableName;
    }

    private String createTableDouble(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 DOUBLE(4,2), data_col2 DOUBLE(4,2), data_col3 DOUBLE(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '11.11', DOUBLE '11.11', DOUBLE '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '21.21', DOUBLE '21.21', DOUBLE '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '12.34', DOUBLE '43.21', DOUBLE '88.34')");
        return tableName;
    }

    private String createTableInt(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 INT, data_col2 INT, data_col3 INT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 1, 1)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, 2, 2)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 3, 3)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 4, 4)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 5, 5)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(606, 606, 606)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(123, 123, 123)");
        return tableName;
    }

    private String createTableReal(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 REAL(4,2), data_col2 REAL(4,2), data_col3 REAL(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '11.11', REAL '11.11', REAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '21.21', REAL '21.21', REAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '12.34', REAL '43.21', REAL '88.34')");
        return tableName;
    }

    private String createTableSmallInt(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 SMALLINT, data_col2 SMALLINT, data_col3 SMALLINT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '1', SMALLINT '1', SMALLINT '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '2', SMALLINT '2', SMALLINT '2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '3', SMALLINT '3', SMALLINT '3')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '4', SMALLINT '4', SMALLINT '4')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '5', SMALLINT '5', SMALLINT '5')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '-32768', SMALLINT '32767', SMALLINT '0')");
        return tableName;
    }

    private String createTableString(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 STRING, data_col2 STRING, data_col3 STRING)", tableName);
        return tableName;
    }

    private String createTableTimestamp(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 TIMESTAMP, data_col2 TIMESTAMP, data_col3 TIMESTAMP)", tableName);
        return tableName;
    }

    private String createTableTinyInt(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 TINYINT, data_col2 TINYINT, data_col3 TINYINT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '1', TINYINT '1', TINYINT '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '2', TINYINT '2', TINYINT '2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '3', TINYINT '3', TINYINT '3')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '4', TINYINT '4', TINYINT '4')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '5', TINYINT '5', TINYINT '5')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '-127', TINYINT '0', TINYINT '127')");
        return tableName;
    }

    private String createTableVarBinary(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 VARBINARY(5), data_col2 VARBINARY(5), data_col3 VARBINARY(5))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('11111', '11111', '11111')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('22222', '22222', '22222')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('33333', '33333', '33333')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('44444', '44444', '44444')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('55555', '55555', '55555')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('12351', '12341', '15311')");
        return tableName;
    }

    private String createTableVarChar(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 VARCHAR(10), data_col2 VARCHAR(10), data_col3 VARCHAR(10))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('aaa', 'aaa', 'aaa')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('bbb', 'bbb', 'bbb')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('ccc', 'ccc', 'ccc')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('ddd', 'ddd', 'ddd')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('eee', 'eee', 'eee')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('tester', 'tester', 'tester')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('blah', 'blah', 'blah')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('nothing', 'nothing', 'nothing')");
        return tableName;
    }

    private String getNewTableName()
    {
        MaterializedResult result = computeActual("SHOW TABLES FROM hive.test");
        String tableName = "table" + ((Thread.currentThread().getStackTrace())[2]).getMethodName().toLowerCase(Locale.ROOT);
        int newIndex = 1;
        ArrayList<String> results = new ArrayList<>();
        for (MaterializedRow item : result.getMaterializedRows()) {
            results.add(item.getField(0).toString());
        }
        int size = result.getRowCount();
        for (int i = 0; i < size; i++) {
            if (results.contains(tableName + newIndex)) {
                newIndex++;
            }
            else {
                break;
            }
        }
        return "hive.test." + tableName + newIndex;
    }

    private String getNewIndexName()
    {
        MaterializedResult result = computeActual("SHOW INDEX");
        String indexName = "index" + ((Thread.currentThread().getStackTrace())[2]).getMethodName().toLowerCase(Locale.ROOT);
        int newIndex = 1;
        ArrayList<String> results = new ArrayList<>();
        for (MaterializedRow item : result.getMaterializedRows()) {
            results.add(item.getField(0).toString());
        }
        int size = result.getRowCount();
        for (int i = 0; i < size; i++) {
            if (results.contains(indexName + newIndex)) {
                newIndex++;
            }
            else {
                break;
            }
        }
        return indexName + newIndex;
    }

    // Get the split count and MaterializedResult in one pair to return.
    private Pair<Integer, MaterializedResult> getSplitAndMaterializedResult(String testerQuery)
    {
        String testerQueryID = "";
        int testerSplits = 0;

        // Select the entry with specifics
        MaterializedResult queryResult = computeActual(testerQuery);

        // Get queries executed and query ID
        MaterializedResult systemQueriesResult = computeActual("SELECT * FROM system.runtime.queries ORDER BY query_id DESC");
        for (MaterializedRow item : systemQueriesResult.getMaterializedRows()) {
            // Find query to match and get the query_id of that query for getting sum of splits later
            if (testerQueryID.equals("") && item.getField(4).toString().equals(testerQuery)) {
                testerQueryID = item.getField(0).toString();
            }
        }

        assertNotEquals(testerQueryID, "");

        // Select entries for tasks done using the previously retrieved query ID amd get sum of splits
        MaterializedResult splitsResult = computeActual("SELECT splits " +
                "FROM system.runtime.tasks AS t1, system.runtime.queries AS t2 " +
                "WHERE t1.query_id = t2.query_id " +
                "AND t2.query_id = '" + testerQueryID + "'AND t2.query = '" + testerQuery + "'");
        for (MaterializedRow item : splitsResult.getMaterializedRows()) {
            // Sum up the splits
            testerSplits += Integer.parseInt(item.getField(0).toString());
        }

        return new Pair<>(testerSplits, queryResult);
    }

    // Compare the two results are consistent.
    private static boolean verifyEqualResults(MaterializedResult result1, MaterializedResult result2)
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
