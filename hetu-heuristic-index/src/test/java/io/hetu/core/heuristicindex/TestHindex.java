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
import io.prestosql.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHindex
        extends TestIndexResources
{
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
}
