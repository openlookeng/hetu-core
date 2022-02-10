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

import io.airlift.units.DataSize;
import io.prestosql.spi.heuristicindex.Pair;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.MaterializedRow;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHindex
        extends TestIndexResources
{
    private static final long INDEX_LOAD_WAIT_TIME = 600;

    // Tests the supported data types with the different index types while queries utilize the BETWEEN operator.
    // Tests omit BitmapIndex because BitmapIndex's row filtering should have combined inserts, it is tested separately
    @Test(dataProvider = "supportedDataTypesBetweenValues")
    public void testDataTypesBetweenValues(String indexType, String column, String queryCondition)
            throws Exception
    {
        System.out.println("Running testDataTypesBetweenValues[indexType: " + indexType +
                ", column: " + column + ", queryCondition: " + queryCondition + "]");

        String tableName = getNewTableName();
        createTableSupportedDataTypes(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + column + " " + queryCondition;

        // Create index
        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType + " ON " + tableName + " (" + column + ")");

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal for" +
                " index type: " + indexType + ", condition: " + queryCondition);
        assertTrue(splitsLoadingIndex > splitsIndexLoaded,
                "The splits with index loaded should be lower than splits before index:" +
                        " index type: " + indexType + " condition: " + queryCondition +
                        " splitsLoadingIndex: " + splitsLoadingIndex +
                        " splitsIndexLoaded: " + splitsIndexLoaded);
    }

    // Tests to see the difference in number of splits used without and with the usage of index with specified data.
    // This test case is a basic test case for BitmapIndex, since the table is not insertion combined, this test
    // only compares BitmapIndex's input row counts with base query. Separate tests are implemented to further test BitmapIndex.
    @Test(dataProvider = "splitsWithIndexAndData")
    public void testSplitsWithIndexAndData(String indexType, String dataType)
            throws Exception
    {
        System.out.println("Running testSplitsWithIndexAndData[indexType: " + indexType +
                ", dataType: " + dataType + "]");

        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        String testerQuery = createTableDataTypeWithQuery(tableName, dataType);
        String baseQuery = "SELECT * FROM " + tableName;

        // Get splits and result
        assertQuerySucceeds(baseQuery);
        long inputRowsBaseQuery = getInputRowsOfLastQueryExecution(baseQuery);

        // Create index
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (data_col2)");

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();
        long inputRowsLoadingIndex = getInputRowsOfLastQueryExecution(testerQuery);

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();
        long inputRowsIndexLoaded = getInputRowsOfLastQueryExecution(testerQuery);

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal for" +
                " index type: " + indexType + ", data type: " + dataType);
        if (indexType.toLowerCase(Locale.ROOT).equals("bitmap")) {
            assertTrue(inputRowsBaseQuery > inputRowsIndexLoaded,
                    "The numbers of input rows should decrease after index loaded:" +
                            " index type: " + indexType + " data type: " + dataType +
                            " inputRowsBaseQuery: " + inputRowsBaseQuery +
                            " inputRowsLoadingIndex: " + inputRowsLoadingIndex +
                            " inputRowsIndexLoaded: " + inputRowsIndexLoaded);
        }
        else {
            assertTrue(splitsLoadingIndex > splitsIndexLoaded,
                    "The splits with index loaded should be lower than splits before index:" +
                            " index type: " + indexType + " data type: " + dataType +
                            " splitsLoadingIndex: " + splitsLoadingIndex +
                            " splitsIndexLoaded: " + splitsIndexLoaded);
        }
    }

    @Test(dataProvider = "tableData1")
    public void testIndexAutoloadCache(String indexType, String queryVariable, String queryValue)
            throws Exception
    {
        System.out.println("Running testIndexAutoloadUpdateCache[indexType: " + indexType +
                ", queryVariable: " + queryVariable + ", queryValue: " + queryValue + "]");

        String tableName = getNewTableName();
        createTable1(tableName);
        String indexName = getNewIndexName();
        long threadRefreshRate = 5000; // this is the rate that background thread gets executed

        // original split num
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        int splitsBeforeIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        //create index
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (" + queryVariable + ")" + " WITH (autoload = true)");

        Thread.sleep(threadRefreshRate);
        // split num after using index
        int splitsAfterIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        //update index
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'new1'), (8, 'new2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");
        assertQuerySucceeds("UPDATE INDEX " + indexName);
        Thread.sleep(threadRefreshRate);
        //split num after updating data and autoloading index
        int splitsAfterIndexUpdate = getSplitAndMaterializedResult(testerQuery).getFirst();

        //drop index
        assertQuerySucceeds("DROP INDEX " + indexName);
        Thread.sleep(threadRefreshRate);
        int splitsDropIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        assertTrue(splitsBeforeIndex > splitsAfterIndex, "splits should be fewer after index creation");
        assertTrue(splitsBeforeIndex > splitsAfterIndexUpdate, "splits should be more after adding more data");
        assertTrue(splitsBeforeIndex < splitsDropIndex, "splits be more even without any index after dropping index due to data addition");
        assertTrue(splitsAfterIndex < splitsAfterIndexUpdate, "splits should be more after adding data and update index");
        assertTrue(splitsAfterIndex < splitsDropIndex, "splits should be more anyway after dropping index because data volume increased");
        assertTrue(splitsAfterIndexUpdate < splitsDropIndex, "the number of splits after dropping index is the largest");
    }

    // Tests data consistency and splits for which table data is changed after index creation.
    @Test(dataProvider = "tableData1")
    public void testDataConsistencyWithAdditionChange(String indexType, String queryVariable, String queryValue)
            throws Exception
    {
        System.out.println("Running testDataConsistencyWithAdditionChange[indexType: " + indexType +
                ", queryVariable: " + queryVariable + ", queryValue: " + queryValue + "]");

        String tableName = getNewTableName();
        createTable1(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        int splitsBeforeIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Create index to use for testing splits
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (" + queryVariable + ")");

        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'new1'), (8, 'new2')");

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
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
            throws Exception
    {
        System.out.println("Running testDataConsistencyWithDataDeletionChange[indexType: " + indexType +
                ", queryVariable: " + queryVariable + ", queryValue: " + queryValue + "]");

        String tableName = getNewTableName();
        createTable2(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        int splitsBeforeIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Create index to use for testing splits
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (" + queryVariable + ")");

        Pair<Integer, MaterializedResult> resultPairLoadingIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsLoadingIndex = resultPairLoadingIndex.getFirst();
        MaterializedResult resultLoadingIndex = resultPairLoadingIndex.getSecond();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
        int splitsIndexLoaded = resultPairIndexLoaded.getFirst();
        MaterializedResult resultIndexLoaded = resultPairIndexLoaded.getSecond();

        assertQuerySucceeds("DELETE FROM " + tableName);

        Pair<Integer, MaterializedResult> resultPairDeletingData = getSplitAndMaterializedResult(testerQuery);
        int splitsDeletingData = resultPairDeletingData.getFirst();
        MaterializedResult resultDeletingData = resultPairDeletingData.getSecond();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

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
            throws Exception
    {
        System.out.println("Running testNullDataHandling[indexType: " + indexType +
                ", queryVariable: " + queryVariable + ", queryValue: " + queryValue + "]");

        String tableName = getNewTableName();
        createTableNullData(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + queryVariable + "=" + queryValue;
        String indexName = getNewIndexName();

        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create index to use for testing splits
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (" + queryVariable + ")");

        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
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
            throws Exception
    {
        String tableName = getNewTableName();
        String testerQuery = "SELECT * FROM " + tableName + " WHERE id = 2";
        createTable1(tableName);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create indices
        String indexName1 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING btree ON " + tableName + " (id)");
        String indexName2 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING bitmap ON " + tableName + " (id)");
        String indexName3 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName3 + " USING bloom ON " + tableName + " (id)");
        String indexName4 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName4 + " USING minmax ON " + tableName + " (id)");

        // Get splits and result
        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
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
            throws Exception
    {
        System.out.println("Running testIndexDeletionBeforeSplitsAffected[indexType: " + indexType + "]");

        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        String testerQuery = "SELECT * FROM " + tableName + " WHERE id = 2";
        createTable1(tableName);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairBeforeIndex = getSplitAndMaterializedResult(testerQuery);
        int splitsBeforeIndex = resultPairBeforeIndex.getFirst();
        MaterializedResult resultBeforeIndex = resultPairBeforeIndex.getSecond();

        // Create index
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

        // Drop index before changes
        assertQuerySucceeds("DROP INDEX " + indexName);

        // Get splits and result
        int splitsLoadingIndex = getSplitAndMaterializedResult(testerQuery).getFirst();

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        // Get splits and result
        Pair<Integer, MaterializedResult> resultPairIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery);
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
        System.out.println("Running testIndexIfNotExistsCreation[indexType: " + indexType + "]");

        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        createTable1(tableName);

        // Create index
        assertQuerySucceeds("CREATE INDEX IF NOT EXISTS " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

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
        System.out.println("Running testIndexWithCapitalColumnNameCreation[indexType: " + indexType + "]");

        String tableName = getNewTableName();
        assertQuerySucceeds("CREATE TABLE " + tableName + " (P1 INTEGER, P2 VARCHAR(10))");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");

        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType + " ON " + tableName + " (P1)");
    }

    @Test(dataProvider = "queryOperatorTest")
    public void testQueryOperator(String testerQuery, String indexType)
            throws Exception
    {
        System.out.println("Running testIndexWithCapitalColumnNameCreation[testerQuery: " + testerQuery +
                ", indexType: " + indexType + "]");

        String tableName = getNewTableName();
        createTable1(tableName);
        String tmpTesterQuery = "SELECT * FROM " + tableName + " WHERE " + testerQuery;
        String indexName = getNewIndexName();

        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

        MaterializedResult resultLoadingIndex = computeActual(tmpTesterQuery);

        // Wait before continuing
        Thread.sleep(INDEX_LOAD_WAIT_TIME);

        MaterializedResult resultIndexLoaded = computeActual(tmpTesterQuery);

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded),
                "The results should be equal for " + tmpTesterQuery + " " + indexType);
    }

    @Test
    public void testIndicesDroppedWithTable()
    {
        String tableName = getNewTableName();
        createTable1(tableName);

        String testerQuery = "SHOW INDEX";
        Pair<Integer, MaterializedResult> resultPairIndexCreation = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation = resultPairIndexCreation.getSecond();
        int initialSize = resultIndexCreation.getRowCount();

        String indexName1 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName1 + " USING btree ON " + tableName + " (id)");
        String indexName2 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName2 + " USING bitmap ON " + tableName + " (id)");
        String indexName3 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName3 + " USING bloom ON " + tableName + " (id)");
        String indexName4 = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName4 + " USING minmax ON " + tableName + " (id)");

        Pair<Integer, MaterializedResult> resultPairIndexCreation1 = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation1 = resultPairIndexCreation1.getSecond();
        int size = resultIndexCreation1.getRowCount();
        assertEquals(size - initialSize, 4);

        assertQuerySucceeds("DROP TABLE " + tableName);

        Pair<Integer, MaterializedResult> resultPairIndexCreation2 = getSplitAndMaterializedResult(testerQuery);
        MaterializedResult resultIndexCreation2 = resultPairIndexCreation2.getSecond();
        size = resultIndexCreation2.getRowCount();
        assertEquals(size - initialSize, 0);
    }

    @Test
    public void testShowIndexSize()
    {
        System.out.println("Running testShowIndexSize");

        String tableName = getNewTableName();
        String indexType = "bloom";
        String indexName = getNewIndexName();
        createTable1(tableName);

        // Create index
        assertQuerySucceeds("CREATE INDEX IF NOT EXISTS " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

        // Get `SHOW INDEX` query output of this index
        String testerQuery = "SHOW INDEX " + indexName;
        MaterializedResult result = getSplitAndMaterializedResult(testerQuery).getSecond();
        List<Object> resultFields = result.getMaterializedRows().get(0).getFields();

        // the index of size in the `SHOW INDEX` output
        // `sizeLocation` may need to be changed if new fields are added to the output
        int sizeLocation = 5;
        String indexSizeStr = (String) resultFields.get(sizeLocation);
        DataSize indexSize = DataSize.valueOf(indexSizeStr);
        assertTrue(indexSize.toBytes() != 0);

        //update index
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(7, 'new1'), (8, 'new2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");
        assertQuerySucceeds("UPDATE INDEX " + indexName);

        // Get `SHOW INDEX` query output of this index after UPDATE INDEX
        MaterializedResult newResult = getSplitAndMaterializedResult(testerQuery).getSecond();
        List<Object> newResultFields = newResult.getMaterializedRows().get(0).getFields();

        String newIndexSizeStr = (String) newResultFields.get(sizeLocation);
        DataSize newIndexSize = DataSize.valueOf(newIndexSizeStr);
        assertTrue(newIndexSize.toBytes() != 0);
        assertTrue(newIndexSize.toBytes() > indexSize.toBytes());
    }
}
