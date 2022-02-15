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

import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class TestHindexBitmapIndex
        extends TestIndexResources
{
    @Test(dataProvider = "bitmapOperatorInputRowsTest")
    public void testBitmapOperatorInputRows(String predicateQuery)
            throws Exception
    {
        System.out.println("Running testBitmapOperatorInputRows[predicateQuery: " + predicateQuery + "]");

        String tableName = getNewTableName();
        createTableBitmapSupportedDataTypes(tableName);
        String indexName = getNewIndexName();

        String tmpPredicateQuery = "SELECT * FROM " + tableName + " WHERE " + predicateQuery;

        assertQuerySucceeds("CREATE INDEX " + indexName + " USING bitmap ON " + tableName + " (p1)");

        MaterializedResult predicateQueryResultLoadingIndex = computeActual(tmpPredicateQuery);
        long predicateQueryInputRowsLoadingIndex = getInputRowsOfLastQueryExecution(tmpPredicateQuery);

        // Wait before continuing
        Thread.sleep(1000);

        MaterializedResult predicateQueryResultIndexLoaded = computeActual(tmpPredicateQuery);
        long predicateQueryInputRowsIndexLoaded = getInputRowsOfLastQueryExecution(tmpPredicateQuery);

        assertTrue(verifyEqualResults(predicateQueryResultLoadingIndex, predicateQueryResultIndexLoaded),
                "The results should be equal.");
        assertTrue(predicateQueryInputRowsLoadingIndex > predicateQueryInputRowsIndexLoaded ||
                        predicateQueryInputRowsIndexLoaded == 0,
                "Predicate query with index loaded should have the least input rows. " +
                        "predicateQueryInputRowsLoadingIndex: " + predicateQueryInputRowsLoadingIndex +
                        " predicateQueryInputRowsIndexLoaded: " + predicateQueryInputRowsIndexLoaded);
    }

    @Test(dataProvider = "bitmapSupportedDataTypesBetweenValues")
    public void testBitmapDataTypesBetweenValues(String column, String queryCondition)
            throws Exception
    {
        System.out.println("Running testBitmapDataTypesBetweenValues[queryCondition: " + column + " " + queryCondition + "]");

        String indexType = "bitmap";
        String tableName = getNewTableName();
        createTableBitmapSupportedDataTypes(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + column + " " + queryCondition;

        // Create index
        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType + " ON " + tableName + " (" + column + ")");

        MaterializedResult resultLoadingIndex = getSplitAndMaterializedResult(testerQuery).getSecond();
        long inputRowsLoadingIndex = getInputRowsOfLastQueryExecution(testerQuery);

        // Wait before continuing
        Thread.sleep(1000);

        MaterializedResult resultIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery).getSecond();
        long inputRowsIndexLoaded = getInputRowsOfLastQueryExecution(testerQuery);

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal for" +
                " index type: " + indexType + ", condition: " + queryCondition);
        assertTrue(inputRowsLoadingIndex > inputRowsIndexLoaded,
                "The numbers of input rows should decrease after index loaded:" +
                        " index type: " + indexType + " condition: " + queryCondition +
                        " inputRowsLoadingIndex: " + inputRowsLoadingIndex +
                        " inputRowsIndexLoaded: " + inputRowsIndexLoaded);
    }

    @Test(dataProvider = "bitmapSupportedDataTypesRangedValues")
    public void testBitmapSupportedDataTypesRangedValues(String column, String queryCondition)
            throws Exception
    {
        System.out.println("Running testBitmapSupportedDataTypesRangedValues[queryCondition: " + column + " " + queryCondition + "]");

        String tableName = getNewTableName();
        createTableSupportedRangedTypes(tableName);
        String testerQuery = "SELECT * FROM " + tableName + " WHERE " + column + " " + queryCondition;
        String indexType = "bitmap";

        // Create index
        String indexName = getNewIndexName();
        assertQuerySucceeds("CREATE INDEX " + indexName + " USING " + indexType + " ON " + tableName + " (" + column + ")");

        MaterializedResult resultLoadingIndex = getSplitAndMaterializedResult(testerQuery).getSecond();
        long inputRowsLoadingIndex = getInputRowsOfLastQueryExecution(testerQuery);

        // Wait before continuing
        Thread.sleep(1000);

        MaterializedResult resultIndexLoaded = runTwiceGetSplitAndMaterializedResult(testerQuery).getSecond();
        long inputRowsIndexLoaded = getInputRowsOfLastQueryExecution(testerQuery);

        assertTrue(verifyEqualResults(resultLoadingIndex, resultIndexLoaded), "The results should be equal for" +
                " index type: " + indexType + ", condition: " + queryCondition);
        assertTrue(inputRowsLoadingIndex > inputRowsIndexLoaded,
                "The numbers of input rows should decrease after index is loaded:" +
                        " index type: " + indexType + " condition: " + queryCondition +
                        " inputRowsLoadingIndex: " + inputRowsLoadingIndex +
                        " inputRowsIndexLoaded: " + inputRowsIndexLoaded);
    }
}
