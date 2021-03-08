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

import io.prestosql.testing.MaterializedResult;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class TestHindexBitmapIndex
        extends TestIndexResources
{
    @Test(dataProvider = "bitmapOperatorInputRowsTest")
    public void testBitmapOperatorInputRows(String predicateQuery, String baseQuery)
            throws Exception
    {
        String tableName = getNewTableName();
        createTable2(tableName);
        String indexName = getNewIndexName();

        // baseQuery is the query at which all, most or at least more selected than predicateQuery
        // baseQuery does not use indexing, predicateQuery does
        // predicateQuery should have less input rows than baseQuery
        baseQuery = "SELECT * FROM " + tableName + baseQuery;
        predicateQuery = "SELECT * FROM " + tableName + " WHERE " + predicateQuery;

        MaterializedResult baseQueryResult = computeActual(baseQuery);
        long baseQueryInputRows = getInputRowsOfLastQueryExecution(baseQuery);

        safeCreateIndex("CREATE INDEX " + indexName + " USING bitmap ON " + tableName + " (key)");

        MaterializedResult predicateQueryResultLoadingIndex = computeActual(predicateQuery);

        // Wait before continuing
        Thread.sleep(1000);

        MaterializedResult predicateQueryResultIndexLoaded = computeActual(predicateQuery);
        long predicateQueryInputRowsIndexLoaded = getInputRowsOfLastQueryExecution(predicateQuery);

        assertTrue(verifyEqualResults(predicateQueryResultLoadingIndex, predicateQueryResultIndexLoaded),
                "The results should be equal.");
        assertTrue(baseQueryResult.getRowCount() > predicateQueryResultIndexLoaded.getRowCount(),
                "Predicate query with index loaded should have less results than base query. " +
                        "baseQueryResult row count: " + baseQueryResult.getRowCount() +
                        " predicateQueryResultIndexLoaded row count: " + predicateQueryResultIndexLoaded.getRowCount());
        assertTrue(baseQueryInputRows > predicateQueryInputRowsIndexLoaded,
                "Predicate query with index loaded should have less input rows than base query. " +
                        "baseQueryInputRows: " + baseQueryInputRows +
                        " predicateQueryInputRowsIndexLoaded: " + predicateQueryInputRowsIndexLoaded);
    }
}
