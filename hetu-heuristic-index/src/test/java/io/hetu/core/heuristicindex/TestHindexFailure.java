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
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.ParsingException;
import org.testng.annotations.Test;

public class TestHindexFailure
        extends TestIndexResources
{
    // Tests the case of failing to delete index because the name of index is wrong.
    // Wrong name cases are:
    // using catalog name, wrong catalog name, schema name, wrong schema name, table name, wrong table name, column name, wrong column name
    @Test(dataProvider = "indexTypes")
    public void testIndexDeletionWithWrongNames(String indexType)
            throws Exception
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        createTable1(tableName);

        // Create index
        safeCreateIndex("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

        assertContains("DROP INDEX hive", "Index 'hive' does not exist");
        assertContains("DROP INDEX HDFS", "Index 'hdfs' does not exist");
        assertContains("DROP INDEX TEST", "Index 'test' does not exist");
        assertContains("DROP INDEX wrongtest", "Index 'wrongtest' does not exist");
        String[] table = tableName.split("\\.");
        assertContains("DROP INDEX " + table[2], "Index '" + table[2] + "' does not exist");
        assertContains("DROP INDEX " + table[2] + "uncreated",
                "line 1:1: Index '" + table[2] + "uncreated' does not exist");
        assertContains("DROP INDEX id", "Index 'id' does not exist");
        assertContains("DROP INDEX wrongcolumn", "Index 'wrongcolumn' does not exist");
    }

    // Tests the case of failing to delete index because index is already deleted
    @Test(dataProvider = "indexTypes")
    public void testIndexDuplicateDeletion(String indexType)
            throws Exception
    {
        String tableName = getNewTableName();
        String indexName = getNewIndexName();
        createTable1(tableName);

        // Create index
        safeCreateIndex("CREATE INDEX " + indexName + " USING " +
                indexType + " ON " + tableName + " (id)");

        assertQuerySucceeds("DROP INDEX " + indexName);
        assertContains("DROP INDEX " + indexName,
                "Index '" + indexName + "' does not exist");
    }

    // Tests the case where more than one index is created on the same table and same column (Error case).
    @Test(dataProvider = "tableData3")
    public void testMultipleSameIndexCreation(String indexType, String queryVariable)
            throws Exception
    {
        String tableName = getNewTableName();
        createTable1(tableName);

        String indexName1 = getNewIndexName();
        safeCreateIndex("CREATE INDEX " + indexName1 + " USING " +
                indexType + " ON " + tableName + " (" + queryVariable + ")");

        String indexName2 = getNewIndexName();
        assertContains("CREATE INDEX " + indexName2 + " USING " +
                        indexType + " ON " + tableName + " (" + queryVariable + ")",
                "Index with same (table,column,indexType) already exists");
    }

    // Tests the case where the table at which the index is trying to be created is empty (Error case).
    @Test(dataProvider = "tableData3")
    public void testEmptyTableIndexCreation(String indexType, String queryVariable)
            throws IllegalStateException
    {
        String tableName = getNewTableName();
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + tableName + " (" + queryVariable + ")",
                "The table is empty. No index will be created.");
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
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "The table is empty. No index will be created.");
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
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "CREATE INDEX is not supported in catalog 'system'");
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
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "CREATE INDEX is not supported in catalog 'nonexisting'");
    }

    // Tests the case where index is trying to be created without schema name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithoutSchemaCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String tableName = getNewTableName();
        String wrongTableName = "hive.nonexistingschema." + tableName.substring(10);
        createEmptyTable(tableName);

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "Table '" + wrongTableName + "' is invalid");
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
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "Table '" + wrongTableName + "' is invalid");
    }

    // Tests the case where index is trying to be created without table name (Error case).
    @Test(dataProvider = "tableData3")
    public void testIndexWithoutTableCreation(String indexType, String queryVariable)
            throws SemanticException
    {
        String wrongTableName = "hive.test";

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "Table 'hive." + wrongTableName + "' is invalid");
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
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + wrongTableName + " (" + queryVariable + ")",
                "Table '" + wrongTableName + "' is invalid");
    }

    // Tests the case where index is trying to be created without column name (Error case).
    @Test(dataProvider = "indexTypes")
    public void testIndexWithoutColumnCreation(String indexType)
    {
        String tableName = getNewTableName();
        createTable1(tableName);
        // Error of "mismatched input ')'. Expecting: <identifier>" exists
        // But code style does not allow ) to exist inside a string without having ( before it.

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING " + indexType +
                " ON " + tableName + " ()", "mismatched input ')'");
    }

    // Tests the case where index is trying to be created with a wrong column name (Error case).
    @Test(dataProvider = "indexTypes")
    public void testIndexWithWrongColumnCreation(String indexType)
            throws SemanticException
    {
        String tableName = getNewTableName();
        createTable1(tableName);

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING " +
                        indexType + " ON " + tableName + " (wrong_column)",
                "Column 'wrong_column' cannot be resolved");
    }

    // Tests the case where index is trying to be created with a wrong filter name (Error case).
    @Test
    public void testIndexWithWrongFilterCreation()
            throws ParsingException
    {
        String tableName = getNewTableName();
        createTable1(tableName);

        String indexName = getNewIndexName();
        assertContains("CREATE INDEX " + indexName + " USING wrong_filter ON " + tableName + " (id)",
                "mismatched input 'wrong_filter'");
    }
}
