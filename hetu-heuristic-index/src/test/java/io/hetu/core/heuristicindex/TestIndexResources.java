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
import io.prestosql.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Locale;

import static org.testng.Assert.assertNotEquals;

public class TestIndexResources
        extends AbstractTestQueryFramework
{
    @SuppressWarnings("unused")
    public TestIndexResources()
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

    @DataProvider(name = "tableData1")
    public Object[][] tableData1()
    {
        return new Object[][] {{"bitmap", "id", "2"}, {"bloom", "id", "2"}, {"minmax", "id", "2"}, {"btree", "id", "2"}};
    }

    @DataProvider(name = "tableData2")
    public Object[][] tableData2()
    {
        return new Object[][] {{"bitmap", "key", "2"}, {"bloom", "key", "2"}, {"minmax", "key", "2"}, {"btree", "key", "2"}};
    }

    @DataProvider(name = "btreeTable1Operators")
    public Object[][] btreeTable1Operators()
    {
        return new Object[][] {{"key2 = 11"}, {"key2 > 12"}, {"key2 >= 12"}, {"key2 < 15"}, {"key2 <= 12"},
                {"key2 BETWEEN 12 AND 15"}, {"key2 IN (11, 12)"}, {"key2 = 11 or 12"}};
    }

    @DataProvider(name = "nullDataHandling")
    public Object[][] nullDataHandling()
    {
        return new Object[][] {{"bitmap", "col1", "111"}, {"bitmap", "col2", "222"}, {"bitmap", "col3", "33"},
                {"bloom", "col1", "111"}, {"bloom", "col2", "222"}, {"bloom", "col3", "33"},
                {"minmax", "col1", "111"}, {"minmax", "col2", "222"}, {"minmax", "col3", "33"},
                {"btree", "col1", "111"}, {"btree", "col2", "222"}, {"btree", "col3", "33"}};
    }

    @DataProvider(name = "tableData3")
    public Object[][] tableData3()
    {
        return new Object[][] {{"bitmap", "id"}, {"bloom", "id"}, {"minmax", "id"}, {"btree", "id"}};
    }

    @DataProvider(name = "indexTypes")
    public Object[][] indexTypes()
    {
        return new Object[][] {{"BITMAP"}, {"BLOOM"}, {"MINMAX"}, {"BTREE"}};
    }

    String safeCreateTable(String query, String tableName)
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

    String createEmptyTable(String tableName)
    {
        return safeCreateTable("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))", tableName);
    }

    String createTable1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");
        return tableName;
    }

    String createTable2(String tableName)
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

    String createTableNullData(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName + " (col1 INTEGER, col2 BIGINT, col3 TINYINT)", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(111, NULL, NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, BIGINT '222', NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, NULL, TINYINT '33')");
        return tableName;
    }

    String createBtreeTable1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT)" +
                " WITH (partitioned_by = ARRAY['key2'])", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
        return tableName;
    }

    String createBtreeTableTransact1(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (key1 INT, key2 INT)" +
                " WITH (transactional = true, partitioned_by = ARRAY['key2'])", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
        return tableName;
    }

    String createBtreeTableMultiPart1(String tableName)
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

    String[] createTableDataTypeWithQuery(String tableName, String dataType)
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

    String createTableBigInt(String tableName)
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

    String createTableBoolean(String tableName)
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

    String createTableChar(String tableName)
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

    String createTableDate(String tableName)
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

    String createTableDecimal(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 DECIMAL(4,2), data_col2 DECIMAL(4,2), data_col3 DECIMAL(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '11.11', DECIMAL '11.11', DECIMAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '21.21', DECIMAL '21.21', DECIMAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '12.34', DECIMAL '43.21', DECIMAL '88.34')");
        return tableName;
    }

    String createTableDouble(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 DOUBLE(4,2), data_col2 DOUBLE(4,2), data_col3 DOUBLE(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '11.11', DOUBLE '11.11', DOUBLE '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '21.21', DOUBLE '21.21', DOUBLE '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '12.34', DOUBLE '43.21', DOUBLE '88.34')");
        return tableName;
    }

    String createTableInt(String tableName)
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

    String createTableReal(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 REAL(4,2), data_col2 REAL(4,2), data_col3 REAL(4,2))", tableName);
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '11.11', REAL '11.11', REAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '21.21', REAL '21.21', REAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '12.34', REAL '43.21', REAL '88.34')");
        return tableName;
    }

    String createTableSmallInt(String tableName)
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

    String createTableString(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 STRING, data_col2 STRING, data_col3 STRING)", tableName);
        return tableName;
    }

    String createTableTimestamp(String tableName)
    {
        tableName = safeCreateTable("CREATE TABLE " + tableName +
                " (data_col1 TIMESTAMP, data_col2 TIMESTAMP, data_col3 TIMESTAMP)", tableName);
        return tableName;
    }

    String createTableTinyInt(String tableName)
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

    String createTableVarBinary(String tableName)
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

    String createTableVarChar(String tableName)
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

    String getNewTableName()
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

    String getNewIndexName()
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
    Pair<Integer, MaterializedResult> getSplitAndMaterializedResult(String testerQuery)
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
