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
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIndexResources
        extends AbstractTestQueryFramework
{
    @SuppressWarnings("unused")
    public TestIndexResources()
    {
        super(HindexQueryRunner::createQueryRunner);
    }

    void safeCreateIndex(String sql)
            throws Exception
    {
        Exception e = null;
        for (int i = 0; i < 3; i++) {
            try {
                assertQuerySucceeds(sql);
                return;
            }
            catch (Exception newE) {
                e = newE;
            }
        }
        throw e;
    }

    void assertContains(String query, String contained)
    {
        try {
            assertQuerySucceeds(query);
            throw new AssertionError("Expected create index to fail.");
        }
        catch (AssertionError e) {
            assertTrue(e.getCause().toString().contains(contained));
        }
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

    @DataProvider(name = "bitmapOperatorInputRowsTest")
    public Object[][] bitmapOperatorInputRowsTest()
    {
        return new Object[][] {
                {"key = 5", ""}, {"key IN (1, 2)", ""}, {"key BETWEEN 3 AND 7", " WHERE key BETWEEN 2 AND 8"},
                {"key < 5", " WHERE key <= 5"}, {"key <= 5", " WHERE key <= 8"},
                {"key >= 5", " WHERE key >= 3"}, {"key > 5", " WHERE key >= 5"},
                {"key < 0", " WHERE key > -1"}, {"key <= -1", " WHERE key >= 0"},
                {"key >= 100", " WHERE key >= 3"}, {"key > 99", " WHERE key >= 5"},
                {"key <> 5", ""}, {"key NOT IN (1, 2)", ""}, {"key BETWEEN 7 AND 3", " WHERE key BETWEEN 2 AND 8"},
                {"key > 5 AND key < 5", ""}, {"key > 5 OR key < 5", ""}, {"key < 5 AND key > 5", ""}, {"key < 5 OR key > 5", ""},
                {"key = 5 AND key IN (5)", ""}, {"key <> 5 AND key IN (5)", ""}, {"key <> 5 OR key NOT IN (5)", ""},
                {"key <> 5 AND key NOT IN (5)", ""}, {"key = 5 AND key NOT IN (5)", ""}, {"key = 5 OR key IN (5)", ""},
                {"key BETWEEN 3 AND 5 OR key = 6", ""}, {"key BETWEEN 3 AND 5 AND key = 4", ""}};
    }

    @DataProvider(name = "queryOperatorTest")
    public Object[][] queryOperatorTest()
    {
        return new Object[][] {
                {"id = 3", "bitmap"}, {"id = 3", "bloom"}, {"id = 3", "btree"}, {"id = 3", "minmax"},
                {"id <> 3", "bitmap"}, {"id <> 3", "bloom"}, {"id <> 3", "btree"}, {"id <> 3", "minmax"},
                {"id < 3", "bitmap"}, {"id < 3", "bloom"}, {"id < 3", "btree"}, {"id < 3", "minmax"},
                {"id > 3", "bitmap"}, {"id > 3", "bloom"}, {"id > 3", "btree"}, {"id > 3", "minmax"},
                {"id <= 3", "bitmap"}, {"id <= 3", "bloom"}, {"id <= 3", "btree"}, {"id <= 3", "minmax"},
                {"id >= 3", "bitmap"}, {"id >= 3", "bloom"}, {"id >= 3", "btree"}, {"id >= 3", "minmax"},
                {"id IN (1, 2)", "bitmap"}, {"id IN (1, 2)", "bloom"}, {"id IN (1, 2)", "btree"}, {"id IN (1, 2)", "minmax"},
                {"id IN (3, 3)", "bitmap"}, {"id IN (3, 3)", "bloom"}, {"id IN (3, 3)", "btree"}, {"id IN (3, 3)", "minmax"},
                {"id NOT IN (1, 2)", "bitmap"}, {"id NOT IN (1, 2)", "bloom"}, {"id NOT IN (1, 2)", "btree"}, {"id NOT IN (1, 2)", "minmax"},
                {"id BETWEEN 3 AND 5", "bitmap"}, {"id BETWEEN 3 AND 5", "bloom"}, {"id BETWEEN 3 AND 5", "btree"}, {"id BETWEEN 3 AND 5", "minmax"},
                {"id BETWEEN 3 AND 3", "bitmap"}, {"id BETWEEN 3 AND 3", "bloom"}, {"id BETWEEN 3 AND 3", "btree"}, {"id BETWEEN 3 AND 3", "minmax"},
                {"id > 3 AND id < 3", "bitmap"}, {"id > 3 AND id < 3", "bloom"}, {"id > 3 AND id < 3", "btree"}, {"id > 3 AND id < 3", "minmax"},
                {"id > 3 OR id < 3", "bitmap"}, {"id > 3 OR id < 3", "bloom"}, {"id > 3 OR id < 3", "btree"}, {"id > 3 OR id < 3", "minmax"},
                {"id < 3 AND id > 3", "bitmap"}, {"id < 3 AND id > 3", "bloom"}, {"id < 3 AND id > 3", "btree"}, {"id < 3 AND id > 3", "minmax"},
                {"id < 3 OR id > 3", "bitmap"}, {"id < 3 OR id > 3", "bloom"}, {"id < 3 OR id > 3", "btree"}, {"id < 3 OR id > 3", "minmax"},
                {"id <> 3 OR id NOT IN (3)", "bitmap"}, {"id <> 3 OR id NOT IN (3)", "bloom"}, {"id <> 3 OR id NOT IN (3)", "btree"}, {"id <> 3 OR id NOT IN (3)", "minmax"},
                {"id <> 3 AND id NOT IN (3)", "bitmap"}, {"id <> 3 AND id NOT IN (3)", "bloom"}, {"id <> 3 AND id NOT IN (3)", "btree"}, {"id <> 3 AND id NOT IN (3)", "minmax"},
                {"id = 3 OR id IN (3)", "bitmap"}, {"id = 3 OR id IN (3)", "bloom"}, {"id = 3 OR id IN (3)", "btree"}, {"id = 3 OR id IN (3)", "minmax"},
                {"id = 3 AND id NOT IN (3)", "bitmap"}, {"id = 3 AND id NOT IN (3)", "bloom"}, {"id = 3 AND id NOT IN (3)", "btree"}, {"id = 3 AND id NOT IN (3)", "minmax"},
                {"id = 3 AND id IN (3)", "bitmap"}, {"id = 3 AND id IN (3)", "bloom"}, {"id = 3 AND id IN (3)", "btree"}, {"id = 3 AND id IN (3)", "minmax"},
                {"id <> 3 AND id IN (3)", "bitmap"}, {"id <> 3 AND id IN (3)", "bloom"}, {"id <> 3 AND id IN (3)", "btree"}, {"id <> 3 AND id IN (3)", "minmax"},
                {"id BETWEEN 1 AND 3 OR id = 5", "bitmap"}, {"id BETWEEN 1 AND 3 OR id = 5", "bloom"}, {"id BETWEEN 1 AND 3 OR id = 5", "btree"}, {"id BETWEEN 1 AND 3 OR id = 5", "minmax"},
                {"id BETWEEN 1 AND 3 AND id = 2", "bitmap"}, {"id BETWEEN 1 AND 3 AND id = 2", "bloom"}, {"id BETWEEN 1 AND 3 AND id = 2", "btree"}, {"id BETWEEN 1 AND 3 AND id = 2", "minmax"}};
    }

    void createEmptyTable(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))");
    }

    void createTable1(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (id INTEGER, name VARCHAR(10))");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 'test')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, '123'), (3, 'temp')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 'data'), (9, 'ttt'), (5, 'num')");
    }

    void createTable2(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (key BIGINT, status VARCHAR(7), price DECIMAL(5,2), date DATE)");
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
    }

    void createTableNullData(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (col1 INTEGER, col2 BIGINT, col3 TINYINT)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(111, NULL, NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, BIGINT '222', NULL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(NULL, NULL, TINYINT '33')");
    }

    void createBtreeTable1(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (key1 INT, key2 INT)" + " WITH (partitioned_by = ARRAY['key2'])");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
    }

    void createBtreeTableTransact1(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (key1 INT, key2 INT)" + " WITH (transactional = true, partitioned_by = ARRAY['key2'])");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(0, 10), (1, 11), (2, 12), (1, 11), (3, 13)," +
                " (1, 11), (2, 12), (4, 14), (3, 13), (5, 15), (6, 16)");
    }

    void createBtreeTableMultiPart1(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (key1 INT, key2 INT, key3 INT, key4 INT, key5 INT)" +
                " WITH (partitioned_by = ARRAY['key3', 'key4', 'key5'])");
        assertQuerySucceeds("INSERT INTO " + tableName +
                " VALUES(1, 11, 111, 1111, 11111), (2, 22, 222, 2222, 22222), (3, 33, 333, 3333, 33333)," +
                " (2, 22, 222, 2222, 22222), (5, 55, 555, 5555, 55555), (1, 11, 111, 1111, 11111)," +
                " (6, 66, 666, 6666, 66666), (7, 77, 777, 7777, 77777), (2, 22, 222, 2222, 22222), (2, 22, 222, 2222, 22222)");
    }

    @DataProvider(name = "splitsWithIndexAndData")
    public Object[][] splitsWithIndexAndData()
    {
        return new Object[][] {
                {"bitmap", "bigint"}, {"bloom", "bigint"}, {"minmax", "bigint"}, {"btree", "bigint"},
                {"bitmap", "boolean"}, {"bloom", "boolean"}, {"minmax", "boolean"},
                {"bitmap", "char"}, {"bloom", "char"}, {"minmax", "char"},
                {"bitmap", "date"}, {"bloom", "date"}, {"minmax", "date"}, {"btree", "date"},
                {"bitmap", "decimal"}, {"bloom", "decimal"}, {"minmax", "decimal"}, {"btree", "decimal"},
                {"bitmap", "double"}, {"bloom", "double"}, {"minmax", "double"}, {"btree", "double"},
                {"bitmap", "int"}, {"bloom", "int"}, {"minmax", "int"}, {"btree", "int"},
                {"bitmap", "real"}, {"bloom", "real"}, {"minmax", "real"}, {"btree", "real"},
                {"bitmap", "smallint"}, {"bloom", "smallint"}, {"minmax", "smallint"}, {"btree", "smallint"},
                {"bitmap", "tinyint"}, {"bloom", "tinyint"}, {"minmax", "tinyint"}, {"btree", "tinyint"},
                {"bitmap", "varchar"}, {"bloom", "varchar"}, {"minmax", "varchar"}, {"btree", "varchar"}};
    }

    String createTableDataTypeWithQuery(String tableName, String dataType)
            throws InterruptedException
    {
        String query = "SELECT * FROM " + tableName + " WHERE data_col2=";

        switch (dataType.toLowerCase(Locale.ROOT)) {
            case "bigint":
                createTableBigInt(tableName);
                query += "5675354";
                break;
            case "boolean":
                createTableBoolean(tableName);
                query += "BOOLEAN 'true'";
                break;
            case "char":
                createTableChar(tableName);
                query += "CHAR 'z'";
                break;
            case "date":
                createTableDate(tableName);
                query += "DATE '2021-12-21'";
                break;
            case "decimal":
                createTableDecimal(tableName);
                query += "DECIMAL '43.21'";
                break;
            case "double":
                createTableDouble(tableName);
                query += "DOUBLE '21.21'";
                break;
            case "int":
                createTableInt(tableName);
                query += "606";
                break;
            case "real":
                createTableReal(tableName);
                query += "REAL '21.21'";
                break;
            case "smallint":
                createTableSmallInt(tableName);
                query += "32767";
                break;
            case "tinyint":
                createTableTinyInt(tableName);
                query += "0";
                break;
            case "varchar":
                createTableVarChar(tableName);
                query += "'tester'";
                break;
            default:
                throw new InterruptedException("Not supported data type.");
        }
        return query;
    }

    void createTableBigInt(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 BIGINT, data_col2 BIGINT, data_col3 BIGINT)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 1, 1)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, 2, 2)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 3, 3)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 4, 4)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 5, 5)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2818475983711351641, 2818475983711351641, 2818475983711351641)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(9214215753243532641, 9214215753243532641, 9214215753243532641)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5675354, 5675354, 5675354)");
    }

    void createTableBoolean(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 BOOLEAN, data_col2 BOOLEAN, data_col3 BOOLEAN)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'false', BOOLEAN 'false', BOOLEAN 'false')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'false', BOOLEAN 'false', BOOLEAN 'true')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'false', BOOLEAN 'true', BOOLEAN 'false')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'false', BOOLEAN 'true', BOOLEAN 'true')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'true', BOOLEAN 'false', BOOLEAN 'false')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'true', BOOLEAN 'false', BOOLEAN 'true')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'true', BOOLEAN 'true', BOOLEAN 'false')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(BOOLEAN 'true', BOOLEAN 'true', BOOLEAN 'true')");
    }

    void createTableChar(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 CHAR, data_col2 CHAR, data_col3 CHAR)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'a', CHAR 'a', CHAR 'a')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'b', CHAR 'b', CHAR 'b')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'c', CHAR 'c', CHAR 'c')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'd', CHAR 'd', CHAR 'd')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'e', CHAR 'e', CHAR 'e')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(CHAR 'z', CHAR 'z', CHAR 'z')");
    }

    void createTableDate(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 DATE, data_col2 DATE, data_col3 DATE)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '1997-01-01', DATE '1997-01-01', DATE '1997-01-01')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '2021-02-09', DATE '2021-02-09', DATE '2021-02-09')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '2020-03-08', DATE '2020-03-08', DATE '2020-03-08')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '2022-04-07', DATE '2022-04-07', DATE '2022-04-07')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '1899-05-06', DATE '1997-05-06', DATE '1899-05-06')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DATE '2021-12-21', DATE '2021-12-21', DATE '2021-12-21')");
    }

    void createTableDecimal(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 DECIMAL(4,2), data_col2 DECIMAL(4,2), data_col3 DECIMAL(4,2))");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '11.11', DECIMAL '11.11', DECIMAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '21.21', DECIMAL '21.21', DECIMAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '12.34', DECIMAL '43.21', DECIMAL '88.34')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '12.11', DECIMAL '12.11', DECIMAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '13.21', DECIMAL '13.21', DECIMAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DECIMAL '34.45', DECIMAL '34.45', DECIMAL '34.45')");
    }

    void createTableDouble(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 DOUBLE, data_col2 DOUBLE, data_col3 DOUBLE)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '11.11', DOUBLE '11.11', DOUBLE '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '21.21', DOUBLE '21.21', DOUBLE '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(DOUBLE '12.34', DOUBLE '43.21', DOUBLE '88.34')");
    }

    void createTableInt(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 INT, data_col2 INT, data_col3 INT)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(1, 1, 1)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(2, 2, 2)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(3, 3, 3)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(4, 4, 4)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(5, 5, 5)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(606, 606, 606)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(123, 123, 123)");
    }

    void createTableReal(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 REAL, data_col2 REAL, data_col3 REAL)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '11.11', REAL '11.11', REAL '11.11')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '21.21', REAL '21.21', REAL '21.21')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(REAL '12.34', REAL '43.21', REAL '88.34')");
    }

    void createTableSmallInt(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 SMALLINT, data_col2 SMALLINT, data_col3 SMALLINT)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '1', SMALLINT '1', SMALLINT '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '2', SMALLINT '2', SMALLINT '2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '3', SMALLINT '3', SMALLINT '3')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '4', SMALLINT '4', SMALLINT '4')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '5', SMALLINT '5', SMALLINT '5')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(SMALLINT '-32768', SMALLINT '32767', SMALLINT '0')");
    }

    void createTableTinyInt(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 TINYINT, data_col2 TINYINT, data_col3 TINYINT)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '1', TINYINT '1', TINYINT '1')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '2', TINYINT '2', TINYINT '2')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '3', TINYINT '3', TINYINT '3')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '4', TINYINT '4', TINYINT '4')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '5', TINYINT '5', TINYINT '5')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES(TINYINT '-127', TINYINT '0', TINYINT '127')");
    }

    void createTableVarChar(String tableName)
    {
        assertQuerySucceeds("CREATE TABLE " + tableName + " (data_col1 VARCHAR, data_col2 VARCHAR, data_col3 VARCHAR)");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('aaa', 'aaa', 'aaa')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('bbb', 'bbb', 'bbb')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('ccc', 'ccc', 'ccc')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('ddd', 'ddd', 'ddd')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('eee', 'eee', 'eee')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('tester', 'tester', 'tester')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('blah', 'blah', 'blah')");
        assertQuerySucceeds("INSERT INTO " + tableName + " VALUES('nothing', 'nothing', 'nothing')");
    }

    static AtomicInteger count = new AtomicInteger(0);

    String getNewTableName()
    {
        return "hive.test.t" + Integer.valueOf(count.getAndIncrement()).toString();
    }

    String getNewIndexName()
    {
        return "idx" + Integer.valueOf(count.getAndIncrement()).toString();
    }

    // Get the split count and MaterializedResult in one pair to return.
    Pair<Integer, MaterializedResult> getSplitAndMaterializedResult(String testerQuery)
    {
        // Select the entry with specifics
        MaterializedResult queryResult = computeActual(testerQuery);

        String doublyQuotedQuery = testerQuery.replaceAll("'", "''");

        // Get queries executed and query ID to find the task with sum of splits
        String splits = "select sum(splits) from system.runtime.tasks where query_id in " +
                "(select query_id from system.runtime.queries " +
                "where query='" + doublyQuotedQuery + "' order by created desc limit 1)";

        MaterializedResult rows = computeActual(splits);

        assertEquals(rows.getRowCount(), 1);

        MaterializedRow materializedRow = rows.getMaterializedRows().get(0);
        int fieldCount = materializedRow.getFieldCount();
        assertEquals(fieldCount, 1,
                "Expected only one column, but got '%d', fiedlCount: " + fieldCount);
        Object value = materializedRow.getField(0);

        return new Pair<>((int) (long) value, queryResult);
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

    long getInputRowsOfLastQueryExecution(String sql)
    {
        String doublyQuotedQuery = sql.replaceAll("'", "''");

        String inputRowsSql = "select sum(raw_input_rows) from system.runtime.tasks where query_id in " +
                "(select query_id from system.runtime.queries where query='" + doublyQuotedQuery + "' order by created desc limit 1)";

        MaterializedResult rows = computeActual(inputRowsSql);

        assertEquals(rows.getRowCount(), 1);

        MaterializedRow materializedRow = rows.getMaterializedRows().get(0);
        int fieldCount = materializedRow.getFieldCount();
        assertEquals(fieldCount, 1,
                "Expected only one column, but got '%d', fiedlCount: " + fieldCount);
        Object value = materializedRow.getField(0);

        return (long) value;
    }
}
