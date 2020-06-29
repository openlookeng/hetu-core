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
package io.hetu.core.plugin.hana;

import static io.hetu.core.plugin.hana.TestingHanaServer.getActualTable;
import static java.lang.Integer.max;

public final class TestHanaSqlUtil
{
    private static final String[] keyWords = {"JOIN", "TABLE"};

    private TestHanaSqlUtil()
    {
    }

    public static String getHandledSql(String sql)
    {
        String sqlType = sql.trim().split(" ")[0].trim();
        String handledSql = handleKeyWordsInSql(sql);

        switch (sqlType) {
            case "WITH":
                handledSql = getHandledWithSql(handledSql);
                break;

            case "EXPLAIN":
                handledSql = getHandledExplainSql(handledSql);
                break;

            case "SHOW":
                handledSql = getHandledShowSql(handledSql);
                break;

            case "TABLE":
            case "(TABLE":
                handledSql = getHandledTableSql(handledSql);
                handledSql = handleFromInSql(handledSql);
                break;

            case "DESC":
                handledSql = getHandledDescSql(handledSql);
                break;

            case "CREATE":
                handledSql = getHandledCreateSql(handledSql);
                break;

            case "DELETE":
                handledSql = getHandledDeleteSql(handledSql);
                break;

            case "DROP":
                handledSql = getHandledDropSql(handledSql);
                break;

            case "PREPARE":
                handledSql = getHandledPrepareSql(handledSql);
                break;

            case "RENAME":
                handledSql = getHandledRenameSql(handledSql);
                break;

            default:
                /*
                 * SELECT
                 * (SELECT
                 * SELECT(SELECT
                 */
                if (sqlType.contains("SELECT")) {
                    handledSql = getHandledSelectSql(handledSql);
                }
                break;
        }

        return handleDotInSql(handledSql);
    }

    private static String handleKeyWordsInSql(String sql)
    {
        if (sql.contains("CREATE") || sql.contains("DROP") || sql.contains("ALTER")) {
            return sql;
        }

        String newSql = sql;
        for (String keyWord : keyWords) {
            newSql = handleKeyWordInSql(newSql, keyWord);
        }

        return newSql;
    }

    private static String handleKeyWordInSql(String sql, String keyWord)
    {
        if (!sql.contains(keyWord)) {
            return sql;
        }

        int beginIndex = sql.indexOf(keyWord) + keyWord.length() + 1;
        if (beginIndex > sql.length()) {
            return sql;
        }

        String table = sql.substring(beginIndex).split(" | \n")[0];
        if (!table.startsWith("(")) {
            return sql.substring(0, beginIndex) + getActualTable(table) + handleKeyWordInSql(sql.substring(beginIndex + table.length()), keyWord);
        }
        else {
            return sql.substring(0, beginIndex) + handleKeyWordInSql(sql.substring(beginIndex), keyWord);
        }
    }

    /* SELECT count(*) FROM customer WHERE NOT EXISTS(SELECT * FROM orders WHERE orders.custkey=customer.custkey)
     * SELECT orders.custkey, orders.orderkey FROM orders WHERE orders.custkey > orders.orderkey AND orders.custkey < 200.3
     * SELECT nationkey, a FROM nation, LATERAL (SELECT max(region.name) FROM region WHERE region.regionkey <=
     * nation.regionkey) t(a) ORDER BY nationkey LIMIT 1
     */
    private static String handleDotInSql(String sql)
    {
        int dotIndex = sql.indexOf(".");
        if (dotIndex == -1) {
            return sql;
        }

        int tableIndex1 = dotIndex - 1;
        while (tableIndex1 >= 0 && sql.charAt(tableIndex1) != ' ') {
            tableIndex1--;
        }
        tableIndex1++;

        int tableIndex2 = dotIndex - 1;
        while (tableIndex2 >= 0 && sql.charAt(tableIndex2) != '=') {
            tableIndex2--;
        }
        tableIndex2++;
        int tableIndex = max(tableIndex1, tableIndex2);

        int tableIndex3 = dotIndex - 1;
        while (tableIndex3 >= 0 && sql.charAt(tableIndex3) != '(') {
            tableIndex3--;
        }
        tableIndex3++;
        tableIndex = max(tableIndex, tableIndex3);

        String table = sql.substring(tableIndex, dotIndex);
        if (table.equalsIgnoreCase("tpch")) {  // exclude tables like tpch.tiny.orders in HanaQueryRunner
            return sql;
        }

        return sql.substring(0, tableIndex) + getActualTable(table) + "." + handleDotInSql(sql.substring(dotIndex + 1));
    }

    private static String getHandledShowSql(String sql)
    {
        int beginIndex = sql.indexOf("FROM");
        if (beginIndex == -1) {
            return sql;
        }
        beginIndex = beginIndex + "FROM".length() + 1;
        String table = sql.substring(beginIndex);
        return sql.substring(0, beginIndex) + getActualTable(table);
    }

    private static String getHandledTableSql(String sql)
    {
        String sqlType = sql.trim().split(" ")[0].trim();
        int beginIndex = sql.indexOf(sqlType) + sqlType.length() + 1;

        String table = sql.substring(beginIndex).split(" ")[0];
        if (table.contains(")")) {
            table = table.substring(0, table.length() - 1);
        }
        return sql.substring(0, beginIndex) + getActualTable(table) + handleKeyWordsInSql(sql.substring(beginIndex + table.length()));
    }

    private static String getHandledDescSql(String sql)
    {
        int beginIndex = sql.indexOf("DESC") + "DESC".length() + 1;
        String table = sql.substring(beginIndex);
        return sql.replace(table, getActualTable(table));
    }

    private static String getHandledExplainSql(String sql)
    {
        int beginIndex = sql.indexOf("SELECT");
        if (beginIndex == -1) {
            return sql;
        }

        return sql.substring(0, beginIndex) + handleFromInSql(sql.substring(beginIndex));
    }

    private static String getHandledWithSql(String sql)
    {
        int beginIndex = sql.indexOf("AS") + ("AS").length() + 2;
        int subEndIndex = sql.substring(beginIndex).indexOf(")") + beginIndex;
        return sql.substring(0, beginIndex) + handleFromInSql(sql.substring(beginIndex, subEndIndex)) + sql.substring(subEndIndex);
    }

    private static String getHandledCreateSql(String sql)
    {
        String createType = sql.trim().split(" | \n")[1];
        String createSql = sql;

        switch (createType) {
            case "TABLE":
                /*
                 * CREATE TABLE orders_* AS SELECT * FROM orders
                 * CREATE TABLE foo (pk bigint)
                 * CREATE TABLE foo AS SELECT FROM orders
                 * CREATE TABLE foo AS SELECT * FROM nation
                 * CREATE TABLE foo (x bigint)
                 * CREATE TABLE test (a integer, a integer)
                 */
                int tableIndex = sql.indexOf("CREATE TABLE") + "CREATE TABLE".length() + 1;
                String table = sql.substring(tableIndex).split(" ")[0].trim();
                if (table.contains("_")) {
                    break;
                }
            case "VIEW":
                /*
                 * CREATE VIEW foo AS SELECT * FROM orders
                 * CREATE VIEW foo AS SELECT * FROM nation
                 */
                int selectIndex = sql.indexOf("AS");
                if (selectIndex == -1) {
                    break;
                }
                selectIndex = selectIndex + "AS".length() + 1;
                createSql = sql.substring(0, selectIndex) + handleFromInSql(sql.substring(selectIndex));
                break;
            default:
                break;
        }

        return createSql;
    }

    private static String getHandledDeleteSql(String sql)
    {
        /* DELETE FROM orders */
        int tableStartIndex = sql.indexOf("DELETE FROM") + "DELETE FROM".length() + 1;
        String table = sql.substring(tableStartIndex).split(" ")[0].trim();
        int tableEndIndex = sql.indexOf(table) + table.length();
        return sql.substring(0, tableStartIndex) + getActualTable(table) + sql.substring(tableEndIndex);
    }

    private static String getHandledDropSql(String sql)
    {
        /*
         * DROP TABLE orders
         * DROP VIEW view
         * DROP SCHEMA foo
         * DROP TABLE foo
         * DROP VIEW foo
         */
        String dropType = sql.trim().split(" | \n")[1];
        if (!"TABLE".equals(dropType)) {
            return sql;
        }

        int tableStartIndex = sql.indexOf("DROP TABLE") + "DROP TABLE".length() + 1;
        String table = sql.substring(tableStartIndex).split(" ")[0].trim();
        int tableEndIndex = sql.indexOf(table) + table.length();
        return sql.substring(0, tableStartIndex) + getActualTable(table) + sql.substring(tableEndIndex);
    }

    private static String getHandledPrepareSql(String sql)
    {
        /*
         * PREPARE my_query FROM SELECT * FROM orders
         * PREPARE test FROM SELECT * FROM orders
         */
        int selectIndex = sql.indexOf("FROM");
        if (selectIndex == -1) {
            return sql;
        }

        selectIndex = selectIndex + "FROM".length() + 1;
        if (!sql.substring(selectIndex).contains("SELECT")) {
            return sql;
        }

        return sql.substring(0, selectIndex) + handleFromInSql(sql.substring(selectIndex));
    }

    private static String getHandledRenameSql(String sql)
    {
        /*
         * ALTER TABLE orders RENAME TO new_name
         * ALTER TABLE orders RENAME COLUMN orderkey TO new_column_name
         * ALTER SCHEMA foo RENAME TO bar
         * ALTER TABLE foo RENAME TO bar
         */
        String alterType = sql.trim().split(" | \n")[1];
        if (!"TABLE".equals(alterType)) {
            return sql;
        }

        int tableStartIndex = sql.indexOf("ALTER TABLE") + "ALTER TABLE".length() + 1;
        String table = sql.substring(tableStartIndex).split(" ")[0].trim();
        int tableEndIndex = sql.indexOf(table) + table.length();
        return sql.substring(0, tableStartIndex) + getActualTable(table) + sql.substring(tableEndIndex);
    }

    private static String getHandledSelectSql(String sql)
    {
        if (sql.indexOf("FROM") == -1) {  // SELECT 1
            return sql;
        }

        if (sql.indexOf("FROM") + "FROM".length() >= sql.length()) { // SELECT foo FROM
            return sql;
        }

        return handleFromInSql(sql);
    }

    private static String handleFromInSql(String sql)
    {
        int fromIndex = sql.indexOf("FROM");
        if (fromIndex == -1) {
            return sql;
        }

        int beginIndex = fromIndex + "FROM".length() + 1; // SELECT * FROM (SELECT * FROM orders)
        String subSql = sql.substring(beginIndex);
        if (subSql.trim().startsWith("(")) {
            return sql.substring(0, beginIndex) + handleFromInSql(subSql);
        }

        int tableEndIndex = getEndIndexForTable(subSql); // SELECT * FROM\n   orders;
        String table = sql.substring(beginIndex, beginIndex + tableEndIndex).trim();
        int tableStartIndex = subSql.indexOf(table);

        return sql.substring(0, beginIndex) + subSql.substring(0, tableStartIndex) + getActualTable(table)
            + handleFromInSql(sql.substring(beginIndex + tableEndIndex));
    }

    /*
     * tableName
     * tableName)
     * tableName,
     * tableName
     * \n tableName
     */
    private static int getEndIndexForTable(String selectSql)
    {
        String table = selectSql.trim().split(" |\n")[0];
        int spaceLength = selectSql.indexOf(table);

        if (table.contains(")")) {
            return table.indexOf(")") + spaceLength;
        }

        if (table.contains(",")) {
            return table.indexOf(",") + spaceLength;
        }

        return table.length() + spaceLength;
    }
}
