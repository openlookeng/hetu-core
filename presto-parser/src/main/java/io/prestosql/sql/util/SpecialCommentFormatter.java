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
package io.prestosql.sql.util;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SpecialCommentFormatter
{
    private static Map<String, String[]> queryMap = new HashMap<>();
    private static final String SPECIALREGEX = "^.*/\\*\\s*#distinct@\\s*(\\p{Alnum}+\\s*=\\s*(\\p{Alnum}+)" +
            "(\\s*,\\s*\\p{Alnum}+)*)*\\s*#\\s*\\*/.*$";

    private static Pattern specialCommentTemplate = Pattern.compile(SPECIALREGEX, Pattern.MULTILINE);

    private SpecialCommentFormatter()
    {}

    public static void verifyAndSetTableDistinctColumns(String sql)
    {
        Matcher matcher = specialCommentTemplate.matcher(sql);
        if (matcher.matches()) {
            insertIntoTableColumnMap(extractSpecialComment(sql));
        }
    }

    private static String extractSpecialComment(String sql)
    {
        return sql.substring(sql.indexOf("@") + 1, sql.lastIndexOf("#"));
    }

    private static void insertIntoTableColumnMap(String comment)
    {
        if (!queryMap.isEmpty()) {
            queryMap.clear();
        }
        comment = comment.replaceAll("\\s", "");
        String[] tableAndColumns = comment.split("=");
        String[] columns = tableAndColumns[1].split(",");
        queryMap.put(tableAndColumns[0], columns);
    }

    public static Map<String, String[]> getUniqueColumnTableMap()
    {
        return queryMap;
    }
}
