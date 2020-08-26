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
package io.hetu.core.sql.migration.tool;

import io.hetu.core.migration.source.hive.HiveSqlParser;
import io.hetu.core.sql.migration.parser.HiveParser;
import io.prestosql.sql.parser.ParsingOptions;
import org.codehaus.jettison.json.JSONObject;

public class HiveSqlConverter
        extends SqlSyntaxConverter
{
    private HiveParser hiveSqlParser;
    private ParsingOptions parsingOptions;
    private static HiveSqlConverter hiveSqlConverter;

    private HiveSqlConverter(ParsingOptions parsingOptions)
    {
        this.hiveSqlParser = new HiveParser();
        this.parsingOptions = parsingOptions;
    }

    public static synchronized HiveSqlConverter getHiveSqlConverter(ParsingOptions parsingOptions)
    {
        if (hiveSqlConverter == null) {
            hiveSqlConverter = new HiveSqlConverter(parsingOptions);
        }

        return hiveSqlConverter;
    }

    @Override
    public JSONObject convert(String sql)
    {
        return hiveSqlParser.invokeParser(sql, HiveSqlParser::singleStatement, parsingOptions);
    }
}
