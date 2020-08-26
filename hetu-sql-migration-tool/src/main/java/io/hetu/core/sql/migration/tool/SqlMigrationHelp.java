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

public final class SqlMigrationHelp
{
    private SqlMigrationHelp() {}

    public static String getHelpText()
    {
        return "" +
                "This command line is to convert other sql syntax (e.g. Hive)to ANSI SQL:2003 syntax.\n" +
                "In thi cli, you can input the sql statement end with ';' and get the converted sql promptly.\n" +
                "Supported commands:\n" +
                "QUIT\n" +
                "EXIT\n" +
                "HISTORY    - show the history inputs.\n" +
                "!chtype typeValue;      - change the input sql syntax type of current session to typeValue. e.g. !chtype hive; change it to hive" +
                "\n" +
                "";
    }
}
