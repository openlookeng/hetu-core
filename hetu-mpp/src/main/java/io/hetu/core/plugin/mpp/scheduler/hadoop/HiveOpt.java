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
package io.hetu.core.plugin.mpp.scheduler.hadoop;

import io.airlift.log.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveOpt
{
    public static Logger logger = Logger.get(HiveOpt.class);

    private HiveOpt()
    {
    }

    public static void createExternalTable(String hiveUrl, String hiveUser, String hivePasswd, String hsqlDrop, String hsqlCreate,
                                           String tblName, String schemaInfo, String auxPath)
    {
        logger.info(tblName + ": Hive External Table has not existed, create it!");
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection connection = DriverManager.getConnection(hiveUrl, hiveUser, hivePasswd);
            Statement statement = connection.createStatement();
            String sqlDrop = hsqlDrop.replace("${table_name}", tblName);
            String sqlCreate = hsqlCreate
                    .replace("${table_name}", tblName)
                    .replace("${schema_info}", schemaInfo)
                    .replace("${pipe_to_aux_base_path}", auxPath);
            statement.execute(sqlDrop);
            statement.execute(sqlCreate);
            logger.info("Finished create hive foreign table");
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
