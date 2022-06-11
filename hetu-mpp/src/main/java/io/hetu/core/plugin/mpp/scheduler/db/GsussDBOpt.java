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
package io.hetu.core.plugin.mpp.scheduler.db;

import io.airlift.log.Logger;
import io.hetu.core.plugin.mpp.MppConfig;
import io.hetu.core.plugin.mpp.scheduler.entity.TableSchema;
import io.hetu.core.plugin.mpp.scheduler.utils.Const;
import io.hetu.core.plugin.mpp.scheduler.utils.Util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chengyijian
 * @title: GsussDBOpt
 * @projectName mpp-scheduler
 * @description: GaussDB操作相关
 * @date 2021/8/1210:56
 */

public class GsussDBOpt
{
    public static Logger logger = Logger.get(GsussDBOpt.class);

    private GsussDBOpt()
    {
    }

    public static Connection getConnection(String driver, String url, String username, String passwd)
    {
        Connection conn = null;
        try {
            Class.forName(driver).getConstructor().newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            conn = DriverManager.getConnection(url, username, passwd);
            logger.info("GaussDB Connection succeed!");
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return conn;
    }

    public static void executeSql(Connection conn, String sql)
    {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
            boolean rc = stmt.execute(sql);
            stmt.close();
        }
        catch (SQLException e) {
            if (stmt != null) {
                try {
                    stmt.close();
                }
                catch (SQLException e1) {
                    e1.printStackTrace();
                }
            }
            e.printStackTrace();
        }
    }

    public static Map<String, String> getSchemas(MppConfig mppConfig, String catalog, String schema, String tableName)
    {
        String tblIdentifier = catalog + "." + schema + "." + tableName;

        Map<String, String> schemas = new HashMap<>();

        if (Const.schemasMap.containsKey(tblIdentifier)) {
            TableSchema tableSchema = Const.schemasMap.get(tblIdentifier);
            logger.info(tblIdentifier + " schema has stored at " + tableSchema.getSchemaTime());
            schemas.put("columns", tableSchema.getColumns());
            schemas.put("gsSchema", tableSchema.getGsSchema());
            schemas.put("hiveSchema", tableSchema.getHiveSchema());
            return schemas;
        }
        else {
            logger.info(tblIdentifier + " schema has not got it yet!");
            StringBuilder columns;
            StringBuilder gsSchema;
            StringBuilder hiveSchema;

            Connection conn = GsussDBOpt.getConnection(mppConfig.getGsDriver(), mppConfig.getGsUrl(), mppConfig.getGsUser(), mppConfig.getGsPasswd());

            try {
                DatabaseMetaData dm = conn.getMetaData();
                ResultSet rs = dm.getColumns(catalog, schema, tableName, null);

                columns = new StringBuilder();
                gsSchema = new StringBuilder();
                hiveSchema = new StringBuilder();

                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    String dataType = rs.getString("TYPE_NAME");
                    int columnSize = rs.getInt("COLUMN_SIZE");
                    int decimalDigits = rs.getInt("DECIMAL_DIGITS");

                    columns.append(columnName + ",");
                    gsSchema.append(columnName + " " + Util.getMappingGSType(dataType, columnSize, decimalDigits) + ",");
                    hiveSchema.append(columnName + " " + Util.getMappingHiveType(dataType, columnSize, decimalDigits) + ",");
                }
                String columnsTmp = columns.deleteCharAt(columns.length() - 1).toString();
                String gsSchemaTmp = gsSchema.deleteCharAt(gsSchema.length() - 1).toString();
                String hiveSchemaTmp = hiveSchema.deleteCharAt(hiveSchema.length() - 1).toString();
                String schemaTime = Util.getDate();

                schemas.put("columns", columnsTmp);
                schemas.put("gsSchema", gsSchemaTmp);
                schemas.put("hiveSchema", hiveSchemaTmp);

                Const.schemasMap.put(tblIdentifier, new TableSchema(columnsTmp, gsSchemaTmp, hiveSchemaTmp, schemaTime));
                rs.close();
                conn.close();
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
            return schemas;
        }
    }
}
