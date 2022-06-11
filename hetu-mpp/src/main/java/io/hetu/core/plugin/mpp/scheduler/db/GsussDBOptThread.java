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
import io.hetu.core.plugin.mpp.TableMoveLock;
import io.hetu.core.plugin.mpp.scheduler.utils.Const;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Queue;

/**
 * @author chengyijian
 * @title: GsussDBOpt
 * @projectName mpp-scheduler
 * @description: GaussDB操作相关
 * @date 2021/8/1210:56
 */
public class GsussDBOptThread
        extends Thread
{
    public static Logger logger = Logger.get(GsussDBOptThread.class);
    private String driver;
    private String jdbcUrl;
    private String username;
    private String password;

    private String dropSQL;
    private String createSQL;
    private String insertSQL;
    private String gaussdbSchema;
    private String tableName;
    private String hiveDb;
    private String parentThreadName;

    private Queue<Map.Entry<String, String>> gdsQueue;
    private Map.Entry<String, String> gdsServer;

    public GsussDBOptThread(Queue<Map.Entry<String, String>> gdsQueue, Map.Entry<String, String> gdsServer,
                            String driver, String jdbcUrl, String username, String password,
                            String dropSQL, String createSQL, String insertSQL,
                            String gaussdbSchema, String tableName, String hiveDb, String parentThreadName)
    {
        this.gdsQueue = gdsQueue;
        this.gdsServer = gdsServer;
        this.driver = driver;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.dropSQL = dropSQL;
        this.createSQL = createSQL;
        this.insertSQL = insertSQL;
        this.gaussdbSchema = gaussdbSchema;
        this.tableName = tableName;
        this.hiveDb = hiveDb;
        this.parentThreadName = parentThreadName;
    }

    public Connection getConnection(String username, String passwd)
    {
        String driver = this.driver;
        String jdbcUrl = this.jdbcUrl;
        Connection conn = null;
        try {
            Class.forName(driver).getConstructor().newInstance();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }

        try {
            conn = DriverManager.getConnection(jdbcUrl, username, passwd);
            logger.info("Connection succeed!");
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return conn;
    }

    public static void optTable(Connection conn, String sql)
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

    @Override
    public void run()
    {
        try {
            Connection conn = getConnection(username, password);
            logger.info("GaussDB Drop Create Insert Operation Start");
            optTable(conn, dropSQL);
            optTable(conn, createSQL);
            optTable(conn, insertSQL);
//            List<String> runningThreadList = Const.runningThreadMap.get(gaussdbSchema + "." + tableName);
            List<String> runningThreadList = Const.runningThreadMap.get(hiveDb + "." + tableName);
            synchronized (TableMoveLock.getLock(gaussdbSchema + "." + tableName)) {
                for (String threadName : runningThreadList) {
                    Const.tableStatus.put(hiveDb + "." + tableName, 1, threadName);
//                    Const.tableStatus.put(gaussdbSchema + "." + tableName, 1, threadName);
                    Const.runningThreadMap.removeThread(hiveDb + "." + tableName, threadName);
                }
            }

            logger.info("GaussDB Operation End");
            gdsQueue.add(gdsServer);
            logger.info(gdsServer.getKey() + ":" + gdsServer.getValue() + " has been free!");
            conn.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
