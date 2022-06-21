/*
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.hetu.core.plugin.mpp.scheduler;

import io.airlift.log.Logger;
import io.hetu.core.plugin.mpp.MppConfig;
import io.hetu.core.plugin.mpp.scheduler.db.GsussDBOptThread;
import io.hetu.core.plugin.mpp.scheduler.hadoop.HiveOpt;
import io.hetu.core.plugin.mpp.scheduler.utils.Const;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class Scheduler
{
    public static Logger logger = Logger.get(Scheduler.class);
    public MppConfig mppConfig;
    public Queue<Map.Entry<String, String>> gdsQueue;

    @Inject
    public Scheduler(MppConfig mppConfig)
    {
        this.mppConfig = mppConfig;
        this.gdsQueue = getGdsQueue(mppConfig);
    }

    public Queue<Map.Entry<String, String>> getGdsQueue(MppConfig mppConfig)
    {
        String[] gdsArr = mppConfig.getGdsList().split(",");
        Map<String, String> gdsMaps = new HashMap<>();
        for (String gdsServer : gdsArr) {
            gdsServer.split("\\|");
            gdsMaps.put(gdsServer.split("\\|")[0], gdsServer.split("\\|")[1]);
        }
        Queue<Map.Entry<String, String>> gdsQueue = new LinkedList<>();
        for (Map.Entry<String, String> entry : gdsMaps.entrySet()) {
            gdsQueue.add(entry);
        }
        return gdsQueue;
    }

    public Map.Entry getGDS()
    {
        while (true) {
            if (gdsQueue.isEmpty()) {
                logger.info("GDS queue is empty, please wait...");
                try {
                    Thread.sleep(1000);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else {
                return gdsQueue.poll();
            }
        }
    }

    public void prepareHiveExternalTable(Map<String, String> schemas, String schemaName, String tableName)
    {
        logger.info("Get schemainfo from gaussDB by table name");
//        String hivedbName = gaussdbSchema;
//        String hiveSchemaInfo = schemas.get("hiveSchema");
        String tblIdentifier = schemaName + "." + tableName;

        logger.info("Create hive foreign table using alluxio path by hiveserver2 service");
        String auxPath = mppConfig.getAuxUrl() + mppConfig.getBaseAux() + tableName;
        HiveOpt.createExternalTable(mppConfig.getHiveUrl() + mppConfig.getHiveDb(), mppConfig.getHiveUser(), mppConfig.getHivePasswd(),
                mppConfig.getHsqlDrop(), mppConfig.getHsqlCreate(),
                tableName, schemas.get("hiveSchema"), auxPath);
    }

    public void startGdsProcess(Map.Entry gdsServer, Map<String, String> schemas, String schemaName, String tableName)
    {
        logger.info("Prepare gaussDB GDS process");
        String colSchemaInfo = schemas.get("columns");
        String gsSchemaInfo = schemas.get("gsSchema");

        String gdsForeignLocation = gdsServer.getKey() + "/" + tableName;

        String createSQL = mppConfig.getGsqlCreate()
                .replace("${gaussdb_name}", schemaName)
                .replace("${table_name}", tableName)
                .replace("${gds_foreign_location}", gdsForeignLocation)
                .replace("${table_name}", tableName)
                .replace("${schema_info}", gsSchemaInfo);

        String dropSQL = mppConfig.getGsqlDrop()
                .replace("${gaussdb_name}", schemaName)
                .replace("${table_name}", tableName);

        String insertSQL = mppConfig.getGsqlInsert()
                .replace("${gaussdb_name}", schemaName)
                .replace("${table_name}", tableName)
                .replace("${schema_info}", colSchemaInfo);
        String threadName = Const.tableStatus.getThreadName();

        GsussDBOptThread gsussDBOptThread = new GsussDBOptThread(gdsQueue, gdsServer,
                mppConfig.getGsDriver(), mppConfig.getGsUrl(), mppConfig.getGsUser(), mppConfig.getGsPasswd(),
                dropSQL, createSQL, insertSQL, schemaName, tableName, mppConfig.getHiveDb(), threadName);
        gsussDBOptThread.start();
        logger.info("GaussDB GDS process thread start");
    }
}
