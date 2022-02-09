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

package io.hetu.core.plugin.carbondata;

import com.google.common.annotations.VisibleForTesting;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.security.ConnectorIdentity;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.hive.util.HiveCarbonUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.CompactionType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.emptyList;

public class CarbondataAutoVacuumThread
{
    private static final Logger LOG =
            LogServiceFactory.getLogService(CarbondataAutoVacuumThread.class.getName());
    private static final ConcurrentSkipListSet<Future<?>> queuedTasks = new ConcurrentSkipListSet<>();
    private final CarbondataTableReader carbondataTableReader;
    private final long majorVacuumSegSize;
    private final long minorVacuumSegCount;
    private final ScheduledExecutorService executorService;
    private final HdfsEnvironment hdfsEnvironment;
    private final List<ConnectorVacuumTableInfo> connectorVacuumTableInfoList;
    private final Map<String, Long> needToVacuumTablesMap;
    private final HdfsEnvironment.HdfsContext hdfsContext;
    private static CarbondataAutoVacuumThread instance;
    private static boolean enableTracingCleanupTask;
    private static CarbondataAutoVacuumThreadInfo carbondataAutoVacuumThreadInfo;

    public static synchronized CarbondataAutoVacuumThread getInstanceAutoVacuum()
    {
        if (instance == null) {
            instance = new CarbondataAutoVacuumThread(carbondataAutoVacuumThreadInfo.carbondataTableReader, carbondataAutoVacuumThreadInfo.hdfsEnvironment,
                    carbondataAutoVacuumThreadInfo.majorVacuumSegSize, carbondataAutoVacuumThreadInfo.minorVacuumSegCount, carbondataAutoVacuumThreadInfo.executorService);
        }
        return instance;
    }

    public static synchronized void setAutoVacuumInfo(CarbondataTableReader carbondataTableReader, HdfsEnvironment hdfsEnvironment,
                                                      long majorVacuumSegSize, long minorVacuumSegCount,
                                                      ScheduledExecutorService vacuumExecutorService)
    {
        if (carbondataAutoVacuumThreadInfo == null) {
            carbondataAutoVacuumThreadInfo = new CarbondataAutoVacuumThreadInfo(carbondataTableReader, hdfsEnvironment,
                    majorVacuumSegSize, minorVacuumSegCount, vacuumExecutorService);
        }
    }

    private CarbondataAutoVacuumThread(CarbondataTableReader carbondataTableReader, HdfsEnvironment hdfsEnvironment,
                                       long majorVacuumSegSize, long minorVacuumSegCount,
                                       ScheduledExecutorService vacuumExecutorService)
    {
        this.carbondataTableReader = carbondataTableReader;
        this.hdfsEnvironment = hdfsEnvironment;
        this.majorVacuumSegSize = majorVacuumSegSize;
        this.minorVacuumSegCount = minorVacuumSegCount;
        this.connectorVacuumTableInfoList = new ArrayList<>();
        this.needToVacuumTablesMap = new ConcurrentHashMap<>();
        this.executorService = vacuumExecutorService;
        this.hdfsContext = new HdfsEnvironment.HdfsContext(
                new ConnectorIdentity("openLooKeng", Optional.empty(), Optional.empty()));
    }

    class AutoVacuumScanTask
            implements Runnable
    {
        SemiTransactionalHiveMetastore metastore;
        String schemaName;

        AutoVacuumScanTask(SemiTransactionalHiveMetastore metastore)
        {
            this(metastore, null);
        }

        AutoVacuumScanTask(SemiTransactionalHiveMetastore metastore, String schemaName)
        {
            this.metastore = metastore;
            this.schemaName = schemaName;
        }

        public void run()
        {
            hdfsEnvironment.doAs("openLooKeng", () -> {
                // it will scan through all DB and Tables
                if (null == schemaName) {
                    scanSchemasAndTables(metastore);
                }
                else {
                    scanSchemas(metastore, schemaName);
                }
            });
        }
    }

    private void scanSchemasAndTables(SemiTransactionalHiveMetastore metastore)
    {
        try {
            for (String schemaName : metastore.getAllDatabases()) {
                //when multiple task are there, every schema is submit to task, to scan and identify vacuum tables.
                if (enableTracingCleanupTask) {
                    queuedTasks.add(executorService.submit(new AutoVacuumScanTask(metastore, schemaName)));
                }
                else {
                    executorService.submit(new AutoVacuumScanTask(metastore, schemaName));
                }
            }
        }
        catch (RuntimeException e) {
            LOG.error("Error when scanning schema", e);
            //ignore error cases
        }
    }

    private void identifyVacuumTables(Table target, String schemaName, String tableName, String dbNameTableName)
    {
        Configuration initialConfiguration = ConfigurationUtils.toJobConf(hdfsEnvironment
                .getConfiguration(hdfsContext, new Path(target.getStorage().getLocation())));

        Properties schema = MetastoreUtil.getHiveSchema(target);
        schema.setProperty("tablePath", target.getStorage().getLocation());
        CarbonTable carbonTable;
        try {
            carbonTable = CarbondataMetadata.getCarbonTable(schemaName, tableName, schema, initialConfiguration, carbondataTableReader);
        }
        catch (RuntimeException e) {
            return;
        }
        try {
            if (null != carbonTable) {
                CarbonLoadModel carbonLoadModel = HiveCarbonUtil.getCarbonLoadModel(schema, initialConfiguration);
                // in carbondata we support only miner vacuum
                List<List<LoadMetadataDetails>> allGroupedSegList = CarbondataHetuCompactorUtil.identifyAndGroupSegmentsToBeMerged(carbonLoadModel,
                        initialConfiguration, CompactionType.MINOR, majorVacuumSegSize, minorVacuumSegCount);

                if (!allGroupedSegList.isEmpty()) {
                    ConnectorVacuumTableInfo connectorVacuumTableInfo = new ConnectorVacuumTableInfo(dbNameTableName, false);

                    synchronized (connectorVacuumTableInfoList) {
                        //Selected Minor Vacuum tables are inserted in to list
                        connectorVacuumTableInfoList.add(connectorVacuumTableInfo);
                    }
                }
            }
        }
        catch (RuntimeException e) {
            LOG.error("error when identify Vacuum Table " + schemaName + "." + tableName, e);
            //Ignore error cases and continue
        }
    }

    private void scanSchemas(SemiTransactionalHiveMetastore metastore, String schemaName)
    {
        try {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
                Optional<Table> target = metastore.getTable(new HiveIdentity(hdfsContext.getIdentity()), schemaName, tableName);
                if (!target.isPresent()) {
                    continue;
                }
                String dbNameTableName = target.get().getDatabaseName() + "." + target.get().getTableName();

                if (needToVacuumTablesMap.containsKey(dbNameTableName)) {
                    continue;
                }

                if (target.get().getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
                    identifyVacuumTables(target.get(), schemaName, tableName, dbNameTableName);
                }
            }
        }
        catch (RuntimeException e) {
            LOG.error("error when scanning Tables", e);
            //Ignore error cases and continue
        }
    }

    public static List<ConnectorVacuumTableInfo> getAutoVacuumTableList(SemiTransactionalHiveMetastore metastore)
    {
        CarbondataAutoVacuumThread instanceAutoVacuum = getInstanceAutoVacuum();
        if (null == instanceAutoVacuum) {
            return null;
        }

        List<ConnectorVacuumTableInfo> connectorVacuumTableInfos = new ArrayList<>();
        synchronized (instanceAutoVacuum.connectorVacuumTableInfoList) {
            connectorVacuumTableInfos.addAll(instanceAutoVacuum.connectorVacuumTableInfoList);
            instanceAutoVacuum.connectorVacuumTableInfoList.clear();
        }
        //trigger task to do scanning of tables
        instanceAutoVacuum.submitTaskScanning(instanceAutoVacuum, metastore);
        return connectorVacuumTableInfos;
    }

    private void submitTaskScanning(CarbondataAutoVacuumThread instanceAutoVacuum, SemiTransactionalHiveMetastore metastore)
    {
        //trigger task to do scanning of tables
        if (enableTracingCleanupTask) {
            queuedTasks.add(instanceAutoVacuum.executorService.submit(new AutoVacuumScanTask(metastore)));
        }
        else {
            instanceAutoVacuum.executorService.submit(new AutoVacuumScanTask(metastore));
        }
    }

    public static void removeTableFromVacuumTablesMap(String schemaTableName)
    {
        CarbondataAutoVacuumThread autoVacuumObj = getInstanceAutoVacuum();
        if ((null != autoVacuumObj) && (null != autoVacuumObj.needToVacuumTablesMap.get(schemaTableName))) {
            autoVacuumObj.needToVacuumTablesMap.remove(schemaTableName);
        }
    }

    public static void addTableToVacuumTablesMap(String schemaTableName)
    {
        CarbondataAutoVacuumThread autoVacuumObj = getInstanceAutoVacuum();
        if (null != autoVacuumObj && (null != autoVacuumObj.needToVacuumTablesMap.get(schemaTableName))) {
            autoVacuumObj.needToVacuumTablesMap.put(schemaTableName, CarbonUpdateUtil.readCurrentTime());
        }
    }

    @VisibleForTesting
    public static void enableTracingVacuumTask(boolean isEnabled)
    {
        enableTracingCleanupTask = isEnabled;
    }

    @VisibleForTesting
    public static void waitForSubmittedVacuumTasksFinish()
    {
        queuedTasks.stream().forEach(f -> {
            try {
                f.get();
            }
            catch (InterruptedException e) {
                LOG.debug("Interrupted to get the result of autoCleanup : " + e);
            }
            catch (ExecutionException e) {
                LOG.debug("Exception to get the result of autoCleanup : " + e);
            }
        });
        queuedTasks.clear();
        LOG.info("All autocleanup tasks finished");
    }

    static class CarbondataAutoVacuumThreadInfo
    {
        public final CarbondataTableReader carbondataTableReader;
        public final HdfsEnvironment hdfsEnvironment;
        public final long majorVacuumSegSize;
        public final long minorVacuumSegCount;
        public final ScheduledExecutorService executorService;
        public final List<ConnectorVacuumTableInfo> connectorVacuumTableInfoList;
        public final Map<String, Long> needToVacuumTablesMap;

        private CarbondataAutoVacuumThreadInfo(CarbondataTableReader carbondataTableReader, HdfsEnvironment hdfsEnvironment,
                                               long majorVacuumSegSize, long minorVacuumSegCount,
                                               ScheduledExecutorService vacuumExecutorService)
        {
            this.carbondataTableReader = carbondataTableReader;
            this.hdfsEnvironment = hdfsEnvironment;
            this.majorVacuumSegSize = majorVacuumSegSize;
            this.minorVacuumSegCount = minorVacuumSegCount;
            this.connectorVacuumTableInfoList = new ArrayList<>();
            this.needToVacuumTablesMap = new ConcurrentHashMap<>();
            this.executorService = vacuumExecutorService;
        }
    }
}
