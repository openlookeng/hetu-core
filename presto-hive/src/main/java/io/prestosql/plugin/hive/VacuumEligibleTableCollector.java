/*
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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class VacuumEligibleTableCollector
{
    private static VacuumEligibleTableCollector instance;
    private final ScheduledExecutorService executorService;
    private final Logger log = Logger.get(VacuumEligibleTableCollector.class);

    private SemiTransactionalHiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private int vacuumDeltaNumThreshold;
    private double vacuumDeltaPercentThreshold;
    private List<ConnectorVacuumTableInfo> vacuumTableList = Collections.synchronizedList(new ArrayList<>());
    private Map<String, ConnectorVacuumTableInfo> inProgressVacuums = new ConcurrentHashMap<>();

    private VacuumTableCollectorTask task = new VacuumTableCollectorTask();

    private VacuumEligibleTableCollector(SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            int vacuumDeltaNumThreshold,
            double vacuumDeltaPercentThreshold,
            ScheduledExecutorService executorService)
    {
        this.metastore = metastore;
        this.hdfsEnvironment = hdfsEnvironment;
        this.vacuumDeltaNumThreshold = vacuumDeltaNumThreshold;
        this.vacuumDeltaPercentThreshold = vacuumDeltaPercentThreshold;
        this.executorService = executorService;
    }

    public static synchronized void createInstance(SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment, int vacuumDeltaNumThreshold, double vacuumDeltaPercentThreshold,
            ScheduledExecutorService executorService, long vacuumCollectorInterval)
    {
        if (instance == null) {
            instance = new VacuumEligibleTableCollector(metastore, hdfsEnvironment, vacuumDeltaNumThreshold, vacuumDeltaPercentThreshold, executorService);
            //Initialize the file systems
            HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(new ConnectorIdentity("openLooKeng", Optional.empty(), Optional.empty()));
            try {
                hdfsEnvironment.getFileSystem(context, new Path("/"));
            }
            catch (IOException e) {
            }
            // Also start preparing vacuumTableList
            instance.executorService.scheduleAtFixedRate(instance.task,
                    0, vacuumCollectorInterval, TimeUnit.MILLISECONDS);
        }
    }

    public static void finishVacuum(String schemaTable)
    {
        if (instance.inProgressVacuums.containsKey(schemaTable)) {
            instance.inProgressVacuums.remove(schemaTable);
        }
    }

    static List<ConnectorVacuumTableInfo> getVacuumTableList(SemiTransactionalHiveMetastore metastore,
                    HdfsEnvironment hdfsEnvironment, int vacuumDeltaNumThreshold,
                    double vacuumDeltaPercentThreshold, ScheduledExecutorService executorService, long vacuumCollectorInterval)
    {
        createInstance(metastore, hdfsEnvironment, vacuumDeltaNumThreshold, vacuumDeltaPercentThreshold, executorService, vacuumCollectorInterval);
        synchronized (instance) {
            instance.metastore = metastore;
            ImmutableList<ConnectorVacuumTableInfo> newList = ImmutableList.copyOf(instance.vacuumTableList);
            instance.vacuumTableList.clear();
            return newList;
        }
    }

    private synchronized void addToVacuumTableList(List<ConnectorVacuumTableInfo> tablesForVacuum)
    {
        for (ConnectorVacuumTableInfo tableInfo : tablesForVacuum) {
            if (!inProgressVacuums.containsKey(tableInfo.getSchemaTableName())
                    && !vacuumTableList.contains(tableInfo)) {
                inProgressVacuums.put(tableInfo.getSchemaTableName(), tableInfo);
                vacuumTableList.add(tableInfo);
            }
        }
    }

    private class VacuumTableCollectorTask
            implements Runnable
    {
        private Table getTable(String schemaName, String tableName, SemiTransactionalHiveMetastore metastore)
        {
            HiveIdentity identity = new HiveIdentity(new ConnectorIdentity("openLooKeng", Optional.empty(), Optional.empty()));
            Optional<Table> table = metastore.getTable(identity, schemaName, tableName);
            if (!table.isPresent() || table.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
                throw new TableNotFoundException(new SchemaTableName(schemaName, tableName));
            }
            return table.get();
        }

        @Override
        public void run()
        {
            try {
                collectTablesForVacuum();
            }
            catch (Exception e) {
                log.info("Error while collecting tables for auto-vacuum" + e.toString());
            }
        }

        private void collectTablesForVacuum()
        {
            SemiTransactionalHiveMetastore taskMetastore = metastore;
            List<String> databases = taskMetastore.getAllDatabases();
            for (String database : databases) {
                //Let each database get analyzed asynchronously.
                executorService.submit(() -> {
                    try {
                        scanDatabase(database, taskMetastore);
                    }
                    catch (Exception e) {
                        log.info("Error while scanning database for vacuum" + e.toString());
                    }
                });
            }
        }

        private void scanDatabase(String database, SemiTransactionalHiveMetastore taskMetastore)
        {
            Optional<List<String>> tables = taskMetastore.getAllTables(database);
            if (tables.isPresent()) {
                List<ConnectorVacuumTableInfo> tablesForVacuum = new ArrayList<>();
                for (String table : tables.get()) {
                    if (inProgressVacuums.containsKey(appendTableWithSchema(database, table))) {
                        log.debug("Auto-vacuum is in progress for table: " + appendTableWithSchema(database, table));
                        continue;
                    }
                    Table tableInfo = getTable(database, table, taskMetastore);
                    if (isTransactional(tableInfo)) {
                        ConnectorIdentity connectorIdentity = new ConnectorIdentity("openLooKeng", Optional.empty(), Optional.empty());
                        HiveIdentity identity = new HiveIdentity(connectorIdentity);
                        Optional<List<String>> partitions = taskMetastore.getPartitionNames(identity, database, table);
                        String tablePath = getLocation(tableInfo);
                        HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(connectorIdentity);
                        hdfsEnvironment.doAs("openLooKeng", () -> {
                            try {
                                // For Hive partitioned table
                                if (partitions.isPresent() && partitions.get().size() > 0) {
                                    for (String partitionName : partitions.get()) {
                                        String partitionPath = tablePath + "/" + partitionName;
                                        boolean updated = determineVacuumType(partitionPath, database, table, tablesForVacuum, tableInfo.getParameters(), hdfsContext);
                                        // If auto-vacuum condition satisfies for 1 partition,
                                        // stop checking for other partitions. Since auto-vacuum runs
                                        // on entire table.
                                        if (updated) {
                                            break;
                                        }
                                    }
                                }
                                else {
                                    determineVacuumType(tablePath, database, table, tablesForVacuum, tableInfo.getParameters(), hdfsContext);
                                }
                            }
                            catch (Exception e) {
                                log.info("Exception while determining vacuum type for table: " + database + "." + table + ": " + e.toString());
                            }
                        });
                    }
                }
                addToVacuumTableList(tablesForVacuum);
            }
        }

        private String getLocation(Table tableInfo)
        {
            return tableInfo.getStorage().getLocation();
        }

        private boolean isTransactional(Table tableInfo)
        {
            if (!tableInfo.getParameters().containsKey("transactional")) {
                return false;
            }
            return tableInfo.getParameters().get("transactional").equalsIgnoreCase("true");
        }

        private boolean determineVacuumType(String path, String schema, String table, List<ConnectorVacuumTableInfo> tablesForVacuum, Map<String, String> parameters, HdfsEnvironment.HdfsContext hdfsContext)
                throws IOException
        {
            log.debug("Determining vacuum type for path: " + path);
            Path tablePath = new Path(path);
            AcidUtils.Directory dir = getDirectory(hdfsContext, tablePath);
            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, tablePath);

            boolean noBase = false;
            Path base = dir.getBaseDirectory();
            long baseSize = 0;
            if (base != null) {
                baseSize += sumDirSize(fs, base);
            }
            List<HadoopShims.HdfsFileStatusWithId> originals = dir.getOriginalFiles();
            for (HadoopShims.HdfsFileStatusWithId origStat : originals) {
                baseSize += origStat.getFileStatus().getLen();
            }
            long deltaSize = 0;
            List<AcidUtils.ParsedDelta> deltas = dir.getCurrentDirectories();
            for (AcidUtils.ParsedDelta delta : deltas) {
                deltaSize += sumDirSize(fs, delta.getPath());
            }

            logStats(schema, table, baseSize, deltaSize, dir.getCurrentDirectories().size());

            if (baseSize == 0 && deltaSize > 0) {
                noBase = true;
            }
            else {
                boolean bigEnough = (float) deltaSize / (float) baseSize > vacuumDeltaPercentThreshold;
                if (bigEnough) {
                    ConnectorVacuumTableInfo vacuumTable = new ConnectorVacuumTableInfo(appendTableWithSchema(schema, table), true);
                    tablesForVacuum.add(vacuumTable);
                    return true;
                }
            }
            if (dir.getCurrentDirectories().size() > vacuumDeltaNumThreshold) {
                boolean isFull = false;
                //If insert-only table or first time vacuum, then it should be full vacuum.
                if (AcidUtils.isInsertOnlyTable(parameters) || noBase) {
                    isFull = true;
                }
                ConnectorVacuumTableInfo vacuumTable = new ConnectorVacuumTableInfo(appendTableWithSchema(schema, table), isFull);
                tablesForVacuum.add(vacuumTable);
                return true;
            }
            return false;
        }

        private void logStats(String schema, String table, long baseSize, long deltaSize, int numOfDeltaDir)
        {
            log.debug(String.format("Auto-vacuum stats for table '%s': baseSize='%d', delatSize='%d', numOfDeltaDir='%d'",
                    appendTableWithSchema(schema, table),
                    baseSize,
                    deltaSize,
                    numOfDeltaDir));
        }

        private String appendTableWithSchema(String schema, String table)
        {
            return schema + "." + table;
        }

        public AcidUtils.Directory getDirectory(HdfsEnvironment.HdfsContext hdfsContext, Path tablePath)
                throws IOException
        {
            Configuration conf = hdfsEnvironment.getConfiguration(hdfsContext, tablePath);
            ValidReaderWriteIdList validWriteIds = new ValidReaderWriteIdList();
            return AcidUtils.getAcidState(tablePath, conf, validWriteIds);
        }

        private long sumDirSize(FileSystem fs, Path dir)
                throws IOException
        {
            long size = 0;
            FileStatus[] buckets = fs.listStatus(dir, FileUtils.HIDDEN_FILES_PATH_FILTER);
            for (int i = 0; i < buckets.length; i++) {
                size += buckets[i].getLen();
            }
            return size;
        }
    }
}
