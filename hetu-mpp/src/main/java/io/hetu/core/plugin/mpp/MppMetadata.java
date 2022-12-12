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
package io.hetu.core.plugin.mpp;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.hetu.core.plugin.mpp.scheduler.Scheduler;
import io.hetu.core.plugin.mpp.scheduler.db.GsussDBOpt;
import io.hetu.core.plugin.mpp.scheduler.entity.ETLInfo;
import io.hetu.core.plugin.mpp.scheduler.utils.Const;
import io.hetu.core.plugin.mpp.scheduler.utils.Util;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveErrorCode;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadata;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static io.prestosql.plugin.hive.HiveUtil.getPartitionKeyColumnHandles;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class MppMetadata
        extends HiveMetadata
{
    public Scheduler scheduler;
    public MppConfig mppConfig;
    public boolean createsOfNonManagedTablesEnabled;
    public static Logger logger = Logger.get(MppMetadata.class);

    @Inject
    public MppMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean tableCreatesWithLocationAllowed,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            String prestoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            AccessControlMetadata accessControlMetadata,
            boolean autoVacuumEnabled,
            int vacuumDeltaNumThreshold,
            double vacuumDeltaPercentThreshold,
            ScheduledExecutorService vacuumExecutorService,
            Optional<Duration> vacuumCollectorInterval,
            ScheduledExecutorService hiveMetastoreClientService,
            Scheduler scheduler,
            MppConfig mppConfig)
    {
        super(metastore, hdfsEnvironment, partitionManager, writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled, tableCreatesWithLocationAllowed,
                typeManager, locationService, partitionUpdateCodec, typeTranslator,
                prestoVersion, hiveStatisticsProvider, accessControlMetadata,
                autoVacuumEnabled, vacuumDeltaNumThreshold, vacuumDeltaPercentThreshold,
                vacuumExecutorService, vacuumCollectorInterval, hiveMetastoreClientService, null);
        this.scheduler = scheduler;
        this.mppConfig = mppConfig;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        List<String> dbList = metastore.getAllDatabases();
        return dbList;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            return ImmutableList.of(schemaName.get());
        }
        return listSchemaNames(session);
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        String gsSchemaName = tableName.getSchemaName();
        String schemaName = mppConfig.getHiveDb();
        String tblName = tableName.getTableName();
        String threadName = Const.tableStatus.getThreadName();
        Optional<Table> table = metastore.getTable(new HiveIdentity(session), schemaName, tableName.getTableName());
        String tblIdentifier = schemaName + "." + tblName;
        logger.info("Mpp scheduler for " + tblIdentifier + " started");
        if (!table.isPresent()) {
            logger.info("Hive(Mpp) table " + tblIdentifier + " is not present");
            determineWhetherToETL(gsSchemaName, schemaName, tblName, threadName, tblIdentifier);
        }
        else {
            if (Const.etlInfoMap.containsKey(tblIdentifier) && Const.tableStatus.tableStatusKeysExists(tblIdentifier)) {
//                have etled at least onece
                if (mppConfig.isEtlReuse()) {
                    logger.info("Hive(Mpp) table " + tblIdentifier + " existed and reuse it");
                }
                else {
                    logger.info("Hive(Mpp) " + tblIdentifier + " existed in this app runtime but not reuse it and will redo!");
                    determineWhetherToETL(gsSchemaName, schemaName, tblName, threadName, tblIdentifier);
                }
            }
            else {
//                have etled in last restart,
//                or have etled and still etling
//                or just create and etling
//                  we can use two strategies to judge: rules and time interval
                if (Const.tableStatus.tableStatusKeysExists(tblIdentifier)) {
                    logger.info("[2]Hive(MPP) " + tblIdentifier + " Table is existed and is etling by others and reuse it!");
                }
                else {
                    logger.info("[2]Hive(MPP) " + tblIdentifier + " existed in last app runtime but not reuse it and will redo!");
                    determineWhetherToETL(gsSchemaName, schemaName, tblName, threadName, tblIdentifier);
                }
            }
        }

        table = metastore.getTable(new HiveIdentity(session), schemaName, tableName.getTableName());

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(tableName).isPresent()) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        MetastoreUtil.verifyOnline(tableName, Optional.empty(), MetastoreUtil.getProtectMode(table.get()), table.get().getParameters());

        Map<String, String> parameters = new HashMap<>();
        parameters.putAll(table.get().getParameters());

        String format = table.get().getStorage().getStorageFormat().getOutputFormatNullable();
        if (format != null) {
            parameters.put(STORAGE_FORMAT, format);
        }

        return new HiveTableHandle(
                schemaName,
                tableName.getTableName(),
                parameters,
                getPartitionKeyColumnHandles(table.get()),
                HiveBucketing.getHiveBucketHandle(table.get()));
    }

    private void determineWhetherToETL(String gsSchemaName, String schemaName, String tblName, String threadName, String tblIdentifier)
    {
        List<String> runningTaskList = Const.runningThreadMap.get(tblIdentifier);
        Const.tableStatus.put(tblIdentifier, 0);
        Const.etlInfoMap.put(tblIdentifier, new ETLInfo(0, Util.getDate(), ""));
        int size;
        String lock = TableMoveLock.getLock(tblIdentifier);
        Map<String, String> schemas = null;
        synchronized (lock) {
            runningTaskList.add(threadName);
            Const.runningThreadMap.put(tblIdentifier, runningTaskList);
            size = Const.runningThreadMap.get(tblIdentifier).size();
            if (size == 1) {
                schemas = GsussDBOpt.getSchemas(mppConfig, "", gsSchemaName, tblName);
                logger.info("Record table " + tblIdentifier + " into tableStatus and eltlInfoMap");
                scheduler.prepareHiveExternalTable(schemas, schemaName, tblName);
            }
        }
        if (size == 1) {
            Map.Entry gdsServer = scheduler.getGDS();
            scheduler.startGdsProcess(gdsServer, schemas, gsSchemaName, tblName);
        }
    }
}
