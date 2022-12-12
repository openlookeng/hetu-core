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

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.hetu.core.plugin.mpp.scheduler.Scheduler;
import io.prestosql.plugin.hive.ForHive;
import io.prestosql.plugin.hive.ForHiveMetastore;
import io.prestosql.plugin.hive.ForHiveTransactionHeartbeats;
import io.prestosql.plugin.hive.ForHiveVacuum;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveMetadataFactory;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.LocationService;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.PartitionUpdate;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.security.AccessControlMetadataFactory;
import io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Objects.requireNonNull;

public class MppMetadataFactory
        extends HiveMetadataFactory
{
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean tableCreatesWithLocationAllowed;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final BoundedExecutor renameExecution;
    private final ScheduledExecutorService hiveVacuumService;
    private final TypeTranslator typeTranslator;
    private final String prestoVersion;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;
    private final ScheduledExecutorService hiveMetastoreClientService;
    private final Duration vacuumCleanupRecheckInterval;
    private final int vacuumDeltaNumThreshold;
    private final double vacuumDeltaPercentThreshold;
    private final boolean autoVacuumEnabled;
    private Optional<Duration> vacuumCollectorInterval;
    protected final int hmsWriteBatchSize;
    private Scheduler scheduler;
    private MppConfig mppConfig;
    SemiTransactionalHiveMetastore semiTransactionalHiveMetastore;

    @Inject
    @SuppressWarnings("deprecation")
    public MppMetadataFactory(
            HiveConfig hiveConfig,
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            @ForHive ExecutorService executorService,
            @ForHiveVacuum ScheduledExecutorService hiveVacuumService,
            @ForHiveMetastore ScheduledExecutorService hiveMetastoreClientService,
            @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            NodeVersion nodeVersion,
            AccessControlMetadataFactory accessControlMetadataFactory,
            Scheduler scheduler,
            MppConfig mppConfig)
    {
        this(
                metastore,
                hdfsEnvironment,
                partitionManager,
                hiveConfig.getMaxConcurrentFileRenames(),
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.getWritesToNonManagedTablesEnabled(),
                hiveConfig.getCreatesOfNonManagedTablesEnabled(),
                hiveConfig.getTableCreatesWithLocationAllowed(),
                hiveConfig.getPerTransactionMetastoreCacheMaximumSize(),
                hiveConfig.getHiveTransactionHeartbeatInterval(),
                hiveConfig.getVacuumCleanupRecheckInterval(),
                typeManager,
                locationService,
                partitionUpdateCodec,
                executorService,
                hiveVacuumService,
                heartbeatService,
                hiveMetastoreClientService,
                typeTranslator,
                nodeVersion.toString(),
                accessControlMetadataFactory,
                hiveConfig.getVacuumDeltaNumThreshold(),
                hiveConfig.getVacuumDeltaPercentThreshold(),
                hiveConfig.getAutoVacuumEnabled(),
                hiveConfig.getVacuumCollectorInterval(),
                hiveConfig.getMetastoreWriteBatchSize(),
                scheduler,
                mppConfig);
    }

    public MppMetadataFactory(
            HiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            int maxConcurrentFileRenames,
            boolean skipDeletionForAlter,
            boolean skipTargetCleanupOnRollback,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean tableCreatesWithLocationAllowed,
            long perTransactionCacheMaximumSize,
            Optional<Duration> hiveTransactionHeartbeatInterval,
            Duration vacuumCleanupRecheckInterval,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ExecutorService executorService,
            ScheduledExecutorService hiveVacuumService,
            ScheduledExecutorService heartbeatService,
            ScheduledExecutorService hiveMetastoreClientService,
            TypeTranslator typeTranslator,
            String prestoVersion,
            AccessControlMetadataFactory accessControlMetadataFactory,
            int vacuumDeltaNumThreshold,
            double vacuumDeltaPercentThreshold,
            boolean autoVacuumEnabled,
            Optional<Duration> vacuumCollectorInterval,
            int hmsWriteBatchSize,
            Scheduler scheduler,
            MppConfig mppConfig)
    {
        super(metastore, hdfsEnvironment, partitionManager, maxConcurrentFileRenames,
                skipDeletionForAlter, skipTargetCleanupOnRollback, writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled, tableCreatesWithLocationAllowed, perTransactionCacheMaximumSize,
                hiveTransactionHeartbeatInterval, vacuumCleanupRecheckInterval, typeManager, locationService,
                partitionUpdateCodec, executorService, hiveVacuumService, heartbeatService,
                hiveMetastoreClientService, typeTranslator, prestoVersion, accessControlMetadataFactory,
                vacuumDeltaNumThreshold, vacuumDeltaPercentThreshold, autoVacuumEnabled, vacuumCollectorInterval,
                hmsWriteBatchSize, null);

        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.tableCreatesWithLocationAllowed = tableCreatesWithLocationAllowed;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory, "accessControlMetadataFactory is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval, "hiveTransactionHeartbeatInterval is null");
        this.vacuumCleanupRecheckInterval = requireNonNull(vacuumCleanupRecheckInterval, "vacuumCleanupInterval is null");

        renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        this.hiveVacuumService = requireNonNull(hiveVacuumService, "hiveVacuumService is null");
        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
        this.hiveMetastoreClientService = requireNonNull(hiveMetastoreClientService, "heartbeatService is null");
        this.vacuumDeltaNumThreshold = vacuumDeltaNumThreshold;
        this.vacuumDeltaPercentThreshold = vacuumDeltaPercentThreshold;
        this.autoVacuumEnabled = autoVacuumEnabled;
        this.vacuumCollectorInterval = vacuumCollectorInterval;
        this.hmsWriteBatchSize = hmsWriteBatchSize;
        this.scheduler = scheduler;
        this.mppConfig = mppConfig;
    }

    @Override
    public HiveMetadata get()
    {
        semiTransactionalHiveMetastore = new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                CachingHiveMetastore.memoizeMetastore(this.metastore, perTransactionCacheMaximumSize), // per-transaction cache
                renameExecution,
                hiveVacuumService,
                vacuumCleanupRecheckInterval,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                hiveTransactionHeartbeatInterval,
                heartbeatService,
                hiveMetastoreClientService,
                hmsWriteBatchSize);

        return new MppMetadata(
                semiTransactionalHiveMetastore,
                hdfsEnvironment,
                partitionManager,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                tableCreatesWithLocationAllowed,
                typeManager,
                locationService,
                partitionUpdateCodec,
                typeTranslator,
                prestoVersion,
                new MetastoreHiveStatisticsProvider(semiTransactionalHiveMetastore, statsCache, samplePartitionCache),
                accessControlMetadataFactory.create(semiTransactionalHiveMetastore),
                autoVacuumEnabled,
                vacuumDeltaNumThreshold,
                vacuumDeltaPercentThreshold,
                hiveVacuumService,
                vacuumCollectorInterval,
                hiveMetastoreClientService,
                scheduler,
                mppConfig);
    }
}
