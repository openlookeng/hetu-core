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

import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.metastore.CachingHiveMetastore;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadataFactory;
import io.prestosql.plugin.hive.statistics.MetastoreHiveStatisticsProvider;
import io.prestosql.plugin.hive.statistics.TableColumnStatistics;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class HiveMetadataFactory
        implements Supplier<TransactionalMetadata>
{
    protected final Map<String, TableColumnStatistics> statsCache = new ConcurrentHashMap();
    protected final Map<Table, MetastoreHiveStatisticsProvider.SamplePartition> samplePartitionCache = new ConcurrentHashMap();

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

    @Inject
    @SuppressWarnings("deprecation")
    public HiveMetadataFactory(
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
            AccessControlMetadataFactory accessControlMetadataFactory)
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
                hiveConfig.getMetastoreWriteBatchSize());
    }

    public HiveMetadataFactory(
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
            int hmsWriteBatchSize)
    {
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
    }

    @Override
    public HiveMetadata get()
    {
        SemiTransactionalHiveMetastore localMetastore = new SemiTransactionalHiveMetastore(
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

        return new HiveMetadata(
                localMetastore,
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
                new MetastoreHiveStatisticsProvider(localMetastore, statsCache, samplePartitionCache),
                accessControlMetadataFactory.create(localMetastore),
                autoVacuumEnabled,
                vacuumDeltaNumThreshold,
                vacuumDeltaPercentThreshold,
                hiveVacuumService,
                vacuumCollectorInterval,
                hiveMetastoreClientService);
    }
}
