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

package io.hetu.core.plugin.carbondata;

import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import io.hetu.core.plugin.carbondata.impl.CarbondataTableReader;
import io.prestosql.plugin.hive.ForHive;
import io.prestosql.plugin.hive.ForHiveMetastore;
import io.prestosql.plugin.hive.ForHiveTransactionHeartbeats;
import io.prestosql.plugin.hive.HdfsEnvironment;
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
import org.joda.time.DateTimeZone;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class CarbondataMetadataFactory
        extends HiveMetadataFactory
{
    private static final Logger log = Logger.get(HiveMetadataFactory.class);
    private final boolean allowCorruptWritesForTesting;
    private final boolean skipDeletionForAlter;
    private final boolean skipTargetCleanupOnRollback;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    private final boolean tableCreatesWithLocationAllowed;
    private final long perTransactionCacheMaximumSize;
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    private final TypeManager typeManager;
    private final LocationService locationService;
    private final BoundedExecutor renameExecution;
    private final ScheduledExecutorService vacuumExecutorService;
    private final ScheduledExecutorService hiveMetastoreClientService;
    private final TypeTranslator typeTranslator;
    private final String hetuVersion;
    private final AccessControlMetadataFactory accessControlMetadataFactory;
    private final JsonCodec partitionUpdateCodec;
    private final JsonCodec segmentInfoCodec;
    private final Optional<Duration> hiveTransactionHeartbeatInterval;
    private final ScheduledExecutorService heartbeatService;
    private final CarbondataTableReader carbondataTableReader;
    private final String carbondataTableStore;
    private final long carbondataMinorVacuumSegmentCount;
    private final long carbondataMajorVacuumSegmentSize;
    private final Duration vacuumCleanupInterval;

    @Inject
    public CarbondataMetadataFactory(CarbondataConfig carbondataConfig, HiveMetastore metastore,
                                     HdfsEnvironment hdfsEnvironment, HivePartitionManager partitionManager,
                                     @ForHive ExecutorService executorService,
                                     @ForCarbonVacuum ScheduledExecutorService vacuumExecutorService,
                                     @ForHiveTransactionHeartbeats ScheduledExecutorService heartbeatService,
                                     @ForHiveMetastore ScheduledExecutorService hiveMetastoreClientService,
                                     TypeManager typeManager, LocationService locationService,
                                     JsonCodec<PartitionUpdate> partitionUpdateCodec,
                                     JsonCodec<CarbondataSegmentInfoUtil> segmentInfoCodec,
                                     TypeTranslator typeTranslator, NodeVersion nodeVersion,
                                     AccessControlMetadataFactory accessControlMetadataFactory,
                                     CarbondataTableReader carbondataTableReader)
    {
        this(metastore, hdfsEnvironment, partitionManager, carbondataConfig.getDateTimeZone(),
                carbondataConfig.getMaxConcurrentFileRenames(),
                carbondataConfig.getAllowCorruptWritesForTesting(),
                carbondataConfig.isSkipDeletionForAlter(),
                carbondataConfig.isSkipTargetCleanupOnRollback(),
                true,
                carbondataConfig.getCreatesOfNonManagedTablesEnabled(),
                carbondataConfig.getTableCreatesWithLocationAllowed(),
                carbondataConfig.getPerTransactionMetastoreCacheMaximumSize(),
                carbondataConfig.getHiveTransactionHeartbeatInterval(),
                carbondataConfig.getVacuumCleanupRecheckInterval(), typeManager, locationService,
                partitionUpdateCodec, segmentInfoCodec, executorService,
                vacuumExecutorService, heartbeatService, hiveMetastoreClientService, typeTranslator, nodeVersion.toString(),
                accessControlMetadataFactory, carbondataTableReader, carbondataConfig.getStoreLocation(),
                carbondataConfig.getMajorVacuumSegSize(), carbondataConfig.getMinorVacuumSegCount(),
                carbondataConfig.getAutoVacuumEnable());
    }

    public CarbondataMetadataFactory(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment,
                                     HivePartitionManager partitionManager, DateTimeZone timeZone,
                                     int maxConcurrentFileRenames,
                                     boolean allowCorruptWritesForTesting, boolean skipDeletionForAlter,
                                     boolean skipTargetCleanupOnRollback, boolean writesToNonManagedTablesEnabled,
                                     boolean createsOfNonManagedTablesEnabled, boolean tableCreatesWithLocationAllowed,
                                     long perTransactionCacheMaximumSize,
                                     Optional<Duration> hiveTransactionHeartbeatInterval,
                                     Duration vacuumCleanupInterval,
                                     TypeManager typeManager, LocationService locationService,
                                     JsonCodec<PartitionUpdate> partitionUpdateCodec,
                                     JsonCodec<CarbondataSegmentInfoUtil> segmentInfoCodec,
                                     ExecutorService executorService, ScheduledExecutorService vacuumExecutorService, ScheduledExecutorService heartbeatService,
                                     ScheduledExecutorService hiveMetastoreClientService,
                                     TypeTranslator typeTranslator, String hetuVersion,
                                     AccessControlMetadataFactory accessControlMetadataFactory,
                                     CarbondataTableReader carbondataTableReader, String storeLocation, long majorVacuumSegSize, long minorVacuumSegCount,
                                     boolean autoVacuumEnable)
    {
        super(metastore,
                hdfsEnvironment,
                partitionManager,
                timeZone,
                maxConcurrentFileRenames,
                allowCorruptWritesForTesting,
                skipDeletionForAlter,
                skipTargetCleanupOnRollback,
                writesToNonManagedTablesEnabled,
                createsOfNonManagedTablesEnabled,
                tableCreatesWithLocationAllowed,
                perTransactionCacheMaximumSize,
                hiveTransactionHeartbeatInterval,
                vacuumCleanupInterval,
                typeManager,
                locationService,
                partitionUpdateCodec,
                executorService,
                vacuumExecutorService,
                heartbeatService,
                hiveMetastoreClientService,
                typeTranslator,
                hetuVersion,
                accessControlMetadataFactory,
                2, 0.0, false, Optional.of(new Duration(5, TimeUnit.MINUTES)));
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;
        this.skipDeletionForAlter = skipDeletionForAlter;
        this.skipTargetCleanupOnRollback = skipTargetCleanupOnRollback;
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.tableCreatesWithLocationAllowed = tableCreatesWithLocationAllowed;
        this.perTransactionCacheMaximumSize = perTransactionCacheMaximumSize;
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.segmentInfoCodec = requireNonNull(segmentInfoCodec, "segmentInfoCodec is null");
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.hetuVersion = requireNonNull(hetuVersion, "hetuVersion is null");
        this.accessControlMetadataFactory = requireNonNull(accessControlMetadataFactory,
                "accessControlMetadataFactory is null");
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            log.warn(
                    "Hive writes are disabled. To write data to Hive, your JVM timezone must match the " +
                            "Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments",
                    timeZone.getID());
        }

        this.renameExecution = new BoundedExecutor(executorService, maxConcurrentFileRenames);
        this.vacuumExecutorService = requireNonNull(vacuumExecutorService, "vacuumExecutorService is null");
        this.hiveMetastoreClientService = requireNonNull(hiveMetastoreClientService, "hiveMetastoreClientService is null");
        this.vacuumCleanupInterval = requireNonNull(vacuumCleanupInterval,
                "vacuumCleanupInterval is null");
        this.hiveTransactionHeartbeatInterval = requireNonNull(hiveTransactionHeartbeatInterval,
                "hiveTransactionHeartbeatInterval is null");

        this.heartbeatService = requireNonNull(heartbeatService, "heartbeatService is null");
        this.carbondataTableReader = requireNonNull(carbondataTableReader, "tableReader is null");
        this.carbondataTableStore = storeLocation;
        this.carbondataMinorVacuumSegmentCount = minorVacuumSegCount;
        this.carbondataMajorVacuumSegmentSize = majorVacuumSegSize;

        if (autoVacuumEnable) {
            CarbondataAutoVacuumThread.setAutoVacuumInfo(this.carbondataTableReader, this.hdfsEnvironment,
                    majorVacuumSegSize, minorVacuumSegCount, vacuumExecutorService);
        }
    }

    @Override
    public HiveMetadata get()
    {
        SemiTransactionalHiveMetastore metastore =
                new SemiTransactionalHiveMetastore(this.hdfsEnvironment,
                        CachingHiveMetastore.memoizeMetastore(this.metastore, this.perTransactionCacheMaximumSize),
                        this.renameExecution,
                        vacuumExecutorService, this.vacuumCleanupInterval, this.skipDeletionForAlter,
                        this.skipTargetCleanupOnRollback,
                        this.hiveTransactionHeartbeatInterval,
                        this.heartbeatService, hiveMetastoreClientService);

        return new CarbondataMetadata(metastore,
                this.hdfsEnvironment,
                this.partitionManager,
                this.timeZone,
                this.allowCorruptWritesForTesting,
                this.writesToNonManagedTablesEnabled,
                this.createsOfNonManagedTablesEnabled,
                this.tableCreatesWithLocationAllowed,
                this.typeManager,
                this.locationService,
                this.partitionUpdateCodec,
                this.segmentInfoCodec,
                this.typeTranslator,
                this.hetuVersion,
                new MetastoreHiveStatisticsProvider(metastore),
                this.accessControlMetadataFactory.create(metastore),
                carbondataTableReader,
                this.carbondataTableStore,
                this.carbondataMajorVacuumSegmentSize,
                this.carbondataMinorVacuumSegmentCount,
                vacuumExecutorService,
                hiveMetastoreClientService);
    }
}
