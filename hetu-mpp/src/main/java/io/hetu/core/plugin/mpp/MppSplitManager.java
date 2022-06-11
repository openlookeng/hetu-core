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
import com.google.common.collect.Ordering;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import io.hetu.core.plugin.mpp.scheduler.entity.ETLInfo;
import io.hetu.core.plugin.mpp.scheduler.utils.Const;
import io.hetu.core.plugin.mpp.scheduler.utils.Util;
import io.prestosql.plugin.hive.BackgroundHiveSplitLoader;
import io.prestosql.plugin.hive.CoercionPolicy;
import io.prestosql.plugin.hive.DirectoryLister;
import io.prestosql.plugin.hive.ForHive;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveBucketHandle;
import io.prestosql.plugin.hive.HiveBucketing;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetadata;
import io.prestosql.plugin.hive.HiveNotReadableException;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HivePartitionMetadata;
import io.prestosql.plugin.hive.HiveSplitLoader;
import io.prestosql.plugin.hive.HiveSplitManager;
import io.prestosql.plugin.hive.HiveSplitSource;
import io.prestosql.plugin.hive.HiveStorageFormat;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.HiveTransactionHandle;
import io.prestosql.plugin.hive.HiveVacuumSplitSource;
import io.prestosql.plugin.hive.HiveVacuumTableHandle;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.VersionEmbedder;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedSplitSource;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.type.TypeManager;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SERVER_SHUTTING_DOWN;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.GROUPED_SCHEDULING;
import static java.util.Objects.requireNonNull;

public class MppSplitManager
        extends HiveSplitManager
{
    public static Logger logger = Logger.get(MppSplitManager.class);
    public static final String OBJECT_NOT_READABLE = "object_not_readable";

    private final Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider;
    private final HivePartitionManager partitionManager;
    private final NamenodeStats namenodeStats;
    private final HdfsEnvironment hdfsEnvironment;
    private final DirectoryLister directoryLister;
    private final Executor executor;
    private final CoercionPolicy coercionPolicy;
    private final int maxOutstandingSplits;
    private final DataSize maxOutstandingSplitsSize;
    private final int minPartitionBatchSize;
    private final int maxPartitionBatchSize;
    private final int maxInitialSplits;
    private final int splitLoaderConcurrency;
    private final int maxSplitsPerSecond;
    private final boolean recursiveDfsWalkerEnabled;
    private final CounterStat highMemorySplitSourceCounter;
    private final TypeManager typeManager;
    private final HiveConfig hiveConfig;

    @Inject
    public MppSplitManager(
            HiveConfig hiveConfig,
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            HivePartitionManager partitionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            @ForHive ExecutorService executorService,
            VersionEmbedder versionEmbedder,
            TypeManager typeManager,
            CoercionPolicy coercionPolicy)
    {
        this(
                metastoreProvider,
                partitionManager,
                namenodeStats,
                hdfsEnvironment,
                directoryLister,
                versionEmbedder.embedVersion(new BoundedExecutor(executorService, hiveConfig.getMaxSplitIteratorThreads())),
                coercionPolicy,
                new CounterStat(),
                hiveConfig.getMaxOutstandingSplits(),
                hiveConfig.getMaxOutstandingSplitsSize(),
                hiveConfig.getMinPartitionBatchSize(),
                hiveConfig.getMaxPartitionBatchSize(),
                hiveConfig.getMaxInitialSplits(),
                hiveConfig.getSplitLoaderConcurrency(),
                hiveConfig.getMaxSplitsPerSecond(),
                hiveConfig.getRecursiveDirWalkerEnabled(),
                typeManager,
                hiveConfig);
    }

    public MppSplitManager(
            Function<HiveTransactionHandle, SemiTransactionalHiveMetastore> metastoreProvider,
            HivePartitionManager partitionManager,
            NamenodeStats namenodeStats,
            HdfsEnvironment hdfsEnvironment,
            DirectoryLister directoryLister,
            Executor executor,
            CoercionPolicy coercionPolicy,
            CounterStat highMemorySplitSourceCounter,
            int maxOutstandingSplits,
            DataSize maxOutstandingSplitsSize,
            int minPartitionBatchSize,
            int maxPartitionBatchSize,
            int maxInitialSplits,
            int splitLoaderConcurrency,
            @Nullable Integer maxSplitsPerSecond,
            boolean recursiveDfsWalkerEnabled,
            TypeManager typeManager,
            HiveConfig hiveConfig)
    {
        super(metastoreProvider, partitionManager, namenodeStats,
                hdfsEnvironment, directoryLister, executor, coercionPolicy,
                highMemorySplitSourceCounter, maxOutstandingSplits,
                maxOutstandingSplitsSize, minPartitionBatchSize,
                maxPartitionBatchSize, maxInitialSplits, splitLoaderConcurrency,
                maxSplitsPerSecond, recursiveDfsWalkerEnabled, typeManager, hiveConfig);

        this.metastoreProvider = requireNonNull(metastoreProvider, "metastore is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.executor = new ErrorCodedExecutor(executor);
        this.coercionPolicy = requireNonNull(coercionPolicy, "coercionPolicy is null");
        this.highMemorySplitSourceCounter = requireNonNull(highMemorySplitSourceCounter, "highMemorySplitSourceCounter is null");
        checkArgument(maxOutstandingSplits >= 1, "maxOutstandingSplits must be at least 1");
        this.maxOutstandingSplits = maxOutstandingSplits;
        this.maxOutstandingSplitsSize = maxOutstandingSplitsSize;
        this.minPartitionBatchSize = minPartitionBatchSize;
        this.maxPartitionBatchSize = maxPartitionBatchSize;
        this.maxInitialSplits = maxInitialSplits;
        this.splitLoaderConcurrency = splitLoaderConcurrency;
        this.maxSplitsPerSecond = firstNonNull(maxSplitsPerSecond, Integer.MAX_VALUE);
        this.recursiveDfsWalkerEnabled = recursiveDfsWalkerEnabled;
        this.typeManager = typeManager;
        this.hiveConfig = hiveConfig;
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Optional<QueryType> queryType,
            Map<String, Object> queryInfo,
            Set<TupleDomain<ColumnMetadata>> userDefinedCachePredicates,
            boolean partOfReuse)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTable.getSchemaTableName();

        String tblIdentifier = tableName.getSchemaName() + "." + tableName.getTableName();
        Integer recode = Const.tableStatus.get(tblIdentifier) != null ? Const.tableStatus.get(tblIdentifier) : -1;

        if (recode == -1) {
            logger.info("Have not find the gaussdb table's status info, maybe this is a reuse scheduler!");
        }
        else {
            logger.info("Find the gaussdb table's etl status info!");
            while (recode != 1) {
                try {
                    Thread.sleep(2000);
                    logger.info("Waitting to complete GDS process transporting data to alluxio");
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                recode = Const.tableStatus.get(tblIdentifier);
            }

            Const.tableStatus.remove(tblIdentifier);
            ETLInfo etlInfo = Const.etlInfoMap.get(tblIdentifier);
            Const.etlInfoMap.put(tblIdentifier, new ETLInfo(1, etlInfo.getStartTime(), Util.getDate()));
        }

        // get table metadata
        SemiTransactionalHiveMetastore metastore = metastoreProvider.apply((HiveTransactionHandle) transaction);
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        if (table.getStorage().getStorageFormat().getInputFormat().contains("carbon")) {
            throw new PrestoException(NOT_SUPPORTED, "Hive connector can't read carbondata tables");
        }

        // verify table is not marked as non-readable
        String tableNotReadable = table.getParameters().get(OBJECT_NOT_READABLE);
        if (!isNullOrEmpty(tableNotReadable)) {
            throw new HiveNotReadableException(tableName, Optional.empty(), tableNotReadable);
        }

        // get partitions
        List<HivePartition> partitions = partitionManager.getOrLoadPartitions(session, metastore, new HiveIdentity(session), hiveTable);

        // short circuit if we don't have any partitions
        if (partitions.isEmpty()) {
            return new FixedSplitSource(ImmutableList.of());
        }

        // get buckets from first partition (arbitrary)
        Optional<HiveBucketing.HiveBucketFilter> bucketFilter = hiveTable.getBucketFilter();

        // validate bucket bucketed execution
        Optional<HiveBucketHandle> bucketHandle = hiveTable.getBucketHandle();
        if ((splitSchedulingStrategy == GROUPED_SCHEDULING) && !bucketHandle.isPresent()) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "SchedulingPolicy is bucketed, but BucketHandle is not present");
        }

        // sort partitions
        partitions = Ordering.natural().onResultOf(HivePartition::getPartitionId).reverse().sortedCopy(partitions);

        Iterable<HivePartitionMetadata> hivePartitions = getPartitionMetadata(session, metastore, table, tableName, partitions, bucketHandle.map(HiveBucketHandle::toTableBucketProperty));

        HiveSplitLoader hiveSplitLoader = new BackgroundHiveSplitLoader(
                table,
                hivePartitions,
                hiveTable.getCompactEffectivePredicate(),
                BackgroundHiveSplitLoader.BucketSplitInfo.createBucketSplitInfo(bucketHandle, bucketFilter),
                session,
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                executor,
                splitLoaderConcurrency,
                recursiveDfsWalkerEnabled,
                metastore.getValidWriteIds(session, hiveTable, queryType.map(t -> t == QueryType.VACUUM).orElse(false))
                        .map(validTxnWriteIdList -> validTxnWriteIdList.getTableValidWriteIdList(table.getDatabaseName() + "." + table.getTableName())),
                dynamicFilterSupplier,
                queryType,
                queryInfo,
                typeManager);

        HiveSplitSource splitSource;
        HiveStorageFormat hiveStorageFormat = HiveMetadata.extractHiveStorageFormat(table);
        switch (splitSchedulingStrategy) {
            case UNGROUPED_SCHEDULING:
                splitSource = HiveSplitSource.allAtOnce(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        partOfReuse ? 0 : maxInitialSplits, //For reuse, we should make sure to have same split size all time for a table.
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        maxSplitsPerSecond,
                        hiveSplitLoader,
                        executor,
                        new CounterStat(),
                        dynamicFilterSupplier,
                        userDefinedCachePredicates,
                        typeManager,
                        hiveConfig,
                        hiveStorageFormat);
                break;
            case GROUPED_SCHEDULING:
                splitSource = HiveSplitSource.bucketed(
                        session,
                        table.getDatabaseName(),
                        table.getTableName(),
                        partOfReuse ? 0 : maxInitialSplits, //For reuse, we should make sure to have same split size all time for a table.
                        maxOutstandingSplits,
                        maxOutstandingSplitsSize,
                        maxSplitsPerSecond,
                        hiveSplitLoader,
                        executor,
                        new CounterStat(),
                        dynamicFilterSupplier,
                        userDefinedCachePredicates,
                        typeManager,
                        hiveConfig,
                        hiveStorageFormat);
                break;
            default:
                throw new IllegalArgumentException("Unknown splitSchedulingStrategy: " + splitSchedulingStrategy);
        }
        hiveSplitLoader.start(splitSource);

        if (queryType.isPresent() && queryType.get() == QueryType.VACUUM) {
            HdfsEnvironment.HdfsContext hdfsContext = new HdfsEnvironment.HdfsContext(session, table.getDatabaseName(), table.getTableName());
            return new HiveVacuumSplitSource(splitSource, (HiveVacuumTableHandle) queryInfo.get("vacuumHandle"), hdfsEnvironment, hdfsContext, session);
        }

        return splitSource;
    }

    private static class ErrorCodedExecutor
            implements Executor
    {
        private final Executor delegate;

        private ErrorCodedExecutor(Executor delegate)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
        }

        @Override
        public void execute(Runnable command)
        {
            try {
                delegate.execute(command);
            }
            catch (RejectedExecutionException e) {
                throw new PrestoException(SERVER_SHUTTING_DOWN, "Server is shutting down", e);
            }
        }
    }
}
