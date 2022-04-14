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

import com.google.common.base.Suppliers;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Streams;
import com.google.common.io.CharStreams;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.HiveSplit.BucketConversion;
import io.prestosql.plugin.hive.HiveVacuumTableHandle.Range;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.plugin.hive.util.HiveFileIterator;
import io.prestosql.plugin.hive.util.HiveFileIterator.NestedDirectoryNotAllowedException;
import io.prestosql.plugin.hive.util.InternalHiveSplitFactory;
import io.prestosql.plugin.hive.util.ResumableTask;
import io.prestosql.plugin.hive.util.ResumableTasks;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.resourcegroups.QueryType;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.common.ValidCompactorWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hive.common.util.Ref;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.HoodieROTablePathFilter;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntPredicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.prestosql.plugin.hive.HiveSessionProperties.isDynamicFilteringSplitFilteringEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isForceLocalScheduling;
import static io.prestosql.plugin.hive.HiveUtil.checkCondition;
import static io.prestosql.plugin.hive.HiveUtil.getBucketNumber;
import static io.prestosql.plugin.hive.HiveUtil.getFooterCount;
import static io.prestosql.plugin.hive.HiveUtil.getHeaderCount;
import static io.prestosql.plugin.hive.HiveUtil.getInputFormat;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.S3SelectPushdown.shouldEnablePushdownForTable;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.getPartitionLocation;
import static io.prestosql.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.FAIL;
import static io.prestosql.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.IGNORED;
import static io.prestosql.plugin.hive.util.HiveFileIterator.NestedDirectoryPolicy.RECURSE;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.common.FileUtils.HIDDEN_FILES_PATH_FILTER;

public class BackgroundHiveSplitLoader
        implements HiveSplitLoader
{
    private static final Logger LOG = Logger.get(BackgroundHiveSplitLoader.class);

    private static final Pattern DELETE_DELTA_PATTERN = Pattern.compile("delete_delta_(\\d+)_(\\d+)(_\\d+)?");

    private static final ListenableFuture<?> COMPLETED_FUTURE = immediateFuture(null);

    private final Table table;
    private final TupleDomain<? extends ColumnHandle> compactEffectivePredicate;
    private final Optional<BucketSplitInfo> tableBucketInfo;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final int loaderConcurrency;
    private final boolean recursiveDirWalkerEnabled;
    private final Executor executor;
    private final ConnectorSession session;
    private final ConcurrentLazyQueue<HivePartitionMetadata> partitions;
    private final Deque<Iterator<InternalHiveSplit>> fileIterators = new ConcurrentLinkedDeque<>();
    private final Optional<ValidWriteIdList> validWriteIds;
    private final Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier;
    private final Configuration configuration;
    private final Supplier<HoodieROTablePathFilter> hoodiePathFilterSupplier;

    // Purpose of this lock:
    // * Write lock: when you need a consistent view across partitions, fileIterators, and hiveSplitSource.
    // * Read lock: when you need to modify any of the above.
    //   Make sure the lock is held throughout the period during which they may not be consistent with each other.
    // Details:
    // * When write lock is acquired, except the holder, no one can do any of the following:
    // ** poll from (or check empty) partitions
    // ** poll from (or check empty) or push to fileIterators
    // ** push to hiveSplitSource
    // * When any of the above three operations is carried out, either a read lock or a write lock must be held.
    // * When a series of operations involving two or more of the above three operations are carried out, the lock
    //   must be continuously held throughout the series of operations.
    // Implications:
    // * if you hold a read lock but not a write lock, you can do any of the above three operations, but you may
    //   see a series of operations involving two or more of the operations carried out half way.
    private final ReentrantReadWriteLock taskExecutionLock = new ReentrantReadWriteLock();

    private HiveSplitSource hiveSplitSource;
    private volatile boolean stopped;
    private Optional<QueryType> queryType;
    private Map<String, Object> queryInfo;
    private TypeManager typeManager;
    private JobConf jobConf;

    private final Map<ColumnHandle, DynamicFilter> cachedDynamicFilters = new ConcurrentHashMap<>();

    public BackgroundHiveSplitLoader(
            Table table,
            Iterable<HivePartitionMetadata> partitions,
            TupleDomain<? extends ColumnHandle> compactEffectivePredicate,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Executor executor,
            int loaderConcurrency,
            boolean recursiveDirWalkerEnabled,
            Optional<ValidWriteIdList> validWriteIds,
            Supplier<List<Set<DynamicFilter>>> dynamicFilterSupplier,
            Optional<QueryType> queryType,
            Map<String, Object> queryInfo,
            TypeManager typeManager)
    {
        this.table = table;
        this.compactEffectivePredicate = compactEffectivePredicate;
        this.tableBucketInfo = tableBucketInfo;
        this.loaderConcurrency = loaderConcurrency;
        this.typeManager = typeManager;
        this.session = session;
        this.hdfsEnvironment = hdfsEnvironment;
        this.namenodeStats = namenodeStats;
        this.directoryLister = directoryLister;
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.executor = executor;
        this.hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
        this.validWriteIds = requireNonNull(validWriteIds, "validWriteIds is null");
        this.dynamicFilterSupplier = dynamicFilterSupplier;
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.queryInfo = requireNonNull(queryInfo, "queryproperties is null");
        this.partitions = new ConcurrentLazyQueue<>(getPrunedPartitions(partitions));
        Path path = new Path(getPartitionLocation(table, getPrunedPartitions(partitions).iterator().next().getPartition()));
        configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        jobConf = ConfigurationUtils.toJobConf(configuration);
        this.hoodiePathFilterSupplier = Suppliers.memoize(() -> new HoodieROTablePathFilter(configuration));
    }

    /**
     * Get pruned partitions, if applicable.
     */
    private Iterable<HivePartitionMetadata> getPrunedPartitions(Iterable<HivePartitionMetadata> partitions)
    {
        if (AcidUtils.isTransactionalTable(table.getParameters()) &&
                (queryType.map(t -> t == QueryType.VACUUM).orElse(false))) {
            String vacuumPartition = (String) queryInfo.get("partition");
            if (vacuumPartition != null && !vacuumPartition.isEmpty()) {
                List<HivePartitionMetadata> list = new ArrayList<>();
                for (Iterator<HivePartitionMetadata> it = partitions.iterator(); it.hasNext(); ) {
                    HivePartitionMetadata next = it.next();
                    if (vacuumPartition.equals(next.getHivePartition().getPartitionId())) {
                        return ImmutableList.of(next);
                    }
                }
            }
        }
        return partitions;
    }

    @Override
    public void start(HiveSplitSource splitSource)
    {
        this.hiveSplitSource = splitSource;
        for (int i = 0; i < loaderConcurrency; i++) {
            ResumableTasks.submit(executor, new HiveSplitLoaderTask());
        }
    }

    @Override
    public void stop()
    {
        stopped = true;
    }

    private class HiveSplitLoaderTask
            implements ResumableTask
    {
        @Override
        public TaskStatus process()
        {
            while (true) {
                if (stopped) {
                    return TaskStatus.finished();
                }
                ListenableFuture<?> future;
                taskExecutionLock.readLock().lock();
                try {
                    future = hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> loadSplits());
                }
                catch (Exception e) {
                    if (e instanceof IOException) {
                        e = new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, e);
                    }
                    else if (!(e instanceof PrestoException)) {
                        e = new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR, e);
                    }
                    // Fail the split source before releasing the execution lock
                    // Otherwise, a race could occur where the split source is completed before we fail it.
                    hiveSplitSource.fail(e);
                    checkState(stopped);
                    return TaskStatus.finished();
                }
                // For TestBackgroundHiveSplitLoader.testPropagateException
                catch (Error e) {
                    hiveSplitSource.fail(e);
                    return TaskStatus.finished();
                }
                finally {
                    taskExecutionLock.readLock().unlock();
                }
                invokeNoMoreSplitsIfNecessary();
                if (!future.isDone()) {
                    return TaskStatus.continueOn(future);
                }
            }
        }
    }

    private void invokeNoMoreSplitsIfNecessary()
    {
        taskExecutionLock.readLock().lock();
        try {
            // This is an opportunistic check to avoid getting the write lock unnecessarily
            if (!partitions.isEmpty() || !fileIterators.isEmpty()) {
                return;
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
            return;
        }
        finally {
            taskExecutionLock.readLock().unlock();
        }

        taskExecutionLock.writeLock().lock();
        try {
            // the write lock guarantees that no one is operating on the partitions, fileIterators, or hiveSplitSource, or half way through doing so.
            if (partitions.isEmpty() && fileIterators.isEmpty()) {
                // It is legal to call `noMoreSplits` multiple times or after `stop` was called.
                // Nothing bad will happen if `noMoreSplits` implementation calls methods that will try to obtain a read lock because the lock is re-entrant.
                hiveSplitSource.noMoreSplits();
            }
        }
        catch (Exception e) {
            hiveSplitSource.fail(e);
            checkState(stopped, "Task is not marked as stopped even though it failed");
        }
        finally {
            taskExecutionLock.writeLock().unlock();
        }
    }

    private ListenableFuture<?> loadSplits()
            throws IOException
    {
        Iterator<InternalHiveSplit> splits = fileIterators.poll();
        if (splits == null) {
            HivePartitionMetadata partition = partitions.poll();
            if (partition == null) {
                return COMPLETED_FUTURE;
            }
            return loadPartition(partition);
        }

        while (splits.hasNext() && !stopped) {
            ListenableFuture<?> future = hiveSplitSource.addToQueue(splits.next());
            if (!future.isDone()) {
                fileIterators.addFirst(splits);
                return future;
            }
        }

        // No need to put the iterator back, since it's either empty or we've stopped
        return COMPLETED_FUTURE;
    }

    private ListenableFuture<?> loadPartition(HivePartitionMetadata partition)
            throws IOException
    {
        HivePartition hivePartition = partition.getHivePartition();
        String partitionName = hivePartition.getPartitionId();
        Properties schema = getPartitionSchema(table, partition.getPartition());
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition());
        TupleDomain<HiveColumnHandle> effectivePredicate = (TupleDomain<HiveColumnHandle>) compactEffectivePredicate;

        if (dynamicFilterSupplier != null && isDynamicFilteringSplitFilteringEnabled(session)) {
            if (isPartitionFiltered(partitionKeys, dynamicFilterSupplier.get(), typeManager)) {
                // Avoid listing files and creating splits from a partition if it has been pruned due to dynamic filters
                return COMPLETED_FUTURE;
            }
        }

        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, schema, false, jobConf);
        FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
        boolean s3SelectPushdownEnabled = shouldEnablePushdownForTable(session, table, path.toString(), partition.getPartition());

        if (inputFormat instanceof SymlinkTextInputFormat) {
            if (tableBucketInfo.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Bucketed table in SymlinkTextInputFormat is not yet supported");
            }

            // TODO: This should use an iterator like the HiveFileIterator
            ListenableFuture<?> lastResult = COMPLETED_FUTURE;
            for (Path targetPath : getTargetPathsFromSymlink(fs, path)) {
                // The input should be in TextInputFormat.
                TextInputFormat targetInputFormat = new TextInputFormat();
                // the splits must be generated using the file system for the target path
                // get the configuration for the target path -- it may be a different hdfs instance
                FileSystem targetFilesystem = hdfsEnvironment.getFileSystem(hdfsContext, targetPath);
                jobConf.setInputFormat(TextInputFormat.class);
                targetInputFormat.configure(jobConf);
                FileInputFormat.setInputPaths(jobConf, targetPath);
                InputSplit[] targetSplits = targetInputFormat.getSplits(jobConf, 0);

                InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                        targetFilesystem,
                        partitionName,
                        inputFormat,
                        schema,
                        partitionKeys,
                        effectivePredicate,
                        partition.getColumnCoercions(),
                        Optional.empty(),
                        isForceLocalScheduling(session),
                        s3SelectPushdownEnabled);
                lastResult = addSplitsToSource(targetSplits, splitFactory);
                if (stopped) {
                    return COMPLETED_FUTURE;
                }
            }
            return lastResult;
        }

        Optional<BucketConversion> bucketConversion = Optional.empty();
        boolean bucketConversionRequiresWorkerParticipation = false;
        if (partition.getPartition().isPresent()) {
            Optional<HiveBucketProperty> partitionBucketProperty = partition.getPartition().get().getStorage().getBucketProperty();
            if (tableBucketInfo.isPresent() && partitionBucketProperty.isPresent()) {
                int readBucketCount = tableBucketInfo.get().getReadBucketCount();
                BucketingVersion bucketingVersion = partitionBucketProperty.get().getBucketingVersion(); // TODO can partition's bucketing_version be different from table's?
                int partitionBucketCount = partitionBucketProperty.get().getBucketCount();
                // Validation was done in HiveSplitManager#getPartitionMetadata.
                // Here, it's just trying to see if its needs the BucketConversion.
                if (readBucketCount != partitionBucketCount) {
                    bucketConversion = Optional.of(new BucketConversion(bucketingVersion, readBucketCount, partitionBucketCount, tableBucketInfo.get().getBucketColumns()));
                    if (readBucketCount > partitionBucketCount) {
                        bucketConversionRequiresWorkerParticipation = true;
                    }
                }
            }
        }
        InternalHiveSplitFactory splitFactory = new InternalHiveSplitFactory(
                fs,
                partitionName,
                inputFormat,
                schema,
                partitionKeys,
                effectivePredicate,
                partition.getColumnCoercions(),
                bucketConversionRequiresWorkerParticipation ? bucketConversion : Optional.empty(),
                isForceLocalScheduling(session),
                s3SelectPushdownEnabled);

        // To support custom input formats, we want to call getSplits()
        // on the input format to obtain file splits.
        if (!isHudiParquetInputFormat(inputFormat) && shouldUseFileSplitsFromInputFormat(inputFormat)) {
            if (tableBucketInfo.isPresent()) {
                throw new PrestoException(NOT_SUPPORTED, "Presto cannot read bucketed partition in an input format with UseFileSplitsFromInputFormat annotation: " + inputFormat.getClass().getSimpleName());
            }

            if (AcidUtils.isTransactionalTable(table.getParameters())) {
                throw new PrestoException(NOT_SUPPORTED, "Hive transactional tables in an input format with UseFileSplitsFromInputFormat annotation are not supported: " + inputFormat.getClass().getSimpleName());
            }

            FileInputFormat.setInputPaths(jobConf, path);
            InputSplit[] splits = inputFormat.getSplits(jobConf, 0);

            return addSplitsToSource(splits, splitFactory);
        }

        PathFilter pathFilter = isHudiParquetInputFormat(inputFormat) ? hoodiePathFilterSupplier.get() : path1 -> true;

        // S3 Select pushdown works at the granularity of individual S3 objects,
        // therefore we must not split files when it is enabled.
        boolean splittable = getHeaderCount(schema) == 0 && getFooterCount(schema) == 0 && !s3SelectPushdownEnabled;

        List<Path> readPaths;
        Optional<DeleteDeltaLocations> deleteDeltaLocations;
        long min = Long.MAX_VALUE;
        long max = Long.MIN_VALUE;
        if (AcidUtils.isTransactionalTable(table.getParameters())) {
            boolean isVacuum = queryType.map(type -> type == QueryType.VACUUM).orElse(false);
            AcidUtils.Directory directory = hdfsEnvironment.doAs(hdfsContext.getIdentity().getUser(), () -> {
                ValidWriteIdList writeIdList = validWriteIds.orElseThrow(() -> new IllegalStateException("No validWriteIds present"));
                if (isVacuum) {
                    writeIdList = new ValidCompactorWriteIdList(writeIdList.writeToString()) {
                        @Override
                        public RangeResponse isWriteIdRangeValid(long minWriteId, long maxWriteId)
                        {
                            //For unknown reasons.. ValidCompactorWriteIdList#isWriteIdRangeValid() doesnot
                            // check for aborted transactions and AcidUtils.getAcidState() adds aborted transaction to both aborted and working lists.
                            //Avoid this by overriding.
                            RangeResponse writeIdRangeValid = super.isWriteIdRangeValid(minWriteId, maxWriteId);
                            if (writeIdRangeValid == RangeResponse.NONE) {
                                return RangeResponse.NONE;
                            }
                            else if (super.isWriteIdRangeAborted(minWriteId, maxWriteId) == RangeResponse.ALL) {
                                return RangeResponse.NONE;
                            }
                            return writeIdRangeValid;
                        }
                    };
                }
                return AcidUtils.getAcidState(
                        path,
                        configuration,
                        writeIdList,
                        Ref.from(false),
                        true,
                        table.getParameters());
            });

            if (AcidUtils.isFullAcidTable(table.getParameters())) {
                // From Hive version >= 3.0, delta/base files will always have file '_orc_acid_version' with value >= '2'.
                Path baseOrDeltaPath = directory.getBaseDirectory() != null
                        ? directory.getBaseDirectory()
                        : (directory.getCurrentDirectories().size() > 0 ? directory.getCurrentDirectories().get(0).getPath() : null);

                if (baseOrDeltaPath != null && AcidUtils.OrcAcidVersion.getAcidVersionFromMetaFile(baseOrDeltaPath, fs) < 2) {
                    throw new PrestoException(NOT_SUPPORTED, "Hive transactional tables are supported with Hive 3.0 and only after a major compaction has been run");
                }
            }

            readPaths = new ArrayList<>();

            boolean isFullVacuum = isVacuum ? Boolean.valueOf(queryInfo.get("FULL").toString()) : false;

            if (isFullVacuum) {
                //Base will contain everything
                min = 0;
            }
            // base
            //In case of vacuum, include only in case of Full vacuum.
            if (directory.getBaseDirectory() != null && (!isVacuum || isFullVacuum)) {
                readPaths.add(directory.getBaseDirectory());
                if (isVacuum) {
                    min = 0;
                    max = AcidUtils.parseBase(directory.getBaseDirectory());
                }
            }

            // delta directories
            for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
                if (!delta.isDeleteDelta()) {
                    readPaths.add(delta.getPath());
                }
                //In case of Minor compaction, all delete_delta files should be compacted separately,
                else if (isVacuum && !isFullVacuum) {
                    readPaths.add(delta.getPath());
                }
                if (isVacuum) {
                    min = Math.min(delta.getMinWriteId(), min);
                    max = Math.max(delta.getMaxWriteId(), max);
                }
            }

            // Create a registry of delete_delta directories for the partition
            DeleteDeltaLocations.Builder deleteDeltaLocationsBuilder = DeleteDeltaLocations.builder(path);
            for (AcidUtils.ParsedDelta delta : directory.getCurrentDirectories()) {
                //In case of minor compaction, delete_delta directories should not be used for masking.
                if (delta.isDeleteDelta() && (!isVacuum || isFullVacuum)) {
                    //For unknown reasons ParseDelta.getStatementId() returns 0, though parsed statement is -1;
                    //This creates issue while trying to locate the delete_delta directory.
                    //So parsing again.
                    OptionalInt statementId = getStatementId(delta.getPath().getName());
                    int stmtId = statementId.orElse(0);
                    deleteDeltaLocationsBuilder.addDeleteDelta(delta.getPath(), delta.getMinWriteId(), delta.getMaxWriteId(), stmtId);
                }
            }

            deleteDeltaLocations = deleteDeltaLocationsBuilder.build();

            if (!directory.getOriginalFiles().isEmpty()) {
                LOG.info("Now supporting read from non-ACID files in ACID reader");
                // non-ACID file
                int numberOfBuckets = Integer.parseInt(schema.getProperty("bucket_count"));
                long[] bucketStartRowOffset = new long[Integer.max(numberOfBuckets, 1)];
                for (HadoopShims.HdfsFileStatusWithId f : directory.getOriginalFiles()) {
                    Path currFilePath = f.getFileStatus().getPath();
                    int currBucketNumber = getBucketNumber(currFilePath.getName()).getAsInt();
                    fileIterators.addLast(createInternalHiveSplitIterator(currFilePath, fs, splitFactory, splittable, deleteDeltaLocations, Optional.of(bucketStartRowOffset[currBucketNumber]), pathFilter));
                    try {
                        Reader copyReader = OrcFile.createReader(f.getFileStatus().getPath(),
                                OrcFile.readerOptions(configuration));
                        bucketStartRowOffset[currBucketNumber] += copyReader.getNumberOfRows();
                    }
                    catch (Exception e) {
                        throw new PrestoException(NOT_SUPPORTED, e.getMessage());
                    }
                }
            }

            if (isVacuum && !readPaths.isEmpty()) {
                Object vacuumHandle = queryInfo.get("vacuumHandle");
                if (vacuumHandle != null && vacuumHandle instanceof HiveVacuumTableHandle) {
                    HiveVacuumTableHandle hiveVacuumTableHandle = (HiveVacuumTableHandle) vacuumHandle;
                    hiveVacuumTableHandle.addRange(partitionName, new Range(min, max));
                }
            }
        }
        else {
            readPaths = ImmutableList.of(path);
            deleteDeltaLocations = Optional.empty();
        }

        // Bucketed partitions are fully loaded immediately since all files must be loaded to determine the file to bucket mapping
        if (tableBucketInfo.isPresent()) {
            ListenableFuture<?> lastResult = immediateFuture(null); // TODO document in addToQueue() that it is sufficient to hold on to last returned future
            for (Path readPath : readPaths) {
                lastResult = hiveSplitSource.addToQueue(getBucketedSplits(readPath, fs, splitFactory,
                        tableBucketInfo.get(), bucketConversion, getDeleteDeltaLocationFor(readPath, deleteDeltaLocations), pathFilter));
            }
            return lastResult;
        }

        for (Path readPath : readPaths) {
            fileIterators.addLast(createInternalHiveSplitIterator(readPath, fs, splitFactory, splittable,
                    getDeleteDeltaLocationFor(readPath, deleteDeltaLocations), Optional.empty(), pathFilter));
        }

        return COMPLETED_FUTURE;
    }

    private Optional<DeleteDeltaLocations> getDeleteDeltaLocationFor(Path readPath, Optional<DeleteDeltaLocations> allDeleteDeltaLocations)
    {
        if (!allDeleteDeltaLocations.isPresent() || allDeleteDeltaLocations.get().getDeleteDeltas().isEmpty()) {
            return allDeleteDeltaLocations;
        }
        /*
         * Source delta/base files' record can be deleted in only delete_delta directories having greater writeId
         * than source file's writeId.
         * Therefore, skipping delta_directories which lesser/same writeId as source will avoid unnecessary
         * reads and memory.
         */
        Long sourceWriteId = AcidUtils.extractWriteId(readPath);
        sourceWriteId = (sourceWriteId == null) ? 0 : sourceWriteId;
        if (sourceWriteId == 0) {
            return allDeleteDeltaLocations;
        }
        long sId = sourceWriteId.longValue();
        DeleteDeltaLocations allLocations = allDeleteDeltaLocations.get();
        List<WriteIdInfo> filteredWriteIds = allLocations.getDeleteDeltas().stream()
                .filter(writeIdInfo -> writeIdInfo.getMaxWriteId() > sId).collect(Collectors.toList());
        if (filteredWriteIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new DeleteDeltaLocations(allLocations.getPartitionLocation(), filteredWriteIds));
    }

    private ListenableFuture<?> addSplitsToSource(InputSplit[] targetSplits, InternalHiveSplitFactory splitFactory)
            throws IOException
    {
        ListenableFuture<?> lastResult = COMPLETED_FUTURE;
        for (InputSplit inputSplit : targetSplits) {
            Optional<InternalHiveSplit> internalHiveSplit = splitFactory.createInternalHiveSplit((FileSplit) inputSplit);
            if (internalHiveSplit.isPresent()) {
                lastResult = hiveSplitSource.addToQueue(internalHiveSplit.get());
            }
            if (stopped) {
                return COMPLETED_FUTURE;
            }
        }
        return lastResult;
    }

    private static boolean isHudiParquetInputFormat(InputFormat<?, ?> inputFormat)
    {
        if (inputFormat instanceof HoodieParquetRealtimeInputFormat) {
            return false;
        }
        return inputFormat instanceof HoodieParquetInputFormat;
    }

    private static boolean shouldUseFileSplitsFromInputFormat(InputFormat<?, ?> inputFormat)
    {
        return Arrays.stream(inputFormat.getClass().getAnnotations())
                .map(Annotation::annotationType)
                .map(Class::getSimpleName)
                .anyMatch(name -> name.equals("UseFileSplitsFromInputFormat"));
    }

    private Iterator<InternalHiveSplit> createInternalHiveSplitIterator(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, boolean splittable, Optional<DeleteDeltaLocations> deleteDeltaLocations, Optional<Long> startRowOffsetOfFile, PathFilter pathFilter)
    {
        return Streams.stream(new HiveFileIterator(table, path, fileSystem, directoryLister, namenodeStats, recursiveDirWalkerEnabled ? RECURSE : IGNORED, pathFilter))
                .map(status -> splitFactory.createInternalHiveSplit(status, splittable, deleteDeltaLocations, startRowOffsetOfFile))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .iterator();
    }

    private List<InternalHiveSplit> getBucketedSplits(Path path, FileSystem fileSystem, InternalHiveSplitFactory splitFactory, BucketSplitInfo bucketSplitInfo, Optional<BucketConversion> bucketConversion, Optional<DeleteDeltaLocations> deleteDeltaLocations, PathFilter pathFilter)
    {
        int readBucketCount = bucketSplitInfo.getReadBucketCount();
        int tableBucketCount = bucketSplitInfo.getTableBucketCount();
        int partitionBucketCount = bucketConversion.map(BucketConversion::getPartitionBucketCount).orElse(tableBucketCount);
        int bucketCount = max(readBucketCount, partitionBucketCount);

        // list all files in the partition
        List<LocatedFileStatus> files = new ArrayList<>(partitionBucketCount);
        try {
            Iterators.addAll(files, new HiveFileIterator(table, path, fileSystem, directoryLister, namenodeStats, FAIL, pathFilter));
        }
        catch (NestedDirectoryNotAllowedException e) {
            // Fail here to be on the safe side. This seems to be the same as what Hive does
            throw new PrestoException(
                    HiveErrorCode.HIVE_INVALID_BUCKET_FILES,
                    format("Hive table '%s' is corrupt. Found sub-directory in bucket directory for partition: %s",
                            table.getSchemaTableName(),
                            splitFactory.getPartitionName()));
        }

        // build mapping of file name to bucket
        ListMultimap<Integer, LocatedFileStatus> bucketFiles = ArrayListMultimap.create();
        for (LocatedFileStatus file : files) {
            String fileName = file.getPath().getName();
            OptionalInt bucket = getBucketNumber(fileName);
            if (bucket.isPresent()) {
                bucketFiles.put(bucket.getAsInt(), file);
                continue;
            }

            // legacy mode requires exactly one file per bucket
            if (files.size() != partitionBucketCount) {
                throw new PrestoException(HiveErrorCode.HIVE_INVALID_BUCKET_FILES, format(
                        "Hive table '%s' is corrupt. File '%s' does not match the standard naming pattern, and the number " +
                                "of files in the directory (%s) does not match the declared bucket count (%s) for partition: %s",
                        table.getSchemaTableName(),
                        fileName,
                        files.size(),
                        partitionBucketCount,
                        splitFactory.getPartitionName()));
            }

            // sort FileStatus objects per `org.apache.hadoop.hive.ql.metadata.Table#getSortedPaths()`
            files.sort(null);

            // use position in sorted list as the bucket number
            bucketFiles.clear();
            for (int i = 0; i < files.size(); i++) {
                bucketFiles.put(i, files.get(i));
            }
            break;
        }

        // convert files internal splits
        List<InternalHiveSplit> splitList = new ArrayList<>();
        for (int bucketNumber = 0; bucketNumber < bucketCount; bucketNumber++) {
            // Physical bucket #. This determine file name. It also determines the order of splits in the result.
            int partitionBucketNumber = bucketNumber % partitionBucketCount;
            // Logical bucket #. Each logical bucket corresponds to a "bucket" from engine's perspective.
            int readBucketNumber = bucketNumber % readBucketCount;

            boolean containsEligibleTableBucket = false;
            boolean containsIneligibleTableBucket = false;
            for (int tableBucketNumber = bucketNumber % tableBucketCount; tableBucketNumber < tableBucketCount; tableBucketNumber += bucketCount) {
                // table bucket number: this is used for evaluating "$bucket" filters.
                if (bucketSplitInfo.isTableBucketEnabled(tableBucketNumber)) {
                    containsEligibleTableBucket = true;
                }
                else {
                    containsIneligibleTableBucket = true;
                }
            }

            if (containsEligibleTableBucket && containsIneligibleTableBucket) {
                throw new PrestoException(
                        NOT_SUPPORTED,
                        "The bucket filter cannot be satisfied. There are restrictions on the bucket filter when all the following is true: " +
                                "1. a table has a different buckets count as at least one of its partitions that is read in this query; " +
                                "2. the table has a different but compatible bucket number with another table in the query; " +
                                "3. some buckets of the table is filtered out from the query, most likely using a filter on \"$bucket\". " +
                                "(table name: " + table.getTableName() + ", table bucket count: " + tableBucketCount + ", " +
                                "partition bucket count: " + partitionBucketCount + ", effective reading bucket count: " + readBucketCount + ")");
            }
            if (containsEligibleTableBucket) {
                for (LocatedFileStatus file : bucketFiles.get(partitionBucketNumber)) {
                    // OrcDeletedRows will load only delete delta files matching current bucket (same file name),
                    // so we can pass all delete delta locations here, without filtering.
                    splitFactory.createInternalHiveSplit(file, readBucketNumber, deleteDeltaLocations)
                            .ifPresent(splitList::add);
                }
            }
        }
        return splitList;
    }

    static OptionalInt getStatementId(String deleteDeltaFileName)
    {
        Matcher matcher = DELETE_DELTA_PATTERN.matcher(deleteDeltaFileName);
        if (matcher.matches()) {
            String statementId = matcher.group(3);
            if (statementId == null) {
                return OptionalInt.of(-1);
            }
            return OptionalInt.of(Integer.valueOf(statementId.substring(1)));
        }
        return OptionalInt.empty();
    }

    private static List<Path> getTargetPathsFromSymlink(FileSystem fileSystem, Path symlinkDir)
    {
        try {
            FileStatus[] symlinks = fileSystem.listStatus(symlinkDir, HIDDEN_FILES_PATH_FILTER);
            List<Path> targets = new ArrayList<>();

            for (FileStatus symlink : symlinks) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(symlink.getPath()), StandardCharsets.UTF_8))) {
                    CharStreams.readLines(reader).stream()
                            .map(Path::new)
                            .forEach(targets::add);
                }
            }
            return targets;
        }
        catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_BAD_DATA, "Error parsing symlinks from: " + symlinkDir, e);
        }
    }

    private static List<HivePartitionKey> getPartitionKeys(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        List<Column> keys = table.getPartitionColumns();
        List<String> values = partition.get().getValues();
        checkCondition(keys.size() == values.size(), HiveErrorCode.HIVE_INVALID_METADATA, "Expected %s partition key values, but got %s", keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            HiveType hiveType = keys.get(i).getType();
            if (!hiveType.isSupportedType()) {
                throw new PrestoException(NOT_SUPPORTED, format("Unsupported Hive type %s found in partition keys of table %s.%s", hiveType, table.getDatabaseName(), table.getTableName()));
            }
            String value = values.get(i);
            checkCondition(value != null, HiveErrorCode.HIVE_INVALID_PARTITION_VALUE, "partition key value cannot be null for field: %s", name);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    private static Properties getPartitionSchema(Table table, Optional<Partition> partition)
    {
        if (!partition.isPresent()) {
            return getHiveSchema(table);
        }
        return getHiveSchema(partition.get(), table);
    }

    public Table getTable()
    {
        return table;
    }

    public static class BucketSplitInfo
    {
        private final List<HiveColumnHandle> bucketColumns;
        private final int tableBucketCount;
        private final int readBucketCount;
        private final IntPredicate bucketFilter;

        public static Optional<BucketSplitInfo> createBucketSplitInfo(Optional<HiveBucketHandle> bucketHandle, Optional<HiveBucketing.HiveBucketFilter> bucketFilter)
        {
            requireNonNull(bucketHandle, "bucketHandle is null");
            requireNonNull(bucketFilter, "buckets is null");

            if (!bucketHandle.isPresent()) {
                checkArgument(!bucketFilter.isPresent(), "bucketHandle must be present if bucketFilter is present");
                return Optional.empty();
            }

            int localTableBucketCount = bucketHandle.get().getTableBucketCount();
            int localReadBucketCount = bucketHandle.get().getReadBucketCount();

            if (localTableBucketCount != localReadBucketCount && bucketFilter.isPresent()) {
                // TODO: remove when supported
                throw new PrestoException(NOT_SUPPORTED, "Filter on \"$bucket\" is not supported when the table has partitions with different bucket counts");
            }

            List<HiveColumnHandle> localBucketColumns = bucketHandle.get().getColumns();
            IntPredicate predicate = bucketFilter
                    .<IntPredicate>map(filter -> filter.getBucketsToKeep()::contains)
                    .orElse(bucket -> true);
            return Optional.of(new BucketSplitInfo(localBucketColumns, localTableBucketCount, localReadBucketCount, predicate));
        }

        private BucketSplitInfo(List<HiveColumnHandle> bucketColumns, int tableBucketCount, int readBucketCount, IntPredicate bucketFilter)
        {
            this.bucketColumns = ImmutableList.copyOf(requireNonNull(bucketColumns, "bucketColumns is null"));
            this.tableBucketCount = tableBucketCount;
            this.readBucketCount = readBucketCount;
            this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        }

        public List<HiveColumnHandle> getBucketColumns()
        {
            return bucketColumns;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getReadBucketCount()
        {
            return readBucketCount;
        }

        /**
         * Evaluates whether the provided table bucket number passes the bucket predicate.
         * A bucket predicate can be present in two cases:
         * <ul>
         * <li>Filter on "$bucket" column. e.g. {@code "$bucket" between 0 and 100}
         * <li>Single-value equality filter on all bucket columns. e.g. for a table with two bucketing columns,
         * {@code bucketCol1 = 'a' AND bucketCol2 = 123}
         * </ul>
         */
        public boolean isTableBucketEnabled(int tableBucketNumber)
        {
            return bucketFilter.test(tableBucketNumber);
        }
    }
}
