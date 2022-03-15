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
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.concurrent.MoreFutures;
import io.airlift.json.JsonCodec;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.HiveVacuumTableHandle.Range;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.PageIndexer;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSink;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.ParsedDelta;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_TOO_MANY_OPEN_PARTITIONS;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HivePageSink
        implements ConnectorPageSink
{
    private static final Logger log = Logger.get(HivePageSink.class);

    private static final int MAX_PAGE_POSITIONS = 4096;

    private final HiveWriterFactory writerFactory;

    private final int[] dataColumnInputIndex; // ordinal of columns (not counting sample weight column)
    private final int[] partitionColumnsInputIndex; // ordinal of columns (not counting sample weight column)
    private final int rowIdColumnIndex;
    private final HiveACIDWriteType acidWriteType;

    private final int[] bucketColumns;
    private final HiveBucketFunction bucketFunction;

    private final HiveWriterPagePartitioner pagePartitioner;
    private final HdfsEnvironment hdfsEnvironment;

    private final int maxOpenWriters;
    private final ListeningExecutorService writeVerificationExecutor;

    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;

    private final List<HiveWriter> writers = new ArrayList<>();

    // Snapshot: parameters used to construct writer instances
    private final List<WriterParam> writerParams = new ArrayList<>();

    protected final ConnectorSession session;
    private final List<Block> nullBlocks;

    private long rows;
    private long writtenBytes;
    private long systemMemoryUsage;
    private long validationCpuNanos;
    protected final List<HiveColumnHandle> inputColumns;
    private final TypeManager typeManager;
    protected final HiveWritableTableHandle writableTableHandle;
    private final ThreadLocal<Map<String, Options>> vacuumOptionsMap = ThreadLocal.withInitial(() -> null);
    private VaccumOp vacuumOp;

    public HivePageSink(
            HiveWriterFactory writerFactory,
            List<HiveColumnHandle> inputColumns,
            Optional<HiveBucketProperty> bucketProperty,
            PageIndexerFactory pageIndexerFactory,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            int maxOpenWriters,
            ListeningExecutorService writeVerificationExecutor,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            ConnectorSession session,
            HiveACIDWriteType acidWriteType,
            HiveWritableTableHandle handle)
    {
        this.writerFactory = requireNonNull(writerFactory, "writerFactory is null");
        this.acidWriteType = acidWriteType;
        this.writableTableHandle = requireNonNull(handle, "hive table handle is null");

        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null");
        this.typeManager = requireNonNull(typeManager, "typemMnager is null");

        requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.maxOpenWriters = maxOpenWriters;
        this.writeVerificationExecutor = requireNonNull(writeVerificationExecutor, "writeVerificationExecutor is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");

        requireNonNull(bucketProperty, "bucketProperty is null");
        this.pagePartitioner = new HiveWriterPagePartitioner(
                inputColumns,
                bucketProperty.isPresent() ||
                        (handle.getTableStorageFormat() == HiveStorageFormat.ORC &&
                                HiveACIDWriteType.isRowIdNeeded(acidWriteType) &&
                                HiveACIDWriteType.VACUUM_UNIFY != acidWriteType &&
                                !isInsertOnlyTable()),
                isVacuumOperationValid() && !isInsertOnlyTable(),
                pageIndexerFactory,
                typeManager);

        // determine the input index of the partition columns and data columns
        // and determine the input index and type of bucketing columns
        ImmutableList.Builder<Integer> partitionColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> dataColumnsInputIndex = ImmutableList.builder();
        ImmutableList.Builder<Type> dataColumnTypes = ImmutableList.builder();
        Object2IntMap<String> dataColumnNameToIdMap = new Object2IntOpenHashMap<>();
        Map<String, HiveType> dataColumnNameToTypeMap = new HashMap<>();
        // sample weight column is passed separately, so index must be calculated without this column
        int inputIndex;
        for (inputIndex = 0; inputIndex < inputColumns.size(); inputIndex++) {
            HiveColumnHandle column = inputColumns.get(inputIndex);
            if (column.isPartitionKey()) {
                partitionColumns.add(inputIndex);
            }
            else {
                dataColumnsInputIndex.add(inputIndex);
                dataColumnNameToIdMap.put(column.getName(), inputIndex);
                dataColumnNameToTypeMap.put(column.getName(), column.getHiveType());
                dataColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
        }
        rowIdColumnIndex = HiveACIDWriteType.isRowIdNeeded(acidWriteType) ? inputIndex : -1;
        this.partitionColumnsInputIndex = Ints.toArray(partitionColumns.build());
        this.dataColumnInputIndex = Ints.toArray(dataColumnsInputIndex.build());

        if (bucketProperty.isPresent()) {
            BucketingVersion bucketingVersion = bucketProperty.get().getBucketingVersion();
            int bucketCount = bucketProperty.get().getBucketCount();
            bucketColumns = bucketProperty.get().getBucketedBy().stream()
                    .mapToInt(dataColumnNameToIdMap::get)
                    .toArray();
            List<HiveType> bucketColumnTypes = bucketProperty.get().getBucketedBy().stream()
                    .map(dataColumnNameToTypeMap::get)
                    .collect(toList());
            bucketFunction = new HiveBucketFunction(bucketingVersion, bucketCount, bucketColumnTypes);
        }
        else if (handle.getTableStorageFormat() == HiveStorageFormat.ORC &&
                HiveACIDWriteType.isRowIdNeeded(acidWriteType) &&
                !isInsertOnlyTable()) {
            bucketColumns = new int[]{rowIdColumnIndex};
            bucketFunction = new HiveBucketFunction(BucketingVersion.BUCKETING_V2,
                    HiveBucketing.MAX_BUCKET_NUMBER,
                    ImmutableList.of(HiveColumnHandle.updateRowIdHandle().getHiveType()),
                    true);
        }
        else {
            bucketColumns = null;
            bucketFunction = null;
        }

        if (acidWriteType == HiveACIDWriteType.DELETE) {
            //Null blocks will be used in case of delete
            ImmutableList.Builder<Block> localNullBlocks = ImmutableList.builder();
            for (Type dataColumnType : dataColumnTypes.build()) {
                BlockBuilder blockBuilder = dataColumnType.createBlockBuilder(null, 1, 0);
                blockBuilder.appendNull();
                localNullBlocks.add(blockBuilder.build());
            }
            this.nullBlocks = localNullBlocks.build();
        }
        else {
            this.nullBlocks = ImmutableList.of();
        }

        this.session = requireNonNull(session, "session is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getRowsWritten()
    {
        return rows;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return systemMemoryUsage;
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#commit -> RecordWriter#close
        ListenableFuture<Collection<Slice>> result = hdfsEnvironment.doAs(session.getUser(), this::doFinish);
        if (!session.isSnapshotEnabled()) {
            return MoreFutures.toCompletableFuture(result);
        }

        // Will merge all sub files and call doFinish again, so clear the total bytes
        writtenBytes = 0;
        ListenableFuture<Collection<Slice>> mergedResult = hdfsEnvironment.doAs(session.getUser(), this::mergeFiles);
        // Use mergedResult as return value (indexed at 1)
        return MoreFutures.toCompletableFuture(Futures.transform(Futures.allAsList(result, mergedResult), results -> results.get(1), directExecutor()));
    }

    private ListenableFuture<Collection<Slice>> doFinish()
    {
        ImmutableList.Builder<Slice> partitionUpdates = ImmutableList.builder();
        List<Callable<Object>> verificationTasks = new ArrayList<>();
        for (HiveWriter writer : writers) {
            if (writer == null) {
                continue;
            }
            writer.commit();
            PartitionUpdate partitionUpdate = writer.getPartitionUpdate();
            partitionUpdates.add(wrappedBuffer(partitionUpdateCodec.toJsonBytes(partitionUpdate)));
            writer.getVerificationTask()
                    .map(Executors::callable)
                    .ifPresent(verificationTasks::add);
        }
        List<Slice> result = partitionUpdates.build();

        writtenBytes += writers.stream()
                .filter(Objects::nonNull)
                .mapToLong(HiveWriter::getWrittenBytes)
                .sum();
        validationCpuNanos += writers.stream()
                .filter(Objects::nonNull)
                .mapToLong(HiveWriter::getValidationCpuNanos)
                .sum();
        writers.clear();

        if (vacuumOp != null) {
            vacuumOp.close();
        }

        if (verificationTasks.isEmpty()) {
            return Futures.immediateFuture(result);
        }

        try {
            List<ListenableFuture<?>> futures = writeVerificationExecutor.invokeAll(verificationTasks).stream()
                    .map(future -> (ListenableFuture<?>) future)
                    .collect(toList());
            return Futures.transform(Futures.allAsList(futures), input -> result, directExecutor());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    private ListenableFuture<Collection<Slice>> mergeFiles()
    {
        // When snapshot is enabled, each snapshot produces a sub file, named file.n, for each "writer",
        // where "file" is what the file would be named without snapshot, and n is a number starting from 0.
        // Need to merge these sub files to a single one at the end.
        checkState(writers.isEmpty());
        try {
            for (WriterParam param : writerParams) {
                // Construct writers for the merged files
                HiveWriter hiveWriter = writerFactory.createWriterForSnapshotMerge(param.partitionValues, param.bucket, Optional.empty());
                writers.add(hiveWriter);
            }
            writerFactory.mergeSubFiles(writers);
            // Finish up writers for merged files, to get final results and stats
            return doFinish();
        }
        catch (IOException e) {
            log.debug("exception '%s' while merging subfile", e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void abort()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#rollback -> RecordWriter#close
        hdfsEnvironment.doAs(session.getUser(), () -> doAbort(false));
    }

    @Override
    public void cancelToResume()
    {
        // Must be wrapped in doAs entirely
        // Implicit FileSystem initializations are possible in HiveRecordWriter#Cancel -> RecordWriter#close
        hdfsEnvironment.doAs(session.getUser(), () -> doAbort(true));
    }

    private void doAbort(boolean isCancel)
    {
        Optional<Exception> rollbackException = Optional.empty();
        for (HiveWriter writer : writers) {
            // writers can contain nulls if an exception is thrown when doAppend expends the writer list
            if (writer != null) {
                try {
                    writer.rollback(isCancel);
                }
                catch (Exception e) {
                    log.warn("exception '%s' while rollback on %s", e, writer);
                    rollbackException = Optional.of(e);
                }
            }
        }
        writers.clear();

        if (rollbackException.isPresent()) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", rollbackException.get());
        }
    }

    @Override
    public VacuumResult vacuum(ConnectorPageSourceProvider pageSourceProvider,
                               ConnectorTransactionHandle transactionHandle,
                               ConnectorTableHandle connectorTableHandle,
                               List<ConnectorSplit> splits)
    {
        if (vacuumOp == null) {
            vacuumOp = new VaccumOp(pageSourceProvider, transactionHandle, connectorTableHandle, splits);
        }
        Page page = vacuumOp.processNext();
        return new VacuumResult(page, vacuumOp.isFinished());
    }

    private class VaccumOp
    {
        List<ConnectorPageSource> pageSources;
        Iterator<Page> sortedPagesForVacuum;
        Set<Integer> allBucketList = new HashSet<>();
        Map<String, Set<Integer>> partitionToBuckets = new HashMap<>();
        Map<String, List<HivePartitionKey>> partitionToKeys = new HashMap<>();
        AtomicInteger rowsWritten = new AtomicInteger();
        AtomicBoolean emptyfileWriten = new AtomicBoolean();

        private VaccumOp(ConnectorPageSourceProvider pageSourceProvider,
                         ConnectorTransactionHandle transactionHandle,
                         ConnectorTableHandle connectorTableHandle,
                         List<ConnectorSplit> splits)
        {
            List<HiveSplit> hiveSplits = new ArrayList<>();
            splits.forEach(split -> {
                hiveSplits.addAll(((HiveSplitWrapper) split).getSplits());
            });
            vacuumOptionsMap.set(initVacuumOptions(hiveSplits));
            //In case of no
            hiveSplits.stream().forEach(split -> {
                String partitionName = split.getPartitionName();
                if (partitionName != null && !partitionName.isEmpty()) {
                    Set<Integer> partitionBuckets = partitionToBuckets.computeIfAbsent(partitionName,
                            (partition) -> new HashSet<>());
                    List<HivePartitionKey> partitionKeys = split.getPartitionKeys();
                    partitionToKeys.put(partitionName, partitionKeys);
                    partitionBuckets.add(split.getBucketNumber().orElse(0));
                }
                allBucketList.add(split.getBucketNumber().orElse(0));
            });

            List<ColumnHandle> localInputColumns = new ArrayList<>(HivePageSink.this.inputColumns);
            if (isInsertOnlyTable() || acidWriteType == HiveACIDWriteType.VACUUM_UNIFY) {
                //Insert only tables Just need to merge contents together. No processing required.
                //During vacuum unify, all buckets will be merged to one.
                //There is no need to sort again. sort_by is valid only on bucketed table,
                //for which VACUUM_UNIFY is not valid.
                List<HiveSplitWrapper> multiSplits = hiveSplits.stream()
                        .map(HiveSplitWrapper::wrap)
                        .collect(toList());
                if (isInsertOnlyTable()) {
                    Collections.sort(multiSplits, this::compareInsertOnlySplits);
                }
                else if (acidWriteType == HiveACIDWriteType.VACUUM_UNIFY) {
                    HiveColumnHandle rowIdHandle = HiveColumnHandle.updateRowIdHandle();
                    localInputColumns.add(rowIdHandle);
                }

                pageSources = multiSplits.stream()
                        .map(split -> pageSourceProvider.createPageSource(
                                transactionHandle, session, split, connectorTableHandle, localInputColumns))
                        .collect(toList());
                List<Iterator<Page>> pageSourceIterators = HiveUtil.getPageSourceIterators(pageSources);
                sortedPagesForVacuum = Iterators.concat(pageSourceIterators.iterator());
            }
            else {
                HiveColumnHandle rowIdHandle = HiveColumnHandle.updateRowIdHandle();
                localInputColumns.add(rowIdHandle);

                List<HiveSplitWrapper> multiSplits = hiveSplits.stream()
                        .map(HiveSplitWrapper::wrap)
                        .collect(toList());

                pageSources = multiSplits.stream()
                        .map(split -> pageSourceProvider.createPageSource(
                                transactionHandle, session, split, connectorTableHandle, localInputColumns))
                        .collect(toList());
                List<Type> columnTypes = localInputColumns.stream()
                        .map(c -> ((HiveColumnHandle) c).getHiveType().getType(typeManager))
                        .collect(toList());
                //Last index for rowIdHandle
                List<Integer> sortFields = ImmutableList.of(localInputColumns.size() - 1);
                sortedPagesForVacuum = HiveUtil.getMergeSortedPages(pageSources, columnTypes, sortFields,
                        ImmutableList.of(SortOrder.ASC_NULLS_FIRST));
            }
        }

        Page processNext()
        {
            if (sortedPagesForVacuum.hasNext()) {
                Page page = sortedPagesForVacuum.next();
                appendPage(page);
                rowsWritten.addAndGet(page.getPositionCount());
                return page;
            }
            if (rowsWritten.get() == 0) {
                //In case this partition/table have 0 rows, then create empty file.
                if (partitionToBuckets.isEmpty()) {
                    createEmptyFiles(ImmutableList.of(), allBucketList);
                }
                else {
                    partitionToBuckets.entrySet().stream().forEach(entry -> {
                        String partitionName = entry.getKey();
                        List<HivePartitionKey> partitionKeys = partitionToKeys.get(partitionName);
                        Set<Integer> buckets = entry.getValue();
                        createEmptyFiles(partitionKeys, buckets);
                    });
                }
            }
            return null;
        }

        /*
         * When all rows of table are deleted, then vacuum will not generate any of the compacted file.
         * So at the end, need to generate empty bucket files in base directory to indicate all rows are deleted.
         */
        private synchronized void createEmptyFiles(List<HivePartitionKey> partitionKeys, Set<Integer> bucketNumbers)
        {
            if (emptyfileWriten.get()) {
                return;
            }
            PageBuilder builder;
            if (partitionKeys != null && !partitionKeys.isEmpty()) {
                List<Type> partitionTypes = inputColumns.stream()
                        .filter(HiveColumnHandle::isPartitionKey)
                        .map(HiveColumnHandle::getHiveType)
                        .map((t) -> t.getType(typeManager))
                        .collect(toList());
                builder = new PageBuilder(partitionTypes);
                for (int i = 0; i < partitionKeys.size(); i++) {
                    HivePartitionKey partitionKey = partitionKeys.get(i);
                    Type type = partitionTypes.get(i);
                    Object partitionColumnValue = HiveUtil.typedPartitionKey(partitionKey.getValue(), type, partitionKey.getName());
                    RunLengthEncodedBlock block = RunLengthEncodedBlock.create(type, partitionColumnValue, 1);
                    type.appendTo(block, 0, builder.getBlockBuilder(i));
                }
                builder.declarePosition();
            }
            else {
                builder = new PageBuilder(ImmutableList.of());
            }
            Page partitionColumns = builder.build();
            String partitionName = writerFactory.getPartitionName(partitionColumns, 0).orElse(HivePartition.UNPARTITIONED_ID);
            bucketNumbers.forEach((bucket) -> {
                List<String> partitionValues = writerFactory.getPartitionValues(partitionColumns, 0);
                writers.add(writerFactory.createWriter(partitionValues, OptionalInt.of(bucket), getVacuumOptions(partitionName)));
                // TODO-cp-I2BZ0A: vacuum is not supported with snapshot
                writerParams.add(null);
            });
            emptyfileWriten.compareAndSet(false, true);
        }

        boolean isFinished()
        {
            return !sortedPagesForVacuum.hasNext();
        }

        private Map<String, Options> initVacuumOptions(List<HiveSplit> hiveSplits)
        {
            return hdfsEnvironment.doAs(session.getUser(), () -> {
                Map<String, Options> localVacuumOptionsMap = new HashMap<>();
                //Findout the minWriteId and maxWriteId for current compaction.
                HiveVacuumTableHandle vacuumTableHandle = (HiveVacuumTableHandle) writableTableHandle;
                for (HiveSplit split : hiveSplits) {
                    String partition = split.getPartitionName();
                    Options options = localVacuumOptionsMap.get(partition);
                    if (options == null) {
                        options = new Options(writerFactory.getConf()).maximumWriteId(-1).minimumWriteId(Long.MAX_VALUE);
                        localVacuumOptionsMap.put(partition, options);
                    }
                    if (vacuumTableHandle.isFullVacuum()) {
                        //Major compaction, need to write the base
                        options.writingBase(true);
                        Range range = getOnlyElement(vacuumTableHandle.getRanges().get(partition));
                        options.minimumWriteId(range.getMin());
                        if (vacuumTableHandle.isUnifyVacuum()) {
                            options.maximumWriteId(vacuumTableHandle.getLocationHandle().getJsonSerializablewriteIdInfo().get().getMaxWriteId());
                        }
                        else {
                            options.maximumWriteId(range.getMax());
                        }
                        Path bucketFile = new Path(split.getPath());
                        OptionalInt bucketNumber = vacuumTableHandle.isUnifyVacuum() ? OptionalInt.of(0) : HiveUtil.getBucketNumber(bucketFile.getName());
                        if (bucketNumber.isPresent()) {
                            options.bucket(bucketNumber.getAsInt());
                        }
                        else {
                            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Error while parsing split info for vacuum");
                        }
                    }
                    else {
                        Path bucketFile = new Path(split.getPath());
                        try {
                            Options currentOptions = new Options(writerFactory.getConf());
                            if (isInsertOnlyTable()) {
                                Path parent = bucketFile.getParent();
                                if (parent.getName().startsWith(AcidUtils.BASE_PREFIX)) {
                                    long baseWriteId = AcidUtils.parseBase(parent);
                                    currentOptions.writingBase(true);
                                    currentOptions.minimumWriteId(0);
                                    currentOptions.maximumWriteId(baseWriteId);
                                }
                                else if (parent.getName().startsWith(AcidUtils.DELTA_PREFIX)) {
                                    ParsedDelta parsedDelta = AcidUtils.parsedDelta(parent, parent.getFileSystem(writerFactory.getConf()));
                                    currentOptions.maximumWriteId(parsedDelta.getMaxWriteId());
                                    currentOptions.minimumWriteId(parsedDelta.getMinWriteId());
                                }
                            }
                            else {
                                currentOptions = AcidUtils.parseBaseOrDeltaBucketFilename(bucketFile, writerFactory.getConf());
                            }
                            if (currentOptions.isWritingBase() || options.isWritingBase()) {
                                options.writingBase(true);
                            }
                            else if (options.isWritingDeleteDelta() || AcidUtils.isDeleteDelta(bucketFile.getParent())) {
                                options.writingDeleteDelta(true);
                            }

                            if (currentOptions.getMinimumWriteId() < options.getMinimumWriteId()) {
                                options.minimumWriteId(currentOptions.getMinimumWriteId());
                            }
                            if (currentOptions.getMaximumWriteId() > options.getMaximumWriteId()) {
                                options.maximumWriteId(currentOptions.getMaximumWriteId());
                            }
                            options.bucket(currentOptions.getBucketId());
                        }
                        catch (IOException e) {
                            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, "Error while parsing split info for vacuum", e);
                        }
                        Range suitableRange = getOnlyElement(vacuumTableHandle.getRanges().get(partition));
                        options.minimumWriteId(suitableRange.getMin());
                        options.maximumWriteId(suitableRange.getMax());
                    }
                }
                return localVacuumOptionsMap;
            });
        }

        void close()
        {
            pageSources.forEach(c -> {
                try {
                    c.close();
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        /**
         * Compares the INSERT_ONLY transactional table's splits to read in the same sequence of insert.
         */
        private int compareInsertOnlySplits(HiveSplitWrapper o1, HiveSplitWrapper o2)
        {
            if (o1.getFilePath().equals(o2.getFilePath())) {
                //same file different splits.
                return Long.compare(o1.getStartIndex(), o2.getStartIndex());
            }
            Path p1 = new Path(o1.getFilePath());
            Path p2 = new Path(o2.getFilePath());
            String p1Parent = p1.getParent().getName();
            String p2Parent = p2.getParent().getName();
            if (p1Parent.equals(p2Parent)) {
                //Same parent
                return p1.getName().compareTo(p2.getName());
            }
            boolean isP1AcidDir = p1Parent.startsWith(AcidUtils.BASE_PREFIX) || p1Parent.startsWith(AcidUtils.DELTA_PREFIX);
            boolean isP2AcidDir = p2Parent.startsWith(AcidUtils.BASE_PREFIX) || p2Parent.startsWith(AcidUtils.DELTA_PREFIX);
            if (isP1AcidDir && isP2AcidDir) {
                //Both are ACID inserts
                if (p1Parent.startsWith(AcidUtils.BASE_PREFIX)) {
                    //base will have  higher priority
                    return -1;
                }
                else if (p2Parent.startsWith(AcidUtils.BASE_PREFIX)) {
                    return 1;
                }
                return p1Parent.compareTo(p2Parent);
            }
            //Both are not acid
            if (!isP1AcidDir && !isP2AcidDir) {
                return p1.getName().compareTo(p2.getName());
            }
            //o1 is Original
            if (!isP1AcidDir) {
                return -1;
            }
            return 1;
        }
    }

    private Optional<Options> getVacuumOptions(String partition)
    {
        Map<String, Options> partitionToOptions = vacuumOptionsMap.get();
        if (partitionToOptions == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(partitionToOptions.get(partition));
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        if (page.getPositionCount() > 0) {
            // Must be wrapped in doAs entirely
            // Implicit FileSystem initializations are possible in HiveRecordWriter#addRow or #createWriter
            hdfsEnvironment.doAs(session.getUser(), () -> doAppend(page));
        }

        return NOT_BLOCKED;
    }

    private void doAppend(Page inputPage)
    {
        Page page = inputPage;
        while (page.getPositionCount() > MAX_PAGE_POSITIONS) {
            Page chunk = page.getRegion(0, MAX_PAGE_POSITIONS);
            page = page.getRegion(MAX_PAGE_POSITIONS, page.getPositionCount() - MAX_PAGE_POSITIONS);
            writePage(chunk);
        }

        writePage(page);
    }

    private void writePage(Page page)
    {
        int[] writerIndexes = getWriterIndexes(page);

        // position count for each writer
        int[] sizes = new int[writers.size()];
        for (int index : writerIndexes) {
            sizes[index]++;
        }

        // record which positions are used by which writer
        int[][] writerPositions = new int[writers.size()][];
        int[] counts = new int[writers.size()];

        for (int position = 0; position < page.getPositionCount(); position++) {
            int index = writerIndexes[position];

            int count = counts[index];
            if (count == 0) {
                writerPositions[index] = new int[sizes[index]];
            }
            writerPositions[index][count] = position;
            counts[index] = count + 1;
        }

        // invoke the writers
        Page dataPage = getDataPage(page);
        for (int index = 0; index < writerPositions.length; index++) {
            int[] positions = writerPositions[index];
            if (positions == null) {
                continue;
            }

            // If write is partitioned across multiple writers, filter page using dictionary blocks
            Page pageForWriter = dataPage;
            if (positions.length != dataPage.getPositionCount()) {
                verify(positions.length == counts[index]);
                pageForWriter = pageForWriter.getPositions(positions, 0, positions.length);
            }

            HiveWriter writer = writers.get(index);

            long currentWritten = writer.getWrittenBytes();
            long currentMemory = writer.getSystemMemoryUsage();

            writer.append(pageForWriter);

            writtenBytes += (writer.getWrittenBytes() - currentWritten);
            systemMemoryUsage += (writer.getSystemMemoryUsage() - currentMemory);
        }
        rows += page.getPositionCount();
    }

    private int[] getWriterIndexes(Page page)
    {
        Page partitionColumns = extractColumns(page, partitionColumnsInputIndex);
        Block bucketBlock = buildBucketBlock(page);
        Block operationIdBlock = buildAcidOperationBlock(page);
        int[] writerIndexes = pagePartitioner.partitionPage(partitionColumns, bucketBlock, operationIdBlock);
        if (pagePartitioner.getMaxIndex() >= maxOpenWriters) {
            throw new PrestoException(HIVE_TOO_MANY_OPEN_PARTITIONS, format("Exceeded limit of %s open writers for partitions/buckets", maxOpenWriters));
        }

        // expand writers list to new size
        while (writers.size() <= pagePartitioner.getMaxIndex()) {
            writers.add(null);
        }
        // The 2 lists may start with different sizes (e.g. writer is 0 after resume; but writerParams has entries),
        // They will end up with same size after the loops
        while (writerParams.size() <= pagePartitioner.getMaxIndex()) {
            writerParams.add(null);
        }

        // create missing writers
        for (int position = 0; position < page.getPositionCount(); position++) {
            int writerIndex = writerIndexes[position];
            if (writers.get(writerIndex) != null) {
                continue;
            }

            Optional<String> partitionName = writerFactory.getPartitionName(partitionColumns, position);
            String partition = partitionName.orElse(HivePartition.UNPARTITIONED_ID);
            OptionalInt bucketNumber = OptionalInt.empty();
            Optional<Options> partitionOptions = getVacuumOptions(partition);
            if (bucketBlock != null) {
                bucketNumber = OptionalInt.of(bucketBlock.getInt(position, 0));
            }
            else if (acidWriteType == HiveACIDWriteType.VACUUM_UNIFY) {
                bucketNumber = OptionalInt.of(0);
            }
            else if (isVacuumOperationValid() && isInsertOnlyTable()) {
                bucketNumber = OptionalInt.of(partitionOptions.get().getBucketId());
            }
            else if (session.getTaskId().isPresent() && writerFactory.isTxnTable()) {
                //Use the taskId and driverId to get bucketId for ACID table
                bucketNumber = generateBucketNumber(partitionColumns.getChannelCount() != 0);
            }
            List<String> partitionValues = writerFactory.getPartitionValues(partitionColumns, position);
            HiveWriter writer = writerFactory.createWriter(partitionValues, bucketNumber, partitionOptions);
            // Snapshot: record what parameters were used to construct the writer. Vacuum is not supported currently.
            writerParams.set(writerIndex, new WriterParam(partitionValues, bucketNumber, writer.getFilePath()));
            writers.set(writerIndex, writer);
        }
        verify(writers.size() == pagePartitioner.getMaxIndex() + 1);
        // After snapshots are taken, the writer list is cleared. New pages may skip over certain writer indexes.
        verify(session.isSnapshotEnabled() || !writers.contains(null));

        return writerIndexes;
    }

    private OptionalInt generateBucketNumber(boolean isPartitionedTable)
    {
        if (session.getTaskId().isPresent() && session.getDriverId().isPresent() && writerFactory.isTxnTable() &&
                (!(isPartitionedTable && HiveSessionProperties.isWritePartitionDistributionEnabled(session)))) {
            int taskId = session.getTaskId().getAsInt();
            int driverId = session.getDriverId().getAsInt();
            int taskWriterCount = session.getTaskWriterCount();
            //taskId starts from 0.
            //driverId starts from 0 and will be < taskWriterCount.
            //taskWriterCount starts from 1
            // for taskId n, buckets will be between n*taskWriterCount (inclusive) and (n+1)*taskWriterCount (exclusive)
            int bucketNumber = taskId * taskWriterCount + driverId;
            return OptionalInt.of(bucketNumber);
        }
        return OptionalInt.empty();
    }

    private Page getDataPage(Page page)
    {
        Block[] blocks = null;
        if (HiveACIDWriteType.isRowIdNeeded(acidWriteType) && !isInsertOnlyTable()) {
            //For UPDATE/DELETE source page will have extra block for row_id column.
            blocks = new Block[dataColumnInputIndex.length + 1];
            blocks[dataColumnInputIndex.length] = page.getBlock(rowIdColumnIndex);
        }
        else {
            blocks = new Block[dataColumnInputIndex.length];
        }
        for (int i = 0; i < dataColumnInputIndex.length; i++) {
            if (acidWriteType == HiveACIDWriteType.DELETE) {
                //For delete remaining data not required as these will be ignored during write.
                //But this will reduce the data size of sort buffer
                blocks[i] = new RunLengthEncodedBlock(nullBlocks.get(i), page.getPositionCount());
            }
            else {
                int dataColumn = dataColumnInputIndex[i];
                blocks[i] = page.getBlock(dataColumn);
            }
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private Block buildBucketBlock(Page page)
    {
        if (acidWriteType == HiveACIDWriteType.VACUUM_UNIFY) {
            //There is no pre bucket block in case of unify
            return null;
        }
        if (bucketFunction == null) {
            return null;
        }

        IntArrayBlockBuilder bucketColumnBuilder = new IntArrayBlockBuilder(null, page.getPositionCount());
        Page bucketColumnsPage = extractColumns(page, bucketColumns);
        for (int position = 0; position < page.getPositionCount(); position++) {
            int bucket = bucketFunction.getBucket(bucketColumnsPage, position);
            bucketColumnBuilder.writeInt(bucket);
        }
        return bucketColumnBuilder.build();
    }

    /**
     * In case of minor VACUUM there is a chance of delete_delta files to be written along with delta_files.
     * partition and bucket value can be same for both delete_delta and delete, so to differentiate these writers
     * include another column 'operationId' for hashing.
     */
    private Block buildAcidOperationBlock(Page page)
    {
        if (!isVacuumOperationValid() || isInsertOnlyTable()) {
            //Supported only for ORC format
            return null;
        }

        IntArrayBlockBuilder operationIdBuilder = new IntArrayBlockBuilder(null, page.getPositionCount());
        Block rowIdBlock = page.getBlock(rowIdColumnIndex);
        for (int position = 0; position < page.getPositionCount(); position++) {
            RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(position);
            int operationId = rowBlock.getRawFieldBlocks()[4].getInt(0, 0);
            if (operationId == HiveACIDWriteType.DELETE.getOperationId()) {
                operationIdBuilder.writeInt(operationId);
            }
            else {
                operationIdBuilder.writeInt(0);
            }
        }
        return operationIdBuilder.build();
    }

    private boolean isVacuumOperationValid()
    {
        return HiveACIDWriteType.isVacuum(acidWriteType) &&
                writableTableHandle != null &&
                writableTableHandle.getTableStorageFormat() == HiveStorageFormat.ORC;
    }

    private boolean isInsertOnlyTable()
    {
        Optional<Table> table = writableTableHandle.getPageSinkMetadata().getTable();
        return table.isPresent() && AcidUtils.isInsertOnlyTable(table.get().getParameters());
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    private static class HiveWriterPagePartitioner
    {
        private final PageIndexer pageIndexer;

        public HiveWriterPagePartitioner(
                List<HiveColumnHandle> inputColumns,
                boolean bucketed,
                boolean isVacuum,
                PageIndexerFactory pageIndexerFactory,
                TypeManager typeManager)
        {
            requireNonNull(inputColumns, "inputColumns is null");
            requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");

            List<Type> partitionColumnTypes = inputColumns.stream()
                    .filter(HiveColumnHandle::isPartitionKey)
                    .map(column -> typeManager.getType(column.getTypeSignature()))
                    .collect(toList());

            if (bucketed) {
                partitionColumnTypes.add(INTEGER);
            }

            if (isVacuum) {
                partitionColumnTypes.add(INTEGER);
            }

            this.pageIndexer = pageIndexerFactory.createPageIndexer(partitionColumnTypes);
        }

        public int[] partitionPage(Page inputPartitionColumns, Block bucketBlock, Block operationIdBlock)
        {
            Page partitionColumns = inputPartitionColumns;
            if (bucketBlock != null) {
                Block[] blocks = new Block[partitionColumns.getChannelCount() + 1];
                for (int i = 0; i < partitionColumns.getChannelCount(); i++) {
                    blocks[i] = partitionColumns.getBlock(i);
                }
                blocks[blocks.length - 1] = bucketBlock;
                partitionColumns = new Page(partitionColumns.getPositionCount(), blocks);
            }
            if (operationIdBlock != null) {
                Block[] blocks = new Block[partitionColumns.getChannelCount() + 1];
                for (int i = 0; i < partitionColumns.getChannelCount(); i++) {
                    blocks[i] = partitionColumns.getBlock(i);
                }
                blocks[blocks.length - 1] = operationIdBlock;
                partitionColumns = new Page(partitionColumns.getPositionCount(), blocks);
            }
            return pageIndexer.indexPage(partitionColumns);
        }

        public int getMaxIndex()
        {
            return pageIndexer.getMaxIndex();
        }
    }

    @Override
    public Object capture(BlockEncodingSerdeProvider serdeProvider)
    {
        // TODO-cp-I2BZ0A: What to do about this? How to capture its state?
        checkState(vacuumOp == null);

        try {
            // Commit current set of sub files. New writers will be created for new sub files.
            hdfsEnvironment.doAs(session.getUser(), this::doFinish).get();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        // TODO-cp-I2BZ0A: ClassNotFoundException when using "State" class, because Hive classes are from a different classloader
        Map<String, Object> state = new HashMap<>();
        state.put("pagePartitioner", pagePartitioner.pageIndexer.capture(serdeProvider));
        state.put("writerFactory", writerFactory.capture());
        state.put("writerParams", new ArrayList<>(writerParams.stream().map(p -> {
            Map<String, Object> map = new HashMap<>();
            map.put("partitionValues", p.partitionValues);
            map.put("bucket", p.bucket.isPresent() ? p.bucket.getAsInt() : null);
            map.put("filePath", p.filePath);
            return map;
        }).collect(toList())));
        state.put("rows", rows);
        state.put("writtenBytes", writtenBytes);
        state.put("systemMemoryUsage", systemMemoryUsage);
        state.put("validationCpuNanos", validationCpuNanos);
        return state;
    }

    @Override
    public void restore(Object obj, BlockEncodingSerdeProvider serdeProvider, long resumeCount)
    {
        checkState(writers.isEmpty());
        // TODO-cp-I2BZ0A: ClassNotFoundException when using "State" class, because Hive classes are from a different classloader
        Map<String, Object> state = (Map<String, Object>) obj;
        pagePartitioner.pageIndexer.restore(state.get("pagePartitioner"), serdeProvider);
        writerFactory.restore(state.get("writerFactory"), resumeCount);
        writerParams.clear();
        writerParams.addAll(((List<Map<String, Object>>) state.get("writerParams")).stream().map(p -> {
            return new WriterParam(
                    (List<String>) p.get("partitionValues"),
                    p.get("bucket") == null ? OptionalInt.empty() : OptionalInt.of((Integer) p.get("bucket")),
                    (String) p.get("filePath"));
        }).collect(toList()));
        rows = (Long) state.get("rows");
        writtenBytes = (Long) state.get("writtenBytes");
        systemMemoryUsage = (Long) state.get("systemMemoryUsage");
        validationCpuNanos = (Long) state.get("validationCpuNanos");
    }

    private static class State
            implements Serializable
    {
        private Object pagePartitioner;
        private Object writerFactory;
        private List<WriterParam> writerParams;
        private long rows;
        private long writtenBytes;
        private long systemMemoryUsage;
        private long validationCpuNanos;
    }

    private static class WriterParam
            implements Serializable
    {
        private final List<String> partitionValues;
        private final OptionalInt bucket;
        private final String filePath;

        private WriterParam(List<String> partitionValues, OptionalInt bucket, String filePath)
        {
            this.partitionValues = partitionValues;
            this.bucket = bucket;
            this.filePath = filePath;
        }
    }
}
