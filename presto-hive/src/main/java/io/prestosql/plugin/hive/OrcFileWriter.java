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
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.prestosql.orc.OrcWriter;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.plugin.hive.orc.OrcAcidRowId;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.IntArrayBlockBuilder;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.type.Type;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.BucketCodec;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.orc.impl.AcidStats;
import org.apache.orc.impl.OrcAcidUtils;
import org.joda.time.DateTimeZone;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED;
import static java.util.Objects.requireNonNull;

public class OrcFileWriter
        implements HiveFileWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(OrcFileWriter.class).instanceSize();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final String ACID_KEY_INDEX_NAME = "hive.acid.key.index";

    private final OrcWriter orcWriter;
    private final Callable<Void> rollbackAction;
    private final int[] fileInputColumnIndexes;
    private final List<Block> nullBlocks;
    private final List<Block> dataNullBlocks;
    private final Optional<Supplier<OrcDataSource>> validationInputFactory;

    private final Optional<AcidOutputFormat.Options> acidOptions;
    private StringBuilder indexKey = new StringBuilder();
    private OrcAcidRowId lastKey;
    private long writeId;
    private long indexWriteId;
    private int encodedBucketId;
    private int indexEncodedBucketId;
    private long rowId;
    private long indexRowId;
    private Optional<HiveACIDWriteType> acidWriteType;
    private AcidStats acidStats = new AcidStats();
    private Path path;

    private long validationCpuNanos;
    private Optional<HiveFileWriter> deleteDeltaFileWriter;

    public OrcFileWriter(
            OrcDataSink orcDataSink,
            Callable<Void> rollbackAction,
            List<String> columnNames,
            List<Type> fileColumnTypes,
            List<Type> dataFileColumnTypes,
            CompressionKind compression,
            OrcWriterOptions options,
            boolean writeLegacyVersion,
            int[] fileInputColumnIndexes,
            Map<String, String> metadata,
            DateTimeZone hiveStorageTimeZone,
            Optional<Supplier<OrcDataSource>> validationInputFactory,
            OrcWriteValidationMode validationMode,
            OrcWriterStats stats,
            Optional<AcidOutputFormat.Options> acidOptions,
            Optional<HiveACIDWriteType> acidWriteType,
            Optional<HiveFileWriter> deleteDeltaFileWriter,
            Path path)
    {
        requireNonNull(orcDataSink, "orcDataSink is null");

        this.path = path;
        orcWriter = new OrcWriter(
                orcDataSink,
                columnNames,
                fileColumnTypes,
                compression,
                options,
                writeLegacyVersion,
                metadata,
                hiveStorageTimeZone,
                validationInputFactory.isPresent(),
                validationMode,
                stats,
                Optional.of(flushStripeCallback()),
                Optional.of(closeCallback()));
        this.deleteDeltaFileWriter = deleteDeltaFileWriter;
        this.rollbackAction = requireNonNull(rollbackAction, "rollbackAction is null");

        this.fileInputColumnIndexes = requireNonNull(fileInputColumnIndexes, "outputColumnInputIndexes is null");

        ImmutableList.Builder<Block> nullBlocks = ImmutableList.builder();
        for (Type fileColumnType : fileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            nullBlocks.add(blockBuilder.build());
        }
        this.nullBlocks = nullBlocks.build();
        ImmutableList.Builder<Block> dataNullBlocks = ImmutableList.builder();
        for (Type fileColumnType : dataFileColumnTypes) {
            BlockBuilder blockBuilder = fileColumnType.createBlockBuilder(null, 1, 0);
            blockBuilder.appendNull();
            dataNullBlocks.add(blockBuilder.build());
        }
        this.dataNullBlocks = dataNullBlocks.build();
        this.validationInputFactory = validationInputFactory;
        this.acidOptions = acidOptions;
        this.lastKey = new OrcAcidRowId(-1, -1, -1);
        this.rowId = -1;
        if (acidOptions.isPresent()) {
            writeId = acidOptions.get().getMaximumWriteId();
            encodedBucketId = BucketCodec.V1.encode(acidOptions.get());
        }
        this.acidWriteType = acidWriteType;
    }

    @Override
    public void initWriter(boolean isAcid, Path path, FileSystem fileSystem)
    {
        if (isAcid && isFullAcid()) {
            if (deleteDeltaFileWriter.isPresent()) {
                AcidOutputFormat.Options deleteOptions = acidOptions.get().clone().writingDeleteDelta(true);
                Path deletePath = AcidUtils.createFilename(path.getParent().getParent(), deleteOptions);
                deleteDeltaFileWriter.get().initWriter(isAcid, deletePath, fileSystem);
            }
            try {
                AcidUtils.OrcAcidVersion.writeVersionFile(path.getParent(), fileSystem);
            }
            catch (IOException e) {
                if (e instanceof AlreadyBeingCreatedException
                        || (e instanceof RemoteException && ((RemoteException) e).unwrapRemoteException(AlreadyBeingCreatedException.class) != e)
                        || (e instanceof FileAlreadyExistsException)) {
                    //Ignore the exception as same file is being created by another task in parallel.
                    return;
                }
                throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
            }
        }
    }

    private boolean isFullAcid()
    {
        if (!acidOptions.isPresent()) {
            return false;
        }
        Properties tableProperties = acidOptions.get().getTableProperties();
        return tableProperties == null || !AcidUtils.isInsertOnlyTable(tableProperties);
    }

    Callable<Void> flushStripeCallback()
    {
        return () -> {
            if (!isFullAcid()) {
                return null;
            }
            OrcAcidRowId currentKey = new OrcAcidRowId(indexWriteId, indexEncodedBucketId, indexRowId);
            if (lastKey.compareTo(currentKey) < 0) {
                indexKey.append(indexWriteId);
                indexKey.append(",");
                indexKey.append(indexEncodedBucketId);
                indexKey.append(",");
                indexKey.append(indexRowId);
                indexKey.append(";");
                lastKey = currentKey;
            }
            return null;
        };
    }

    Callable<Void> closeCallback()
    {
        return () -> {
            if (!isFullAcid()) {
                return null;
            }
            OrcAcidRowId currentKey = new OrcAcidRowId(indexWriteId, indexEncodedBucketId, indexRowId);
            if (lastKey.compareTo(currentKey) < 0) {
                flushStripeCallback().call();
            }
            orcWriter.addUserMetadata(ACID_KEY_INDEX_NAME, indexKey.toString());
            orcWriter.addUserMetadata(OrcAcidUtils.ACID_STATS, acidStats.serialize());
            return null;
        };
    }

    @Override
    public long getWrittenBytes()
    {
        return orcWriter.getWrittenBytes() + orcWriter.getBufferedBytes();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return INSTANCE_SIZE + orcWriter.getRetainedBytes();
    }

    @Override
    public void appendRows(Page dataPage)
    {
        if (deleteDeltaFileWriter.isPresent()) {
            //Forward to delete writer
            deleteDeltaFileWriter.get().appendRows(dataPage);
        }
        Block[] dataBlocks = new Block[fileInputColumnIndexes.length];
        for (int i = 0; i < fileInputColumnIndexes.length; i++) {
            int inputColumnIndex = fileInputColumnIndexes[i];
            if (inputColumnIndex < 0) {
                dataBlocks[i] = new RunLengthEncodedBlock(dataNullBlocks.get(i), dataPage.getPositionCount());
            }
            else {
                dataBlocks[i] = dataPage.getBlock(inputColumnIndex);
            }
        }
        Block[] blocks = null;
        int i = 0;
        int totalColumns;
        if (isFullAcid()) {
            Block rowIdBlock = null;
            if (HiveACIDWriteType.isRowIdNeeded(acidWriteType.get())) {
                Block block = dataPage.getBlock(dataPage.getChannelCount() - 1);
                rowIdBlock = block.getLoadedBlock();
            }
            totalColumns = 6;
            blocks = new Block[totalColumns];
            //operation
            blocks[i++] = insertOperationId(dataPage, rowIdBlock, acidWriteType.get().getOperationId());
            //originalTransactionId
            blocks[i++] = insertOriginalTransaction(dataPage, rowIdBlock, writeId);
            //bucketId
            //Bucket Id is encoded to include some extra information from options.
            blocks[i++] = insertBucketIdBlock(dataPage, rowIdBlock, encodedBucketId);
            //rowId
            //rowId is incremental within a delta file./
            blocks[i++] = insertRowIdBlock(dataPage, rowIdBlock);
            //currentTransactionId
            blocks[i++] = insertCurrentTransaction(dataPage, rowIdBlock, writeId);
            boolean isDelete = acidWriteType.get() == HiveACIDWriteType.DELETE ||
                    (acidWriteType.get() == HiveACIDWriteType.VACUUM &&
                            acidOptions.map(o -> o.isWritingDeleteDelta()).orElse(false));
            blocks[i] = !isDelete ? RowBlock.fromFieldBlocks(dataPage.getPositionCount(), Optional.empty(), dataBlocks)
                    : new RunLengthEncodedBlock(nullBlocks.get(nullBlocks.size() - 1), dataPage.getPositionCount());
            // statistics required to read from hive-cli for historical reasons.
            if (isDelete) {
                acidStats.deletes += dataPage.getPositionCount();
            }
            else {
                acidStats.inserts += dataPage.getPositionCount();
            }
        }
        else {
            blocks = dataBlocks;
        }

        Page page = new Page(dataPage.getPositionCount(), blocks);
        try {
            orcWriter.write(page);
        }
        catch (IOException | UncheckedIOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    private Block insertOperationId(Page dataPage, Block rowIdBlock, int value)
    {
        BlockBuilder builder = new IntArrayBlockBuilder(null, dataPage.getPositionCount());
        boolean keepOriginal = acidWriteType.map(HiveACIDWriteType::isVacuum).orElse(false);
        int valueToWrite = value;
        for (int j = 0; j < dataPage.getPositionCount(); j++) {
            if (rowIdBlock != null && keepOriginal) {
                RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(j);
                valueToWrite = rowBlock.getRawFieldBlocks()[4].getInt(0, 0);
            }
            builder.writeInt(valueToWrite);
        }
        return builder.build();
    }

    private Block insertOriginalTransaction(Page dataPage, Block rowIdBlock, long value)
    {
        BlockBuilder builder = new LongArrayBlockBuilder(null, dataPage.getPositionCount());
        boolean keepOriginal = acidWriteType.map(t -> (t == HiveACIDWriteType.DELETE || t == HiveACIDWriteType.VACUUM)).orElse(false);
        long valueToWrite = value;
        for (int j = 0; j < dataPage.getPositionCount(); j++) {
            if (rowIdBlock != null && keepOriginal) {
                RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(j);
                valueToWrite = rowBlock.getRawFieldBlocks()[0].getLong(0, 0);
            }
            builder.writeLong(valueToWrite);
        }
        return builder.build();
    }

    private Block insertCurrentTransaction(Page dataPage, Block rowIdBlock, long value)
    {
        BlockBuilder builder = new LongArrayBlockBuilder(null, dataPage.getPositionCount());
        boolean keepOriginal = acidWriteType.map(t -> (t == HiveACIDWriteType.VACUUM)).orElse(false);
        long valueToWrite = value;
        for (int j = 0; j < dataPage.getPositionCount(); j++) {
            if (rowIdBlock != null && keepOriginal) {
                RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(j);
                valueToWrite = rowBlock.getRawFieldBlocks()[3].getLong(0, 0);
            }
            builder.writeLong(valueToWrite);
        }
        indexWriteId = valueToWrite;
        return builder.build();
    }

    private Block insertRowIdBlock(Page dataPage, Block rowIdBlock)
    {
        BlockBuilder builder = new LongArrayBlockBuilder(null, dataPage.getPositionCount());
        boolean keepOriginal = acidWriteType.map(t -> (t == HiveACIDWriteType.DELETE || t == HiveACIDWriteType.VACUUM)).orElse(false);
        long valueToWrite = -1;
        for (int j = 0; j < dataPage.getPositionCount(); j++) {
            valueToWrite = (rowId + 1);
            if (rowIdBlock != null && keepOriginal) {
                RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(j);
                valueToWrite = rowBlock.getRawFieldBlocks()[2].getLong(0, 0);
            }
            else {
                ++rowId;
            }
            builder.writeLong(valueToWrite);
        }
        indexRowId = valueToWrite;
        return builder.build();
    }

    private Block insertBucketIdBlock(Page dataPage, Block rowIdBlock, int value)
    {
        //In case of VACUUM_UNIFY need to map bucketId to fileName.
        boolean keepOriginal = acidWriteType.map(t -> (t == HiveACIDWriteType.DELETE || t == HiveACIDWriteType.VACUUM)).orElse(false);
        BlockBuilder builder = new IntArrayBlockBuilder(null, dataPage.getPositionCount());
        int valueToWrite = value;
        for (int j = 0; j < dataPage.getPositionCount(); j++) {
            if (rowIdBlock != null && keepOriginal) {
                RowBlock rowBlock = (RowBlock) rowIdBlock.getSingleValueBlock(j);
                valueToWrite = rowBlock.getRawFieldBlocks()[1].getInt(0, 0);
            }
            builder.writeInt(valueToWrite);
        }
        indexEncodedBucketId = valueToWrite;
        return builder.build();
    }

    @Override
    public void commit()
    {
        try {
            if (deleteDeltaFileWriter.isPresent()) {
                deleteDeltaFileWriter.get().commit();
            }
            orcWriter.close();
        }
        catch (IOException | UncheckedIOException e) {
            try {
                rollbackAction.call();
            }
            catch (Exception ignored) {
                // ignore
            }
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }

        if (validationInputFactory.isPresent()) {
            try {
                try (OrcDataSource input = validationInputFactory.get().get()) {
                    long startThreadCpuTime = THREAD_MX_BEAN.getCurrentThreadCpuTime();
                    orcWriter.validate(input);
                    validationCpuNanos += THREAD_MX_BEAN.getCurrentThreadCpuTime() - startThreadCpuTime;
                }
            }
            catch (IOException | UncheckedIOException e) {
                throw new PrestoException(HIVE_WRITE_VALIDATION_FAILED, e);
            }
        }
    }

    @Override
    public void rollback()
    {
        try {
            try {
                if (deleteDeltaFileWriter.isPresent()) {
                    deleteDeltaFileWriter.get().rollback();
                }
                orcWriter.close();
            }
            finally {
                rollbackAction.call();
            }
        }
        catch (Exception e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
        }
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public ImmutableList<String> getExtraPartitionFiles()
    {
        if (deleteDeltaFileWriter.isPresent()) {
            OrcFileWriter deleteFileWriter = (OrcFileWriter) deleteDeltaFileWriter.get();
            Path deletePath = deleteFileWriter.path.getParent();
            return ImmutableList.of(deletePath.getName());
        }
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("writer", orcWriter)
                .toString();
    }
}
