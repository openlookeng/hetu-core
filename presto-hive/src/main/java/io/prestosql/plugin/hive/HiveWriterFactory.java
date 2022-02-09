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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.event.client.EventClient;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior;
import io.prestosql.plugin.hive.LocationService.WriteInfo;
import io.prestosql.plugin.hive.PartitionUpdate.UpdateMode;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.util.TempFileReader;
import io.prestosql.spi.NodeManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageSorter;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.SortOrder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.ReflectionUtil;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_TABLE_READ_ONLY;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_WRITER_OPEN_ERROR;
import static io.prestosql.plugin.hive.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.HiveUtil.getColumnTypes;
import static io.prestosql.plugin.hive.HiveWriteUtils.createPartitionValues;
import static io.prestosql.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.prestosql.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.prestosql.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.prestosql.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;

public class HiveWriterFactory
{
    private static Logger log = Logger.get(HiveWriterFactory.class);

    private static final int MAX_BUCKET_COUNT = 100_000;
    private static final int BUCKET_NUMBER_PADDING = Integer.toString(MAX_BUCKET_COUNT - 1).length();

    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final String schemaName;
    private final String tableName;

    private final List<DataColumn> dataColumns;

    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final Map<String, String> additionalTableParameters;
    protected final LocationHandle locationHandle;
    protected final LocationService locationService;
    private final String queryId;

    private final HivePageSinkMetadataProvider pageSinkMetadataProvider;
    private final TypeManager typeManager;
    private final HdfsEnvironment hdfsEnvironment;
    private final PageSorter pageSorter;
    private final JobConf conf;

    private final Table table;
    private final DataSize sortBufferSize;
    private final int maxOpenSortFiles;
    private final boolean immutablePartitions;
    private final InsertExistingPartitionsBehavior insertExistingPartitionsBehavior;
    private final DateTimeZone parquetTimeZone;

    private final ConnectorSession session;
    private final OptionalInt bucketCount;
    private final List<SortingColumn> sortedBy;

    private final NodeManager nodeManager;
    private final EventClient eventClient;
    private final Map<String, String> sessionProperties;

    private final HiveWriterStats hiveWriterStats;
    private final HiveACIDWriteType acidWriteType;

    private final OrcFileWriterFactory orcFileWriterFactory;

    // Snapshot: instead of writing to a "file", each snapshot is stored in a sub file "file.0", "file.1", etc.
    // These sub files are then merged to the final file when the operator finishes.
    private boolean isSnapshotEnabled;
    // The snapshotSuffixes list records the "resumeCount" for each sub file index.
    // File suffix includes both the resumeCount and the sub file index.
    // This ensures that different runs create files with different names, to avoid any potential collision.
    private final List<Long> snapshotSuffixes = new ArrayList<>();
    private long resumeCount;

    public HiveWriterFactory(
            Set<HiveFileWriterFactory> fileWriterFactories,
            String schemaName,
            String tableName,
            boolean isCreateTable,
            HiveACIDWriteType acidWriteType,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            Map<String, String> additionalTableParameters,
            OptionalInt bucketCount,
            List<SortingColumn> sortedBy,
            LocationHandle locationHandle,
            LocationService locationService,
            String queryId,
            HivePageSinkMetadataProvider pageSinkMetadataProvider,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            PageSorter pageSorter,
            DataSize sortBufferSize,
            int maxOpenSortFiles,
            boolean immutablePartitions,
            DateTimeZone parquetTimeZone,
            ConnectorSession session,
            NodeManager nodeManager,
            EventClient eventClient,
            HiveSessionProperties hiveSessionProperties,
            HiveWriterStats hiveWriterStats,
            OrcFileWriterFactory orcFileWriterFactory)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.additionalTableParameters = ImmutableMap.copyOf(requireNonNull(additionalTableParameters, "additionalTableParameters is null"));
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.queryId = requireNonNull(queryId, "queryId is null");

        this.pageSinkMetadataProvider = requireNonNull(pageSinkMetadataProvider, "pageSinkMetadataProvider is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.sortBufferSize = requireNonNull(sortBufferSize, "sortBufferSize is null");
        this.maxOpenSortFiles = maxOpenSortFiles;
        this.immutablePartitions = immutablePartitions;

        if (acidWriteType == HiveACIDWriteType.INSERT_OVERWRITE) {
            //In case of ACID txn tables, dont delete old data. Just create new base in same partition.
            if (pageSinkMetadataProvider.getTable().isPresent() &&
                    AcidUtils.isTransactionalTable(pageSinkMetadataProvider.getTable().get().getParameters())) {
                this.insertExistingPartitionsBehavior = InsertExistingPartitionsBehavior.APPEND;
            }
            else {
                this.insertExistingPartitionsBehavior = InsertExistingPartitionsBehavior.OVERWRITE;
            }
        }
        else if (acidWriteType == HiveACIDWriteType.UPDATE) {
            // if the write type is update, then ignore the session property
            this.insertExistingPartitionsBehavior = InsertExistingPartitionsBehavior.APPEND;
        }
        else {
            this.insertExistingPartitionsBehavior = HiveSessionProperties.getInsertExistingPartitionsBehavior(session);
        }

        if (immutablePartitions) {
            checkArgument(insertExistingPartitionsBehavior != InsertExistingPartitionsBehavior.APPEND, "insertExistingPartitionsBehavior cannot be APPEND");
        }
        this.parquetTimeZone = requireNonNull(parquetTimeZone, "parquetTimeZone is null");

        this.acidWriteType = acidWriteType;
        // divide input columns into partition and data columns
        requireNonNull(inputColumns, "inputColumns is null");
        ImmutableList.Builder<String> localPartitionColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> localPartitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<DataColumn> localDataColumns = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            HiveType hiveType = column.getHiveType();
            if (column.isPartitionKey()) {
                localPartitionColumnNames.add(column.getName());
                localPartitionColumnTypes.add(typeManager.getType(column.getTypeSignature()));
            }
            else {
                localDataColumns.add(new DataColumn(column.getName(), hiveType));
            }
        }
        this.partitionColumnNames = localPartitionColumnNames.build();
        this.partitionColumnTypes = localPartitionColumnTypes.build();
        this.dataColumns = localDataColumns.build();

        Path writePath;
        if (isCreateTable) {
            this.table = null;
            WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
            checkArgument(writeInfo.getWriteMode() != DIRECT_TO_TARGET_EXISTING_DIRECTORY, "CREATE TABLE write mode cannot be DIRECT_TO_TARGET_EXISTING_DIRECTORY");
            writePath = writeInfo.getWritePath();
        }
        else {
            Optional<Table> localTable = pageSinkMetadataProvider.getTable();
            if (!localTable.isPresent()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Table %s.%s was dropped during insert", schemaName, tableName));
            }
            this.table = localTable.get();
            writePath = locationService.getQueryWriteInfo(locationHandle).getWritePath();
        }

        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        if (bucketCount.isPresent()) {
            checkArgument(bucketCount.getAsInt() < MAX_BUCKET_COUNT, "bucketCount must be smaller than " + MAX_BUCKET_COUNT);
        }

        this.sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));

        this.session = requireNonNull(session, "session is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.eventClient = requireNonNull(eventClient, "eventClient is null");

        requireNonNull(hiveSessionProperties, "hiveSessionProperties is null");
        this.sessionProperties = hiveSessionProperties.getSessionProperties().stream()
                .collect(toImmutableMap(PropertyMetadata::getName,
                        entry -> session.getProperty(entry.getName(), entry.getJavaType()).toString()));

        Configuration localConf = hdfsEnvironment.getConfiguration(new HdfsContext(session, schemaName, tableName), writePath);
        this.conf = toJobConf(localConf);

        // make sure the FileSystem is created with the correct Configuration object
        try {
            hdfsEnvironment.getFileSystem(session.getUser(), writePath, localConf);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + writePath, e);
        }

        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");

        this.orcFileWriterFactory = requireNonNull(orcFileWriterFactory, "orcFileWriterFactory is null");

        this.isSnapshotEnabled = session.isSnapshotEnabled();
    }

    JobConf getConf()
    {
        return conf;
    }

    List<String> getPartitionValues(Page partitionColumns, int position)
    {
        return createPartitionValues(partitionColumnTypes, partitionColumns, position);
    }

    Optional<String> getPartitionName(Page partitionColumns, int position)
    {
        List<String> partitionValues = createPartitionValues(partitionColumnTypes, partitionColumns, position);
        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            partitionName = Optional.of(FileUtils.makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }
        return partitionName;
    }

    public HiveWriter createWriter(List<String> partitionValues, OptionalInt bucketNumber, Optional<Options> vacuumOptions)
    {
        return createWriter(partitionValues, bucketNumber, vacuumOptions, false);
    }

    public HiveWriter createWriterForSnapshotMerge(List<String> partitionValues, OptionalInt bucketNumber, Optional<Options> vacuumOptions)
    {
        return createWriter(partitionValues, bucketNumber, vacuumOptions, true);
    }

    private HiveWriter createWriter(List<String> partitionValues, OptionalInt bucketNumber, Optional<Options> vacuumOptions, boolean forMerge)
    {
        boolean isTxnTable = isTxnTable();
        if (bucketCount.isPresent()) {
            checkArgument(bucketNumber.isPresent(), "Bucket not provided for bucketed table");
            checkArgument(bucketNumber.getAsInt() < bucketCount.getAsInt(), "Bucket number %s must be less than bucket count %s", bucketNumber, bucketCount);
        }
        else {
            checkArgument(isTxnTable || !bucketNumber.isPresent(), "Bucket number provided by for table that is not bucketed");
        }

        String fileName;
        if (bucketNumber.isPresent()) {
            fileName = computeBucketedFileName(queryId, bucketNumber.getAsInt());
        }
        else {
            // Snapshot: don't use UUID. File name needs to be deterministic.
            if (isSnapshotEnabled) {
                fileName = String.format(ENGLISH, "%s_%d_%d_%d", queryId, session.getTaskId().getAsInt(), session.getPipelineId().getAsInt(), session.getDriverId().getAsInt());
            }
            else {
                fileName = queryId + "_" + randomUUID();
            }
        }

        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            partitionName = Optional.of(FileUtils.makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }

        // attempt to get the existing partition (if this is an existing partitioned table)
        Optional<Partition> partition = Optional.empty();
        if (!partitionValues.isEmpty() && table != null) {
            partition = pageSinkMetadataProvider.getPartition(partitionValues);
        }

        UpdateMode updateMode;
        Properties schema;
        WriteInfo writeInfo;
        StorageFormat outputStorageFormat;
        if (!partition.isPresent()) {
            if (table == null) {
                // Write to: a new partition in a new partitioned table,
                //           or a new unpartitioned table.
                updateMode = UpdateMode.NEW;
                schema = new Properties();
                schema.setProperty(IOConstants.COLUMNS, dataColumns.stream()
                        .map(DataColumn::getName)
                        .collect(joining(",")));
                schema.setProperty(IOConstants.COLUMNS_TYPES, dataColumns.stream()
                        .map(DataColumn::getHiveType)
                        .map(HiveType::getHiveTypeName)
                        .map(HiveTypeName::toString)
                        .collect(joining(":")));
                setAdditionalSchemaProperties(schema);
                if (!partitionName.isPresent()) {
                    // new unpartitioned table
                    writeInfo = locationService.getTableWriteInfo(locationHandle, false);
                }
                else {
                    // a new partition in a new partitioned table
                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());

                    if (!writeInfo.getWriteMode().isWritePathSameAsTargetPath()) {
                        // When target path is different from write path,
                        // verify that the target directory for the partition does not already exist
                        if (HiveWriteUtils.pathExists(new HdfsContext(session, schemaName, tableName), hdfsEnvironment, writeInfo.getTargetPath())) {
                            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS, format(
                                    "Target directory for new partition '%s' of table '%s.%s' already exists: %s",
                                    partitionName,
                                    schemaName,
                                    tableName,
                                    writeInfo.getTargetPath()));
                        }
                    }
                }
            }
            else {
                // Write to: a new partition in an existing partitioned table,
                //           or an existing unpartitioned table
                if (partitionName.isPresent()) {
                    // a new partition in an existing partitioned table
                    updateMode = UpdateMode.NEW;
                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());
                }
                else {
                    switch (insertExistingPartitionsBehavior) {
                        case APPEND:
                            checkState(!immutablePartitions);
                            updateMode = UpdateMode.APPEND;
                            writeInfo = locationService.getTableWriteInfo(locationHandle, false);
                            break;
                        case OVERWRITE:
                            updateMode = UpdateMode.OVERWRITE;
                            writeInfo = locationService.getTableWriteInfo(locationHandle, true);
                            break;
                        case ERROR:
                            throw new PrestoException(HIVE_TABLE_READ_ONLY, "Unpartitioned Hive tables are immutable");
                        default:
                            throw new IllegalArgumentException("Unsupported insert existing table behavior: " + insertExistingPartitionsBehavior);
                    }
                }

                schema = getHiveSchema(table);
            }

            if (partitionName.isPresent()) {
                // Write to a new partition
                outputStorageFormat = fromHiveStorageFormat(partitionStorageFormat);
            }
            else {
                // Write to a new/existing unpartitioned table
                outputStorageFormat = fromHiveStorageFormat(tableStorageFormat);
            }
        }
        else {
            // Write to: an existing partition in an existing partitioned table
            if (insertExistingPartitionsBehavior == InsertExistingPartitionsBehavior.APPEND) {
                // Append to an existing partition
                checkState(!immutablePartitions);
                updateMode = UpdateMode.APPEND;
                // Check the column types in partition schema match the column types in table schema
                List<Column> tableColumns = table.getDataColumns();
                List<Column> existingPartitionColumns = partition.get().getColumns();
                for (int i = 0; i < min(existingPartitionColumns.size(), tableColumns.size()); i++) {
                    HiveType tableType = tableColumns.get(i).getType();
                    HiveType partitionType = existingPartitionColumns.get(i).getType();
                    if (!tableType.equals(partitionType)) {
                        throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                        "You are trying to write into an existing partition in a table. " +
                                        "The table schema has changed since the creation of the partition. " +
                                        "Inserting rows into such partition is not supported. " +
                                        "The column '%s' in table '%s' is declared as type '%s', " +
                                        "but partition '%s' declared column '%s' as type '%s'.",
                                tableColumns.get(i).getName(),
                                tableName,
                                tableType,
                                partitionName,
                                existingPartitionColumns.get(i).getName(),
                                partitionType));
                    }
                }

                HiveWriteUtils.checkPartitionIsWritable(partitionName.get(), partition.get());

                outputStorageFormat = partition.get().getStorage().getStorageFormat();
                schema = getHiveSchema(partition.get(), table);

                writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());
            }
            else if (insertExistingPartitionsBehavior == InsertExistingPartitionsBehavior.OVERWRITE) {
                // Overwrite an existing partition
                //
                // The behavior of overwrite considered as if first dropping the partition and inserting a new partition, thus:
                // * No partition writable check is required.
                // * Table schema and storage format is used for the new partition (instead of existing partition schema and storage format).
                updateMode = UpdateMode.OVERWRITE;

                outputStorageFormat = fromHiveStorageFormat(partitionStorageFormat);
                schema = getHiveSchema(table);

                writeInfo = locationService.getPartitionWriteInfo(locationHandle, Optional.empty(), partitionName.get());
                checkWriteMode(writeInfo);
            }
            else if (insertExistingPartitionsBehavior == InsertExistingPartitionsBehavior.ERROR) {
                throw new PrestoException(HIVE_PARTITION_READ_ONLY, "Cannot insert into an existing partition of Hive table: " + partitionName.get());
            }
            else {
                throw new IllegalArgumentException(format("Unsupported insert existing partitions behavior: %s", insertExistingPartitionsBehavior));
            }
        }

        schema.putAll(additionalTableParameters);
        if (acidWriteType != HiveACIDWriteType.DELETE) {
            validateSchema(partitionName, schema);
        }

        Path path;
        Optional<AcidOutputFormat.Options> acidOptions;
        String fileNameWithExtension;

        if (isTxnTable) {
            WriteIdInfo writeIdInfo = locationHandle.getJsonSerializablewriteIdInfo().get();
            AcidOutputFormat.Options options = new AcidOutputFormat.Options(conf)
                    .minimumWriteId(writeIdInfo.getMinWriteId())
                    .maximumWriteId(writeIdInfo.getMaxWriteId())
                    .statementId(writeIdInfo.getStatementId())
                    .bucket(bucketNumber.isPresent() ? bucketNumber.getAsInt() : 0);
            if (acidWriteType == HiveACIDWriteType.DELETE) {
                //to support delete as insert
                options.writingDeleteDelta(true);
            }
            else if (acidWriteType == HiveACIDWriteType.INSERT_OVERWRITE) {
                //In case of ACID txn tables, dont delete old data. Just create new base in same partition.
                options.writingBase(true);
            }
            if (vacuumOptions.isPresent() && HiveACIDWriteType.isVacuum(acidWriteType)) {
                Options vOptions = vacuumOptions.get();
                //Use the original bucket file number itself.
                //Compacted delta directories will not have statementId
                options.maximumWriteId(vOptions.getMaximumWriteId())
                        .minimumWriteId(vOptions.getMinimumWriteId())
                        .writingBase(vOptions.isWritingBase())
                        .writingDeleteDelta(vOptions.isWritingDeleteDelta())
                        .bucket(vOptions.getBucketId())
                        .statementId(-1);
            }
            if (AcidUtils.isInsertOnlyTable(schema)) {
                String subdir;
                if (options.isWritingBase()) {
                    subdir = AcidUtils.baseDir(options.getMaximumWriteId());
                }
                else if (HiveACIDWriteType.isVacuum(acidWriteType)) {
                    //Only for Minor compacted delta will not have statement Id.
                    subdir = AcidUtils.deltaSubdir(options.getMinimumWriteId(), options.getMaximumWriteId());
                }
                else {
                    subdir = AcidUtils.deltaSubdir(options.getMinimumWriteId(), options.getMaximumWriteId(), options.getStatementId());
                }
                Path parentDir = new Path(writeInfo.getWritePath(), subdir);
                fileName = String.format("%06d", options.getBucketId()) + "_0" + getFileExtension(conf, outputStorageFormat);
                path = new Path(parentDir, fileName);
                Properties properties = new Properties();
                properties.setProperty("transactional_properties", "insert_only");
                options.tableProperties(properties);
            }
            else {
                path = AcidUtils.createFilename(writeInfo.getWritePath(), options);
            }
            //In case of ACID entire delta directory should be renamed from staging directory.
            fileNameWithExtension = path.getParent().getName();
            acidOptions = Optional.of(options);
        }
        else {
            fileNameWithExtension = fileName + getFileExtension(conf, outputStorageFormat);
            path = new Path(writeInfo.getWritePath(), fileNameWithExtension);
            acidOptions = Optional.empty();
        }

        FileSystem fileSystem;
        try {
            fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, conf);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_OPEN_ERROR, e);
        }

        if (isSnapshotEnabled) {
            // Snapshot: use a recognizable name pattern, in case they need to be deleted/renamed
            String oldFileName = path.getName();
            String newFileName = toSnapshotFileName(oldFileName, queryId);
            path = new Path(path.getParent(), newFileName);
            if (fileNameWithExtension.equals(oldFileName)) {
                fileNameWithExtension = newFileName;
            }
        }
        HiveFileWriter hiveFileWriter = null;
        if (isSnapshotEnabled && !forMerge) {
            // Add a suffix to file name for sub files
            String oldFileName = path.getName();
            String newFileName = toSnapshotSubFile(oldFileName);
            path = new Path(path.getParent(), newFileName);
            if (fileNameWithExtension.equals(oldFileName)) {
                fileNameWithExtension = newFileName;
            }
            // Always create a simple ORC writer for snapshot files. These will be merged in the end.
            logContainingFolderInfo(fileSystem, path, "Creating SnapshotTempFileWriter for %s", path);
            try {
                Path finalPath = path;
                hiveFileWriter = new SnapshotTempFileWriter(
                        orcFileWriterFactory.createOrcDataSink(session, fileSystem, path),
                        dataColumns.stream()
                                .map(column -> column.getHiveType().getType(typeManager))
                                .collect(Collectors.toList()));
            }
            catch (IOException e) {
                throw new PrestoException(HiveErrorCode.HIVE_WRITER_OPEN_ERROR, "Error creating ORC file", e);
            }
        }
        else {
            conf.set("table.write.path", writeInfo.getWritePath().toString());
            for (HiveFileWriterFactory fileWriterFactory : fileWriterFactories) {
                Optional<HiveFileWriter> fileWriter = fileWriterFactory.createFileWriter(
                        path,
                        dataColumns.stream()
                                .map(DataColumn::getName)
                                .collect(toList()),
                        outputStorageFormat,
                        schema,
                        conf,
                        session,
                        acidOptions,
                        Optional.of(acidWriteType));
                if (fileWriter.isPresent()) {
                    hiveFileWriter = fileWriter.get();
                    break;
                }
            }

            if (isSnapshotEnabled) {
                // TODO-cp-I2BZ0A: assuming all files to be of ORC type
                checkState(hiveFileWriter instanceof OrcFileWriter, "Only support ORC format with snapshot");
                logContainingFolderInfo(fileSystem, path, "Creating file writer for final result: %s", path);
            }

            if (hiveFileWriter == null) {
                hiveFileWriter = new RecordFileWriter(
                        path,
                        dataColumns.stream()
                                .map(DataColumn::getName)
                                .collect(toList()),
                        outputStorageFormat,
                        schema,
                        partitionStorageFormat.getEstimatedWriterSystemMemoryUsage(),
                        conf,
                        typeManager,
                        parquetTimeZone,
                        session);
            }
            if (isTxnTable) {
                hiveFileWriter.initWriter(true, path, fileSystem);
            }
        }

        Path finalPath = path;
        String writerImplementation = hiveFileWriter.getClass().getName();

        Consumer<HiveWriter> onCommit;
        if (isSnapshotEnabled && !forMerge) {
            // Only send "commit" event for the merged file
            onCommit = hiveWriter -> {};
        }
        else {
            onCommit = hiveWriter -> {
                Optional<Long> size;
                try {
                    size = Optional.of(hdfsEnvironment.getFileSystem(session.getUser(), finalPath, conf).getFileStatus(finalPath).getLen());
                }
                catch (IOException | RuntimeException e) {
                    // Do not fail the query if file system is not available
                    size = Optional.empty();
                }

                eventClient.post(new WriteCompletedEvent(
                        session.getQueryId(),
                        finalPath.toString(),
                        schemaName,
                        tableName,
                        partitionName.orElse(null),
                        outputStorageFormat.getOutputFormat(),
                        writerImplementation,
                        nodeManager.getCurrentNode().getVersion(),
                        nodeManager.getCurrentNode().getHost(),
                        session.getIdentity().getPrincipal().map(Principal::getName).orElse(null),
                        nodeManager.getEnvironment(),
                        sessionProperties,
                        size.orElse(null),
                        hiveWriter.getRowCount()));
            };
        }

        if (!sortedBy.isEmpty() || (isTxnTable() && HiveACIDWriteType.isUpdateOrDelete(acidWriteType))) {
            List<Type> types = dataColumns.stream()
                    .map(column -> column.getHiveType().getType(typeManager))
                    .collect(Collectors.toList());

            Map<String, Integer> columnIndexes = new HashMap<>();
            for (int i = 0; i < dataColumns.size(); i++) {
                columnIndexes.put(dataColumns.get(i).getName(), i);
            }
            if (sortedBy.isEmpty() &&
                    isTxnTable() && HiveACIDWriteType.isUpdateOrDelete(acidWriteType)) {
                //Add $rowId column as the last column in the page
                types.add(HiveColumnHandle.updateRowIdHandle().getHiveType().getType(typeManager));
                columnIndexes.put(HiveColumnHandle.UPDATE_ROW_ID_COLUMN_NAME, dataColumns.size());
            }

            List<Integer> sortFields = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            List<SortingColumn> sortigColumns = this.sortedBy;
            if (sortedBy.isEmpty() &&
                    isTxnTable() && HiveACIDWriteType.isUpdateOrDelete(acidWriteType)) {
                sortigColumns = ImmutableList.of(new SortingColumn(HiveColumnHandle.UPDATE_ROW_ID_COLUMN_NAME, SortingColumn.Order.ASCENDING));
            }
            for (SortingColumn column : sortigColumns) {
                Integer index = columnIndexes.get(column.getColumnName());
                if (index == null) {
                    throw new PrestoException(HIVE_INVALID_METADATA, format("Sorting column '%s' does not exist in table '%s.%s'", column.getColumnName(), schemaName, tableName));
                }
                sortFields.add(index);
                sortOrders.add(column.getOrder().getSortOrder());
            }

            FileSystem sortFileSystem = fileSystem;
            String child = ".tmp-sort." + path.getName();
            Path tempFilePrefix = new Path(path.getParent(), child);

            hiveFileWriter = new SortingFileWriter(
                    sortFileSystem,
                    tempFilePrefix,
                    hiveFileWriter,
                    sortBufferSize,
                    maxOpenSortFiles,
                    types,
                    sortFields,
                    sortOrders,
                    pageSorter,
                    (fs, p) -> orcFileWriterFactory.createOrcDataSink(session, fs, p));
        }

        return new HiveWriter(
                hiveFileWriter,
                partitionName,
                updateMode,
                fileNameWithExtension,
                writeInfo.getWritePath().toString(),
                writeInfo.getTargetPath().toString(),
                path.toString(),
                onCommit,
                // Snapshot: only update stats when merging files
                isSnapshotEnabled && !forMerge ? null : hiveWriterStats,
                hiveFileWriter.getExtraPartitionFiles());
    }

    public boolean isTxnTable()
    {
        Map<String, String> tableParameters = table != null ? this.table.getParameters() : additionalTableParameters;
        return tableParameters != null && AcidUtils.isTransactionalTable(tableParameters);
    }

    private void validateSchema(Optional<String> partitionName, Properties schema)
    {
        // existing tables may have columns in a different order
        List<String> fileColumnNames = getColumnNames(schema);
        List<HiveType> fileColumnHiveTypes = getColumnTypes(schema);

        // verify we can write all input columns to the file
        Map<String, DataColumn> inputColumnMap = dataColumns.stream()
                .collect(toMap(DataColumn::getName, identity()));
        Set<String> missingColumns = Sets.difference(inputColumnMap.keySet(), new HashSet<>(fileColumnNames));
        if (!missingColumns.isEmpty()) {
            throw new PrestoException(NOT_FOUND, format("Table %s.%s does not have columns %s", schema, tableName, missingColumns));
        }
        if (fileColumnNames.size() != fileColumnHiveTypes.size()) {
            throw new PrestoException(HIVE_INVALID_METADATA, format(
                    "Partition '%s' in table '%s.%s' has mismatched metadata for column names and types",
                    partitionName,
                    schemaName,
                    tableName));
        }

        // verify the file types match the input type
        // todo adapt input types to the file types as Hive does
        for (int fileIndex = 0; fileIndex < fileColumnNames.size(); fileIndex++) {
            String columnName = fileColumnNames.get(fileIndex);
            HiveType fileColumnHiveType = fileColumnHiveTypes.get(fileIndex);
            HiveType inputHiveType = inputColumnMap.get(columnName).getHiveType();

            if (!fileColumnHiveType.equals(inputHiveType)) {
                // todo this should be moved to a helper
                throw new PrestoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                        "" +
                                "There is a mismatch between the table and partition schemas. " +
                                "The column '%s' in table '%s.%s' is declared as type '%s', " +
                                "but partition '%s' declared column '%s' as type '%s'.",
                        columnName,
                        schemaName,
                        tableName,
                        inputHiveType,
                        partitionName,
                        columnName,
                        fileColumnHiveType));
            }
        }
    }

    public static String computeBucketedFileName(String queryId, int bucket)
    {
        String paddedBucket = Strings.padStart(Integer.toString(bucket), BUCKET_NUMBER_PADDING, '0');
        return format("0%s_0_%s", paddedBucket, queryId);
    }

    protected void checkWriteMode(WriteInfo writeInfo)
    {
        checkState(writeInfo.getWriteMode() != DIRECT_TO_TARGET_EXISTING_DIRECTORY, "Overwriting existing partition doesn't support DIRECT_TO_TARGET_EXISTING_DIRECTORY write mode");
    }

    public static String getFileExtension(JobConf conf, StorageFormat storageFormat)
    {
        // text format files must have the correct extension when compressed
        if (!HiveConf.getBoolVar(conf, COMPRESSRESULT) || !HiveIgnoreKeyTextOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return "";
        }

        String compressionCodecClass = conf.get("mapred.output.compression.codec");
        if (compressionCodecClass == null) {
            return new DefaultCodec().getDefaultExtension();
        }

        try {
            Class<? extends CompressionCodec> codecClass = conf.getClassByName(compressionCodecClass).asSubclass(CompressionCodec.class);
            return ReflectionUtil.newInstance(codecClass, conf).getDefaultExtension();
        }
        catch (ClassNotFoundException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Compression codec not found: " + compressionCodecClass, e);
        }
        catch (RuntimeException e) {
            throw new PrestoException(HIVE_UNSUPPORTED_FORMAT, "Failed to load compression codec: " + compressionCodecClass, e);
        }
    }

    public static class DataColumn
    {
        private final String name;
        private final HiveType hiveType;

        public DataColumn(String name, HiveType hiveType)
        {
            this.name = requireNonNull(name, "name is null");
            this.hiveType = requireNonNull(hiveType, "hiveType is null");
        }

        public String getName()
        {
            return name;
        }

        public HiveType getHiveType()
        {
            return hiveType;
        }
    }

    protected void setAdditionalSchemaProperties(Properties schema)
    {
        // No additional properties to set for Hive tables
    }

    static String toSnapshotFileName(String fileName, String queryId)
    {
        return fileName + "_snapshot_" + queryId;
    }

    static boolean isSnapshotFile(String fileName, String queryId)
    {
        String identifier = "_snapshot_" + queryId;
        return fileName.contains(identifier);
    }

    static boolean isSnapshotSubFile(String fileName, String queryId)
    {
        return getSnapshotSubFileIndex(fileName, queryId) >= 0;
    }

    static long getSnapshotSubFileIndex(String fileName, String queryId)
    {
        String identifier = "_snapshot_" + queryId;
        int index = fileName.indexOf(identifier);
        if (index < 0) {
            // Not a snapshot file
            return index;
        }
        index += identifier.length();
        if (index == fileName.length()) {
            // Doesn't have a suffix
            return -1;
        }
        String suffix = fileName.substring(fileName.indexOf('_', index) + 1); // Skip over '.' and '_'
        return Long.valueOf(suffix);
    }

    static String removeSnapshotFileName(String fileName, String queryId)
    {
        String identifier = "_snapshot_" + queryId;
        int index = fileName.indexOf(identifier);
        return index > 0 ? fileName.substring(0, index) : fileName;
    }

    private String toSnapshotSubFile(String path)
    {
        return toSnapshotSubFile(path, resumeCount, snapshotSuffixes.size());
    }

    private String toSnapshotSubFile(String path, long resumeCount, int index)
    {
        return path + '.' + resumeCount + '_' + index;
    }

    public void mergeSubFiles(List<HiveWriter> writers)
            throws IOException
    {
        if (writers.isEmpty()) {
            return;
        }

        FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), new Path(writers.get(0).getFilePath()), conf);

        List<Type> types = dataColumns.stream()
                .map(column -> column.getHiveType().getType(typeManager))
                .collect(toList());

        for (HiveWriter writer : writers) {
            String filePath = writer.getFilePath();

            Path path = new Path(filePath);
            logContainingFolderInfo(fileSystem, path, "Merging snapshot files to result file: %s", path);

            // The snapshotSuffixes list records the "resumeCount" for each suffix.
            // It doesn't has an entry for the current set of files, so an entry is added first.
            // The resumeCount helps distinguish files created by different runs.
            snapshotSuffixes.add(resumeCount);
            for (int i = 0; i < snapshotSuffixes.size(); i++) {
                long resume = snapshotSuffixes.get(i);
                Path file = new Path(toSnapshotSubFile(filePath, resume, i));
                if (fileSystem.exists(file)) {
                    // TODO-cp-I2BZ0A: assuming all files to be of ORC type.
                    // Using same parameters as used by SortingFileWriter
                    FileStatus fileStatus = fileSystem.getFileStatus(file);
                    try (TempFileReader reader = new TempFileReader(types, new HdfsOrcDataSource(
                            new OrcDataSourceId(file.toString()),
                            fileStatus.getLen(),
                            new DataSize(1, MEGABYTE),
                            new DataSize(8, MEGABYTE),
                            new DataSize(8, MEGABYTE),
                            false,
                            fileSystem.open(file),
                            new FileFormatDataSourceStats(),
                            fileStatus.getModificationTime()))) {
                        while (reader.hasNext()) {
                            writer.append(reader.next());
                        }
                    }
                    // DO NOT delete the sub file, in case we need to resume. Delete them when the query finishes.
                }
            }
        }
    }

    private void logContainingFolderInfo(FileSystem fileSystem, Path path, String message, Object... params)
    {
        try {
            if (log.isDebugEnabled()) {
                log.debug(message, params);
                Arrays.stream(fileSystem.listStatus(path.getParent())).forEach(file -> {
                    log.debug("%d\t%s", file.getLen(), file.getPath());
                });
            }
        }
        catch (IOException e) {
            log.debug(e, "Failed to list folder content for %s: %s", path, e.getMessage());
        }
    }

    public Object capture()
    {
        // hiveWriterStats is not captured. They are not updated for sub-files.
        // Increment suffix so that each resume generates a new set of files
        snapshotSuffixes.add(resumeCount);
        return snapshotSuffixes;
    }

    public void restore(Object obj, long resumeCount)
    {
        snapshotSuffixes.clear();
        snapshotSuffixes.addAll((List<Long>) obj);
        this.resumeCount = resumeCount;
    }
}
