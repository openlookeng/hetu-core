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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.orc.metadata.CompressionKind;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcPageSourceFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.AcidOutputFormat;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import static io.prestosql.plugin.hive.HiveUtil.getColumnNames;
import static io.prestosql.plugin.hive.HiveUtil.getColumnTypes;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class OrcFileWriterFactory
        implements HiveFileWriterFactory
{
    private static final Logger log = Logger.get(OrcFileWriterFactory.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats stats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;
    private final boolean writeLegacyVersion;

    @Inject
    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            HiveConfig hiveConfig,
            FileFormatDataSourceStats readStats,
            OrcFileWriterConfig config)
    {
        this(
                hdfsEnvironment,
                typeManager,
                nodeVersion,
                hiveConfig.isOrcWriteLegacyVersion(),
                readStats,
                requireNonNull(config, "config is null").toOrcWriterOptions());
    }

    public OrcFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            boolean writeLegacyVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterOptions orcWriterOptions)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.writeLegacyVersion = writeLegacyVersion;
        this.readStats = requireNonNull(readStats, "stats is null");
        this.orcWriterOptions = requireNonNull(orcWriterOptions, "orcWriterOptions is null");
    }

    @Managed
    @Flatten
    public OrcWriterStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<HiveFileWriter> createFileWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            JobConf configuration,
            ConnectorSession session,
            Optional<AcidOutputFormat.Options> acidOptions,
            Optional<HiveACIDWriteType> acidWriteType)
    {
        if (!OrcOutputFormat.class.getName().equals(storageFormat.getOutputFormat())) {
            return Optional.empty();
        }

        CompressionKind compression = getCompression(schema, configuration);

        // existing tables and partitions may have columns in a different order than the writer is providing, so build
        // an index to rearrange columns in the proper order
        List<String> fileColumnNames = getColumnNames(schema);
        List<Type> fileColumnTypes = getColumnTypes(schema).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());
        List<Type> dataFileColumnTypes = fileColumnTypes;

        int[] fileInputColumnIndexes = fileColumnNames.stream()
                .mapToInt(inputColumnNames::indexOf)
                .toArray();

        Optional<HiveFileWriter> deleteDeltaWriter = Optional.empty();
        if (AcidUtils.isTablePropertyTransactional(schema) && !AcidUtils.isInsertOnlyTable(schema)) {
            ImmutableList<String> orcFileColumnNames = ImmutableList.of(OrcPageSourceFactory.ACID_COLUMN_OPERATION,
                    OrcPageSourceFactory.ACID_COLUMN_ORIGINAL_TRANSACTION,
                    OrcPageSourceFactory.ACID_COLUMN_BUCKET,
                    OrcPageSourceFactory.ACID_COLUMN_ROW_ID,
                    OrcPageSourceFactory.ACID_COLUMN_CURRENT_TRANSACTION,
                    OrcPageSourceFactory.ACID_COLUMN_ROW_STRUCT);

            ImmutableList.Builder<RowType.Field> fieldsBuilder = ImmutableList.builder();
            for (int i = 0; i < fileColumnNames.size(); i++) {
                fieldsBuilder.add(new RowType.Field(Optional.of(fileColumnNames.get(i)), fileColumnTypes.get(i)));
            }
            ImmutableList<Type> orcFileColumnTypes = ImmutableList.of(INTEGER,
                    BIGINT,
                    INTEGER,
                    BIGINT,
                    BIGINT,
                    RowType.from(fieldsBuilder.build()));
            fileColumnNames = orcFileColumnNames;
            fileColumnTypes = orcFileColumnTypes;
            if (acidWriteType.isPresent() && acidWriteType.get() == HiveACIDWriteType.UPDATE) {
                AcidOutputFormat.Options deleteOptions = acidOptions.get().clone().writingDeleteDelta(true);
                Path deleteDeltaPath = AcidUtils.createFilename(path.getParent().getParent(), deleteOptions);
                deleteDeltaWriter = createFileWriter(deleteDeltaPath, inputColumnNames, storageFormat, schema, configuration,
                        session, Optional.of(deleteOptions), Optional.of(HiveACIDWriteType.DELETE));
            }
        }

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getUser(), path, configuration);
            OrcDataSink orcDataSink = createOrcDataSink(session, fileSystem, path);

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (HiveSessionProperties.isOrcOptimizedWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        FileStatus fileStatus = fileSystem.getFileStatus(path);
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(path.toString()),
                                fileStatus.getLen(),
                                HiveSessionProperties.getOrcMaxMergeDistance(session),
                                HiveSessionProperties.getOrcMaxBufferSize(session),
                                HiveSessionProperties.getOrcStreamBufferSize(session),
                                false,
                                fileSystem.open(path),
                                readStats,
                                fileStatus.getModificationTime());
                    }
                    catch (IOException e) {
                        throw new PrestoException(HiveErrorCode.HIVE_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            Callable<Void> rollbackAction = () -> {
                log.debug("RollBack action to delete file %s", path);
                fileSystem.delete(path, false);
                return null;
            };

            return Optional.of(new OrcFileWriter(
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    dataFileColumnTypes,
                    compression,
                    orcWriterOptions
                            .withStripeMinSize(HiveSessionProperties.getOrcOptimizedWriterMinStripeSize(session))
                            .withStripeMaxSize(HiveSessionProperties.getOrcOptimizedWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(HiveSessionProperties.getOrcOptimizedWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(HiveSessionProperties.getOrcOptimizedWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(HiveSessionProperties.getOrcStringStatisticsLimit(session)),
                    writeLegacyVersion,
                    fileInputColumnIndexes,
                    ImmutableMap.<String, String>builder()
                            .put(HiveMetadata.PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(HiveMetadata.PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .put("hive.acid.version", String.valueOf(AcidUtils.OrcAcidVersion.ORC_ACID_VERSION))
                            .build(),
                    validationInputFactory,
                    HiveSessionProperties.getOrcOptimizedWriterValidateMode(session),
                    stats,
                    acidOptions,
                    acidWriteType,
                    deleteDeltaWriter,
                    path));
        }
        catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    /**
     * Allow subclass to replace data sink implementation.
     */
    protected OrcDataSink createOrcDataSink(ConnectorSession session, FileSystem fileSystem, Path path)
            throws IOException
    {
        log.debug("Creation of OrcDataSink for file %s", path);
        return new OutputStreamOrcDataSink(fileSystem.create(path));
    }

    private static CompressionKind getCompression(Properties schema, JobConf configuration)
    {
        String compressionName = OrcConf.COMPRESS.getString(schema, configuration);
        if (compressionName == null) {
            return CompressionKind.ZLIB;
        }

        CompressionKind compression;
        try {
            compression = CompressionKind.valueOf(compressionName.toUpperCase(ENGLISH));
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(HiveErrorCode.HIVE_UNSUPPORTED_FORMAT, "Unknown ORC compression type " + compressionName);
        }
        return compression;
    }
}
