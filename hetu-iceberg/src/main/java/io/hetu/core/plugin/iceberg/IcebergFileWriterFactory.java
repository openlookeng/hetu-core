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
package io.hetu.core.plugin.iceberg;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.orc.OrcDataSink;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcWriterOptions;
import io.prestosql.orc.OrcWriterStats;
import io.prestosql.orc.OutputStreamOrcDataSink;
import io.prestosql.parquet.writer.ParquetWriterOptions;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.NodeVersion;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.OrcWriterConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.weakref.jmx.Managed;

import javax.inject.Inject;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITER_OPEN_ERROR;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_WRITE_VALIDATION_FAILED;
import static io.hetu.core.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_COLUMNS_KEY;
import static io.hetu.core.plugin.iceberg.IcebergMetadata.ORC_BLOOM_FILTER_FPP_KEY;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getCompressionCodec;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcStringStatisticsLimit;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxDictionaryMemory;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeRows;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcWriterMaxStripeSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcWriterMinStripeSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcWriterValidateMode;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getParquetWriterBatchSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getParquetWriterBlockSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getParquetWriterPageSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.isOrcWriterValidate;
import static io.hetu.core.plugin.iceberg.TypeConverter.toOrcType;
import static io.hetu.core.plugin.iceberg.TypeConverter.toTrinoType;
import static io.hetu.core.plugin.iceberg.util.PrimitiveTypeMapBuilder.makeTypeMap;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_QUERY_ID_NAME;
import static io.prestosql.plugin.hive.HiveMetadata.PRESTO_VERSION_NAME;
import static io.prestosql.plugin.hive.HiveTableProperties.ORC_BLOOM_FILTER_FPP;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Double.parseDouble;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.apache.iceberg.io.DeleteSchemaUtil.pathPosSchema;
import static org.apache.iceberg.parquet.ParquetSchemaUtil.convert;

public class IcebergFileWriterFactory
{
    private static final Schema POSITION_DELETE_SCHEMA = pathPosSchema();
    private static final MetricsConfig FULL_METRICS_CONFIG = MetricsConfig.fromProperties(ImmutableMap.of(DEFAULT_WRITE_METRICS_MODE, "full"));
    private static final Splitter COLUMN_NAMES_SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final NodeVersion nodeVersion;
    private final FileFormatDataSourceStats readStats;
    private final OrcWriterStats orcWriterStats = new OrcWriterStats();
    private final OrcWriterOptions orcWriterOptions;

    @Inject
    public IcebergFileWriterFactory(
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            NodeVersion nodeVersion,
            FileFormatDataSourceStats readStats,
            OrcWriterConfig orcWriterConfig)
    {
        checkArgument(!requireNonNull(orcWriterConfig, "orcWriterConfig is null").isUseLegacyVersion(), "the ORC writer shouldn't be configured to use a legacy version");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.nodeVersion = requireNonNull(nodeVersion, "nodeVersion is null");
        this.readStats = requireNonNull(readStats, "readStats is null");
        this.orcWriterOptions = orcWriterConfig.toOrcWriterOptions();
    }

    @Managed
    public OrcWriterStats getOrcWriterStats()
    {
        return orcWriterStats;
    }

    public IcebergFileWriter createDataFileWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext,
            IcebergFileFormat fileFormat,
            MetricsConfig metricsConfig,
            Map<String, String> storageProperties)
    {
        switch (fileFormat) {
            case PARQUET:
                // TODO use metricsConfig
                return createParquetWriter(outputPath, icebergSchema, jobConf, session, hdfsContext);
            case ORC:
                return createOrcWriter(metricsConfig, outputPath, icebergSchema, jobConf, session, storageProperties);
            default:
                throw new PrestoException(NOT_SUPPORTED, "File format not supported: " + fileFormat);
        }
    }

    public IcebergFileWriter createPositionDeleteWriter(
            Path outputPath,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext,
            IcebergFileFormat fileFormat,
            Map<String, String> storageProperties)
    {
        switch (fileFormat) {
            case PARQUET:
                return createParquetWriter(outputPath, POSITION_DELETE_SCHEMA, jobConf, session, hdfsContext);
            case ORC:
                return createOrcWriter(FULL_METRICS_CONFIG, outputPath, POSITION_DELETE_SCHEMA, jobConf, session, storageProperties);
            default:
                throw new PrestoException(NOT_SUPPORTED, "File format not supported: " + fileFormat);
        }
    }

    private IcebergFileWriter createParquetWriter(
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            HdfsContext hdfsContext)
    {
        List<String> fileColumnNames = icebergSchema.columns().stream()
                .map(Types.NestedField::name)
                .collect(toImmutableList());
        List<Type> fileColumnTypes = icebergSchema.columns().stream()
                .map(column -> toTrinoType(column.type(), typeManager))
                .collect(toImmutableList());

        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity().getUser(), outputPath, jobConf);

            Callable<Void> rollbackAction = () -> {
                fileSystem.delete(outputPath, false);
                return null;
            };

            ParquetWriterOptions parquetWriterOptions = ParquetWriterOptions.builder()
                    .setMaxPageSize(getParquetWriterPageSize(session))
                    .setMaxBlockSize(getParquetWriterBlockSize(session))
                    .setBatchSize(getParquetWriterBatchSize(session))
                    .build();

            return new IcebergParquetFileWriter(
                    hdfsEnvironment.doAs(session.getIdentity().getUser(), () -> fileSystem.create(outputPath)),
                    rollbackAction,
                    fileColumnTypes,
                    convert(icebergSchema, "table"),
                    makeTypeMap(fileColumnTypes, fileColumnNames),
                    parquetWriterOptions,
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    getCompressionCodec(session).getParquetCompressionCodec(),
                    nodeVersion.toString(),
                    outputPath,
                    hdfsEnvironment,
                    hdfsContext);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating Parquet file", e);
        }
    }

    private IcebergFileWriter createOrcWriter(
            MetricsConfig metricsConfig,
            Path outputPath,
            Schema icebergSchema,
            JobConf jobConf,
            ConnectorSession session,
            Map<String, String> storageProperties)
    {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(session.getIdentity().getUser(), outputPath, jobConf);
            OrcDataSink orcDataSink = hdfsEnvironment.doAs(session.getIdentity().getUser(), () -> new OutputStreamOrcDataSink(fileSystem.create(outputPath)));
            Callable<Void> rollbackAction = () -> {
                hdfsEnvironment.doAs(session.getIdentity().getUser(), () -> fileSystem.delete(outputPath, false));
                return null;
            };

            List<Types.NestedField> columnFields = icebergSchema.columns();
            List<String> fileColumnNames = columnFields.stream()
                    .map(Types.NestedField::name)
                    .collect(toImmutableList());
            List<Type> fileColumnTypes = columnFields.stream()
                    .map(Types.NestedField::type)
                    .map(type -> toTrinoType(type, typeManager))
                    .collect(toImmutableList());

            Optional<Supplier<OrcDataSource>> validationInputFactory = Optional.empty();
            if (isOrcWriterValidate(session)) {
                validationInputFactory = Optional.of(() -> {
                    try {
                        return new HdfsOrcDataSource(
                                new OrcDataSourceId(outputPath.toString()),
                                hdfsEnvironment.doAs(session.getIdentity().getUser(), () -> fileSystem.getFileStatus(outputPath).getLen()),
                                new OrcReaderOptions(),
                                hdfsEnvironment.doAs(session.getIdentity().getUser(), () -> fileSystem.open(outputPath)),
                                readStats);
                    }
                    catch (IOException e) {
                        throw new PrestoException(ICEBERG_WRITE_VALIDATION_FAILED, e);
                    }
                });
            }

            return new IcebergOrcFileWriter(
                    metricsConfig,
                    icebergSchema,
                    orcDataSink,
                    rollbackAction,
                    fileColumnNames,
                    fileColumnTypes,
                    toOrcType(icebergSchema),
                    getCompressionCodec(session).getOrcCompressionKind(),
                    withBloomFilterOptions(orcWriterOptions, storageProperties)
                            .withStripeMinSize(getOrcWriterMinStripeSize(session))
                            .withStripeMaxSize(getOrcWriterMaxStripeSize(session))
                            .withStripeMaxRowCount(getOrcWriterMaxStripeRows(session))
                            .withDictionaryMaxMemory(getOrcWriterMaxDictionaryMemory(session))
                            .withMaxStringStatisticsLimit(getOrcStringStatisticsLimit(session)),
                    IntStream.range(0, fileColumnNames.size()).toArray(),
                    ImmutableMap.<String, String>builder()
                            .put(PRESTO_VERSION_NAME, nodeVersion.toString())
                            .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                            .build(),
                    validationInputFactory,
                    getOrcWriterValidateMode(session),
                    orcWriterStats);
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_WRITER_OPEN_ERROR, "Error creating ORC file", e);
        }
    }

    public static OrcWriterOptions withBloomFilterOptions(OrcWriterOptions orcWriterOptions, Map<String, String> storageProperties)
    {
        if (storageProperties.containsKey(ORC_BLOOM_FILTER_COLUMNS_KEY)) {
            if (!storageProperties.containsKey(ORC_BLOOM_FILTER_FPP_KEY)) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, "FPP for Bloom filter is missing");
            }
            try {
                double fpp = parseDouble(storageProperties.get(ORC_BLOOM_FILTER_FPP_KEY));
                return OrcWriterOptions.builderFrom(orcWriterOptions)
                        .setBloomFilterColumns(ImmutableSet.copyOf(COLUMN_NAMES_SPLITTER.splitToList(storageProperties.get(ORC_BLOOM_FILTER_COLUMNS_KEY))))
                        .setBloomFilterFpp(fpp)
                        .build();
            }
            catch (NumberFormatException e) {
                throw new PrestoException(ICEBERG_INVALID_METADATA, format("Invalid value for %s property: %s", ORC_BLOOM_FILTER_FPP, storageProperties.get(ORC_BLOOM_FILTER_FPP_KEY)));
            }
        }
        return orcWriterOptions;
    }
}
