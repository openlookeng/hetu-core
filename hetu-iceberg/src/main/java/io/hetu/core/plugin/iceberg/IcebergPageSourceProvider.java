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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.graph.Traverser;
import io.airlift.json.JsonCodec;
import io.hetu.core.plugin.iceberg.IcebergParquetColumnIOConverter.FieldContext;
import io.hetu.core.plugin.iceberg.delete.DummyFileScanTask;
import io.hetu.core.plugin.iceberg.delete.IcebergPositionDeletePageSink;
import io.hetu.core.plugin.iceberg.delete.TrinoDeleteFilter;
import io.hetu.core.plugin.iceberg.delete.TrinoRow;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.prestosql.orc.metadata.OrcColumnId;
import io.prestosql.orc.metadata.OrcType;
import io.prestosql.parquet.Field;
import io.prestosql.parquet.ParquetCorruptionException;
import io.prestosql.parquet.ParquetDataSource;
import io.prestosql.parquet.ParquetDataSourceId;
import io.prestosql.parquet.ParquetReaderOptions;
import io.prestosql.parquet.RichColumnDescriptor;
import io.prestosql.parquet.predicate.Predicate;
import io.prestosql.parquet.reader.MetadataReader;
import io.prestosql.parquet.reader.ParquetReader;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.ReaderColumns;
import io.prestosql.plugin.hive.ReaderPageSource;
import io.prestosql.plugin.hive.ReaderProjectionsAdapter;
import io.prestosql.plugin.hive.orc.HdfsOrcDataSource;
import io.prestosql.plugin.hive.orc.IcebergOrcPageSource;
import io.prestosql.plugin.hive.orc.IcebergOrcPageSource.ColumnAdaptation;
import io.prestosql.plugin.hive.orc.OrcReaderConfig;
import io.prestosql.plugin.hive.parquet.HdfsParquetDataSource;
import io.prestosql.plugin.hive.parquet.IcebergParquetPageSource;
import io.prestosql.plugin.hive.parquet.ParquetReaderConfig;
import io.prestosql.spi.PageIndexerFactory;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.EmptyPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.ConnectorIdentity;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.data.DeleteFilter;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.ParquetSchemaUtil;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIO;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_CANNOT_OPEN_SPLIT;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_CURSOR_ERROR;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_MISSING_DATA;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcLazyReadSmallRanges;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcMaxBufferSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcMaxMergeDistance;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcMaxReadBlockSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcStreamBufferSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getOrcTinyStripeThreshold;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.getParquetMaxReadBlockSize;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.isOrcBloomFiltersEnabled;
import static io.hetu.core.plugin.iceberg.IcebergSessionProperties.isOrcNestedLazy;
import static io.hetu.core.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.hetu.core.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getColumns;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getLocationProvider;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.hetu.core.plugin.iceberg.TypeConverter.ICEBERG_BINARY_TYPE;
import static io.hetu.core.plugin.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcReader.fullyProjectedLayout;
import static io.prestosql.parquet.ParquetTypeUtils.getColumnIO;
import static io.prestosql.parquet.ParquetTypeUtils.getDescriptors;
import static io.prestosql.parquet.predicate.PredicateUtils.buildPredicate;
import static io.prestosql.parquet.predicate.PredicateUtils.predicateMatches;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.predicate.Utils.nativeValueToBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.type.UuidType.UUID;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.iceberg.MetadataColumns.ROW_POSITION;
import static org.joda.time.DateTimeZone.UTC;

public class IcebergPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats fileFormatDataSourceStats;
    private final OrcReaderOptions orcReaderOptions;
    private final ParquetReaderOptions parquetReaderOptions;
    private final TypeManager typeManager;
    private final FileIoProvider fileIoProvider;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final PageIndexerFactory pageIndexerFactory;
    private final int maxOpenPartitions;

    @Inject
    public IcebergPageSourceProvider(
            HdfsEnvironment hdfsEnvironment,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            OrcReaderConfig orcReaderConfig,
            ParquetReaderConfig parquetReaderConfig,
            TypeManager typeManager,
            FileIoProvider fileIoProvider,
            JsonCodec<CommitTaskData> jsonCodec,
            IcebergFileWriterFactory fileWriterFactory,
            PageIndexerFactory pageIndexerFactory,
            IcebergConfig icebergConfig)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileFormatDataSourceStats = requireNonNull(fileFormatDataSourceStats, "fileFormatDataSourceStats is null");
        this.orcReaderOptions = requireNonNull(orcReaderConfig, "orcReaderConfig is null").toOrcReaderOptions();
        this.parquetReaderOptions = requireNonNull(parquetReaderConfig, "parquetReaderConfig is null").toParquetReaderOptions();
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.fileIoProvider = requireNonNull(fileIoProvider, "fileIoProvider is null");
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.pageIndexerFactory = requireNonNull(pageIndexerFactory, "pageIndexerFactory is null");
        requireNonNull(icebergConfig, "icebergConfig is null");
        this.maxOpenPartitions = icebergConfig.getMaxPartitionsPerWriter();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<ColumnHandle> columns)
    {
        IcebergSplit split = (IcebergSplit) connectorSplit;
        IcebergTableHandle table = (IcebergTableHandle) connectorTable;

        List<IcebergColumnHandle> icebergColumns = columns.stream()
                .map(IcebergColumnHandle.class::cast)
                .collect(toImmutableList());

        HdfsContext hdfsContext = new HdfsContext(session.getIdentity());
        FileIO fileIO = fileIoProvider.createFileIo(hdfsContext, session.getQueryId());
        FileScanTask dummyFileScanTask = new DummyFileScanTask(split.getPath(), split.getDeletes());
        Schema tableSchema = SchemaParser.fromJson(table.getTableSchemaJson());
        // Creating a DeleteFilter with no requestedSchema ensures `deleteFilterRequiredSchema` is only columns needed by the filter.
        List<IcebergColumnHandle> deleteFilterRequiredSchema = getColumns(new TrinoDeleteFilter(
                        dummyFileScanTask,
                        tableSchema,
                        ImmutableList.of(),
                        fileIO)
                        .requiredSchema(),
                typeManager);

        PartitionSpec partitionSpec = PartitionSpecParser.fromJson(tableSchema, split.getPartitionSpecJson());
        org.apache.iceberg.types.Type[] partitionColumnTypes = partitionSpec.fields().stream()
                .map(field -> field.transform().getResultType(tableSchema.findType(field.sourceId())))
                .toArray(org.apache.iceberg.types.Type[]::new);
        PartitionData partitionData = PartitionData.fromJson(split.getPartitionDataJson(), partitionColumnTypes);
        Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(partitionData, partitionSpec);

        List<IcebergColumnHandle> requiredColumns = new ArrayList<>(icebergColumns);
        deleteFilterRequiredSchema.stream()
                .filter(column -> !icebergColumns.contains(column))
                .forEach(requiredColumns::add);
        icebergColumns.stream()
                .filter(IcebergColumnHandle::isUpdateRowIdColumn)
                .findFirst().ifPresent(updateRowIdColumn -> {
                    Set<Integer> alreadyRequiredColumnIds = requiredColumns.stream()
                            .map(IcebergColumnHandle::getId)
                            .collect(toImmutableSet());
                    for (ColumnIdentity requiredColumnIdentity : updateRowIdColumn.getColumnIdentity().getChildren()) {
                        if (!alreadyRequiredColumnIds.contains(requiredColumnIdentity.getId())) {
                            if (requiredColumnIdentity.getId() == ROW_POSITION.fieldId()) {
                                requiredColumns.add(new IcebergColumnHandle(requiredColumnIdentity, BIGINT, ImmutableList.of(), BIGINT, Optional.empty()));
                            }
                            else {
                                requiredColumns.add(getColumnHandle(tableSchema.findField(requiredColumnIdentity.getId()), typeManager));
                            }
                        }
                    }
                });

        TupleDomain<IcebergColumnHandle> effectivePredicate = table.getUnenforcedPredicate()
                .simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
        if (effectivePredicate.isNone()) {
            return new EmptyPageSource();
        }

        ReaderPageSource dataPageSource = createDataPageSource(
                session,
                hdfsContext,
                new Path(split.getPath()),
                split.getStart(),
                split.getLength(),
                split.getFileSize(),
                split.getFileFormat(),
                requiredColumns,
                effectivePredicate,
                table.getNameMappingJson().map(NameMappingParser::fromJson),
                partitionKeys);

        Optional<ReaderProjectionsAdapter> projectionsAdapter = dataPageSource.getReaderColumns().map(readerColumns ->
                new ReaderProjectionsAdapter(
                        requiredColumns,
                        readerColumns,
                        column -> ((IcebergColumnHandle) column).getType(),
                        IcebergPageSourceProvider::applyProjection));

        List<IcebergColumnHandle> readColumns = dataPageSource.getReaderColumns()
                .map(readerColumns -> readerColumns.get().stream().map(IcebergColumnHandle.class::cast).collect(toList()))
                .orElse(requiredColumns);
        DeleteFilter<TrinoRow> deleteFilter = new TrinoDeleteFilter(
                dummyFileScanTask,
                tableSchema,
                readColumns,
                fileIO);

        Optional<PartitionData> partition = partitionSpec.isUnpartitioned() ? Optional.empty() : Optional.of(partitionData);
        LocationProvider locationProvider = getLocationProvider(table.getSchemaTableName(), table.getTableLocation(), table.getStorageProperties());
        Supplier<IcebergPositionDeletePageSink> positionDeleteSink = () -> new IcebergPositionDeletePageSink(
                split.getPath(),
                partitionSpec,
                partition,
                locationProvider,
                fileWriterFactory,
                hdfsEnvironment,
                hdfsContext,
                jsonCodec,
                session,
                split.getFileFormat(),
                table.getStorageProperties(),
                split.getFileRecordCount());

        Supplier<IcebergPageSink> updatedRowPageSinkSupplier = () -> new IcebergPageSink(
                tableSchema,
                PartitionSpecParser.fromJson(tableSchema, table.getPartitionSpecJson()),
                locationProvider,
                fileWriterFactory,
                pageIndexerFactory,
                hdfsEnvironment,
                hdfsContext,
                tableSchema.columns().stream().map(column -> getColumnHandle(column, typeManager)).collect(toImmutableList()),
                jsonCodec,
                session,
                split.getFileFormat(),
                table.getStorageProperties(),
                maxOpenPartitions);

        return new IcebergPageSource(
                tableSchema,
                icebergColumns,
                requiredColumns,
                readColumns,
                dataPageSource.get(),
                projectionsAdapter,
                Optional.of(deleteFilter).filter(filter -> filter.hasPosDeletes() || filter.hasEqDeletes()),
                positionDeleteSink,
                updatedRowPageSinkSupplier,
                table.getUpdatedColumns());
    }

    private ReaderPageSource createDataPageSource(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Path path,
            long start,
            long length,
            long fileSize,
            IcebergFileFormat fileFormat,
            List<IcebergColumnHandle> dataColumns,
            TupleDomain<IcebergColumnHandle> predicate,
            Optional<NameMapping> nameMapping,
            Map<Integer, Optional<String>> partitionKeys)
    {
        long fileSize1 = fileSize;
        try {
            FileStatus fileStatus = hdfsEnvironment.doAs(session.getIdentity().getUser(),
                    () -> hdfsEnvironment.getFileSystem(hdfsContext, path).getFileStatus(path));
            fileSize1 = fileStatus.getLen();
        }
        catch (IOException e) {
            throw new PrestoException(ICEBERG_FILESYSTEM_ERROR, e);
        }

        switch (fileFormat) {
            case ORC:
                return createOrcPageSource(
                        hdfsEnvironment,
                        session.getIdentity(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        fileSize1,
                        dataColumns,
                        predicate,
                        orcReaderOptions
                                .withMaxMergeDistance(getOrcMaxMergeDistance(session))
                                .withMaxBufferSize(getOrcMaxBufferSize(session))
                                .withStreamBufferSize(getOrcStreamBufferSize(session))
                                .withTinyStripeThreshold(getOrcTinyStripeThreshold(session))
                                .withMaxReadBlockSize(getOrcMaxReadBlockSize(session))
                                .withLazyReadSmallRanges(getOrcLazyReadSmallRanges(session))
                                .withNestedLazy(isOrcNestedLazy(session))
                                .withBloomFiltersEnabled(isOrcBloomFiltersEnabled(session)),
                        fileFormatDataSourceStats,
                        typeManager,
                        nameMapping,
                        partitionKeys);
            case PARQUET:
                return createParquetPageSource(
                        hdfsEnvironment,
                        session.getIdentity(),
                        hdfsEnvironment.getConfiguration(hdfsContext, path),
                        path,
                        start,
                        length,
                        fileSize1,
                        dataColumns,
                        parquetReaderOptions
                                .withMaxReadBlockSize(getParquetMaxReadBlockSize(session)),
                        predicate,
                        fileFormatDataSourceStats,
                        nameMapping,
                        partitionKeys);
            default:
                throw new PrestoException(NOT_SUPPORTED, "File format not supported for Iceberg: " + fileFormat);
        }
    }

    private static ReaderPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<IcebergColumnHandle> columns,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            OrcReaderOptions options,
            FileFormatDataSourceStats stats,
            TypeManager typeManager,
            Optional<NameMapping> nameMapping,
            Map<Integer, Optional<String>> partitionKeys)
    {
        OrcDataSource orcDataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity.getUser(), path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity.getUser(), () -> fileSystem.open(path));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    options,
                    inputStream,
                    stats);

            OrcReader reader = OrcReader.createOrcReader(orcDataSource, options)
                    .orElseThrow(() -> new PrestoException(ICEBERG_BAD_DATA, "ORC file is zero length"));

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            if (nameMapping.isPresent() && !hasIds(reader.getRootColumn())) {
                fileColumns = fileColumns.stream()
                        .map(orcColumn -> setMissingFieldIds(orcColumn, nameMapping.get(), ImmutableList.of(orcColumn.getColumnName())))
                        .collect(toImmutableList());
            }

            Map<Integer, OrcColumn> fileColumnsByIcebergId = mapIdsToOrcFileColumns(fileColumns);

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(options.isBloomFiltersEnabled());
            Map<IcebergColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));

            Optional<ReaderColumns> columnProjections = projectColumns(columns);
            Map<Integer, List<List<Integer>>> projectionsByFieldId = columns.stream()
                    .collect(groupingBy(
                            column -> column.getBaseColumnIdentity().getId(),
                            mapping(IcebergColumnHandle::getPath,
                                    Collectors.collectingAndThen(
                                            Collectors.toList(),
                                            x -> Collections.unmodifiableList(x)))));

            List<IcebergColumnHandle> readColumns = columnProjections
                    .map(readerColumns -> (List<IcebergColumnHandle>) readerColumns.get().stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()))
                    .orElse(columns);
            List<OrcColumn> fileReadColumns = new ArrayList<>(readColumns.size());
            List<Type> fileReadTypes = new ArrayList<>(readColumns.size());
            List<OrcReader.ProjectedLayout> projectedLayouts = new ArrayList<>(readColumns.size());
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(readColumns.size());

            for (IcebergColumnHandle column : readColumns) {
                verify(column.isBaseColumn(), "Column projections must be based from a root column");
                OrcColumn orcColumn = fileColumnsByIcebergId.get(column.getId());

                if (column.isIsDeletedColumn()) {
                    columnAdaptations.add(ColumnAdaptation.constantColumn(nativeValueToBlock(BOOLEAN, false)));
                }
                else if (partitionKeys.containsKey(column.getId())) {
                    Type trinoType = column.getType();
                    columnAdaptations.add(ColumnAdaptation.constantColumn(nativeValueToBlock(
                            trinoType,
                            deserializePartitionValue(trinoType, partitionKeys.get(column.getId()).orElse(null), column.getName()))));
                }
                else if (column.isRowPositionColumn()) {
                    columnAdaptations.add(ColumnAdaptation.positionColumn());
                }
                else if (orcColumn != null) {
                    Type readType = getOrcReadType(column.getType(), typeManager);

                    if (column.getType() == UUID && !"UUID".equals(orcColumn.getAttributes().get(ICEBERG_BINARY_TYPE))) {
                        throw new PrestoException(ICEBERG_BAD_DATA, format("Expected ORC column for UUID data to be annotated with %s=UUID: %s", ICEBERG_BINARY_TYPE, orcColumn));
                    }

                    List<List<Integer>> fieldIdProjections = projectionsByFieldId.get(column.getId());
                    OrcReader.ProjectedLayout projectedLayout = IcebergOrcProjectedLayout.createProjectedLayout(orcColumn, fieldIdProjections);

                    int sourceIndex = fileReadColumns.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(readType);
                    projectedLayouts.add(projectedLayout);

                    for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : effectivePredicateDomains.entrySet()) {
                        IcebergColumnHandle predicateColumn = domainEntry.getKey();
                        OrcColumn predicateOrcColumn = fileColumnsByIcebergId.get(predicateColumn.getId());
                        if (predicateOrcColumn != null && column.getColumnIdentity().equals(predicateColumn.getBaseColumnIdentity())) {
                            predicateBuilder.addColumn(predicateOrcColumn.getColumnId(), domainEntry.getValue());
                        }
                    }
                }
                else {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(column.getType()));
                }
            }

            AggregatedMemoryContext memoryUsage = newSimpleAggregatedMemoryContext();
            OrcDataSourceId orcDataSourceId = orcDataSource.getId();
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    predicateBuilder.build(),
                    start,
                    length,
                    UTC,
                    memoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSourceId, exception));
            return new ReaderPageSource(
                    new IcebergOrcPageSource(
                            recordReader,
                            columnAdaptations,
                            orcDataSource,
                            Optional.empty(),
                            Optional.empty(),
                            memoryUsage,
                            stats),
                    columnProjections);
        }
        catch (Exception e) {
            if (orcDataSource != null) {
                try {
                    orcDataSource.close();
                }
                catch (IOException ignored) {
                }
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    private static boolean hasIds(OrcColumn column)
    {
        if (column.getAttributes().containsKey(ORC_ICEBERG_ID_KEY)) {
            return true;
        }

        return column.getNestedColumns().stream().anyMatch(IcebergPageSourceProvider::hasIds);
    }

    private static OrcColumn setMissingFieldIds(OrcColumn column, NameMapping nameMapping, List<String> qualifiedPath)
    {
        MappedField mappedField = nameMapping.find(qualifiedPath);

        ImmutableMap.Builder<String, String> attributes = ImmutableMap.<String, String>builder()
                .putAll(column.getAttributes());
        if (mappedField != null && mappedField.id() != null) {
            attributes.put(ORC_ICEBERG_ID_KEY, String.valueOf(mappedField.id()));
        }

        return new OrcColumn(
                column.getPath(),
                column.getColumnId(),
                column.getColumnName(),
                column.getColumnType(),
                column.getOrcDataSourceId(),
                column.getNestedColumns().stream()
                        .map(nestedColumn -> {
                            ImmutableList.Builder<String> nextQualifiedPath = ImmutableList.<String>builder()
                                    .addAll(qualifiedPath);
                            if (column.getColumnType().equals(OrcType.OrcTypeKind.LIST)) {
                                // The Trino ORC reader uses "item" for list element names, but the NameMapper expects "element"
                                nextQualifiedPath.add("element");
                            }
                            else {
                                nextQualifiedPath.add(nestedColumn.getColumnName());
                            }
                            return setMissingFieldIds(nestedColumn, nameMapping, nextQualifiedPath.build());
                        })
                        .collect(toImmutableList()),
                attributes.build());
    }

    /**
     * Gets the index based dereference chain to get from the readColumnHandle to the expectedColumnHandle
     */
    private static List<Integer> applyProjection(ColumnHandle expectedColumnHandle, ColumnHandle readColumnHandle)
    {
        IcebergColumnHandle expectedColumn = (IcebergColumnHandle) expectedColumnHandle;
        IcebergColumnHandle readColumn = (IcebergColumnHandle) readColumnHandle;
        checkState(readColumn.isBaseColumn(), "Read column path must be a base column");

        ImmutableList.Builder<Integer> dereferenceChain = ImmutableList.builder();
        ColumnIdentity columnIdentity = readColumn.getColumnIdentity();
        for (Integer fieldId : expectedColumn.getPath()) {
            ColumnIdentity nextChild = columnIdentity.getChildByFieldId(fieldId);
            dereferenceChain.add(columnIdentity.getChildIndexByFieldId(fieldId));
            columnIdentity = nextChild;
        }

        return dereferenceChain.build();
    }

    private static Map<Integer, OrcColumn> mapIdsToOrcFileColumns(List<OrcColumn> columns)
    {
        ImmutableMap.Builder<Integer, OrcColumn> columnsById = ImmutableMap.builder();
        Traverser.forTree(OrcColumn::getNestedColumns)
                .depthFirstPreOrder(columns)
                .forEach(column -> {
                    OrcColumnId fieldId = column.getColumnId();
                    if (fieldId != null) {
                        columnsById.put(fieldId.getId(), column);
                    }
                });
        return columnsById.build();
    }

    private static Integer getIcebergFieldId(OrcColumn column)
    {
        String icebergId = column.getAttributes().get(ORC_ICEBERG_ID_KEY);
        verify(icebergId != null, format("column %s does not have %s property", column, ORC_ICEBERG_ID_KEY));
        return Integer.valueOf(icebergId);
    }

    private static Type getOrcReadType(Type columnType, TypeManager typeManager)
    {
        if (columnType == UUID) {
            // ORC spec doesn't have UUID
            // TODO read into Int128ArrayBlock for better performance when operating on read values
            // TODO: Validate that the OrcColumn attribute ICEBERG_BINARY_TYPE is equal to "UUID"
            return VARBINARY;
        }

        if (columnType instanceof ArrayType) {
            return new ArrayType(getOrcReadType(((ArrayType) columnType).getElementType(), typeManager));
        }
        if (columnType instanceof MapType) {
            MapType mapType = (MapType) columnType;
            Type keyType = getOrcReadType(mapType.getKeyType(), typeManager);
            Type valueType = getOrcReadType(mapType.getValueType(), typeManager);
            return new MapType(keyType, valueType, typeManager.getTypeOperators());
        }
        if (columnType instanceof RowType) {
            return RowType.from(((RowType) columnType).getFields().stream()
                    .map(field -> new RowType.Field(field.getName(), getOrcReadType(field.getType(), typeManager)))
                    .collect(toImmutableList()));
        }

        return columnType;
    }

    private static class IdBasedFieldMapperFactory
            implements OrcReader.FieldMapperFactory
    {
        // Stores a mapping between subfield names and ids for every top-level/nested column id
        private final Map<Integer, Map<String, Integer>> fieldNameToIdMappingForTableColumns;

        public IdBasedFieldMapperFactory(List<IcebergColumnHandle> columns)
        {
            requireNonNull(columns, "columns is null");

            ImmutableMap.Builder<Integer, Map<String, Integer>> mapping = ImmutableMap.builder();
            for (IcebergColumnHandle column : columns) {
                // Recursively compute subfield name to id mapping for every column
                if (column.isUpdateRowIdColumn()) {
                    // The update $row_id column contains fields which should not be accounted for in the mapping.
                    continue;
                }
                populateMapping(column.getColumnIdentity(), mapping);
            }

            this.fieldNameToIdMappingForTableColumns = mapping.build();
        }

        @Override
        public OrcReader.FieldMapper create(OrcColumn column)
        {
            Map<Integer, OrcColumn> nestedColumns = uniqueIndex(
                    column.getNestedColumns(),
                    IcebergPageSourceProvider::getIcebergFieldId);

            int icebergId = getIcebergFieldId(column);
            return new IdBasedFieldMapper(nestedColumns, fieldNameToIdMappingForTableColumns.get(icebergId));
        }

        private static void populateMapping(
                ColumnIdentity identity,
                ImmutableMap.Builder<Integer, Map<String, Integer>> fieldNameToIdMappingForTableColumns)
        {
            List<ColumnIdentity> children = identity.getChildren();
            fieldNameToIdMappingForTableColumns.put(
                    identity.getId(),
                    children.stream()
                            // Lower casing is required here because ORC StructColumnReader does the same before mapping
                            .collect(toImmutableMap(child -> child.getName().toLowerCase(ENGLISH), ColumnIdentity::getId)));

            for (ColumnIdentity child : children) {
                populateMapping(child, fieldNameToIdMappingForTableColumns);
            }
        }
    }

    private static class IdBasedFieldMapper
            implements OrcReader.FieldMapper
    {
        private final Map<Integer, OrcColumn> idToColumnMappingForFile;
        private final Map<String, Integer> nameToIdMappingForTableColumns;

        public IdBasedFieldMapper(Map<Integer, OrcColumn> idToColumnMappingForFile, Map<String, Integer> nameToIdMappingForTableColumns)
        {
            this.idToColumnMappingForFile = requireNonNull(idToColumnMappingForFile, "idToColumnMappingForFile is null");
            this.nameToIdMappingForTableColumns = requireNonNull(nameToIdMappingForTableColumns, "nameToIdMappingForTableColumns is null");
        }

        @Override
        public OrcColumn get(String fieldName)
        {
            int fieldId = requireNonNull(
                    nameToIdMappingForTableColumns.get(fieldName),
                    () -> format("Id mapping for field %s not found", fieldName));
            return idToColumnMappingForFile.get(fieldId);
        }
    }

    private static ReaderPageSource createParquetPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorIdentity identity,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<IcebergColumnHandle> regularColumns,
            ParquetReaderOptions options,
            TupleDomain<IcebergColumnHandle> effectivePredicate,
            FileFormatDataSourceStats fileFormatDataSourceStats,
            Optional<NameMapping> nameMapping,
            Map<Integer, Optional<String>> partitionKeys)
    {
        AggregatedMemoryContext memoryContext = newSimpleAggregatedMemoryContext();

        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity.getUser(), path, configuration);
            FSDataInputStream inputStream = hdfsEnvironment.doAs(identity.getUser(), () -> fileSystem.open(path));
            dataSource = new HdfsParquetDataSource(new ParquetDataSourceId(path.toString()), fileSize, inputStream, fileFormatDataSourceStats, options);
            ParquetDataSource theDataSource = dataSource; // extra variable required for lambda below
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(identity.getUser(), () -> MetadataReader.readFooter(theDataSource));
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();
            if (nameMapping.isPresent() && !ParquetSchemaUtil.hasIds(fileSchema)) {
                // NameMapping conversion is necessary because MetadataReader converts all column names to lowercase and NameMapping is case sensitive
                fileSchema = ParquetSchemaUtil.applyNameMapping(fileSchema, convertToLowercase(nameMapping.get()));
            }

            // Mapping from Iceberg field ID to Parquet fields.
            Map<Integer, org.apache.parquet.schema.Type> parquetIdToField = fileSchema.getFields().stream()
                    .filter(field -> field.getId() != null)
                    .collect(toImmutableMap(field -> field.getId().intValue(), Function.identity()));

            Optional<ReaderColumns> columnProjections = projectColumns(regularColumns);
            List<IcebergColumnHandle> readColumns = columnProjections
                    .map(readerColumns -> (List<IcebergColumnHandle>) readerColumns.get().stream().map(IcebergColumnHandle.class::cast).collect(toImmutableList()))
                    .orElse(regularColumns);

            List<org.apache.parquet.schema.Type> parquetFields = readColumns.stream()
                    .map(column -> parquetIdToField.get(column.getId()))
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), parquetFields.stream().filter(Objects::nonNull).collect(toImmutableList()));
            Map<List<String>, RichColumnDescriptor> descriptorsByPath = getDescriptors(fileSchema, requestedSchema);
            TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(descriptorsByPath, effectivePredicate);
            Predicate parquetPredicate = buildPredicate(requestedSchema, parquetTupleDomain, descriptorsByPath, UTC);

            long nextStart = 0;
            ImmutableList.Builder<Long> blockStarts = ImmutableList.builder();
            List<BlockMetaData> blocks = new ArrayList<>();
            for (BlockMetaData block : parquetMetadata.getBlocks()) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (start <= firstDataPage && firstDataPage < start + length &&
                        predicateMatches(parquetPredicate, block, dataSource, descriptorsByPath, parquetTupleDomain)) {
                    blocks.add(block);
                    blockStarts.add(nextStart);
                }
                nextStart += block.getRowCount();
            }

            MessageColumnIO messageColumnIO = getColumnIO(fileSchema, requestedSchema);
            ParquetReader parquetReader = new ParquetReader(
                    Optional.ofNullable(fileMetaData.getCreatedBy()),
                    messageColumnIO,
                    blocks,
                    blockStarts.build(),
                    dataSource,
                    UTC,
                    memoryContext,
                    options);

            ConstantPopulatingPageSource.Builder constantPopulatingPageSourceBuilder = ConstantPopulatingPageSource.builder();
            int parquetSourceChannel = 0;

            ImmutableList.Builder<Type> trinoTypes = ImmutableList.builder();
            ImmutableList.Builder<Optional<Field>> internalFields = ImmutableList.builder();
            ImmutableList.Builder<Boolean> rowIndexChannels = ImmutableList.builder();
            for (int columnIndex = 0; columnIndex < readColumns.size(); columnIndex++) {
                IcebergColumnHandle column = readColumns.get(columnIndex);
                if (column.isIsDeletedColumn()) {
                    constantPopulatingPageSourceBuilder.addConstantColumn(nativeValueToBlock(BOOLEAN, false));
                }
                else if (partitionKeys.containsKey(column.getId())) {
                    Type trinoType = column.getType();
                    constantPopulatingPageSourceBuilder.addConstantColumn(nativeValueToBlock(
                            trinoType,
                            deserializePartitionValue(trinoType, partitionKeys.get(column.getId()).orElse(null), column.getName())));
                }
                else if (column.isRowPositionColumn()) {
                    trinoTypes.add(BIGINT);
                    internalFields.add(Optional.empty());
                    rowIndexChannels.add(true);
                    constantPopulatingPageSourceBuilder.addDelegateColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
                else {
                    rowIndexChannels.add(false);
                    org.apache.parquet.schema.Type parquetField = parquetFields.get(columnIndex);
                    Type trinoType = column.getBaseType();
                    trinoTypes.add(trinoType);

                    if (parquetField == null) {
                        internalFields.add(Optional.empty());
                    }
                    else {
                        // The top level columns are already mapped by name/id appropriately.
                        ColumnIO columnIO = messageColumnIO.getChild(parquetField.getName());
                        internalFields.add(IcebergParquetColumnIOConverter.constructField(new FieldContext(trinoType, column.getColumnIdentity()), columnIO));
                    }

                    constantPopulatingPageSourceBuilder.addDelegateColumn(parquetSourceChannel);
                    parquetSourceChannel++;
                }
            }

            return new ReaderPageSource(
                    constantPopulatingPageSourceBuilder.build(new IcebergParquetPageSource(parquetReader, trinoTypes.build(), rowIndexChannels.build(), internalFields.build())),
                    columnProjections);
        }
        catch (IOException | RuntimeException e) {
            try {
                if (dataSource != null) {
                    dataSource.close();
                }
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = format("Error opening Iceberg split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());

            if (e instanceof ParquetCorruptionException) {
                throw new PrestoException(ICEBERG_BAD_DATA, message, e);
            }

            if (e instanceof BlockMissingException) {
                throw new PrestoException(ICEBERG_MISSING_DATA, message, e);
            }
            throw new PrestoException(ICEBERG_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    /**
     * Create a new NameMapping with the same names but converted to lowercase.
     * @param nameMapping The original NameMapping, potentially containing non-lowercase characters
     */
    private static NameMapping convertToLowercase(NameMapping nameMapping)
    {
        return NameMapping.of(convertToLowercase(nameMapping.asMappedFields().fields()));
    }

    private static MappedFields convertToLowercase(MappedFields mappedFields)
    {
        if (mappedFields == null) {
            return null;
        }
        return MappedFields.of(convertToLowercase(mappedFields.fields()));
    }

    private static List<MappedField> convertToLowercase(List<MappedField> fields)
    {
        return fields.stream()
                .map(mappedField -> {
                    Set<String> lowercaseNames = mappedField.names().stream().map(name -> name.toLowerCase(ENGLISH)).collect(toImmutableSet());
                    return MappedField.of(mappedField.id(), lowercaseNames, convertToLowercase(mappedField.nestedMapping()));
                })
                .collect(toImmutableList());
    }

    private static class IcebergOrcProjectedLayout
            implements OrcReader.ProjectedLayout
    {
        private final Map<Integer, OrcReader.ProjectedLayout> projectedLayoutForFieldId;

        private IcebergOrcProjectedLayout(Map<Integer, OrcReader.ProjectedLayout> projectedLayoutForFieldId)
        {
            this.projectedLayoutForFieldId = ImmutableMap.copyOf(requireNonNull(projectedLayoutForFieldId, "projectedLayoutForFieldId is null"));
        }

        public static OrcReader.ProjectedLayout createProjectedLayout(OrcColumn root, List<List<Integer>> fieldIdDereferences)
        {
            if (fieldIdDereferences.stream().anyMatch(List::isEmpty)) {
                return fullyProjectedLayout();
            }

            Map<Integer, List<List<Integer>>> dereferencesByField = fieldIdDereferences.stream().collect(
                    Collectors.groupingBy(
                            sequence -> sequence.get(0),
                            mapping(sequence -> sequence.subList(1, sequence.size()),
                                    Collectors.collectingAndThen(
                                            Collectors.toList(),
                                            x -> Collections.unmodifiableList(x)))));

            ImmutableMap.Builder<Integer, OrcReader.ProjectedLayout> fieldLayouts = ImmutableMap.builder();
            for (OrcColumn nestedColumn : root.getNestedColumns()) {
                Integer fieldId = getIcebergFieldId(nestedColumn);
                if (dereferencesByField.containsKey(fieldId)) {
                    fieldLayouts.put(fieldId, createProjectedLayout(nestedColumn, dereferencesByField.get(fieldId)));
                }
            }

            return new IcebergOrcProjectedLayout(fieldLayouts.build());
        }

        @Override
        public OrcReader.ProjectedLayout getFieldLayout(OrcColumn orcColumn)
        {
            int fieldId = getIcebergFieldId(orcColumn);
            return projectedLayoutForFieldId.getOrDefault(fieldId, fullyProjectedLayout());
        }
    }

    /**
     * Creates a mapping between the input {@param columns} and base columns if required.
     */
    public static Optional<ReaderColumns> projectColumns(List<IcebergColumnHandle> columns)
    {
        requireNonNull(columns, "columns is null");

        // No projection is required if all columns are base columns
        if (columns.stream().allMatch(IcebergColumnHandle::isBaseColumn)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> projectedColumns = ImmutableList.builder();
        ImmutableList.Builder<Integer> outputColumnMapping = ImmutableList.builder();
        Map<Integer, Integer> mappedFieldIds = new HashMap<>();
        int projectedColumnCount = 0;

        for (IcebergColumnHandle column : columns) {
            int baseColumnId = column.getBaseColumnIdentity().getId();
            Integer mapped = mappedFieldIds.get(baseColumnId);

            if (mapped == null) {
                projectedColumns.add(column.getBaseColumn());
                mappedFieldIds.put(baseColumnId, projectedColumnCount);
                outputColumnMapping.add(projectedColumnCount);
                projectedColumnCount++;
            }
            else {
                outputColumnMapping.add(mapped);
            }
        }

        return Optional.of(new ReaderColumns(projectedColumns.build(), outputColumnMapping.build()));
    }

    private static TupleDomain<ColumnDescriptor> getParquetTupleDomain(Map<List<String>, RichColumnDescriptor> descriptorsByPath, TupleDomain<IcebergColumnHandle> effectivePredicate)
    {
        if (effectivePredicate.isNone()) {
            return TupleDomain.none();
        }

        ImmutableMap.Builder<ColumnDescriptor, Domain> predicate = ImmutableMap.builder();
        effectivePredicate.getDomains().get().forEach((columnHandle, domain) -> {
            String baseType = columnHandle.getType().getTypeSignature().getBase();
            // skip looking up predicates for complex types as Parquet only stores stats for primitives
            if (!baseType.equals(StandardTypes.MAP) && !baseType.equals(StandardTypes.ARRAY) && !baseType.equals(StandardTypes.ROW)) {
                RichColumnDescriptor descriptor = descriptorsByPath.get(ImmutableList.of(columnHandle.getName()));
                if (descriptor != null) {
                    predicate.put(descriptor, domain);
                }
            }
        });
        return TupleDomain.withColumnDomains(predicate.build());
    }

    private static PrestoException handleException(OrcDataSourceId dataSourceId, Exception exception)
    {
        if (exception instanceof PrestoException) {
            return (PrestoException) exception;
        }
        if (exception instanceof OrcCorruptionException) {
            return new PrestoException(ICEBERG_BAD_DATA, exception);
        }
        return new PrestoException(ICEBERG_CURSOR_ERROR, format("Failed to read ORC file: %s", dataSourceId), exception);
    }
}
