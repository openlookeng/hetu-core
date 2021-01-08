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
package io.prestosql.plugin.hive.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCacheProperties;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcFileTail;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceFactory;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.hdfs.BlockMissingException;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.OrcReader.handleCacheLoadException;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.INT;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.LONG;
import static io.prestosql.orc.metadata.OrcType.OrcTypeKind.STRUCT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_BAD_DATA;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_FILE_MISSING_COLUMN_NAMES;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcLazyReadSmallRanges;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxMergeDistance;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcMaxReadBlockSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcStreamBufferSize;
import static io.prestosql.plugin.hive.HiveSessionProperties.getOrcTinyStripeThreshold;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersCacheEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcBloomFiltersEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcFileTailCacheEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcRowDataCacheEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcRowIndexCacheEnabled;
import static io.prestosql.plugin.hive.HiveSessionProperties.isOrcStripeFooterCacheEnabled;
import static io.prestosql.plugin.hive.orc.OrcPageSource.handleException;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;

public class OrcPageSourceFactory
        implements HivePageSourceFactory
{
    // ACID format column names
    public static final String ACID_COLUMN_OPERATION = "operation";
    public static final String ACID_COLUMN_ORIGINAL_TRANSACTION = "originalTransaction";
    public static final String ACID_COLUMN_BUCKET = "bucket";
    public static final String ACID_COLUMN_ROW_ID = "rowId";
    public static final String ACID_COLUMN_CURRENT_TRANSACTION = "currentTransaction";
    public static final String ACID_COLUMN_ROW_STRUCT = "row";
    public static final List<String> EAGER_LOAD_INDEX_ID = ImmutableList.of("BITMAP");

    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private static final Logger log = Logger.get(OrcPageSourceFactory.class);

    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final OrcCacheStore orcCacheStore;
    private final int domainCompactionThreshold;

    @Inject
    public OrcPageSourceFactory(TypeManager typeManager, HiveConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, OrcCacheStore orcCacheStore)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        requireNonNull(config, "config is null");
        this.useOrcColumnNames = config.isUseOrcColumnNames();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.orcCacheStore = orcCacheStore;
        this.domainCompactionThreshold = config.getDomainCompactionThreshold();
    }

    @Override
    public Optional<? extends ConnectorPageSource> createPageSource(
            Configuration configuration,
            ConnectorSession session,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            List<HiveColumnHandle> columns,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            Optional<DynamicFilterSupplier> dynamicFilters,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            boolean splitCacheable)
    {
        if (!HiveUtil.isDeserializerClass(schema, OrcSerde.class)) {
            return Optional.empty();
        }

        // per HIVE-13040 and ORC-162, empty files are allowed
        if (fileSize == 0) {
            return Optional.of(new FixedPageSource(ImmutableList.of()));
        }
        OrcCacheProperties orcCacheProperties = new OrcCacheProperties(
                isOrcFileTailCacheEnabled(session),
                isOrcStripeFooterCacheEnabled(session),
                isOrcRowIndexCacheEnabled(session),
                isOrcBloomFiltersCacheEnabled(session),
                isOrcRowDataCacheEnabled(session) && splitCacheable);
        return Optional.of(createOrcPageSource(
                hdfsEnvironment,
                session.getUser(),
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                useOrcColumnNames,
                isFullAcidTable(Maps.fromProperties(schema)),
                effectivePredicate,
                hiveStorageTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats,
                dynamicFilters,
                deleteDeltaLocations,
                startRowOffsetOfFile,
                indexes,
                splitMetadata,
                orcCacheStore,
                orcCacheProperties,
                domainCompactionThreshold,
                session.isPageMetadataEnabled()));
    }

    public static OrcPageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            boolean isFullAcid,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            DataSize maxMergeDistance,
            DataSize maxBufferSize,
            DataSize streamBufferSize,
            DataSize tinyStripeThreshold,
            DataSize maxReadBlockSize,
            boolean lazyReadSmallRanges,
            boolean orcBloomFiltersEnabled,
            FileFormatDataSourceStats stats,
            Optional<DynamicFilterSupplier> dynamicFilters,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            int domainCompactionThreshold,
            boolean pageMetadataEnabled)
    {
        for (HiveColumnHandle column : columns) {
            checkArgument(
                    column.getColumnType() == HiveColumnHandle.ColumnType.REGULAR || column.getHiveColumnIndex() == HiveColumnHandle.ROW_ID__COLUMN_INDEX,
                    "column type must be regular: %s", column);
        }
        checkArgument(!effectivePredicate.isNone());

        OrcDataSource orcDataSource;
        try {
            //Always create a lazy Stream. HDFS stream opened only when required.
            FSDataInputStream inputStream = new FSDataInputStream(new LazyFSInputStream(() -> {
                FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
                return hdfsEnvironment.doAs(sessionUser, () -> fileSystem.open(path));
            }));
            orcDataSource = new HdfsOrcDataSource(
                    new OrcDataSourceId(path.toString()),
                    fileSize,
                    maxMergeDistance,
                    maxBufferSize,
                    streamBufferSize,
                    lazyReadSmallRanges,
                    inputStream,
                    stats);
        }
        catch (Exception e) {
            if (nullToEmpty(e.getMessage()).trim().equals("Filesystem closed") ||
                    e instanceof FileNotFoundException) {
                throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, splitError(e, path, start, length), e);
        }

        AggregatedMemoryContext systemMemoryUsage = newSimpleAggregatedMemoryContext();
        try {
            OrcDataSource readerLocalDataSource = OrcReader.wrapWithCacheIfTiny(orcDataSource, tinyStripeThreshold);
            OrcFileTail fileTail;
            if (orcCacheProperties.isFileTailCacheEnabled()) {
                try {
                    fileTail = orcCacheStore.getFileTailCache().get(readerLocalDataSource.getId(), () -> OrcPageSourceFactory.createFileTail(orcDataSource));
                }
                catch (UncheckedExecutionException | ExecutionException executionException) {
                    handleCacheLoadException(executionException);
                    log.debug(executionException.getCause(), "Error while caching the Orc file tail. Falling back to default flow");
                    fileTail = OrcPageSourceFactory.createFileTail(orcDataSource);
                }
            }
            else {
                fileTail = OrcPageSourceFactory.createFileTail(orcDataSource);
            }
            OrcReader reader = new OrcReader(readerLocalDataSource, fileTail, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize);

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            List<OrcColumn> fileReadColumns = isFullAcid ? new ArrayList<>(columns.size() + 5) : new ArrayList<>(columns.size());
            List<Type> fileReadTypes = isFullAcid ? new ArrayList<>(columns.size() + 5) : new ArrayList<>(columns.size());
            ImmutableList<String> acidColumnNames = null;
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(columns.size());
            // Only Hive ACID files will begin with bucket_
            boolean fileNameContainsBucket = path.getName().contains("bucket");
            if (isFullAcid && fileNameContainsBucket) { // Skip the acid schema check in case of non-ACID files
                acidColumnNames = ImmutableList.<String>builder().add(ACID_COLUMN_ORIGINAL_TRANSACTION,
                        ACID_COLUMN_BUCKET,
                        ACID_COLUMN_ROW_ID,
                        ACID_COLUMN_CURRENT_TRANSACTION,
                        ACID_COLUMN_OPERATION).build();
                verifyAcidSchema(reader, path);
                Map<String, OrcColumn> acidColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName());
                if (AcidUtils.isDeleteDelta(path.getParent())) {
                    //Avoid reading column data from delete_delta files.
                    // Call will come here in case of Minor VACUUM where all delete_delta files are merge together.
                    fileColumns = ImmutableList.of();
                }
                else {
                    fileColumns = acidColumnsByName.get(ACID_COLUMN_ROW_STRUCT).getNestedColumns();
                }

                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ORIGINAL_TRANSACTION));
                fileReadTypes.add(BIGINT);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_BUCKET));
                fileReadTypes.add(INTEGER);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ROW_ID));
                fileReadTypes.add(BIGINT);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_CURRENT_TRANSACTION));
                fileReadTypes.add(BIGINT);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_OPERATION));
                fileReadTypes.add(INTEGER);
            }

            Map<String, OrcColumn> fileColumnsByName = ImmutableMap.of();
            if (useOrcColumnNames || isFullAcid) {
                verifyFileHasColumnNames(fileColumns, path);

                // Convert column names read from ORC files to lower case to be consistent with those stored in Hive Metastore
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName());
            }

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(orcBloomFiltersEnabled);
            Map<HiveColumnHandle, Domain> effectivePredicateDomains = effectivePredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));
            for (HiveColumnHandle column : columns) {
                OrcColumn orcColumn = null;
                if (useOrcColumnNames || isFullAcid) {
                    orcColumn = fileColumnsByName.get(column.getName());
                }
                else if (column.getHiveColumnIndex() >= 0 && column.getHiveColumnIndex() < fileColumns.size()) {
                    orcColumn = fileColumns.get(column.getHiveColumnIndex());
                }

                Type readType = typeManager.getType(column.getTypeSignature());
                if (orcColumn != null) {
                    int sourceIndex = fileReadColumns.size();
                    columnAdaptations.add(ColumnAdaptation.sourceColumn(sourceIndex));
                    fileReadColumns.add(orcColumn);
                    fileReadTypes.add(readType);

                    Domain domain = effectivePredicateDomains.get(column);
                    if (domain != null) {
                        predicateBuilder.addColumn(orcColumn.getColumnId(), domain);
                    }
                }
                else if (isFullAcid && readType instanceof RowType && column.getName().equalsIgnoreCase(HiveColumnHandle.UPDATE_ROW_ID_COLUMN_NAME)) {
                    HiveType hiveType = column.getHiveType();
                    StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType.getTypeInfo();
                    ImmutableList.Builder<ColumnAdaptation> builder = new ImmutableList.Builder<>();
                    ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                    List<ColumnAdaptation> adaptations = fieldNames.stream()
                            .map(acidColumnNames::indexOf)
                            .map(c -> ColumnAdaptation.sourceColumn(c, false))
                            .collect(Collectors.toList());
                    columnAdaptations.add(ColumnAdaptation.structColumn(structTypeInfo, adaptations));
                }
                else {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(readType));
                }
            }

            Map<String, Domain> domains = effectivePredicate.getDomains().get().entrySet().stream().collect(toMap(e -> e.getKey().getName(), Map.Entry::getValue));
            OrcRecordReader recordReader = reader.createRecordReader(
                    fileReadColumns,
                    fileReadTypes,
                    predicateBuilder.build(),
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSource.getId(), exception),
                    indexes,
                    splitMetadata,
                    domains,
                    orcCacheStore,
                    orcCacheProperties,
                    pageMetadataEnabled);

            OrcDeletedRows deletedRows = new OrcDeletedRows(
                    path.getName(),
                    deleteDeltaLocations,
                    new OrcDeleteDeltaPageSourceFactory(sessionUser,
                            configuration, hdfsEnvironment, maxMergeDistance, maxBufferSize, streamBufferSize,
                            maxReadBlockSize, tinyStripeThreshold, lazyReadSmallRanges, orcBloomFiltersEnabled, stats),
                    sessionUser,
                    configuration,
                    hdfsEnvironment,
                    startRowOffsetOfFile);

            boolean eagerload = false;
            if (indexes.isPresent()) {
                eagerload = indexes.get().stream().anyMatch(indexMetadata -> EAGER_LOAD_INDEX_ID.contains(indexMetadata.getIndex().getId()));
            }

            return new OrcPageSource(
                    recordReader,
                    columnAdaptations,
                    orcDataSource,
                    deletedRows,
                    eagerload,
                    systemMemoryUsage,
                    stats);
        }
        catch (Exception e) {
            try {
                orcDataSource.close();
            }
            catch (IOException ignored) {
            }
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            String message = splitError(e, path, start, length);
            if (e instanceof BlockMissingException) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
    }

    interface FSDataInputStreamProvider
    {
        FSDataInputStream provide()
                throws IOException;
    }

    static class LazyFSInputStream
            extends InputStream
            implements Seekable, PositionedReadable
    {
        private FSDataInputStreamProvider fsDataInputStreamProvider;
        private FSDataInputStream fsDataInputStream;
        private boolean isStreamAvailable;

        public LazyFSInputStream(FSDataInputStreamProvider fsDataInputStreamProvider)
        {
            this.fsDataInputStreamProvider = fsDataInputStreamProvider;
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            ensureActualStream();
            fsDataInputStream.readFully(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer)
                throws IOException
        {
            ensureActualStream();
            fsDataInputStream.readFully(position, buffer);
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            ensureActualStream();
            fsDataInputStream.seek(pos);
        }

        @Override
        public long getPos()
                throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.seekToNewSource(targetPos);
        }

        @Override
        public int read()
                throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.read();
        }

        @Override
        public void close()
                throws IOException
        {
            if (isStreamAvailable) {
                fsDataInputStream.close();
                isStreamAvailable = false;
            }
        }

        private void ensureActualStream()
                throws IOException
        {
            if (isStreamAvailable) {
                return;
            }
            synchronized (this) {
                if (!isStreamAvailable) {
                    fsDataInputStream = fsDataInputStreamProvider.provide();
                }
            }
            isStreamAvailable = true;
        }
    }

    private static OrcFileTail createFileTail(OrcDataSource orcDataSource)
            throws IOException
    {
        return OrcFileTail.readFrom(orcDataSource, Optional.empty());
    }

    private static String splitError(Throwable t, Path path, long start, long length)
    {
        return format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, t.getMessage());
    }

    private static void verifyFileHasColumnNames(List<OrcColumn> columns, Path path)
    {
        if (!columns.isEmpty() && columns.stream().map(OrcColumn::getColumnName).allMatch(physicalColumnName -> DEFAULT_HIVE_COLUMN_NAME_PATTERN.matcher(physicalColumnName).matches())) {
            throw new PrestoException(
                    HIVE_FILE_MISSING_COLUMN_NAMES,
                    "ORC file does not contain column names in the footer: " + path);
        }
    }

    static void verifyAcidSchema(OrcReader orcReader, Path path)
    {
        OrcColumn rootColumn = orcReader.getRootColumn();
        if (rootColumn.getNestedColumns().size() != 6) {
            throw new PrestoException(HIVE_BAD_DATA, format("ORC ACID file should have 6 columns: %s", path));
        }
        verifyAcidColumn(orcReader, 0, ACID_COLUMN_OPERATION, INT, path);
        verifyAcidColumn(orcReader, 1, ACID_COLUMN_ORIGINAL_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 2, ACID_COLUMN_BUCKET, INT, path);
        verifyAcidColumn(orcReader, 3, ACID_COLUMN_ROW_ID, LONG, path);
        verifyAcidColumn(orcReader, 4, ACID_COLUMN_CURRENT_TRANSACTION, LONG, path);
        verifyAcidColumn(orcReader, 5, ACID_COLUMN_ROW_STRUCT, STRUCT, path);
    }

    private static void verifyAcidColumn(OrcReader orcReader, int columnIndex, String columnName, OrcTypeKind columnType, Path path)
    {
        OrcColumn column = orcReader.getRootColumn().getNestedColumns().get(columnIndex);
        if (!column.getColumnName().toLowerCase(ENGLISH).equals(columnName.toLowerCase(ENGLISH))) {
            throw new PrestoException(
                    HIVE_BAD_DATA,
                    format("ORC ACID file column %s should be named %s: %s", columnIndex, columnName, path));
        }
        if (column.getColumnType() != columnType) {
            throw new PrestoException(
                    HIVE_BAD_DATA,
                    format("ORC ACID file %s column should be type %s: %s", columnName, columnType, path));
        }
    }
}
