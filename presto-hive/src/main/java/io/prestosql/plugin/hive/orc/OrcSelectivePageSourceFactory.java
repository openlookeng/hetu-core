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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.orc.OrcCacheProperties;
import io.prestosql.orc.OrcCacheStore;
import io.prestosql.orc.OrcColumn;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcDataSourceId;
import io.prestosql.orc.OrcDataSourceIdWithTimeStamp;
import io.prestosql.orc.OrcFileTail;
import io.prestosql.orc.OrcFileTailCacheKey;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcSelectiveRecordReader;
import io.prestosql.orc.TupleDomainFilter;
import io.prestosql.orc.TupleDomainFilterUtils;
import io.prestosql.orc.TupleDomainOrcPredicate;
import io.prestosql.orc.TupleDomainOrcPredicate.TupleDomainOrcPredicateBuilder;
import io.prestosql.orc.metadata.OrcType.OrcTypeKind;
import io.prestosql.plugin.hive.DeleteDeltaLocations;
import io.prestosql.plugin.hive.FileFormatDataSourceStats;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HivePageSourceProvider;
import io.prestosql.plugin.hive.HiveSelectivePageSourceFactory;
import io.prestosql.plugin.hive.HiveSessionProperties;
import io.prestosql.plugin.hive.HiveType;
import io.prestosql.plugin.hive.HiveUtil;
import io.prestosql.plugin.hive.coercions.HiveCoercer;
import io.prestosql.plugin.hive.orc.OrcPageSource.ColumnAdaptation;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.heuristicindex.IndexMetadata;
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
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.joda.time.DateTimeZone;

import javax.inject.Inject;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
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
import static io.prestosql.plugin.hive.HiveUtil.typedPartitionKey;
import static io.prestosql.plugin.hive.orc.OrcPageSource.handleException;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.ql.io.AcidUtils.isFullAcidTable;

public class OrcSelectivePageSourceFactory
        implements HiveSelectivePageSourceFactory
{
    // ACID format column names
    public static final String ACID_COLUMN_OPERATION = "operation";
    public static final String ACID_COLUMN_ORIGINAL_TRANSACTION = "originalTransaction";
    public static final String ACID_COLUMN_BUCKET = "bucket";
    public static final String ACID_COLUMN_ROW_ID = "rowId";
    public static final String ACID_COLUMN_CURRENT_TRANSACTION = "currentTransaction";
    public static final String ACID_COLUMN_ROW_STRUCT = "row";

    private static final Pattern DEFAULT_HIVE_COLUMN_NAME_PATTERN = Pattern.compile("_col\\d+");
    private final TypeManager typeManager;
    private final boolean useOrcColumnNames;
    private final HdfsEnvironment hdfsEnvironment;
    private final FileFormatDataSourceStats stats;
    private final OrcCacheStore orcCacheStore;
    private final DateTimeZone legacyTimeZone;

    @Inject
    public OrcSelectivePageSourceFactory(TypeManager typeManager, HiveConfig config, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, OrcCacheStore orcCacheStore)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        requireNonNull(config, "config is null");
        this.useOrcColumnNames = config.isUseOrcColumnNames();
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.orcCacheStore = orcCacheStore;
        this.legacyTimeZone = requireNonNull(config, "hiveConfig is null").getOrcLegacyDateTimeZone();
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
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<HiveColumnHandle> domainPredicate,
            Optional<List<TupleDomain<HiveColumnHandle>>> additionPredicates,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            boolean splitCacheable,
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            Map<Integer, HiveCoercer> coercers,
            long dataSourceLastModifiedTime)
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
        if (additionPredicates.isPresent()
                && additionPredicates.get().size() > 0
                && !additionPredicates.get().get(0).isAll()
                && !additionPredicates.get().get(0).isNone()) {
            List<Integer> positions = new ArrayList<>(10);

            return Optional.of(createOrcPageSource(
                    hdfsEnvironment,
                    session,
                    configuration,
                    path,
                    start,
                    length,
                    fileSize,
                    columns,
                    useOrcColumnNames,
                    isFullAcidTable(Maps.fromProperties(schema)),
                    prefilledValues,
                    outputColumns,
                    domainPredicate,
                    legacyTimeZone,
                    typeManager,
                    getOrcMaxMergeDistance(session),
                    getOrcMaxBufferSize(session),
                    getOrcStreamBufferSize(session),
                    getOrcTinyStripeThreshold(session),
                    getOrcMaxReadBlockSize(session),
                    getOrcLazyReadSmallRanges(session),
                    isOrcBloomFiltersEnabled(session),
                    stats,
                    deleteDeltaLocations,
                    startRowOffsetOfFile,
                    indexes,
                    orcCacheStore,
                    orcCacheProperties,
                    additionPredicates.orElseGet(() -> ImmutableList.of()),
                    positions,
                    columnMappings,
                    coercers,
                    dataSourceLastModifiedTime));

            /* Todo(Nitin): For Append Pattern
            */
        }

        return Optional.of(createOrcPageSource(
                hdfsEnvironment,
                session,
                configuration,
                path,
                start,
                length,
                fileSize,
                columns,
                useOrcColumnNames,
                isFullAcidTable(Maps.fromProperties(schema)),
                prefilledValues,
                outputColumns,
                domainPredicate,
                legacyTimeZone,
                typeManager,
                getOrcMaxMergeDistance(session),
                getOrcMaxBufferSize(session),
                getOrcStreamBufferSize(session),
                getOrcTinyStripeThreshold(session),
                getOrcMaxReadBlockSize(session),
                getOrcLazyReadSmallRanges(session),
                isOrcBloomFiltersEnabled(session),
                stats,
                deleteDeltaLocations,
                startRowOffsetOfFile,
                indexes,
                orcCacheStore,
                orcCacheProperties,
                ImmutableList.of(),
                null,
                columnMappings,
                coercers,
                dataSourceLastModifiedTime));
    }

    public static OrcSelectivePageSource createOrcPageSource(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            Configuration configuration,
            Path path,
            long start,
            long length,
            long fileSize,
            List<HiveColumnHandle> columns,
            boolean useOrcColumnNames,
            boolean isFullAcid,
            Map<Integer, String> prefilledValues,
            List<Integer> outputColumns,
            TupleDomain<HiveColumnHandle> domainPredicate,
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
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            OrcCacheStore orcCacheStore,
            OrcCacheProperties orcCacheProperties,
            List<TupleDomain<HiveColumnHandle>> disjunctDomains,
            List<Integer> positions,
            List<HivePageSourceProvider.ColumnMapping> columnMappings,
            Map<Integer, HiveCoercer> coercers,
            long dataSourceLastModifiedTime)
    {
        checkArgument(!domainPredicate.isNone(), "Unexpected NONE domain");
        String sessionUser = session.getUser();
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
                    stats,
                    dataSourceLastModifiedTime);
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
                OrcDataSourceIdWithTimeStamp orcDataSourceIdWithTimeStamp = new OrcDataSourceIdWithTimeStamp(readerLocalDataSource.getId(), readerLocalDataSource.getLastModifiedTime());
                fileTail = orcCacheStore.getFileTailCache().get(new OrcFileTailCacheKey(orcDataSourceIdWithTimeStamp), () -> OrcSelectivePageSourceFactory.createFileTail(orcDataSource));
            }
            else {
                fileTail = OrcSelectivePageSourceFactory.createFileTail(orcDataSource);
            }
            OrcReader reader = new OrcReader(readerLocalDataSource, fileTail, maxMergeDistance, tinyStripeThreshold, maxReadBlockSize);

            List<OrcColumn> fileColumns = reader.getRootColumn().getNestedColumns();
            List<OrcColumn> fileReadColumns = isFullAcid ? new ArrayList<>(columns.size() + 3) : new ArrayList<>(columns.size());
            List<Type> fileReadTypes = isFullAcid ? new ArrayList<>(columns.size() + 3) : new ArrayList<>(columns.size());
            ImmutableList<String> acidColumnNames = null;
            List<ColumnAdaptation> columnAdaptations = new ArrayList<>(columns.size());
            if (isFullAcid && fileColumns.size() != columns.size()) { // Skip the acid schema check in case of non-ACID files
                acidColumnNames = ImmutableList.<String>builder().add(ACID_COLUMN_ORIGINAL_TRANSACTION,
                        ACID_COLUMN_BUCKET,
                        ACID_COLUMN_ROW_ID).build();
                verifyAcidSchema(reader, path);
                Map<String, OrcColumn> acidColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName());
                fileColumns = acidColumnsByName.get(ACID_COLUMN_ROW_STRUCT).getNestedColumns();

                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ORIGINAL_TRANSACTION));
                fileReadTypes.add(BIGINT);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_BUCKET));
                fileReadTypes.add(INTEGER);
                fileReadColumns.add(acidColumnsByName.get(ACID_COLUMN_ROW_ID));
                fileReadTypes.add(BIGINT);
            }

            Map<String, OrcColumn> fileColumnsByName = ImmutableMap.of();
            if (useOrcColumnNames || isFullAcid) {
                verifyFileHasColumnNames(fileColumns, path);

                // Convert column names read from ORC files to lower case to be consistent with those stored in Hive Metastore
                fileColumnsByName = uniqueIndex(fileColumns, orcColumn -> orcColumn.getColumnName());
            }

            TupleDomainOrcPredicateBuilder predicateBuilder = TupleDomainOrcPredicate.builder()
                    .setBloomFiltersEnabled(orcBloomFiltersEnabled);
            Map<HiveColumnHandle, Domain> effectivePredicateDomains = domainPredicate.getDomains()
                    .orElseThrow(() -> new IllegalArgumentException("Effective predicate is none"));

            /* Fixme(Nitin): If same-columns or conditions can be merged as TreeMap in optimization step; below code can be spared */
            Map<HiveColumnHandle, Domain> disjunctPredicateDomains = new HashMap<>();
            disjunctDomains.stream()
                    .forEach(ap -> ap.getDomains().get().forEach((k, v) -> disjunctPredicateDomains.merge(k, v, (v1, v2) -> v1.union(v2))));

            boolean hasParitionKeyORPredicate = disjunctPredicateDomains.keySet().stream().anyMatch(c -> c.isPartitionKey());
            Map<String, List<Domain>> orDomains = new ConcurrentHashMap<>();
            Set<Integer> missingColumns = new HashSet<>();
            for (HiveColumnHandle column : columns) {
                OrcColumn orcColumn = null;
                int missingColumn = -1;
                if (useOrcColumnNames || isFullAcid) {
                    orcColumn = fileColumnsByName.get(column.getName());
                }
                else if (column.getHiveColumnIndex() >= 0) {
                    if (column.getHiveColumnIndex() < fileColumns.size()) {
                        orcColumn = fileColumns.get(column.getHiveColumnIndex());
                    }
                    else {
                        missingColumn = column.getHiveColumnIndex();
                    }
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

                    domain = disjunctPredicateDomains.get(column);
                    if (!hasParitionKeyORPredicate && domain != null) {
                        predicateBuilder.addOrColumn(orcColumn.getColumnId(), domain);
                        orDomains.computeIfAbsent(column.getName(), l -> new ArrayList<>()).add(domain);
                    }
                }
                else if (isFullAcid && readType instanceof RowType && column.getName().equalsIgnoreCase("row__id")) {
                    HiveType hiveType = column.getHiveType();
                    StructTypeInfo structTypeInfo = (StructTypeInfo) hiveType.getTypeInfo();
                    ArrayList<String> fieldNames = structTypeInfo.getAllStructFieldNames();
                    List<ColumnAdaptation> adaptations = fieldNames.stream()
                            .map(acidColumnNames::indexOf)
                            .map(ColumnAdaptation::sourceColumn)
                            .collect(Collectors.toList());
                    columnAdaptations.add(ColumnAdaptation.structColumn(structTypeInfo, adaptations));
                }
                else if (missingColumn >= 0) {
                    missingColumns.add(missingColumn);
                }
                else {
                    columnAdaptations.add(ColumnAdaptation.nullColumn(readType));
                }
            }

            // missingColumns are list of columns which are not part of file but being part of projection.
            // This happens if a table was altered to add more columns.
            predicateBuilder.setMissingColumns(missingColumns);
            Map<Integer, Type> columnTypes = columns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, column -> typeManager.getType(column.getTypeSignature())));

            Map<Integer, String> columnNames = columns.stream()
                    .collect(toImmutableMap(HiveColumnHandle::getHiveColumnIndex, HiveColumnHandle::getName));

            Map<Integer, Object> typedPrefilledValues = new HashMap<>();
            for (Map.Entry prefilledValue : prefilledValues.entrySet()) {
                typedPrefilledValues.put(Integer.valueOf(prefilledValue.getKey().toString()),
                        typedPartitionKey(prefilledValue.getValue().toString(), columnTypes.get(prefilledValue.getKey()), columnNames.get(prefilledValue.getKey())));
            }

            // Convert the predicate to each column id wise. Will be used to associate as filter with each column reader
            Map<Integer, TupleDomainFilter> tupleDomainFilters = toTupleDomainFilters(domainPredicate, ImmutableBiMap.copyOf(columnNames).inverse());
            Map<Integer, List<TupleDomainFilter>> orFilters = new HashMap<>();

            disjunctDomains.stream()
                    .forEach(ap -> toTupleDomainFilters(ap, ImmutableBiMap.copyOf(columnNames).inverse()).entrySet().stream()
                            .forEach(td -> orFilters.computeIfAbsent(td.getKey(), list -> new ArrayList<>()).add(td.getValue())));

            // domains still required by index (refer AbstractOrcRecordReader).
            Map<String, Domain> domainMap = effectivePredicateDomains.entrySet().stream().collect(toMap(e -> e.getKey().getName(), Map.Entry::getValue));
            OrcSelectiveRecordReader recordReader = reader.createSelectiveRecordReader(
                    fileColumns,
                    fileReadColumns,
                    fileReadTypes,
                    outputColumns,
                    columnTypes,
                    tupleDomainFilters,
                    typedPrefilledValues,
                    predicateBuilder.build(),
                    start,
                    length,
                    hiveStorageTimeZone,
                    systemMemoryUsage,
                    INITIAL_BATCH_SIZE,
                    exception -> handleException(orcDataSource.getId(), exception),
                    indexes,
                    domainMap,
                    orcCacheStore,
                    orcCacheProperties,
                    Optional.empty(),
                    orFilters,
                    positions,
                    HiveSessionProperties.isOrcPushdownDataCacheEnabled(session),
                    Maps.transformValues(coercers, Function.class::cast),
                    orDomains,
                    missingColumns);

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

            /* Todo(Nitin): Create a Separate OrcSelectivePageSource and Use MergingPageIterator
             *   to progressively scan and yeild pages. */

            return new OrcSelectivePageSource(
                    recordReader,
                    orcDataSource,
                    systemMemoryUsage,
                    stats,
                    columnMappings,
                    typeManager);
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

    private static Map<Integer, TupleDomainFilter> toTupleDomainFilters(TupleDomain<HiveColumnHandle> domainPredicate, Map<String, Integer> columnIndices)
    {
        // convert the predicate from column name based map to column id based map.
        // toFilter is the function which actually will convert o corresponding Comparator.
        // toFilter will be called lazily during initialization of corresponding column reader (createColumnReaders).
        Map<Integer, TupleDomainFilter> tupleDomainFilterMap = new HashMap<>();
        domainPredicate.transform(columnHandle -> columnIndices.get(columnHandle.getColumnName())).getDomains().get().forEach((k, v) -> tupleDomainFilterMap.put(k, TupleDomainFilterUtils.toFilter(v)));
        return tupleDomainFilterMap;
    }

    interface FSDataInputStreamProvider
    {
        FSDataInputStream provide() throws IOException;
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
        public int read(long position, byte[] buffer, int offset, int length) throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.read(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length) throws IOException
        {
            ensureActualStream();
            fsDataInputStream.readFully(position, buffer, offset, length);
        }

        @Override
        public void readFully(long position, byte[] buffer) throws IOException
        {
            ensureActualStream();
            fsDataInputStream.readFully(position, buffer);
        }

        @Override
        public void seek(long pos) throws IOException
        {
            ensureActualStream();
            fsDataInputStream.seek(pos);
        }

        @Override
        public long getPos() throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.getPos();
        }

        @Override
        public boolean seekToNewSource(long targetPos) throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.seekToNewSource(targetPos);
        }

        @Override
        public int read() throws IOException
        {
            ensureActualStream();
            return fsDataInputStream.read();
        }

        @Override
        public void close() throws IOException
        {
            if (isStreamAvailable) {
                fsDataInputStream.close();
                isStreamAvailable = false;
            }
        }

        private void ensureActualStream() throws IOException
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

    private static OrcFileTail createFileTail(OrcDataSource orcDataSource) throws IOException
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
