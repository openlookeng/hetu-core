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
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.coercions.HiveCoercer;
import io.prestosql.plugin.hive.orc.OrcConcatPageSource;
import io.prestosql.plugin.hive.util.IndexCache;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.RecordPageSource;
import io.prestosql.spi.dynamicfilter.CombinedDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.dynamicfilter.FilteredDynamicFilter;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.heuristicindex.IndexMetadata;
import io.prestosql.spi.heuristicindex.SplitMetadata;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.eclipse.jetty.util.URIUtil;

import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.uniqueIndex;
import static io.prestosql.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.prestosql.plugin.hive.HiveColumnHandle.MAX_PARTITION_KEY_COLUMN_INDEX;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping.toColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.coercions.HiveCoercer.createCoercer;
import static io.prestosql.plugin.hive.metastore.MetastoreUtil.META_PARTITION_COLUMNS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;

public class HivePageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final HdfsEnvironment hdfsEnvironment;
    private final Set<HiveRecordCursorProvider> cursorProviders;
    private final TypeManager typeManager;

    private final Set<HivePageSourceFactory> pageSourceFactories;

    private static final String HIVE_DEFAULT_PARTITION_VALUE = "\\N";
    private final IndexCache indexCache;
    private final Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories;

    @Inject
    public HivePageSourceProvider(
            HiveConfig hiveConfig,
            HdfsEnvironment hdfsEnvironment,
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            TypeManager typeManager,
            IndexCache indexCache,
            Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories)
    {
        requireNonNull(hiveConfig, "hiveConfig is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.cursorProviders = ImmutableSet.copyOf(requireNonNull(cursorProviders, "cursorProviders is null"));
        this.pageSourceFactories = ImmutableSet.copyOf(
                requireNonNull(pageSourceFactories, "pageSourceFactories is null"));
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.indexCache = indexCache;
        this.selectivePageSourceFactories = selectivePageSourceFactories;
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table,
            List<ColumnHandle> columns)
    {
        return createPageSource(transaction, session, split, table, columns, Optional.empty());
    }

    @Override
    public ConnectorPageSource createPageSource(ConnectorTransactionHandle transaction, ConnectorSession session,
            ConnectorSplit split, ConnectorTableHandle table, List<ColumnHandle> columns,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier)
    {
        List<Map<ColumnHandle, DynamicFilter>> dynamicFilters = null;
        if (dynamicFilterSupplier.isPresent()) {
            dynamicFilters = dynamicFilterSupplier.get().getDynamicFilters();
        }

        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<HiveColumnHandle> hiveColumns = columns.stream()
                .map(HiveColumnHandle.class::cast)
                .collect(toList());

        List<HiveSplit> hiveSplits = (((HiveSplitWrapper) split).getSplits());
        if (hiveSplits.size() == 1) {
            HiveSplit hiveSplit = hiveSplits.get(0);
            return createPageSourceInternal(session, dynamicFilterSupplier, dynamicFilters, hiveTable, hiveColumns, hiveSplit);
        }
        List<Map<ColumnHandle, DynamicFilter>> finalDynamicFilters = dynamicFilters;
        List<ConnectorPageSource> pageSources = hiveSplits.stream()
                .map(hiveSplit -> createPageSourceInternal(session, dynamicFilterSupplier, finalDynamicFilters, hiveTable, hiveColumns, hiveSplit))
                .collect(toList());
        return new OrcConcatPageSource(pageSources);
    }

    private ConnectorPageSource createPageSourceInternal(ConnectorSession session,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier,
            List<Map<ColumnHandle, DynamicFilter>> dynamicFilters,
            HiveTableHandle hiveTable,
            List<HiveColumnHandle> hiveColumns,
            HiveSplit hiveSplit)
    {
        Path path = new Path(hiveSplit.getPath());

        List<Set<DynamicFilter>> dynamicFilterList = new ArrayList();
        if (dynamicFilters != null) {
            for (Map<ColumnHandle, DynamicFilter> df : dynamicFilters) {
                Set<DynamicFilter> values = df.values().stream().collect(Collectors.toSet());
                dynamicFilterList.add(values);
            }
        }
        // Filter out splits using partition values and dynamic filters
        if (dynamicFilters != null && !dynamicFilters.isEmpty() && isPartitionFiltered(hiveSplit.getPartitionKeys(), dynamicFilterList, typeManager)) {
            return new FixedPageSource(ImmutableList.of());
        }

        Configuration configuration = hdfsEnvironment.getConfiguration(
                new HdfsEnvironment.HdfsContext(session, hiveSplit.getDatabase(), hiveSplit.getTable()), path);

        Properties schema = hiveSplit.getSchema();
        String columnNameDelimiter = schema.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? schema
                .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
        List<String> partitionColumnNames;
        if (schema.containsKey(META_PARTITION_COLUMNS)) {
            partitionColumnNames = Arrays.asList(schema.getProperty(META_PARTITION_COLUMNS).split(columnNameDelimiter));
        }
        else if (schema.containsKey(META_TABLE_COLUMNS)) {
            partitionColumnNames = Arrays.asList(schema.getProperty(META_TABLE_COLUMNS).split(columnNameDelimiter));
        }
        else {
            partitionColumnNames = new ArrayList<>();
        }

        List<String> tableColumns = hiveColumns.stream().map(cols -> cols.getName()).collect(toList());

        List<String> missingColumns = tableColumns.stream().skip(partitionColumnNames.size()).collect(toList());

        List<IndexMetadata> indexes = new ArrayList<>();
        if (indexCache != null && session.isHeuristicIndexFilterEnabled()) {
            indexes.addAll(this.indexCache.getIndices(session
                            .getCatalog().orElse(null), hiveTable
                            .getSchemaTableName().toString(), hiveSplit, hiveTable.getCompactEffectivePredicate(),
                    hiveTable.getPartitionColumns()));

            /* Bloom/Bitmap indices are checked for given table and added to the possible matchers for pushdown. */
            if (hiveTable.getDisjunctCompactEffectivePredicate().isPresent() && hiveTable.getDisjunctCompactEffectivePredicate().get().size() > 0) {
                hiveTable.getDisjunctCompactEffectivePredicate().get().forEach(orPredicate ->
                        indexes.addAll(this.indexCache.getIndices(session
                                .getCatalog().orElse(null), hiveTable
                                .getSchemaTableName().toString(), hiveSplit, orPredicate, hiveTable
                                .getPartitionColumns())));
            }
        }
        Optional<List<IndexMetadata>> indexOptional =
                indexes == null || indexes.isEmpty() ? Optional.empty() : Optional.of(indexes);
        URI splitUri = URI.create(URIUtil.encodePath(hiveSplit.getPath()));
        SplitMetadata splitMetadata = new SplitMetadata(splitUri.getRawPath(), hiveSplit.getLastModifiedTime());

        TupleDomain<HiveColumnHandle> predicate = TupleDomain.all();
        if (dynamicFilterSupplier.isPresent() && dynamicFilters != null && !dynamicFilters.isEmpty()) {
            if (dynamicFilters.size() == 1) {
                List<HiveColumnHandle> filteredHiveColumnHandles = hiveColumns.stream().filter(column -> dynamicFilters.get(0).containsKey(column)).collect(toList());
                HiveColumnHandle hiveColumnHandle = filteredHiveColumnHandles.get(0);
                Type type = hiveColumnHandle.getColumnMetadata(typeManager).getType();
                predicate = getPredicate(dynamicFilters.get(0).get(hiveColumnHandle), type, hiveColumnHandle);
                if (predicate.isNone()) {
                    predicate = TupleDomain.all();
                }
            }
        }

        /**
         * This is main logical division point to process filter pushdown enabled case (aka as selective read flow).
         * If user configuration orc_predicate_pushdown_enabled is true and if all clause of query can be handled by hive
         * selective read flow, then hiveTable.isSuitableToPush() will be enabled.
         * (Refer HiveMetadata.checkIfSuitableToPush).
         */
        if (hiveTable.isSuitableToPush()) {
            return createSelectivePageSource(selectivePageSourceFactories, configuration,
                    session, hiveSplit, assignUniqueIndicesToPartitionColumns(hiveColumns), typeManager,
                    dynamicFilterSupplier, hiveSplit.getDeleteDeltaLocations(),
                    hiveSplit.getStartRowOffsetOfFile(),
                    indexOptional, hiveSplit.isCacheable(),
                    hiveTable.getCompactEffectivePredicate(),
                    hiveTable.getPredicateColumns(),
                    hiveTable.getDisjunctCompactEffectivePredicate(),
                    hiveSplit.getBucketConversion(),
                    hiveSplit.getBucketNumber(),
                    hiveSplit.getLastModifiedTime(), missingColumns);
        }

        Optional<ConnectorPageSource> pageSource = createHivePageSource(
                cursorProviders,
                pageSourceFactories,
                configuration,
                session,
                path,
                hiveSplit.getBucketNumber(),
                hiveSplit.getStart(),
                hiveSplit.getLength(),
                hiveSplit.getFileSize(),
                hiveSplit.getSchema(),
                hiveTable.getCompactEffectivePredicate().intersect(predicate),
                hiveColumns,
                hiveSplit.getPartitionKeys(),
                typeManager,
                hiveSplit.getColumnCoercions(),
                hiveSplit.getBucketConversion(),
                hiveSplit.isS3SelectPushdownEnabled(),
                dynamicFilterSupplier,
                hiveSplit.getDeleteDeltaLocations(),
                hiveSplit.getStartRowOffsetOfFile(),
                indexOptional,
                splitMetadata,
                hiveSplit.isCacheable(),
                hiveSplit.getLastModifiedTime(),
                hiveSplit.getCustomSplitInfo(), missingColumns);
        if (pageSource.isPresent()) {
            return pageSource.get();
        }
        throw new RuntimeException("Could not find a file reader for split " + hiveSplit);
    }

    /**
     * All partition columns have index as -1, since we are making map of this, we need to assign an unique index.
     * @param columns List of partition columns
     * @return Modified list of columns with unique index.
     */
    private static List<HiveColumnHandle> assignUniqueIndicesToPartitionColumns(List<HiveColumnHandle> columns)
    {
        // Gives a distinct hiveColumnIndex to partitioning columns. Columns are identified by these indices in the rest of the
        // selective read path.
        ImmutableList.Builder<HiveColumnHandle> newColumns = ImmutableList.builder();
        int nextIndex = MAX_PARTITION_KEY_COLUMN_INDEX;
        for (HiveColumnHandle column : columns) {
            if (column.isPartitionKey()) {
                newColumns.add(new HiveColumnHandle(column.getName(), column.getHiveType(), column.getTypeSignature(), nextIndex--, column.getColumnType(), column.getComment()));
            }
            else {
                newColumns.add(column);
            }
        }
        return newColumns.build();
    }

    /**
     * Create selective page source, which will be used for selective reader flow.
     * Unlike normal page source, selective page source required to pass below additional details to reader
     * a. Pre-filled values of all constant.
     * b. Coercion information of all columns.
     * c. Columns which required to be projected.
     * d. Total list of columns which will be read (projection + filter).
     * All these info gets used by reader.
     * @param columns List of all columns being part of scan.
     * @param effectivePredicate Predicates related to AND clause
     * @param predicateColumns Map of all columns handles being part of predicate
     * @param additionPredicates Predicates related to OR clause.
     * Remaining columns are same as for createHivePageSource.
     * @param missingColumns
     * @return
     */
    private static ConnectorPageSource createSelectivePageSource(
            Set<HiveSelectivePageSourceFactory> selectivePageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            HiveSplit split,
            List<HiveColumnHandle> columns,
            TypeManager typeManager,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            boolean splitCacheable,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            Map<String, HiveColumnHandle> predicateColumns,
            Optional<List<TupleDomain<HiveColumnHandle>>> additionPredicates,
            Optional<HiveSplit.BucketConversion> bucketConversion,
            OptionalInt bucketNumber,
            long dataSourceLastModifiedTime, List<String> missingColumns)
    {
        Set<HiveColumnHandle> interimColumns = ImmutableSet.<HiveColumnHandle>builder()
                .addAll(predicateColumns.values())
                .addAll(bucketConversion.map(HiveSplit.BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()))
                .build();

        Path path = new Path(split.getPath());
        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                split.getPartitionKeys(),
                columns,
                ImmutableList.copyOf(interimColumns),
                split.getColumnCoercions(),
                path,
                bucketNumber,
                true, missingColumns);

        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(
                columnMappings);
        Optional<BucketAdaptation> bucketAdaptation = toBucketAdaptation(bucketConversion, regularAndInterimColumnMappings, bucketNumber);
        checkArgument(!bucketAdaptation.isPresent(), "Bucket conversion is not yet supported");

        // Make a list of all PREFILLED columns, which can be passed to reader. Unlike normal flow, selective read
        // flow require to pass this below at reader level as we need to make block of all column values.
        Map<Integer, String> prefilledValues = columnMappings.stream()
                .filter(mapping -> mapping.getKind() == ColumnMappingKind.PREFILLED)
                .collect(toImmutableMap(mapping -> mapping.getHiveColumnHandle().getHiveColumnIndex(), ColumnMapping::getPrefilledValue));

        // Make a map of column required to be coerced. This also needs to be sent to reader level as coercion
        // should be applied before adding values in block.
        Map<Integer, HiveCoercer> coercers = columnMappings.stream()
                .filter(mapping -> mapping.getCoercionFrom().isPresent())
                .collect(toImmutableMap(
                        mapping -> mapping.getHiveColumnHandle().getHiveColumnIndex(),
                        mapping -> createCoercer(typeManager, mapping.getCoercionFrom().get(), mapping.getHiveColumnHandle().getHiveType())));

        List<Integer> outputColumns = columns.stream()
                .map(HiveColumnHandle::getHiveColumnIndex)
                .collect(toImmutableList());

        for (HiveSelectivePageSourceFactory pageSourceFactory : selectivePageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    split.getStart(),
                    split.getLength(),
                    split.getFileSize(),
                    split.getSchema(),
                    toColumnHandles(columnMappings, true),
                    prefilledValues,
                    outputColumns,
                    effectivePredicate,
                    additionPredicates,
                    deleteDeltaLocations,
                    startRowOffsetOfFile,
                    indexes,
                    splitCacheable,
                    columnMappings,
                    coercers,
                    dataSourceLastModifiedTime);
            if (pageSource.isPresent()) {
                return new HivePageSource(
                                columnMappings,
                                Optional.empty(),
                                typeManager,
                                pageSource.get(),
                                dynamicFilterSupplier,
                                session,
                                split.getPartitionKeys());
            }
        }

        throw new IllegalStateException("Could not find a file reader for split " + split);
    }

    public static Optional<ConnectorPageSource> createHivePageSource(
            Set<HiveRecordCursorProvider> cursorProviders,
            Set<HivePageSourceFactory> pageSourceFactories,
            Configuration configuration,
            ConnectorSession session,
            Path path,
            OptionalInt bucketNumber,
            long start,
            long length,
            long fileSize,
            Properties schema,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            List<HiveColumnHandle> hiveColumns,
            List<HivePartitionKey> partitionKeys,
            TypeManager typeManager,
            Map<Integer, HiveType> columnCoercions,
            Optional<HiveSplit.BucketConversion> bucketConversion,
            boolean s3SelectPushdownEnabled,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier,
            Optional<DeleteDeltaLocations> deleteDeltaLocations,
            Optional<Long> startRowOffsetOfFile,
            Optional<List<IndexMetadata>> indexes,
            SplitMetadata splitMetadata,
            boolean splitCacheable,
            long dataSourceLastModifiedTime,
            Map<String, String> customSplitInfo, List<String> missingColumns)
    {
        List<ColumnMapping> columnMappings = ColumnMapping.buildColumnMappings(
                partitionKeys,
                hiveColumns,
                bucketConversion.map(HiveSplit.BucketConversion::getBucketColumnHandles).orElse(ImmutableList.of()),
                columnCoercions,
                path,
                bucketNumber,
                true, missingColumns);
        List<ColumnMapping> regularAndInterimColumnMappings = ColumnMapping.extractRegularAndInterimColumnMappings(
                columnMappings);

        Optional<BucketAdaptation> bucketAdaptation = toBucketAdaptation(bucketConversion, regularAndInterimColumnMappings, bucketNumber);

        for (HivePageSourceFactory pageSourceFactory : pageSourceFactories) {
            Optional<? extends ConnectorPageSource> pageSource = pageSourceFactory.createPageSource(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    toColumnHandles(regularAndInterimColumnMappings, true),
                    effectivePredicate,
                    dynamicFilterSupplier,
                    deleteDeltaLocations,
                    startRowOffsetOfFile,
                    indexes,
                    splitMetadata,
                    splitCacheable,
                    dataSourceLastModifiedTime);
            if (pageSource.isPresent()) {
                return Optional.of(
                        new HivePageSource(
                                columnMappings,
                                bucketAdaptation,
                                typeManager,
                                pageSource.get(),
                                dynamicFilterSupplier,
                                session,
                                partitionKeys));
            }
        }

        for (HiveRecordCursorProvider provider : cursorProviders) {
            // GenericHiveRecordCursor will automatically do the coercion without HiveCoercionRecordCursor
            boolean doCoercion = !(provider instanceof GenericHiveRecordCursorProvider);

            Optional<RecordCursor> cursor = provider.createRecordCursor(
                    configuration,
                    session,
                    path,
                    start,
                    length,
                    fileSize,
                    schema,
                    toColumnHandles(regularAndInterimColumnMappings, doCoercion),
                    effectivePredicate,
                    typeManager,
                    s3SelectPushdownEnabled,
                    customSplitInfo);

            if (cursor.isPresent()) {
                RecordCursor delegate = cursor.get();

                checkArgument(!deleteDeltaLocations.isPresent(), "Delete delta is not supported");

                if (bucketAdaptation.isPresent()) {
                    delegate = new HiveBucketAdapterRecordCursor(
                            bucketAdaptation.get().getBucketColumnIndices(),
                            bucketAdaptation.get().getBucketColumnHiveTypes(),
                            bucketAdaptation.get().getBucketingVersion(),
                            bucketAdaptation.get().getTableBucketCount(),
                            bucketAdaptation.get().getPartitionBucketCount(),
                            bucketAdaptation.get().getBucketToKeep(),
                            typeManager,
                            delegate);
                }

                // Need to wrap RcText and RcBinary into a wrapper, which will do the coercion for mismatch columns
                if (doCoercion) {
                    delegate = new HiveCoercionRecordCursor(regularAndInterimColumnMappings, typeManager, delegate);
                }

                HiveRecordCursor hiveRecordCursor = new HiveRecordCursor(
                        columnMappings,
                        typeManager,
                        delegate);
                List<Type> columnTypes = hiveColumns.stream()
                        .map(input -> typeManager.getType(input.getTypeSignature()))
                        .collect(toList());

                return Optional.of(new RecordPageSource(columnTypes, hiveRecordCursor));
            }
        }

        return Optional.empty();
    }

    public static Optional<BucketAdaptation> toBucketAdaptation(Optional<HiveSplit.BucketConversion> bucketConversion,
                                                         List<ColumnMapping> regularAndInterimColumnMappings,
                                                         OptionalInt bucketNumber)
    {
        return bucketConversion.map(conversion -> {
            Map<Integer, ColumnMapping> hiveIndexToBlockIndex = uniqueIndex(regularAndInterimColumnMappings,
                    columnMapping -> columnMapping.getHiveColumnHandle().getHiveColumnIndex());
            int[] bucketColumnIndices = conversion.getBucketColumnHandles().stream()
                    .mapToInt(columnHandle -> hiveIndexToBlockIndex.get(columnHandle.getHiveColumnIndex()).getIndex())
                    .toArray();
            List<HiveType> bucketColumnHiveTypes = conversion.getBucketColumnHandles().stream()
                    .map(columnHandle -> hiveIndexToBlockIndex.get(
                            columnHandle.getHiveColumnIndex()).getHiveColumnHandle().getHiveType())
                    .collect(toImmutableList());
            return new BucketAdaptation(
                    bucketColumnIndices,
                    bucketColumnHiveTypes,
                    conversion.getBucketingVersion(),
                    conversion.getTableBucketCount(),
                    conversion.getPartitionBucketCount(),
                    bucketNumber.getAsInt());
        });
    }

    public static class ColumnMapping
    {
        private final ColumnMappingKind kind;
        private final HiveColumnHandle hiveColumnHandle;
        private final Optional<String> prefilledValue;
        /**
         * ordinal of this column in the underlying page source or record cursor
         */
        private final OptionalInt index;
        private final Optional<HiveType> coercionFrom;

        public static ColumnMapping regular(HiveColumnHandle hiveColumnHandle, int index, Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.REGULAR);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(),
                    OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping synthesized(HiveColumnHandle hiveColumnHandle, int index,
                Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.REGULAR, hiveColumnHandle, Optional.empty(),
                    OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping prefilled(HiveColumnHandle hiveColumnHandle, String prefilledValue,
                Optional<HiveType> coerceFrom)
        {
            checkArgument(
                    hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.PARTITION_KEY || hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.PREFILLED, hiveColumnHandle, Optional.of(prefilledValue),
                    OptionalInt.empty(), coerceFrom);
        }

        public static ColumnMapping transaction(HiveColumnHandle hiveColumnHandle, int index,
                Optional<HiveType> coerceFrom)
        {
            checkArgument(hiveColumnHandle.getColumnType() == HiveColumnHandle.ColumnType.SYNTHESIZED);
            return new ColumnMapping(ColumnMappingKind.TRANSACTIONID, hiveColumnHandle, Optional.empty(),
                    OptionalInt.of(index), coerceFrom);
        }

        public static ColumnMapping interim(HiveColumnHandle hiveColumnHandle, int index)
        {
            return new ColumnMapping(ColumnMappingKind.INTERIM, hiveColumnHandle, Optional.empty(),
                    OptionalInt.of(index), Optional.empty());
        }

        private ColumnMapping(ColumnMappingKind kind, HiveColumnHandle hiveColumnHandle,
                Optional<String> prefilledValue, OptionalInt index, Optional<HiveType> coerceFrom)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.hiveColumnHandle = requireNonNull(hiveColumnHandle, "hiveColumnHandle is null");
            this.prefilledValue = requireNonNull(prefilledValue, "prefilledValue is null");
            this.index = requireNonNull(index, "index is null");
            this.coercionFrom = requireNonNull(coerceFrom, "coerceFrom is null");
        }

        public ColumnMappingKind getKind()
        {
            return kind;
        }

        public String getPrefilledValue()
        {
            checkState(kind == ColumnMappingKind.PREFILLED);
            return prefilledValue.isPresent() ? prefilledValue.get() : HIVE_DEFAULT_PARTITION_VALUE;
        }

        public HiveColumnHandle getHiveColumnHandle()
        {
            return hiveColumnHandle;
        }

        public int getIndex()
        {
            checkState(
                    kind == ColumnMappingKind.REGULAR || kind == ColumnMappingKind.INTERIM || kind == ColumnMappingKind.TRANSACTIONID);
            return index.getAsInt();
        }

        public Optional<HiveType> getCoercionFrom()
        {
            return coercionFrom;
        }

        /**
         * @param columns columns that need to be returned to engine
         * @param requiredInterimColumns columns that are needed for processing, but shouldn't be returned to engine (may overlaps with columns)
         * @param columnCoercions map from hive column index to hive type
         * @param bucketNumber empty if table is not bucketed, a number within [0, # bucket in table) otherwise
         * @param missingColumns
         */
        public static List<ColumnMapping> buildColumnMappings(
                List<HivePartitionKey> partitionKeys,
                List<HiveColumnHandle> columns,
                List<HiveColumnHandle> requiredInterimColumns,
                Map<Integer, HiveType> columnCoercions,
                Path path,
                OptionalInt bucketNumber,
                boolean filterPushDown, List<String> missingColumns)
        {
            Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
            int regularIndex = 0;
            Set<Integer> regularColumnIndices = new HashSet<>();
            ImmutableList.Builder<ColumnMapping> columnMappings = ImmutableList.builder();
            for (HiveColumnHandle column : columns) {
                Optional<HiveType> localCoercionFrom = Optional.ofNullable(columnCoercions.get(column.getHiveColumnIndex()));
                if (column.getColumnType() == REGULAR) {
                    if (missingColumns.contains(column.getColumnName())) {
                        columnMappings.add(new ColumnMapping(ColumnMappingKind.PREFILLED, column, Optional.empty(),
                                OptionalInt.empty(), localCoercionFrom));
                        continue;
                    }
                    checkArgument(regularColumnIndices.add(column.getHiveColumnIndex()), "duplicate hiveColumnIndex in columns list");

                    columnMappings.add(regular(column, regularIndex, localCoercionFrom));
                    regularIndex++;
                }
                else if (HiveColumnHandle.isUpdateColumnHandle(column)) {
                    columnMappings.add(transaction(column, regularIndex, localCoercionFrom));
                    regularIndex++;
                }
                else {
                    columnMappings.add(prefilled(
                            column,
                            HiveUtil.getPrefilledColumnValue(column, partitionKeysByName.get(column.getName()), path,
                                    bucketNumber),
                            localCoercionFrom));
                }
            }
            for (HiveColumnHandle column : requiredInterimColumns) {
                checkArgument(column.getColumnType() == REGULAR || filterPushDown);
                if (regularColumnIndices.contains(column.getHiveColumnIndex())) {
                    continue; // This column exists in columns. Do not add it again.
                }
                // If coercion does not affect bucket number calculation, coercion doesn't need to be applied here.
                // Otherwise, read of this partition should not be allowed.
                // (Alternatively, the partition could be read as an unbucketed partition. This is not implemented.)
                columnMappings.add(interim(column, regularIndex));
                regularIndex++;
            }
            return columnMappings.build();
        }

        public static List<ColumnMapping> extractRegularAndInterimColumnMappings(List<ColumnMapping> columnMappings)
        {
            return columnMappings.stream()
                    .filter(columnMapping -> columnMapping.getKind() == ColumnMappingKind.REGULAR || columnMapping.getKind() == ColumnMappingKind.INTERIM || columnMapping.getKind() == ColumnMappingKind.TRANSACTIONID)
                    .collect(toImmutableList());
        }

        public static List<HiveColumnHandle> toColumnHandles(List<ColumnMapping> regularColumnMappings,
                boolean doCoercion)
        {
            return regularColumnMappings.stream()
                    .map(columnMapping -> {
                        HiveColumnHandle columnHandle = columnMapping.getHiveColumnHandle();
                        if (!doCoercion || !columnMapping.getCoercionFrom().isPresent()) {
                            return columnHandle;
                        }
                        return new HiveColumnHandle(
                                columnHandle.getName(),
                                columnMapping.getCoercionFrom().get(),
                                columnMapping.getCoercionFrom().get().getTypeSignature(),
                                columnHandle.getHiveColumnIndex(),
                                columnHandle.getColumnType(),
                                Optional.empty());
                    })
                    .collect(toList());
        }
    }

    public static Object getValue(Type type, String partitionValue)
    {
        Class<?> javaType = type.getJavaType();

        if (javaType == long.class) {
            return Long.valueOf(partitionValue);
        }
        if (javaType == double.class) {
            return Double.valueOf(partitionValue);
        }
        if (javaType == boolean.class) {
            return Boolean.valueOf(partitionValue);
        }
        if (javaType == Slice.class) {
            return Slices.utf8Slice(partitionValue);
        }
        return partitionValue;
    }

    protected static Domain modifyDomain(Domain inputDomain, Optional<RowExpression> filter)
    {
        Domain domain = inputDomain;
        Range range = domain.getValues().getRanges().getSpan();
        if (filter.isPresent() && filter.get() instanceof CallExpression) {
            CallExpression call = (CallExpression) filter.get();
            BuiltInFunctionHandle builtInFunctionHandle = (BuiltInFunctionHandle) call.getFunctionHandle();
            String name = builtInFunctionHandle.getSignature().getNameSuffix();
            if (name.contains("$operator$") && Signature.unmangleOperator(name).isComparisonOperator()) {
                switch (Signature.unmangleOperator(name)) {
                    case LESS_THAN:
                        range = Range.lessThan(domain.getType(), range.getHigh().getValue());
                        break;
                    case GREATER_THAN:
                        range = Range.greaterThan(domain.getType(), range.getLow().getValue());
                        break;
                    case LESS_THAN_OR_EQUAL:
                        range = Range.lessThanOrEqual(domain.getType(), range.getHigh().getValue());
                        break;
                    case GREATER_THAN_OR_EQUAL:
                        range = Range.greaterThanOrEqual(domain.getType(), range.getLow().getValue());
                        break;
                    default:
                        return domain;
                }
                domain = Domain.create(ValueSet.ofRanges(range), false);
            }
        }
        return domain;
    }

    private static TupleDomain<HiveColumnHandle> getPredicate(DynamicFilter dynamicFilter, Type type, HiveColumnHandle hiveColumnHandle)
    {
        if (dynamicFilter instanceof CombinedDynamicFilter) {
            List<DynamicFilter> filters = ((CombinedDynamicFilter) dynamicFilter).getFilters();
            List<TupleDomain<HiveColumnHandle>> predicates = filters.stream().map(filter -> getPredicate(filter, type, hiveColumnHandle)).collect(toList());
            return predicates.stream().reduce(TupleDomain.all(), TupleDomain::intersect);
        }
        if (dynamicFilter instanceof FilteredDynamicFilter && !((FilteredDynamicFilter) dynamicFilter).getSetValues().isEmpty()) {
            Domain domain = Domain.create(ValueSet.copyOf(type, ((FilteredDynamicFilter) dynamicFilter).getSetValues()), false);
            domain = modifyDomain(domain, ((FilteredDynamicFilter) dynamicFilter).getFilterExpression());
            return TupleDomain.withColumnDomains(ImmutableMap.of(hiveColumnHandle, domain));
        }
        return TupleDomain.all();
    }

    public enum ColumnMappingKind
    {
        REGULAR,
        PREFILLED,
        INTERIM,
        TRANSACTIONID
    }

    public static class BucketAdaptation
    {
        private final int[] bucketColumnIndices;
        private final List<HiveType> bucketColumnHiveTypes;
        private final BucketingVersion bucketingVersion;
        private final int tableBucketCount;
        private final int partitionBucketCount;
        private final int bucketToKeep;

        public BucketAdaptation(
                int[] bucketColumnIndices,
                List<HiveType> bucketColumnHiveTypes,
                BucketingVersion bucketingVersion,
                int tableBucketCount,
                int partitionBucketCount,
                int bucketToKeep)
        {
            this.bucketColumnIndices = bucketColumnIndices;
            this.bucketColumnHiveTypes = bucketColumnHiveTypes;
            this.bucketingVersion = bucketingVersion;
            this.tableBucketCount = tableBucketCount;
            this.partitionBucketCount = partitionBucketCount;
            this.bucketToKeep = bucketToKeep;
        }

        public int[] getBucketColumnIndices()
        {
            return bucketColumnIndices;
        }

        public List<HiveType> getBucketColumnHiveTypes()
        {
            return bucketColumnHiveTypes;
        }

        public BucketingVersion getBucketingVersion()
        {
            return bucketingVersion;
        }

        public int getTableBucketCount()
        {
            return tableBucketCount;
        }

        public int getPartitionBucketCount()
        {
            return partitionBucketCount;
        }

        public int getBucketToKeep()
        {
            return bucketToKeep;
        }
    }
}
