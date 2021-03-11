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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Suppliers;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.HdfsEnvironment.HdfsContext;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveColumnStatistics;
import io.prestosql.plugin.hive.metastore.HivePrincipal;
import io.prestosql.plugin.hive.metastore.MetastoreUtil;
import io.prestosql.plugin.hive.metastore.Partition;
import io.prestosql.plugin.hive.metastore.PrincipalPrivileges;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.hive.security.AccessControlMetadata;
import io.prestosql.plugin.hive.statistics.HiveStatisticsProvider;
import io.prestosql.plugin.hive.util.ConfigurationUtils;
import io.prestosql.plugin.hive.util.Statistics;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorDeleteAsInsertTableHandle;
import io.prestosql.spi.connector.ConnectorInsertTableHandle;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorNewTableLayout;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.connector.ConnectorOutputTableHandle;
import io.prestosql.spi.connector.ConnectorPartitioningHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTablePartitioning;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.ConnectorUpdateTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableHandle;
import io.prestosql.spi.connector.ConnectorVacuumTableInfo;
import io.prestosql.spi.connector.ConnectorViewDefinition;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.DiscretePredicates;
import io.prestosql.spi.connector.InMemoryRecordSet;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.SystemTable;
import io.prestosql.spi.connector.TableAlreadyExistsException;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.connector.ViewNotFoundException;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.security.GrantInfo;
import io.prestosql.spi.security.PrestoPrincipal;
import io.prestosql.spi.security.Privilege;
import io.prestosql.spi.security.RoleGrant;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ColumnStatisticType;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.statistics.TableStatisticType;
import io.prestosql.spi.statistics.TableStatistics;
import io.prestosql.spi.statistics.TableStatisticsMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.VarcharType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.OpenCSVSerde;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Streams.stream;
import static io.prestosql.plugin.hive.HiveBucketing.containsTimestampBucketedV2;
import static io.prestosql.plugin.hive.HiveStorageFormat.ORC;
import static io.prestosql.plugin.hive.HiveTableProperties.IS_EXTERNAL_TABLE;
import static io.prestosql.plugin.hive.HiveTableProperties.LOCATION_PROPERTY;
import static io.prestosql.plugin.hive.HiveTableProperties.NON_INHERITABLE_PROPERTIES;
import static io.prestosql.plugin.hive.HiveTableProperties.TRANSACTIONAL;
import static io.prestosql.plugin.hive.HiveTableProperties.getExternalLocation;
import static io.prestosql.plugin.hive.HiveTableProperties.getHiveStorageFormat;
import static io.prestosql.plugin.hive.HiveTableProperties.getLocation;
import static io.prestosql.plugin.hive.HiveTableProperties.getPartitionedBy;
import static io.prestosql.plugin.hive.HiveTableProperties.getTransactionalValue;
import static io.prestosql.plugin.hive.HiveTableProperties.isExternalTable;
import static io.prestosql.plugin.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static io.prestosql.plugin.hive.HiveUtil.columnExtraInfo;
import static io.prestosql.plugin.hive.HiveUtil.decodeViewData;
import static io.prestosql.plugin.hive.HiveUtil.encodeViewData;
import static io.prestosql.plugin.hive.HiveUtil.getPartitionKeyColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.hiveColumnHandles;
import static io.prestosql.plugin.hive.HiveUtil.isPrestoView;
import static io.prestosql.plugin.hive.HiveUtil.toPartitionValues;
import static io.prestosql.plugin.hive.HiveUtil.verifyPartitionTypeSupported;
import static io.prestosql.plugin.hive.HiveWriteUtils.isS3FileSystem;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_ANALYZE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.INVALID_SCHEMA_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.prestosql.spi.predicate.TupleDomain.withColumnDomains;
import static io.prestosql.spi.security.PrincipalType.USER;
import static io.prestosql.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public class HiveMetadata
        implements TransactionalMetadata
{
    public static final String PRESTO_VERSION_NAME = "presto_version";
    public static final String PRESTO_QUERY_ID_NAME = "presto_query_id";
    public static final String BUCKETING_VERSION = "bucketing_version";
    public static final String TABLE_COMMENT = "comment";

    public static final String STORAGE_FORMAT = "storage_format";

    private static final String ORC_BLOOM_FILTER_COLUMNS_KEY = "orc.bloom.filter.columns";
    private static final String ORC_BLOOM_FILTER_FPP_KEY = "orc.bloom.filter.fpp";

    private static final String TEXT_SKIP_HEADER_COUNT_KEY = "skip.header.line.count";
    private static final String TEXT_SKIP_FOOTER_COUNT_KEY = "skip.footer.line.count";

    public static final String AVRO_SCHEMA_URL_KEY = "avro.schema.url";

    private static final String CSV_SEPARATOR_KEY = OpenCSVSerde.SEPARATORCHAR;
    private static final String CSV_QUOTE_KEY = OpenCSVSerde.QUOTECHAR;
    private static final String CSV_ESCAPE_KEY = OpenCSVSerde.ESCAPECHAR;

    private final boolean allowCorruptWritesForTesting;
    protected final SemiTransactionalHiveMetastore metastore;
    protected final HdfsEnvironment hdfsEnvironment;
    private final HivePartitionManager partitionManager;
    private final DateTimeZone timeZone;
    protected final TypeManager typeManager;
    protected final LocationService locationService;
    private final JsonCodec<PartitionUpdate> partitionUpdateCodec;
    private final boolean writesToNonManagedTablesEnabled;
    private final boolean createsOfNonManagedTablesEnabled;
    protected final TypeTranslator typeTranslator;
    protected final String prestoVersion;
    private final HiveStatisticsProvider hiveStatisticsProvider;
    private final AccessControlMetadata accessControlMetadata;
    protected final boolean tableCreatesWithLocationAllowed;

    private final int vacuumDeltaNumThreshold;
    private final double vacuumDeltaPercentThreshold;
    private final boolean autoVacuumEnabled;
    protected final ScheduledExecutorService vacuumExecutorService;
    protected final ScheduledExecutorService hiveMetastoreClientService;
    private final long vacuumCollectorInterval;

    private boolean externalTable;

    public HiveMetadata(
            SemiTransactionalHiveMetastore metastore,
            HdfsEnvironment hdfsEnvironment,
            HivePartitionManager partitionManager,
            DateTimeZone timeZone,
            boolean allowCorruptWritesForTesting,
            boolean writesToNonManagedTablesEnabled,
            boolean createsOfNonManagedTablesEnabled,
            boolean tableCreatesWithLocationAllowed,
            TypeManager typeManager,
            LocationService locationService,
            JsonCodec<PartitionUpdate> partitionUpdateCodec,
            TypeTranslator typeTranslator,
            String prestoVersion,
            HiveStatisticsProvider hiveStatisticsProvider,
            AccessControlMetadata accessControlMetadata,
            boolean autoVacuumEnabled,
            int vacuumDeltaNumThreshold,
            double vacuumDeltaPercentThreshold,
            ScheduledExecutorService vacuumExecutorService,
            Optional<Duration> vacuumCollectorInterval,
            ScheduledExecutorService hiveMetastoreClientService)
    {
        this.allowCorruptWritesForTesting = allowCorruptWritesForTesting;

        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
        this.timeZone = requireNonNull(timeZone, "timeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.partitionUpdateCodec = requireNonNull(partitionUpdateCodec, "partitionUpdateCodec is null");
        this.writesToNonManagedTablesEnabled = writesToNonManagedTablesEnabled;
        this.createsOfNonManagedTablesEnabled = createsOfNonManagedTablesEnabled;
        this.tableCreatesWithLocationAllowed = tableCreatesWithLocationAllowed;
        this.typeTranslator = requireNonNull(typeTranslator, "typeTranslator is null");
        this.prestoVersion = requireNonNull(prestoVersion, "prestoVersion is null");
        this.hiveStatisticsProvider = requireNonNull(hiveStatisticsProvider, "hiveStatisticsProvider is null");
        this.accessControlMetadata = requireNonNull(accessControlMetadata, "accessControlMetadata is null");
        this.externalTable = false;

        this.vacuumDeltaNumThreshold = vacuumDeltaNumThreshold;
        this.vacuumDeltaPercentThreshold = vacuumDeltaPercentThreshold;
        this.autoVacuumEnabled = autoVacuumEnabled;
        this.vacuumExecutorService = vacuumExecutorService;
        this.vacuumCollectorInterval = vacuumCollectorInterval.map(Duration::toMillis)
                .orElseThrow(() -> new PrestoException(GENERIC_INTERNAL_ERROR, "Vacuum collector interval is not set correctly"));
        this.hiveMetastoreClientService = hiveMetastoreClientService;
    }

    public SemiTransactionalHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HiveTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        Optional<Table> table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent()) {
            return null;
        }

        // we must not allow system tables due to how permissions are checked in SystemTableAwareAccessControl
        if (getSourceTableNameFromSystemTable(tableName).isPresent()) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Unexpected table present in Hive metastore: " + tableName);
        }

        MetastoreUtil.verifyOnline(tableName, Optional.empty(), MetastoreUtil.getProtectMode(table.get()), table.get().getParameters());

        Map<String, String> parameters = new HashMap<>();
        parameters.putAll(table.get().getParameters());

        String format = table.get().getStorage().getStorageFormat().getOutputFormatNullable();
        if (format != null) {
            parameters.put(STORAGE_FORMAT, format);
        }

        return new HiveTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                parameters,
                getPartitionKeyColumnHandles(table.get()),
                HiveBucketing.getHiveBucketHandle(table.get()));
    }

    @Override
    public ConnectorTableHandle getTableHandleForStatisticsCollection(ConnectorSession session, SchemaTableName tableName, Map<String, Object> analyzeProperties)
    {
        HiveTableHandle handle = getTableHandle(session, tableName);
        if (handle == null) {
            return null;
        }
        Optional<List<List<String>>> partitionValuesList = HiveAnalyzeProperties.getPartitionList(analyzeProperties);
        ConnectorTableMetadata tableMetadata = getTableMetadata(session, handle.getSchemaTableName());
        handle = handle.withAnalyzePartitionValues(partitionValuesList);

        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        partitionValuesList.ifPresent(list -> {
            if (partitionedBy.isEmpty()) {
                throw new PrestoException(INVALID_ANALYZE_PROPERTY, "Partition list provided but table is not partitioned");
            }
            for (List<String> values : list) {
                if (values.size() != partitionedBy.size()) {
                    throw new PrestoException(INVALID_ANALYZE_PROPERTY, "Partition value count does not match partition column count");
                }
            }
        });

        HiveTableHandle table = handle;
        return partitionValuesList
                .map(values -> partitionManager.getPartitions(table, values))
                .map(result -> partitionManager.applyPartitionResult(table, result))
                .orElse(table);
    }

    @Override
    public Optional<SystemTable> getSystemTable(ConnectorSession session, SchemaTableName tableName)
    {
        if (SystemTableHandler.PARTITIONS.matches(tableName)) {
            return getPartitionsSystemTable(session, tableName, SystemTableHandler.PARTITIONS.getSourceTableName(tableName));
        }
        if (SystemTableHandler.PROPERTIES.matches(tableName)) {
            return getPropertiesSystemTable(session, tableName, SystemTableHandler.PROPERTIES.getSourceTableName(tableName));
        }
        return Optional.empty();
    }

    private Optional<SystemTable> getPropertiesSystemTable(ConnectorSession session, SchemaTableName tableName, SchemaTableName sourceTableName)
    {
        Optional<Table> table = metastore.getTable(new HiveIdentity(session), sourceTableName.getSchemaName(), sourceTableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            throw new TableNotFoundException(tableName);
        }
        Map<String, String> sortedTableParameters = ImmutableSortedMap.copyOf(table.get().getParameters());
        List<ColumnMetadata> columns = sortedTableParameters.keySet().stream()
                .map(key -> new ColumnMetadata(key, VarcharType.VARCHAR))
                .collect(toImmutableList());
        List<Type> types = columns.stream()
                .map(ColumnMetadata::getType)
                .collect(toImmutableList());
        Iterable<List<Object>> propertyValues = ImmutableList.of(ImmutableList.copyOf(sortedTableParameters.values()));

        return Optional.of(createSystemTable(new ConnectorTableMetadata(sourceTableName, columns), constraint -> new InMemoryRecordSet(types, propertyValues).cursor()));
    }

    private Optional<SystemTable> getPartitionsSystemTable(ConnectorSession session, SchemaTableName tableName, SchemaTableName sourceTableName)
    {
        HiveTableHandle sourceTableHandle = getTableHandle(session, sourceTableName);

        if (sourceTableHandle == null) {
            return Optional.empty();
        }

        List<HiveColumnHandle> partitionColumns = sourceTableHandle.getPartitionColumns();
        if (partitionColumns.isEmpty()) {
            return Optional.empty();
        }

        List<Type> partitionColumnTypes = partitionColumns.stream()
                .map(HiveColumnHandle::getTypeSignature)
                .map(typeManager::getType)
                .collect(toImmutableList());

        List<ColumnMetadata> partitionSystemTableColumns = partitionColumns.stream()
                .map(column -> new ColumnMetadata(
                        column.getName(),
                        typeManager.getType(column.getTypeSignature()),
                        column.getComment().orElse(null),
                        column.isHidden()))
                .collect(toImmutableList());

        Map<Integer, HiveColumnHandle> fieldIdToColumnHandle =
                IntStream.range(0, partitionColumns.size())
                        .boxed()
                        .collect(toImmutableMap(identity(), partitionColumns::get));

        return Optional.of(createSystemTable(
                new ConnectorTableMetadata(tableName, partitionSystemTableColumns),
                constraint -> {
                    TupleDomain<ColumnHandle> targetTupleDomain = constraint.transform(fieldIdToColumnHandle::get);
                    Predicate<Map<ColumnHandle, NullableValue>> targetPredicate = convertToPredicate(targetTupleDomain);
                    Constraint targetConstraint = new Constraint(targetTupleDomain, targetPredicate);
                    Iterable<List<Object>> records = () ->
                            stream(partitionManager.getPartitions(metastore, new HiveIdentity(session), sourceTableHandle, targetConstraint).getPartitions())
                                    .map(hivePartition ->
                                            IntStream.range(0, partitionColumns.size())
                                                    .mapToObj(fieldIdToColumnHandle::get)
                                                    .map(columnHandle -> hivePartition.getKeys().get(columnHandle).getValue())
                                                    .collect(toList()))
                                    .iterator();

                    return new InMemoryRecordSet(partitionColumnTypes, records).cursor();
                }));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, ((HiveTableHandle) tableHandle).getSchemaTableName());
    }

    private ConnectorTableMetadata getTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        try {
            return doGetTableMetadata(session, tableName);
        }
        catch (PrestoException e) {
            throw e;
        }
        catch (RuntimeException e) {
            // Errors related to invalid or unsupported information in the Metastore should be handled explicitly (eg. as PrestoException(HIVE_INVALID_METADATA)).
            // This is just a catch-all solution so that we have any actionable information when eg. SELECT * FROM information_schema.columns fails.
            throw new RuntimeException("Failed to construct table metadata for table " + tableName, e);
        }
    }

    protected ConnectorTableMetadata doGetTableMetadata(ConnectorSession session, SchemaTableName tableName)
    {
        Optional<Table> table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName());
        if (!table.isPresent() || table.get().getTableType().equals(TableType.VIRTUAL_VIEW.name())) {
            throw new TableNotFoundException(tableName);
        }

        Function<HiveColumnHandle, ColumnMetadata> metadataGetter = columnMetadataGetter(table.get(), typeManager);
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get())) {
            columns.add(metadataGetter.apply(columnHandle));
        }

        // External location property
        ImmutableMap.Builder<String, Object> properties = ImmutableMap.builder();
        properties.put(LOCATION_PROPERTY, table.get().getStorage().getLocation());
        properties.put(IS_EXTERNAL_TABLE, table.get().getTableType().equals(EXTERNAL_TABLE.name()));

        // Storage format property
        try {
            HiveStorageFormat format = extractHiveStorageFormat(table.get());
            properties.put(HiveTableProperties.STORAGE_FORMAT_PROPERTY, format);
        }
        catch (PrestoException ignored) {
            // todo fail if format is not known
        }

        // Partitioning property
        List<String> partitionedBy = table.get().getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toList());
        if (!partitionedBy.isEmpty()) {
            properties.put(HiveTableProperties.PARTITIONED_BY_PROPERTY, partitionedBy);
        }

        // Bucket properties
        table.get().getStorage().getBucketProperty().ifPresent(property -> {
            properties.put(BUCKETING_VERSION, property.getBucketingVersion().getVersion());
            properties.put(HiveTableProperties.BUCKET_COUNT_PROPERTY, property.getBucketCount());
            properties.put(HiveTableProperties.BUCKETED_BY_PROPERTY, property.getBucketedBy());
            properties.put(HiveTableProperties.SORTED_BY_PROPERTY, property.getSortedBy());
        });

        // Is transactional table
        if (Boolean.valueOf(table.get().getParameters().get(TRANSACTIONAL))) {
            properties.put(TRANSACTIONAL, true);
        }

        // ORC format specific properties
        String orcBloomFilterColumns = table.get().getParameters().get(ORC_BLOOM_FILTER_COLUMNS_KEY);
        if (orcBloomFilterColumns != null) {
            properties.put(HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS, Splitter.on(',').trimResults().omitEmptyStrings().splitToList(orcBloomFilterColumns));
        }
        String orcBloomFilterFfp = table.get().getParameters().get(ORC_BLOOM_FILTER_FPP_KEY);
        if (orcBloomFilterFfp != null) {
            properties.put(HiveTableProperties.ORC_BLOOM_FILTER_FPP, Double.parseDouble(orcBloomFilterFfp));
        }

        // Avro specific property
        String avroSchemaUrl = table.get().getParameters().get(AVRO_SCHEMA_URL_KEY);
        if (avroSchemaUrl != null) {
            properties.put(HiveTableProperties.AVRO_SCHEMA_URL, avroSchemaUrl);
        }

        // Textfile specific property
        String textSkipHeaderCount = table.get().getParameters().get(TEXT_SKIP_HEADER_COUNT_KEY);
        if (textSkipHeaderCount != null) {
            properties.put(HiveTableProperties.TEXTFILE_SKIP_HEADER_LINE_COUNT, Integer.valueOf(textSkipHeaderCount));
        }
        String textSkipFooterCount = table.get().getParameters().get(TEXT_SKIP_FOOTER_COUNT_KEY);
        if (textSkipFooterCount != null) {
            properties.put(HiveTableProperties.TEXTFILE_SKIP_FOOTER_LINE_COUNT, Integer.valueOf(textSkipFooterCount));
        }

        // CSV specific property
        getCsvSerdeProperty(table.get(), CSV_SEPARATOR_KEY)
                .ifPresent(csvSeparator -> properties.put(HiveTableProperties.CSV_SEPARATOR, csvSeparator));
        getCsvSerdeProperty(table.get(), CSV_QUOTE_KEY)
                .ifPresent(csvQuote -> properties.put(HiveTableProperties.CSV_QUOTE, csvQuote));
        getCsvSerdeProperty(table.get(), CSV_ESCAPE_KEY)
                .ifPresent(csvEscape -> properties.put(HiveTableProperties.CSV_ESCAPE, csvEscape));

        Optional<String> comment = Optional.ofNullable(table.get().getParameters().get(TABLE_COMMENT));

        // add partitioned columns and bucketed columns into immutableColumns
        ImmutableList.Builder<ColumnMetadata> immutableColumns = ImmutableList.builder();
        List<String> bucketedColumns = new ArrayList<>();
        table.get().getStorage().getBucketProperty().ifPresent(property -> {
            bucketedColumns.addAll(property.getBucketedBy());
        });

        for (HiveColumnHandle columnHandle : hiveColumnHandles(table.get())) {
            if (columnHandle.getColumnType().equals(HiveColumnHandle.ColumnType.PARTITION_KEY)) {
                immutableColumns.add(metadataGetter.apply(columnHandle));
            }
            if (bucketedColumns.contains(columnHandle.getColumnName())) {
                immutableColumns.add(metadataGetter.apply(columnHandle));
            }
        }

        return new ConnectorTableMetadata(tableName, columns.build(), properties.build(), comment, Optional.of(immutableColumns.build()), Optional.of(NON_INHERITABLE_PROPERTIES));
    }

    private static Optional<String> getCsvSerdeProperty(Table table, String key)
    {
        return getSerdeProperty(table, key).map(csvSerdeProperty -> {
            if (csvSerdeProperty.length() > 1) {
                throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Only single character can be set for property: " + key);
            }
            return csvSerdeProperty;
        });
    }

    private static Optional<String> getSerdeProperty(Table table, String key)
    {
        String serdePropertyValue = table.getStorage().getSerdeParameters().get(key);
        String tablePropertyValue = table.getParameters().get(key);
        if (serdePropertyValue != null && tablePropertyValue != null && !tablePropertyValue.equals(serdePropertyValue)) {
            // in Hive one can set conflicting values for the same property, in such case it looks like table properties are used
            throw new PrestoException(
                    HiveErrorCode.HIVE_INVALID_METADATA,
                    format("Different values for '%s' set in serde properties and table properties: '%s' and '%s'", key, serdePropertyValue, tablePropertyValue));
        }
        return firstNonNullable(tablePropertyValue, serdePropertyValue);
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return ((HiveTableHandle) table).getPartitions()
                .map(partitions -> new HiveInputInfo(
                        partitions.stream()
                                .map(HivePartition::getPartitionId)
                                .collect(toImmutableList()),
                        false));
    }

    @Override
    public boolean isHeuristicIndexSupported()
    {
        return true;
    }

    @Override
    public boolean isPreAggregationSupported(ConnectorSession session)
    {
        return true;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllTables(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    private List<String> listSchemas(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isPresent()) {
            return ImmutableList.of(schemaName.get());
        }
        return listSchemaNames(session);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return hiveColumnHandles(table).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public long getTableModificationTime(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        String tableLocation = table.getStorage().getLocation();
        Path tablePath = new Path(tableLocation);
        try {
            FileSystem fileSystem = this.hdfsEnvironment.getFileSystem(new HdfsContext(session, tableName.getSchemaName()), tablePath);
            // We use the directory modification time to represent the table modification time
            // since HDFS is append-only and any table modification will trigger directory update.
            return fileSystem.getFileStatus(tablePath).getModificationTime();
        }
        catch (IOException exception) {
            throw new PrestoException(HiveErrorCode.HIVE_FILESYSTEM_ERROR, "Cannot get the modification time.");
        }
    }

    @SuppressWarnings("TryWithIdenticalCatches")
    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            try {
                columns.put(tableName, getTableMetadata(session, tableName).getColumns());
            }
            catch (HiveViewNotSupportedException e) {
                // view is not supported
            }
            catch (TableNotFoundException e) {
                // table disappeared during listing operation
            }
        }
        return columns.build();
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        if (!HiveSessionProperties.isStatisticsEnabled(session)) {
            return TableStatistics.empty();
        }
        Map<String, ColumnHandle> columns = getColumnHandles(session, tableHandle)
                .entrySet().stream()
                .filter(entry -> !((HiveColumnHandle) entry.getValue()).isHidden())
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        Map<String, Type> columnTypes = columns.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> getColumnMetadata(session, tableHandle, entry.getValue()).getType()));
        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, new HiveIdentity(session), tableHandle, constraint);
        List<HivePartition> partitions = partitionManager.getPartitionsAsList(partitionResult);
        return hiveStatisticsProvider.getTableStatistics(session, ((HiveTableHandle) tableHandle).getSchemaTableName(), columns, columnTypes, partitions);
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (!prefix.getTable().isPresent()) {
            return listTables(session, prefix.getSchema());
        }
        return ImmutableList.of(prefix.toSchemaTableName());
    }

    /**
     * NOTE: This method does not return column comment
     */
    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((HiveColumnHandle) columnHandle).getColumnMetadata(typeManager);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties)
    {
        Optional<String> location = HiveSchemaProperties.getLocation(properties).map(locationUri -> {
            try {
                hdfsEnvironment.getFileSystem(new HdfsContext(session, schemaName), new Path(locationUri));
            }
            catch (IOException e) {
                throw new PrestoException(INVALID_SCHEMA_PROPERTY, "Invalid location URI: " + locationUri, e);
            }
            return locationUri;
        });

        Database database = Database.builder()
                .setDatabaseName(schemaName)
                .setLocation(location)
                .setOwnerType(USER)
                .setOwnerName(session.getUser())
                .build();

        metastore.createDatabase(new HiveIdentity(session), database);
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName)
    {
        // basic sanity check to provide a better error message
        if (!listTables(session, Optional.of(schemaName)).isEmpty() ||
                !listViews(session, Optional.of(schemaName)).isEmpty()) {
            throw new PrestoException(SCHEMA_NOT_EMPTY, "Schema not empty: " + schemaName);
        }
        metastore.dropDatabase(new HiveIdentity(session), schemaName);
    }

    @Override
    public void renameSchema(ConnectorSession session, String source, String target)
    {
        metastore.renameDatabase(new HiveIdentity(session), source, target);
    }

    @Override
    public void createTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, boolean ignoreExisting)
    {
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = HiveTableProperties.getBucketProperty(tableMetadata.getProperties());

        if ((bucketProperty.isPresent() || !partitionedBy.isEmpty()) && HiveTableProperties.getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new PrestoException(NOT_SUPPORTED, "Bucketing/Partitioning columns not supported when Avro schema url is set");
        }

        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat hiveStorageFormat = HiveTableProperties.getHiveStorageFormat(tableMetadata.getProperties());
        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsContext(session, schemaName, tableName));

        hiveStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        boolean external = isExternalTable(tableMetadata.getProperties());
        String externalLocation = getExternalLocation(tableMetadata.getProperties());
        if ((external || (externalLocation != null)) && !createsOfNonManagedTablesEnabled) {
            throw new PrestoException(NOT_SUPPORTED, "Cannot create non-managed Hive table");
        }

        Path targetPath;
        Optional<String> location = getLocation(tableMetadata.getProperties());
        // User specifies the location property
        if (location.isPresent()) {
            if (!tableCreatesWithLocationAllowed) {
                throw new PrestoException(NOT_SUPPORTED, format("Setting %s property is not allowed", LOCATION_PROPERTY));
            }
            targetPath = getPath(new HdfsContext(session, schemaName, tableName), location.get(), external);
        }
        else {
            // User specifies external property, but location property is absent
            if (external) {
                throw new PrestoException(NOT_SUPPORTED, format("Cannot create external Hive table without location. Set it through '%s' property", LOCATION_PROPERTY));
            }

            // User specifies the external location property
            if (externalLocation != null) {
                external = true;
                targetPath = getPath(new HdfsContext(session, schemaName, tableName), externalLocation, true);
            }

            // Default option
            else {
                external = false;
                LocationHandle locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, Optional.empty(), Optional.empty(), HiveWriteUtils.OpertionType.CREATE_TABLE);
                targetPath = locationService.getQueryWriteInfo(locationHandle).getTargetPath();
            }
        }

        Table table = buildTableObject(
                session.getQueryId(),
                schemaName,
                tableName,
                session.getUser(),
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketProperty,
                tableProperties,
                targetPath,
                external,
                prestoVersion);
        PrincipalPrivileges principalPrivileges = MetastoreUtil.buildInitialPrivilegeSet(table.getOwner());
        HiveBasicStatistics basicStatistics = table.getPartitionColumns().isEmpty() ? HiveBasicStatistics.createZeroStatistics() : HiveBasicStatistics.createEmptyStatistics();
        metastore.createTable(
                session,
                table,
                principalPrivileges,
                Optional.empty(),
                ignoreExisting,
                new PartitionStatistics(basicStatistics, ImmutableMap.of()));
    }

    protected Map<String, String> getEmptyTableProperties(ConnectorTableMetadata tableMetadata, Optional<HiveBucketProperty> bucketProperty, HdfsContext hdfsContext)
    {
        HiveStorageFormat hiveStorageFormat = HiveTableProperties.getHiveStorageFormat(tableMetadata.getProperties());
        ImmutableMap.Builder<String, String> tableProperties = ImmutableMap.builder();

        bucketProperty.ifPresent(hiveBucketProperty ->
                tableProperties.put(BUCKETING_VERSION, Integer.toString(hiveBucketProperty.getBucketingVersion().getVersion())));

        // ORC format specific properties
        if (getTransactionalValue(tableMetadata.getProperties())) {
            if (!hiveStorageFormat.equals(HiveStorageFormat.ORC)) {
                // only ORC storage format support ACID
                throw new PrestoException(NOT_SUPPORTED, "Only ORC storage format supports creating transactional table.");
            }

            // set transactional property.
            tableProperties.put(TRANSACTIONAL, Boolean.toString(getTransactionalValue(tableMetadata.getProperties())));
        }
        List<String> columns = HiveTableProperties.getOrcBloomFilterColumns(tableMetadata.getProperties());
        if (columns != null && !columns.isEmpty()) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.ORC, HiveTableProperties.ORC_BLOOM_FILTER_COLUMNS);
            tableProperties.put(ORC_BLOOM_FILTER_COLUMNS_KEY, Joiner.on(",").join(columns));
            tableProperties.put(ORC_BLOOM_FILTER_FPP_KEY, String.valueOf(HiveTableProperties.getOrcBloomFilterFpp(tableMetadata.getProperties())));
        }

        // Avro specific properties
        String avroSchemaUrl = HiveTableProperties.getAvroSchemaUrl(tableMetadata.getProperties());
        if (avroSchemaUrl != null) {
            checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.AVRO, HiveTableProperties.AVRO_SCHEMA_URL);
            tableProperties.put(AVRO_SCHEMA_URL_KEY, validateAndNormalizeAvroSchemaUrl(avroSchemaUrl, hdfsContext));
        }

        // Textfile specific properties
        HiveTableProperties.getTextHeaderSkipCount(tableMetadata.getProperties()).ifPresent(headerSkipCount -> {
            if (headerSkipCount > 0) {
                checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.TEXTFILE, HiveTableProperties.TEXTFILE_SKIP_HEADER_LINE_COUNT);
                tableProperties.put(TEXT_SKIP_HEADER_COUNT_KEY, String.valueOf(headerSkipCount));
            }
            if (headerSkipCount < 0) {
                throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, String.format("Invalid value for %s property: %s", HiveTableProperties.TEXTFILE_SKIP_HEADER_LINE_COUNT, headerSkipCount));
            }
        });

        HiveTableProperties.getTextFooterSkipCount(tableMetadata.getProperties()).ifPresent(footerSkipCount -> {
            if (footerSkipCount > 0) {
                checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.TEXTFILE, HiveTableProperties.TEXTFILE_SKIP_FOOTER_LINE_COUNT);
                tableProperties.put(TEXT_SKIP_FOOTER_COUNT_KEY, String.valueOf(footerSkipCount));
            }
            if (footerSkipCount < 0) {
                throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, String.format("Invalid value for %s property: %s", HiveTableProperties.TEXTFILE_SKIP_FOOTER_LINE_COUNT, footerSkipCount));
            }
        });

        // CSV specific properties
        HiveTableProperties.getCsvProperty(tableMetadata.getProperties(), HiveTableProperties.CSV_ESCAPE)
                .ifPresent(escape -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, HiveTableProperties.CSV_ESCAPE);
                    tableProperties.put(CSV_ESCAPE_KEY, escape.toString());
                });
        HiveTableProperties.getCsvProperty(tableMetadata.getProperties(), HiveTableProperties.CSV_QUOTE)
                .ifPresent(quote -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, HiveTableProperties.CSV_QUOTE);
                    tableProperties.put(CSV_QUOTE_KEY, quote.toString());
                });
        HiveTableProperties.getCsvProperty(tableMetadata.getProperties(), HiveTableProperties.CSV_SEPARATOR)
                .ifPresent(separator -> {
                    checkFormatForProperty(hiveStorageFormat, HiveStorageFormat.CSV, HiveTableProperties.CSV_SEPARATOR);
                    tableProperties.put(CSV_SEPARATOR_KEY, separator.toString());
                });

        // Table comment property
        tableMetadata.getComment().ifPresent(value -> tableProperties.put(TABLE_COMMENT, value));

        return tableProperties.build();
    }

    private static void checkFormatForProperty(HiveStorageFormat actualStorageFormat, HiveStorageFormat expectedStorageFormat, String propertyName)
    {
        if (actualStorageFormat != expectedStorageFormat) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Cannot specify %s table property for storage format: %s", propertyName, actualStorageFormat));
        }
    }

    private String validateAndNormalizeAvroSchemaUrl(String url, HdfsContext context)
    {
        try {
            new URL(url).openStream().close();
            return url;
        }
        catch (MalformedURLException e) {
            // try locally
            if (new File(url).exists()) {
                // hive needs url to have a protocol
                return new File(url).toURI().toString();
            }
            // try hdfs
            try {
                if (!hdfsEnvironment.getFileSystem(context, new Path(url)).exists(new Path(url))) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot locate Avro schema file: " + url);
                }
                return url;
            }
            catch (IOException ex) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "Avro schema file is not a valid file system URI: " + url, ex);
            }
        }
        catch (IOException e) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Cannot open Avro schema file: " + url, e);
        }
    }

    protected Path getPath(HdfsContext context, String location, Boolean external)
    {
        try {
            Path path = new Path(location);
            if (!isS3FileSystem(context, hdfsEnvironment, path)) {
                if (!hdfsEnvironment.getFileSystem(context, path).isDirectory(path)) {
                    throw new PrestoException(INVALID_TABLE_PROPERTY, format("Location is not a directory: %s", location));
                }
            }

            if (!external) {
                return new Path(path, context.getTableName().get());
            }

            return path;
        }
        catch (IllegalArgumentException | IOException e) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Location is not a valid file system URI", e);
        }
    }

    protected void checkPartitionTypesSupported(List<Column> partitionColumns)
    {
        for (Column partitionColumn : partitionColumns) {
            Type partitionType = typeManager.getType(partitionColumn.getType().getTypeSignature());
            verifyPartitionTypeSupported(partitionColumn.getName(), partitionType);
        }
    }

    protected static Table buildTableObject(
            String queryId,
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            BaseStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> additionalTableParameters,
            Path targetPath,
            boolean external,
            String prestoVersion)
    {
        return buildTableObject(
                queryId,
                schemaName,
                tableName,
                tableOwner,
                columnHandles,
                hiveStorageFormat,
                partitionedBy,
                bucketProperty,
                additionalTableParameters,
                targetPath,
                external,
                prestoVersion,
                null);
    }

    protected static Table buildTableObject(
            String queryId,
            String schemaName,
            String tableName,
            String tableOwner,
            List<HiveColumnHandle> columnHandles,
            BaseStorageFormat hiveStorageFormat,
            List<String> partitionedBy,
            Optional<HiveBucketProperty> bucketProperty,
            Map<String, String> additionalTableParameters,
            Path targetPath,
            boolean external,
            String prestoVersion,
            Map<String, String> serdeParameters)
    {
        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());

        Set<String> partitionColumnNames = ImmutableSet.copyOf(partitionedBy);

        ImmutableList.Builder<Column> columns = ImmutableList.builder();
        for (HiveColumnHandle columnHandle : columnHandles) {
            String name = columnHandle.getName();
            HiveType type = columnHandle.getHiveType();
            if (!partitionColumnNames.contains(name)) {
                verify(!columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
                columns.add(new Column(name, type, columnHandle.getComment()));
            }
            else {
                verify(columnHandle.isPartitionKey(), "Column handles are not consistent with partitioned by property");
            }
        }

        ImmutableMap.Builder<String, String> tableParameters = ImmutableMap.<String, String>builder()
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, queryId)
                .putAll(additionalTableParameters);

        if (external) {
            tableParameters.put("EXTERNAL", "TRUE");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(schemaName)
                .setTableName(tableName)
                .setOwner(tableOwner)
                .setTableType((external ? EXTERNAL_TABLE : MANAGED_TABLE).name())
                .setDataColumns(columns.build())
                .setPartitionColumns(partitionColumns)
                .setParameters(tableParameters.build());

        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.fromHiveStorageFormat(hiveStorageFormat))
                .setSerdeParameters(ImmutableMap.of(serdeConstants.SERIALIZATION_FORMAT, "1"))
                .setBucketProperty(bucketProperty)
                .setLocation(targetPath.toString());

        if (null != serdeParameters) {
            tableBuilder.getStorageBuilder().setSerdeParameters(serdeParameters);
        }

        return tableBuilder.build();
    }

    @Override
    public void addColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnMetadata column)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, handle);

        metastore.addColumn(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), column.getName(), HiveType.toHiveType(typeTranslator, column.getType()), column.getComment());
    }

    @Override
    public void renameColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle source, String target)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle sourceHandle = (HiveColumnHandle) source;

        metastore.renameColumn(new HiveIdentity(session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), sourceHandle.getName(), target);
    }

    @Override
    public void dropColumn(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle column)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        failIfAvroSchemaIsSet(session, hiveTableHandle);
        HiveColumnHandle columnHandle = (HiveColumnHandle) column;

        metastore.dropColumn(new HiveIdentity(session), hiveTableHandle.getSchemaName(), hiveTableHandle.getTableName(), columnHandle.getName());
    }

    private void failIfAvroSchemaIsSet(ConnectorSession session, HiveTableHandle handle)
    {
        Table table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (table.getParameters().containsKey(AVRO_SCHEMA_URL_KEY) || table.getStorage().getSerdeParameters().containsKey(AVRO_SCHEMA_URL_KEY)) {
            throw new PrestoException(NOT_SUPPORTED, "ALTER TABLE not supported when Avro schema url is set");
        }
    }

    @Override
    public void renameTable(ConnectorSession session, ConnectorTableHandle tableHandle, SchemaTableName newTableName)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.renameTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), newTableName.getSchemaName(), newTableName.getTableName());
    }

    @Override
    public void setTableComment(ConnectorSession session, ConnectorTableHandle tableHandle, Optional<String> comment)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        metastore.commentTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName(), comment);
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        Optional<Table> target = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName());
        if (!target.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());
    }

    @Override
    public ConnectorTableHandle beginStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        verifyJvmTimeZone();
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return tableHandle;
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, ConnectorTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = handle.getSchemaTableName();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));

        List<Column> partitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = partitionColumns.stream()
                .map(Column::getName)
                .collect(toImmutableList());
        List<HiveColumnHandle> hiveColumnHandles = hiveColumnHandles(table);
        Map<String, Type> columnTypes = hiveColumnHandles.stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));

        Map<List<String>, ComputedStatistics> computedStatisticsMap = Statistics.createComputedStatisticsToPartitionMap(computedStatistics, partitionColumnNames, columnTypes);

        if (partitionColumns.isEmpty()) {
            // commit analyze to unpartitioned table
            metastore.setTableStatistics(identity, table, createPartitionStatistics(session, columnTypes, computedStatisticsMap.get(ImmutableList.<String>of())));
        }
        else {
            List<List<String>> partitionValuesList;
            if (handle.getAnalyzePartitionValues().isPresent()) {
                partitionValuesList = handle.getAnalyzePartitionValues().get();
            }
            else {
                partitionValuesList = metastore.getPartitionNames(identity, handle.getSchemaName(), handle.getTableName())
                        .orElseThrow(() -> new TableNotFoundException(((HiveTableHandle) tableHandle).getSchemaTableName()))
                        .stream()
                        .map(HiveUtil::toPartitionValues)
                        .collect(toImmutableList());
            }

            ImmutableMap.Builder<List<String>, PartitionStatistics> partitionStatistics = ImmutableMap.builder();
            Map<String, Set<ColumnStatisticType>> columnStatisticTypes = hiveColumnHandles.stream()
                    .filter(columnHandle -> !partitionColumnNames.contains(columnHandle.getName()))
                    .filter(column -> !column.isHidden())
                    .collect(toImmutableMap(HiveColumnHandle::getName, column -> ImmutableSet.copyOf(metastore.getSupportedColumnStatistics(typeManager.getType(column.getTypeSignature())))));
            Supplier<PartitionStatistics> emptyPartitionStatistics = Suppliers.memoize(() -> Statistics.createEmptyPartitionStatistics(columnTypes, columnStatisticTypes));

            int usedComputedStatistics = 0;
            for (List<String> partitionValues : partitionValuesList) {
                ComputedStatistics collectedStatistics = computedStatisticsMap.get(partitionValues);
                if (collectedStatistics == null) {
                    partitionStatistics.put(partitionValues, emptyPartitionStatistics.get());
                }
                else {
                    usedComputedStatistics++;
                    partitionStatistics.put(partitionValues, createPartitionStatistics(session, columnTypes, collectedStatistics));
                }
            }
            verify(usedComputedStatistics == computedStatistics.size(), "All computed statistics must be used");
            metastore.setPartitionStatistics(identity, table, partitionStatistics.build());
        }
    }

    @Override
    public HiveOutputTableHandle beginCreateTable(ConnectorSession session, ConnectorTableMetadata tableMetadata, Optional<ConnectorNewTableLayout> layout)
    {
        verifyJvmTimeZone();

        if (getExternalLocation(tableMetadata.getProperties()) != null || isExternalTable(tableMetadata.getProperties())) {
            throw new PrestoException(NOT_SUPPORTED, "External tables cannot be created using CREATE TABLE AS");
        }

        if (HiveTableProperties.getAvroSchemaUrl(tableMetadata.getProperties()) != null) {
            throw new PrestoException(NOT_SUPPORTED, "CREATE TABLE AS not supported when Avro schema url is set");
        }

        HiveStorageFormat tableStorageFormat = HiveTableProperties.getHiveStorageFormat(tableMetadata.getProperties());
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
        Optional<HiveBucketProperty> bucketProperty = HiveTableProperties.getBucketProperty(tableMetadata.getProperties());

        // get the root directory for the database
        SchemaTableName schemaTableName = tableMetadata.getTable();
        String schemaName = schemaTableName.getSchemaName();
        String tableName = schemaTableName.getTableName();

        Map<String, String> tableProperties = getEmptyTableProperties(tableMetadata, bucketProperty, new HdfsContext(session, schemaName, tableName));
        List<HiveColumnHandle> columnHandles = getColumnHandles(tableMetadata, ImmutableSet.copyOf(partitionedBy), typeTranslator);
        HiveStorageFormat partitionStorageFormat = HiveSessionProperties.isRespectTableFormat(session) ? tableStorageFormat : HiveSessionProperties.getHiveStorageFormat(session);

        // unpartitioned tables ignore the partition storage format
        HiveStorageFormat actualStorageFormat = partitionedBy.isEmpty() ? tableStorageFormat : partitionStorageFormat;
        actualStorageFormat.validateColumns(columnHandles);

        Map<String, HiveColumnHandle> columnHandlesByName = Maps.uniqueIndex(columnHandles, HiveColumnHandle::getName);
        List<Column> partitionColumns = partitionedBy.stream()
                .map(columnHandlesByName::get)
                .map(column -> new Column(column.getName(), column.getHiveType(), column.getComment()))
                .collect(toList());
        checkPartitionTypesSupported(partitionColumns);

        Optional<String> location = getLocation(tableMetadata.getProperties());
        if (location.isPresent() && !tableCreatesWithLocationAllowed) {
            throw new PrestoException(NOT_SUPPORTED, format("Setting %s property is not allowed", LOCATION_PROPERTY));
        }

        Optional<WriteIdInfo> writeIdInfo = Optional.empty();
        if (AcidUtils.isTransactionalTable(tableProperties)) {
            //Create the HiveTableHandle for just to obtain writeIds.
            List<HiveColumnHandle> partitionColumnHandles = partitionedBy.stream()
                    .map(columnHandlesByName::get)
                    .collect(toList());
            HiveTableHandle tableHandle = new HiveTableHandle(schemaName,
                    tableName, tableProperties, partitionColumnHandles, Optional.empty());
            Optional<Long> writeId = metastore.getTableWriteId(session, tableHandle, HiveACIDWriteType.INSERT);
            if (!writeId.isPresent()) {
                throw new IllegalStateException("No validWriteIds present");
            }
            writeIdInfo = Optional.of(new WriteIdInfo(writeId.get(), writeId.get(), 0));
        }

        LocationHandle locationHandle;
        if (location.isPresent()) {
            Path path = getPath(new HdfsContext(session, schemaName, tableName), location.get(), false);
            locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, writeIdInfo, Optional.of(path), HiveWriteUtils.OpertionType.CREATE_TABLE_AS);
        }
        else {
            locationHandle = locationService.forNewTable(metastore, session, schemaName, tableName, writeIdInfo, Optional.empty(), HiveWriteUtils.OpertionType.CREATE_TABLE_AS);
        }
        HiveOutputTableHandle result = new HiveOutputTableHandle(
                schemaName,
                tableName,
                columnHandles,
                metastore.generatePageSinkMetadata(new HiveIdentity(session), schemaTableName),
                locationHandle,
                tableStorageFormat,
                partitionStorageFormat,
                partitionedBy,
                bucketProperty,
                session.getUser(),
                tableProperties);

        LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), schemaTableName);

        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishCreateTable(ConnectorSession session, ConnectorOutputTableHandle tableHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        return finishCreateTable(session, tableHandle, fragments, computedStatistics, null);
    }

    public Optional<ConnectorOutputMetadata> finishCreateTable(
            ConnectorSession session,
            ConnectorOutputTableHandle tableHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            Map<String, String> serdeParameters)
    {
        HiveOutputTableHandle handle = (HiveOutputTableHandle) tableHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .collect(toList());

        LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(handle.getLocationHandle());
        Table table = buildTableObject(
                session.getQueryId(),
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getTableOwner(),
                handle.getInputColumns(),
                handle.getTableStorageFormat(),
                handle.getPartitionedBy(),
                handle.getBucketProperty(),
                handle.getAdditionalTableParameters(),
                writeInfo.getTargetPath(),
                externalTable,
                prestoVersion,
                serdeParameters);
        PrincipalPrivileges principalPrivileges = MetastoreUtil.buildInitialPrivilegeSet(handle.getTableOwner());

        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        if (handle.getBucketProperty().isPresent() && HiveSessionProperties.isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                createEmptyFiles(session, partitionUpdate.getWritePath(), table, partition, partitionUpdate.getFileNames());
            }
        }

        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = Statistics.createComputedStatisticsToPartitionMap(computedStatistics, handle.getPartitionedBy(), columnTypes);

        PartitionStatistics tableStatistics;
        if (table.getPartitionColumns().isEmpty()) {
            HiveBasicStatistics basicStatistics = partitionUpdates.stream()
                    .map(PartitionUpdate::getStatistics)
                    .reduce((first, second) -> Statistics.reduce(first, second, Statistics.ReduceOperator.ADD))
                    .orElse(HiveBasicStatistics.createZeroStatistics());
            tableStatistics = createPartitionStatistics(session, basicStatistics, columnTypes, getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));
        }
        else {
            tableStatistics = new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of());
        }

        metastore.createTable(session, table, principalPrivileges, Optional.of(writeInfo.getWritePath()), false, tableStatistics);

        if (!handle.getPartitionedBy().isEmpty()) {
            if (HiveSessionProperties.isRespectTableFormat(session)) {
                verify(handle.getPartitionStorageFormat() == handle.getTableStorageFormat());
            }
            List<? extends Future<?>> futures = partitionUpdates.stream().map(update ->
                    hiveMetastoreClientService.submit(() -> {
                        Partition partition = buildPartitionObject(session, table, update);
                        PartitionStatistics partitionStatistics = createPartitionStatistics(
                                session,
                                update.getStatistics(),
                                columnTypes,
                                getColumnStatistics(partitionComputedStatistics, partition.getValues()));
                        metastore.addPartition(
                                session,
                                handle.getSchemaName(),
                                handle.getTableName(),
                                buildPartitionObject(session, table, update),
                                update.getWritePath(),
                                partitionStatistics,
                                HiveACIDWriteType.NONE);
                    })).collect(toList());
            futures.forEach(future -> {
                try {
                    future.get();
                }
                catch (InterruptedException | ExecutionException ignore) {
                }
            });
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toList())));
    }

    private List<PartitionUpdate> computePartitionUpdatesForMissingBuckets(
            ConnectorSession session,
            HiveWritableTableHandle handle,
            Table table,
            List<PartitionUpdate> partitionUpdates)
    {
        ImmutableList.Builder<PartitionUpdate> partitionUpdatesForMissingBucketsBuilder = ImmutableList.builder();
        HiveStorageFormat storageFormat = table.getPartitionColumns().isEmpty() ? handle.getTableStorageFormat() : handle.getPartitionStorageFormat();
        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            int bucketCount = handle.getBucketProperty().get().getBucketCount();

            List<String> fileNamesForMissingBuckets = computeFileNamesForMissingBuckets(
                    session,
                    table,
                    storageFormat,
                    partitionUpdate.getTargetPath(),
                    bucketCount,
                    partitionUpdate);
            partitionUpdatesForMissingBucketsBuilder.add(new PartitionUpdate(
                    partitionUpdate.getName(),
                    partitionUpdate.getUpdateMode(),
                    partitionUpdate.getWritePath(),
                    partitionUpdate.getTargetPath(),
                    fileNamesForMissingBuckets,
                    0,
                    0,
                    0,
                    partitionUpdate.getMiscData()));
        }
        return partitionUpdatesForMissingBucketsBuilder.build();
    }

    private List<String> computeFileNamesForMissingBuckets(
            ConnectorSession session,
            Table table,
            HiveStorageFormat storageFormat,
            Path targetPath,
            int bucketCount,
            PartitionUpdate partitionUpdate)
    {
        if (partitionUpdate.getFileNames().size() == bucketCount) {
            // fast path for common case
            return ImmutableList.of();
        }
        HdfsContext hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName());
        JobConf conf = ConfigurationUtils.toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, targetPath));
        String fileExtension = HiveWriterFactory.getFileExtension(conf, StorageFormat.fromHiveStorageFormat(storageFormat));
        Set<String> fileNames = ImmutableSet.copyOf(partitionUpdate.getFileNames());
        ImmutableList.Builder<String> missingFileNamesBuilder = ImmutableList.builder();
        for (int i = 0; i < bucketCount; i++) {
            String fileName = HiveWriterFactory.computeBucketedFileName(session.getQueryId(), i) + fileExtension;
            if (!fileNames.contains(fileName)) {
                missingFileNamesBuilder.add(fileName);
            }
        }
        List<String> missingFileNames = missingFileNamesBuilder.build();
        verify(fileNames.size() + missingFileNames.size() == bucketCount);
        return missingFileNames;
    }

    private void createEmptyFiles(ConnectorSession session, Path path, Table table, Optional<Partition> partition, List<String> fileNames)
    {
        JobConf conf = ConfigurationUtils.toJobConf(hdfsEnvironment.getConfiguration(new HdfsContext(session, table.getDatabaseName(), table.getTableName()), path));

        Properties schema;
        StorageFormat format;
        if (partition.isPresent()) {
            schema = MetastoreUtil.getHiveSchema(partition.get(), table);
            format = partition.get().getStorage().getStorageFormat();
        }
        else {
            schema = MetastoreUtil.getHiveSchema(table);
            format = table.getStorage().getStorageFormat();
        }
        hdfsEnvironment.doAs(session.getUser(), () -> {
            for (String fileName : fileNames) {
                writeEmptyFile(session, new Path(path, fileName), conf, schema, format.getSerDe(), format.getOutputFormat());
            }
        });
    }

    private static void writeEmptyFile(ConnectorSession session, Path target, JobConf conf, Properties properties, String serDe, String outputFormatName)
    {
        // Some serializers such as Avro set a property in the schema.
        HiveWriteUtils.initializeSerializer(conf, properties, serDe);

        // The code below is not a try with resources because RecordWriter is not Closeable.
        FileSinkOperator.RecordWriter recordWriter = HiveWriteUtils.createRecordWriter(target, conf, properties, outputFormatName, session);
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(HiveErrorCode.HIVE_WRITER_CLOSE_ERROR, "Error write empty file to Hive", e);
        }
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return beginInsertUpdateInternal(session, tableHandle, Optional.empty(), HiveACIDWriteType.INSERT);
    }

    @Override
    public HiveInsertTableHandle beginInsert(ConnectorSession session, ConnectorTableHandle tableHandle, boolean isOverwrite)
    {
        return beginInsertUpdateInternal(session, tableHandle, Optional.empty(), HiveACIDWriteType.INSERT_OVERWRITE);
    }

    private HiveInsertTableHandle beginInsertUpdateInternal(ConnectorSession session, ConnectorTableHandle tableHandle,
                                                            Optional<String> partition, HiveACIDWriteType writeType)
    {
        verifyJvmTimeZone();

        HiveIdentity identity = new HiveIdentity(session);
        SchemaTableName tableName = ((HiveTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        verifyStorageFormatForCatalog(table.getStorage().getStorageFormat());

        HiveWriteUtils.checkTableIsWritable(table, writesToNonManagedTablesEnabled, writeType);

        for (Column column : table.getDataColumns()) {
            if (!HiveWriteUtils.isWritableType(column.getType())) {
                throw new PrestoException(NOT_SUPPORTED, String.format("Inserting into Hive table %s with column type %s not supported", tableName, column.getType()));
            }
        }

        List<HiveColumnHandle> handles = hiveColumnHandles(table).stream()
                .filter(columnHandle -> !columnHandle.isHidden())
                .collect(toList());

        if (partition.isPresent() && table.getPartitionColumns().isEmpty()) {
            throw new PrestoException(GENERIC_USER_ERROR, String.format("Table %s not partitioned", tableName));
        }

        HiveStorageFormat tableStorageFormat = extractHiveStorageFormat(table);
        if (tableStorageFormat == HiveStorageFormat.TEXTFILE) {
            if (table.getParameters().containsKey(TEXT_SKIP_HEADER_COUNT_KEY)) {
                throw new PrestoException(NOT_SUPPORTED, format("Inserting into Hive table with %s property not supported", TEXT_SKIP_HEADER_COUNT_KEY));
            }
            if (table.getParameters().containsKey(TEXT_SKIP_FOOTER_COUNT_KEY)) {
                throw new PrestoException(NOT_SUPPORTED, format("Inserting into Hive table with %s property not supported", TEXT_SKIP_FOOTER_COUNT_KEY));
            }
        }

        Optional<WriteIdInfo> writeIdInfo = Optional.empty();
        if (AcidUtils.isTransactionalTable(((HiveTableHandle) tableHandle)
                .getTableParameters().orElseThrow(() -> new IllegalStateException("tableParameters missing")))) {
            Optional<Long> writeId = metastore.getTableWriteId(session, (HiveTableHandle) tableHandle, writeType);
            if (!writeId.isPresent()) {
                throw new IllegalStateException("No validWriteIds present");
            }
            writeIdInfo = Optional.of(new WriteIdInfo(writeId.get(), writeId.get(), 0));
        }

        HiveWriteUtils.OpertionType operationType = HiveWriteUtils.OpertionType.INSERT;
        boolean isInsertExistingPartitionsOverwrite = HiveSessionProperties.getInsertExistingPartitionsBehavior(session) ==
                HiveSessionProperties.InsertExistingPartitionsBehavior.OVERWRITE ? true : false;
        if (isInsertExistingPartitionsOverwrite || writeType == HiveACIDWriteType.INSERT_OVERWRITE) {
            operationType = HiveWriteUtils.OpertionType.INSERT_OVERWRITE;
        }
        LocationHandle locationHandle = locationService.forExistingTable(metastore, session, table, writeIdInfo, operationType);
        HiveInsertTableHandle result = new HiveInsertTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                handles,
                metastore.generatePageSinkMetadata(identity, tableName),
                locationHandle,
                table.getStorage().getBucketProperty(),
                tableStorageFormat,
                HiveSessionProperties.isRespectTableFormat(session) ? tableStorageFormat :
                        HiveSessionProperties.getHiveStorageFormat(session),
                writeType == HiveACIDWriteType.INSERT_OVERWRITE);

        LocationService.WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
        metastore.declareIntentionToWrite(session, writeInfo.getWriteMode(), writeInfo.getWritePath(), tableName);
        return result;
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                           ConnectorInsertTableHandle insertHandle,
                                                           Collection<Slice> fragments,
                                                           Collection<ComputedStatistics> computedStatistics)
    {
        return finishInsertInternal(session, insertHandle, fragments, computedStatistics, null, HiveACIDWriteType.INSERT);
    }

    public Optional<ConnectorOutputMetadata> finishInsert(ConnectorSession session,
                                                           ConnectorInsertTableHandle insertHandle,
                                                           Collection<Slice> fragments,
                                                           Collection<ComputedStatistics> computedStatistics,
                                                           List<PartitionUpdate> partitions)
    {
        return finishInsertInternal(session, insertHandle, fragments, computedStatistics, partitions, HiveACIDWriteType.INSERT);
    }

    private Optional<ConnectorOutputMetadata> finishInsertInternal(ConnectorSession session,
            ConnectorInsertTableHandle insertHandle,
            Collection<Slice> fragments,
            Collection<ComputedStatistics> computedStatistics,
            List<PartitionUpdate> partitions,
            HiveACIDWriteType hiveACIDWriteType)
    {
        HiveInsertTableHandle handle = (HiveInsertTableHandle) insertHandle;

        List<PartitionUpdate> partitionUpdates = fragments.stream()
                .map(Slice::getBytes)
                .map(partitionUpdateCodec::fromJson)
                .sorted(Comparator.comparing(PartitionUpdate::getName)) //sort partition updates to ensure same sequence of rename in case of
                .collect(toList());

        HiveStorageFormat tableStorageFormat = handle.getTableStorageFormat();
        partitionUpdates = PartitionUpdate.mergePartitionUpdates(partitionUpdates);

        Table table = metastore.getTable(new HiveIdentity(session), handle.getSchemaName(), handle.getTableName())
                .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
        if (!table.getStorage().getStorageFormat().getInputFormat().equals(tableStorageFormat.getInputFormat()) && HiveSessionProperties.isRespectTableFormat(session)) {
            throw new PrestoException(HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
        }

        if (handle.getBucketProperty().isPresent() && HiveSessionProperties.isCreateEmptyBucketFiles(session)) {
            List<PartitionUpdate> partitionUpdatesForMissingBuckets = computePartitionUpdatesForMissingBuckets(session, handle, table, partitionUpdates);
            // replace partitionUpdates before creating the empty files so that those files will be cleaned up if we end up rollback
            partitionUpdates = PartitionUpdate.mergePartitionUpdates(concat(partitionUpdates, partitionUpdatesForMissingBuckets));
            for (PartitionUpdate partitionUpdate : partitionUpdatesForMissingBuckets) {
                Optional<Partition> partition = table.getPartitionColumns().isEmpty() ? Optional.empty() : Optional.of(buildPartitionObject(session, table, partitionUpdate));
                createEmptyFiles(session, partitionUpdate.getWritePath(), table, partition, partitionUpdate.getFileNames());
            }
        }

        List<String> partitionedBy = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        Map<String, Type> columnTypes = handle.getInputColumns().stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, column -> column.getHiveType().getType(typeManager)));
        Map<List<String>, ComputedStatistics> partitionComputedStatistics = Statistics.createComputedStatisticsToPartitionMap(computedStatistics, partitionedBy, columnTypes);

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            if (partitionUpdate.getName().isEmpty()) {
                // insert into unpartitioned table
                if (!table.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && HiveSessionProperties.isRespectTableFormat(session)) {
                    throw new PrestoException(HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED, "Table format changed during insert");
                }

                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, ImmutableList.of()));

                if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.OVERWRITE) {
                    finishInsertOverwrite(session, handle, table, partitionUpdate, partitionStatistics);
                }
                else if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.NEW || partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.APPEND) {
                    // insert into unpartitioned table
                    metastore.finishInsertIntoExistingTable(
                            session,
                            handle.getSchemaName(),
                            handle.getTableName(),
                            partitionUpdate.getWritePath(),
                            partitionUpdate.getFileNames(),
                            partitionStatistics,
                            hiveACIDWriteType);
                }
                else {
                    throw new IllegalArgumentException("Unsupported update mode: " + partitionUpdate.getUpdateMode());
                }
            }
            else if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.APPEND) {
                // insert into existing partition
                List<String> partitionValues = toPartitionValues(partitionUpdate.getName());
                PartitionStatistics partitionStatistics = createPartitionStatistics(
                        session,
                        partitionUpdate.getStatistics(),
                        columnTypes,
                        getColumnStatistics(partitionComputedStatistics, partitionValues));
                metastore.finishInsertIntoExistingPartition(
                        session,
                        handle.getSchemaName(),
                        handle.getTableName(),
                        partitionValues,
                        partitionUpdate.getWritePath(),
                        partitionUpdate.getFileNames(),
                        partitionStatistics,
                        hiveACIDWriteType);
            }
            else if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.NEW || partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.OVERWRITE) {
                finishInsertInNewPartition(session, handle, table, columnTypes, partitionUpdate, partitionComputedStatistics, hiveACIDWriteType);
            }
            else {
                throw new IllegalArgumentException(format("Unsupported update mode: %s", partitionUpdate.getUpdateMode()));
            }
        }

        if (partitions != null) {
            partitions.addAll(partitionUpdates);
        }

        return Optional.of(new HiveWrittenPartitions(
                partitionUpdates.stream()
                        .map(PartitionUpdate::getName)
                        .collect(toList())));
    }

    @Override
    public HiveUpdateTableHandle beginUpdate(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveInsertTableHandle insertTableHandle = beginInsertUpdateInternal(session, tableHandle, Optional.empty(), HiveACIDWriteType.UPDATE);
        return new HiveUpdateTableHandle(insertTableHandle.getSchemaName(), insertTableHandle.getTableName(),
                insertTableHandle.getInputColumns(), insertTableHandle.getPageSinkMetadata(),
                insertTableHandle.getLocationHandle(), insertTableHandle.getBucketProperty(),
                insertTableHandle.getTableStorageFormat(), insertTableHandle.getPartitionStorageFormat());
    }

    public HiveDeleteAsInsertTableHandle beginDeletesAsInsert(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveInsertTableHandle insertTableHandle = beginInsertUpdateInternal(session, tableHandle, Optional.empty(), HiveACIDWriteType.DELETE);
        //Delete needs only partitionColumn and bucketing columns data
        List<HiveColumnHandle> inputColumns = insertTableHandle.getInputColumns().stream().filter(HiveColumnHandle::isRequired).collect(toList());
        return new HiveDeleteAsInsertTableHandle(insertTableHandle.getSchemaName(), insertTableHandle.getTableName(),
                inputColumns, insertTableHandle.getPageSinkMetadata(),
                insertTableHandle.getLocationHandle(), insertTableHandle.getBucketProperty(),
                insertTableHandle.getTableStorageFormat(), insertTableHandle.getPartitionStorageFormat());
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishUpdate(ConnectorSession session, ConnectorUpdateTableHandle updateHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveUpdateTableHandle updateTableHandle = (HiveUpdateTableHandle) updateHandle;
        HiveInsertTableHandle insertTableHandle = new HiveInsertTableHandle(updateTableHandle.getSchemaName(), updateTableHandle.getTableName(),
                updateTableHandle.getInputColumns(), updateTableHandle.getPageSinkMetadata(),
                updateTableHandle.getLocationHandle(), updateTableHandle.getBucketProperty(),
                updateTableHandle.getTableStorageFormat(), updateTableHandle.getPartitionStorageFormat(), false);
        return finishInsertInternal(session, insertTableHandle, fragments, computedStatistics, null, HiveACIDWriteType.UPDATE);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishDeleteAsInsert(ConnectorSession session, ConnectorDeleteAsInsertTableHandle deleteHandle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveDeleteAsInsertTableHandle deleteTableHandle = (HiveDeleteAsInsertTableHandle) deleteHandle;
        HiveInsertTableHandle insertTableHandle = new HiveInsertTableHandle(deleteTableHandle.getSchemaName(), deleteTableHandle.getTableName(),
                deleteTableHandle.getInputColumns(), deleteTableHandle.getPageSinkMetadata(),
                deleteTableHandle.getLocationHandle(), deleteTableHandle.getBucketProperty(),
                deleteTableHandle.getTableStorageFormat(), deleteTableHandle.getPartitionStorageFormat(), false);
        return finishInsertInternal(session, insertTableHandle, fragments, computedStatistics, null, HiveACIDWriteType.DELETE);
    }

    @Override
    public ConnectorVacuumTableHandle beginVacuum(ConnectorSession session, ConnectorTableHandle tableHandle, boolean full, boolean unify, Optional<String> partition)
    {
        HiveInsertTableHandle insertTableHandle = beginInsertUpdateInternal(session, tableHandle, partition, unify ? HiveACIDWriteType.VACUUM_UNIFY : HiveACIDWriteType.VACUUM);
        if ((!session.getSource().get().isEmpty()) &&
                session.getSource().get().equals("auto-vacuum")) {
            metastore.setVacuumTableHandle((HiveTableHandle) tableHandle);
        }
        return new HiveVacuumTableHandle(insertTableHandle.getSchemaName(), insertTableHandle.getTableName(),
                insertTableHandle.getInputColumns(), insertTableHandle.getPageSinkMetadata(),
                insertTableHandle.getLocationHandle(), insertTableHandle.getBucketProperty(),
                insertTableHandle.getTableStorageFormat(), insertTableHandle.getPartitionStorageFormat(), full, unify, null);
    }

    @Override
    public Optional<ConnectorOutputMetadata> finishVacuum(ConnectorSession session, ConnectorVacuumTableHandle handle, Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
    {
        HiveVacuumTableHandle vacuumTableHandle = (HiveVacuumTableHandle) handle;
        HiveInsertTableHandle insertTableHandle = new HiveInsertTableHandle(vacuumTableHandle.getSchemaName(), vacuumTableHandle.getTableName(),
                vacuumTableHandle.getInputColumns(), vacuumTableHandle.getPageSinkMetadata(),
                vacuumTableHandle.getLocationHandle(), vacuumTableHandle.getBucketProperty(),
                vacuumTableHandle.getTableStorageFormat(), vacuumTableHandle.getPartitionStorageFormat(), false);
        List<PartitionUpdate> partitionUpdates = new ArrayList<>();
        Optional<ConnectorOutputMetadata> connectorOutputMetadata =
                finishInsertInternal(session, insertTableHandle, fragments, computedStatistics, partitionUpdates,
                        vacuumTableHandle.isUnifyVacuum() ? HiveACIDWriteType.VACUUM_UNIFY : HiveACIDWriteType.VACUUM);

        metastore.initiateVacuumCleanupTasks(vacuumTableHandle, session, partitionUpdates);
        return connectorOutputMetadata;
    }

    protected Partition buildPartitionObject(ConnectorSession session, Table table, PartitionUpdate partitionUpdate)
    {
        return Partition.builder()
                .setDatabaseName(table.getDatabaseName())
                .setTableName(table.getTableName())
                .setColumns(table.getDataColumns())
                .setValues(HivePartitionManager.extractPartitionValues(partitionUpdate.getName()))
                .setParameters(ImmutableMap.<String, String>builder()
                        .put(PRESTO_VERSION_NAME, prestoVersion)
                        .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                        .build())
                .withStorage(storage -> storage
                        .setStorageFormat(HiveSessionProperties.isRespectTableFormat(session) ?
                                table.getStorage().getStorageFormat() :
                                StorageFormat.fromHiveStorageFormat(HiveSessionProperties.getHiveStorageFormat(session)))
                        .setLocation(partitionUpdate.getTargetPath().toString())
                        .setBucketProperty(table.getStorage().getBucketProperty())
                        .setSerdeParameters(table.getStorage().getSerdeParameters()))
                .build();
    }

    private PartitionStatistics createPartitionStatistics(
            ConnectorSession session,
            Map<String, Type> columnTypes,
            ComputedStatistics computedStatistics)
    {
        Map<ColumnStatisticMetadata, Block> computedColumnStatistics = computedStatistics.getColumnStatistics();

        Block rowCountBlock = Optional.ofNullable(computedStatistics.getTableStatistics().get(ROW_COUNT))
                .orElseThrow(() -> new VerifyException("rowCount not present"));
        verify(!rowCountBlock.isNull(0), "rowCount must never be null");
        long rowCount = BIGINT.getLong(rowCountBlock, 0);
        HiveBasicStatistics rowCountOnlyBasicStatistics = new HiveBasicStatistics(OptionalLong.empty(), OptionalLong.of(rowCount), OptionalLong.empty(), OptionalLong.empty());
        return createPartitionStatistics(session, rowCountOnlyBasicStatistics, columnTypes, computedColumnStatistics);
    }

    protected PartitionStatistics createPartitionStatistics(
            ConnectorSession session,
            HiveBasicStatistics basicStatistics,
            Map<String, Type> columnTypes,
            Map<ColumnStatisticMetadata, Block> computedColumnStatistics)
    {
        long rowCount = basicStatistics.getRowCount().orElseThrow(() -> new IllegalArgumentException("rowCount not present"));
        Map<String, HiveColumnStatistics> columnStatistics = Statistics.fromComputedStatistics(
                session,
                timeZone,
                computedColumnStatistics,
                columnTypes,
                rowCount);
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    protected static Map<ColumnStatisticMetadata, Block> getColumnStatistics(Map<List<String>, ComputedStatistics> statistics, List<String> partitionValues)
    {
        return Optional.ofNullable(statistics.get(partitionValues))
                .map(ComputedStatistics::getColumnStatistics)
                .orElse(ImmutableMap.of());
    }

    @Override
    public void createView(ConnectorSession session, SchemaTableName viewName, ConnectorViewDefinition definition, boolean replace)
    {
        HiveIdentity identity = new HiveIdentity(session);
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put(TABLE_COMMENT, "Presto View")
                .put(PRESTO_VIEW_FLAG, "true")
                .put(PRESTO_VERSION_NAME, prestoVersion)
                .put(PRESTO_QUERY_ID_NAME, session.getQueryId())
                .build();

        Column dummyColumn = new Column("dummy", HiveType.HIVE_STRING, Optional.empty());

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(viewName.getSchemaName())
                .setTableName(viewName.getTableName())
                .setOwner(session.getUser())
                .setTableType(TableType.VIRTUAL_VIEW.name())
                .setDataColumns(ImmutableList.of(dummyColumn))
                .setPartitionColumns(ImmutableList.of())
                .setParameters(properties)
                .setViewOriginalText(Optional.of(encodeViewData(definition)))
                .setViewExpandedText(Optional.of("/* Presto View */"));

        tableBuilder.getStorageBuilder()
                .setStorageFormat(StorageFormat.VIEW_STORAGE_FORMAT)
                .setLocation("");
        Table table = tableBuilder.build();
        PrincipalPrivileges principalPrivileges = MetastoreUtil.buildInitialPrivilegeSet(session.getUser());

        Optional<Table> existing = metastore.getTable(identity, viewName.getSchemaName(), viewName.getTableName());
        if (existing.isPresent()) {
            if (!replace || !HiveUtil.isPrestoView(existing.get())) {
                throw new ViewAlreadyExistsException(viewName);
            }

            metastore.replaceView(identity, viewName.getSchemaName(), viewName.getTableName(), table, principalPrivileges);
            return;
        }

        try {
            metastore.createTable(session, table, principalPrivileges, Optional.empty(), false, new PartitionStatistics(HiveBasicStatistics.createEmptyStatistics(), ImmutableMap.of()));
        }
        catch (TableAlreadyExistsException e) {
            throw new ViewAlreadyExistsException(e.getTableName());
        }
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        ConnectorViewDefinition view = getView(session, viewName)
                .orElseThrow(() -> new ViewNotFoundException(viewName));

        try {
            metastore.dropTable(session, viewName.getSchemaName(), viewName.getTableName());
        }
        catch (TableNotFoundException e) {
            throw new ViewNotFoundException(e.getTableName());
        }
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (String schemaName : listSchemas(session, optionalSchemaName)) {
            for (String tableName : metastore.getAllViews(schemaName).orElse(emptyList())) {
                tableNames.add(new SchemaTableName(schemaName, tableName));
            }
        }
        return tableNames.build();
    }

    @Override
    public Optional<ConnectorViewDefinition> getView(ConnectorSession session, SchemaTableName viewName)
    {
        // support presto view and hive view
        return metastore.getTable(new HiveIdentity(session), viewName.getSchemaName(), viewName.getTableName())
                .filter(HiveUtil::isView)
                .map(view -> {
                    ConnectorViewDefinition definition;
                    if (isPrestoView(view)) {
                        definition = processPrestoView(view, viewName);
                    }
                    else {
                        // if type is hive view
                        definition = processHiveView(session, view, viewName);
                    }

                    return definition;
                });
    }

    private ConnectorViewDefinition processPrestoView(Table view, SchemaTableName viewName)
    {
        ConnectorViewDefinition definition = decodeViewData(view.getViewOriginalText()
                .orElseThrow(() -> new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "No view original text: " + viewName)));
        // use owner from table metadata if it exists
        if (view.getOwner() != null && !definition.isRunAsInvoker()) {
            definition = new ConnectorViewDefinition(
                    definition.getOriginalSql(),
                    definition.getCatalog(),
                    definition.getSchema(),
                    definition.getColumns(),
                    Optional.of(view.getOwner()),
                    false);
        }

        return definition;
    }

    // support hive view without no HQL and hive UDF
    private ConnectorViewDefinition processHiveView(ConnectorSession session, Table view, SchemaTableName viewName)
    {
        // if the table has not set the schema,the viewOriginalText has no the schema,so we should read the viewExpandedText
        String hiveViewQuery = view.getViewExpandedText()
                .orElseThrow(() -> new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "No view original text: " + viewName));
        hiveViewQuery = hiveViewQuery.replace('`', '"');
        Optional<String> owner;
        if (view.getOwner() != null) {
            String fullNameOwner = view.getOwner();
            int domainIndex = fullNameOwner.indexOf('@');
            owner = Optional.of((domainIndex < 0) ? fullNameOwner : fullNameOwner.substring(0, domainIndex));
        }
        else {
            owner = Optional.empty();
        }
        // get column type from view
        List<ConnectorViewDefinition.ViewColumn> viewColumns = new ArrayList<>();
        for (Column item : view.getDataColumns()) {
            ConnectorViewDefinition.ViewColumn vc = new ConnectorViewDefinition.ViewColumn(item.getName(), item.getType().getTypeSignature());
            viewColumns.add(vc);
        }
        return new ConnectorViewDefinition(
                hiveViewQuery,
                Optional.of(getCatalogName(session)),
                Optional.of(view.getDatabaseName()),
                viewColumns,
                owner,
                !owner.isPresent());
    }

    private String getCatalogName(ConnectorSession session)
    {
        if (session.getCatalog().isPresent()) {
            return session.getCatalog().get();
        }
        return "hive";
    }

    @Override
    public ConnectorTableHandle beginDelete(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        if (AcidUtils.isInsertOnlyTable(handle.getTableParameters().get())) {
            throw new PrestoException(NOT_SUPPORTED, "Attempt to do delete on table " + handle.getTableName() +
                    " that is insert-only transactional");
        }
        else {
            throw new PrestoException(NOT_SUPPORTED, "This connector only supports delete where one or more partitions" +
                    " are deleted entirely for Non-Transactional tables");
        }
    }

    @Override
    public ColumnHandle getUpdateRowIdColumnHandle(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return HiveColumnHandle.updateRowIdHandle();
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle)
    {
        return Optional.of(handle);
    }

    @Override
    public Optional<ConnectorTableHandle> applyDelete(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) handle;
        if (constraint == null) {
            return Optional.of(handle);
        }
        HiveIdentity identity = new HiveIdentity(session);
        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, identity, handle, constraint);
        HiveTableHandle newHandle = partitionManager.applyPartitionResult(hiveTableHandle, partitionResult);
        return Optional.of(newHandle);
    }

    @Override
    public OptionalLong executeDelete(ConnectorSession session, ConnectorTableHandle deleteHandle)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle handle = (HiveTableHandle) deleteHandle;

        Optional<Table> table = metastore.getTable(identity, handle.getSchemaName(), handle.getTableName());
        if (!table.isPresent()) {
            throw new TableNotFoundException(handle.getSchemaTableName());
        }

        if (table.get().getPartitionColumns().isEmpty()) {
            metastore.truncateUnpartitionedTable(session, handle.getSchemaName(), handle.getTableName());
        }
        else {
            for (HivePartition hivePartition : partitionManager.getOrLoadPartitions(metastore, identity, handle)) {
                metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), toPartitionValues(hivePartition.getPartitionId()));
            }
        }
        // it is too expensive to determine the exact number of deleted rows
        return OptionalLong.empty();
    }

    @VisibleForTesting
    static Predicate<Map<ColumnHandle, NullableValue>> convertToPredicate(TupleDomain<ColumnHandle> tupleDomain)
    {
        return bindings -> tupleDomain.contains(TupleDomain.fromFixedValues(bindings));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle hiveTable = (HiveTableHandle) table;

        List<ColumnHandle> partitionColumns = ImmutableList.copyOf(hiveTable.getPartitionColumns());
        List<HivePartition> partitions = partitionManager.getOrLoadPartitions(metastore, identity, hiveTable);

        TupleDomain<ColumnHandle> predicate = createPredicate(partitionColumns, partitions);

        if (hiveTable.isSuitableToPush()) {
            Table hmsTable = metastore.getTable(identity, hiveTable.getSchemaName(), hiveTable.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(hiveTable.getSchemaTableName()));

            ImmutableMap.Builder<ColumnHandle, Domain> pushedDown = ImmutableMap.builder();
            pushedDown.putAll(hiveTable.getCompactEffectivePredicate().getDomains().get().entrySet().stream()
                    .collect(toMap(e -> (ColumnHandle) e.getKey(), e -> e.getValue())));

            predicate = predicate.intersect(withColumnDomains(pushedDown.build()));
        }

        Optional<DiscretePredicates> discretePredicates = Optional.empty();
        if (!partitionColumns.isEmpty()) {
            // Do not create tuple domains for every partition at the same time!
            // There can be a huge number of partitions so use an iterable so
            // all domains do not need to be in memory at the same time.
            Iterable<TupleDomain<ColumnHandle>> partitionDomains = Iterables.transform(partitions, (hivePartition) -> TupleDomain.fromFixedValues(hivePartition.getKeys()));
            discretePredicates = Optional.of(new DiscretePredicates(partitionColumns, partitionDomains));
        }

        Optional<ConnectorTablePartitioning> tablePartitioning = Optional.empty();
        if (HiveSessionProperties.isBucketExecutionEnabled(session) && hiveTable.getBucketHandle().isPresent()) {
            tablePartitioning = hiveTable.getBucketHandle().map(bucketing -> new ConnectorTablePartitioning(
                    new HivePartitioningHandle(
                            bucketing.getBucketingVersion(),
                            bucketing.getReadBucketCount(),
                            bucketing.getColumns().stream()
                                    .map(HiveColumnHandle::getHiveType)
                                    .collect(toImmutableList()),
                            OptionalInt.empty()),
                    bucketing.getColumns().stream()
                            .map(ColumnHandle.class::cast)
                            .collect(toList())));
        }

        return new ConnectorTableProperties(
                predicate,
                tablePartitioning,
                Optional.empty(),
                discretePredicates,
                ImmutableList.of());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle, Constraint constraint)
    {
        return applyFilter(session, tableHandle, constraint, ImmutableList.of(), ImmutableSet.of(), false);
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle tableHandle,
                                                                                   Constraint constraint, List<Constraint> disjuctConstaints,
                                                                                   Set<ColumnHandle> allColumnHandles,
                                                                                   boolean pushPartitionsOnly)
    {
        HiveIdentity identity = new HiveIdentity(session);
        HiveTableHandle handle = (HiveTableHandle) tableHandle;
        checkArgument(!handle.getAnalyzePartitionValues().isPresent() || constraint.getSummary().isAll(), "Analyze should not have a constraint");

        HivePartitionResult partitionResult = partitionManager.getPartitions(metastore, identity, handle, constraint);

        HiveTableHandle newHandle = partitionManager.applyPartitionResult(handle, partitionResult);

        // the goal here is to pushdown all the constraints/predicates to HivePageSourceProvider
        // in case some pre-filtering can be done using the heuristic-index
        // however, during scheduling we can't be sure a column will have a heuristic-index.
        // therefore, filtering should still be done using the filter operator,
        // hence the unenforced constraints below includes all constraints (minus partitions)
        ImmutableMap.Builder<HiveColumnHandle, Domain> pushedDown = ImmutableMap.builder();
        pushedDown.putAll(partitionResult.getUnenforcedConstraint().getDomains().get().entrySet().stream()
                .collect(toMap(e -> (HiveColumnHandle) e.getKey(), e -> e.getValue())));

        TupleDomain<HiveColumnHandle> newEffectivePredicate = newHandle.getCompactEffectivePredicate()
                .intersect(handle.getCompactEffectivePredicate())
                .intersect(withColumnDomains(pushedDown.build()));

        ImmutableList.Builder<TupleDomain<HiveColumnHandle>> builder = ImmutableList.builder();
        disjuctConstaints.stream().forEach(c -> {
            TupleDomain<HiveColumnHandle> newSubDomain = withColumnDomains(c.getSummary()
                            .getDomains().get().entrySet()
                            .stream().collect(toMap(e -> (HiveColumnHandle) e.getKey(), e -> e.getValue())))
                    .subtract(newEffectivePredicate);
            if (!newSubDomain.isNone()) {
                builder.add(newSubDomain);
            }
        });

        // Get list of all columns involved in predicate
        Set<String> predicateColumnNames = new HashSet<>();
        newEffectivePredicate.getDomains().get().keySet().stream()
                .map(HiveColumnHandle::getColumnName)
                .forEach(predicateColumnNames::add);

        List<TupleDomain<HiveColumnHandle>> newEffectivePredicates = null;
        boolean isSuitableToPush = false;
        if (HiveSessionProperties.isOrcPredicatePushdownEnabled(session)) {
            isSuitableToPush = checkIfSuitableToPush(allColumnHandles, tableHandle, session);
        }

        if (isSuitableToPush && HiveSessionProperties.isOrcDisjunctPredicatePushdownEnabled(session)) {
            newEffectivePredicates = builder.build();

            newEffectivePredicates.stream().forEach(nfp ->
                    nfp.getDomains().get().keySet().stream()
                            .map(HiveColumnHandle::getColumnName)
                            .forEach(predicateColumnNames::add));
        }

        if (isSuitableToPush
                    && partitionResult.getEnforcedConstraint().equals(newEffectivePredicate)
                    && (newEffectivePredicates == null || newEffectivePredicates.size() == 0)) {
            isSuitableToPush = false;
        }

        // Get column handle
        Map<String, ColumnHandle> columnHandles = getColumnHandles(session, handle);

        // map predicate columns to hive column handles
        Map<String, HiveColumnHandle> predicateColumns = predicateColumnNames.stream()
                .map(columnHandles::get)
                .map(HiveColumnHandle.class::cast)
                .filter(HiveColumnHandle::isRegular)
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));

        newHandle = new HiveTableHandle(
                newHandle.getSchemaName(),
                newHandle.getTableName(),
                newHandle.getTableParameters(),
                newHandle.getPartitionColumns(),
                newHandle.getPartitions(),
                newEffectivePredicate,
                newHandle.getEnforcedConstraint(),
                newHandle.getBucketHandle(),
                newHandle.getBucketFilter(),
                newHandle.getAnalyzePartitionValues(),
                predicateColumns,
                Optional.ofNullable(newEffectivePredicates),
                isSuitableToPush);

        if (pushPartitionsOnly && handle.getPartitions().equals(newHandle.getPartitions()) &&
                handle.getCompactEffectivePredicate().equals(newHandle.getCompactEffectivePredicate()) &&
                handle.getBucketFilter().equals(newHandle.getBucketFilter())) {
            return Optional.empty();
        }

        if (!pushPartitionsOnly && isSuitableToPush) {
            Table table = metastore.getTable(identity, handle.getSchemaName(), handle.getTableName())
                    .orElseThrow(() -> new TableNotFoundException(handle.getSchemaTableName()));
            return Optional.of(new ConstraintApplicationResult<>(newHandle, TupleDomain.all()));
        }

        // note here that all unenforced constraints will still be applied using the filter operator
        return Optional.of(new ConstraintApplicationResult<>(newHandle, partitionResult.getUnenforcedConstraint()));
    }

    /**
     * This function will be called only user enabled pushdown (i.e. orc_predicate_pushdown_enabled=true).
     * Then further check if pushdown can be supported by connector. It support iff below all condition satisfies.
     * 1. Storage Format should be only ORC.
     * 2. Table to be scanned is not transactional table (so effectively DELETE/UPDATE also not supported).
     * 3. Also columns part of the scan are of any primitive data-type except byte.
     * NOTE: This should be adjusted as we continue to support additional functionality.
     * @param allColumnHandles set of all column handles being part of scan.
     * @param tableHandle table handle
     * @param session current session handler
     * @return return true if current scan can support pushdown otherwise false.
     */
    protected boolean checkIfSuitableToPush(Set<ColumnHandle> allColumnHandles, ConnectorTableHandle tableHandle, ConnectorSession session)
    {
        // We allow predicate pushdown only for non-transaction table of HIVE ORC storage format.
        if (getHiveStorageFormat(getTableMetadata(session, tableHandle).getProperties()) != ORC
                || getTransactionalValue(getTableMetadata(session, tableHandle).getProperties())) {
            return false;
        }

        for (ColumnHandle handle : allColumnHandles) {
            HiveColumnHandle hiveColumnHandle = (HiveColumnHandle) handle;
            // Non-primitive data type(e.g. STRUCT, MAP, LIST) and BYTE are not supported to pushdown.
            // UPDATE/DELETE which has explicit column $rowId of STRUCT Type, will be not allowed to pushdown.
            // NOTE: Incase STRUCT type supported, support of UPDATE/DELETE should be checked or should be handled here.
            if (hiveColumnHandle.getHiveType().getCategory().equals(PRIMITIVE) == false
                    || hiveColumnHandle.getHiveType().equals(HiveType.HIVE_BYTE)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public Optional<ConnectorPartitioningHandle> getCommonPartitioningHandle(ConnectorSession session, ConnectorPartitioningHandle left, ConnectorPartitioningHandle right)
    {
        HivePartitioningHandle leftHandle = (HivePartitioningHandle) left;
        HivePartitioningHandle rightHandle = (HivePartitioningHandle) right;

        if (!leftHandle.getHiveTypes().equals(rightHandle.getHiveTypes())) {
            return Optional.empty();
        }
        if (leftHandle.getBucketingVersion() != rightHandle.getBucketingVersion()) {
            return Optional.empty();
        }
        if (leftHandle.getBucketCount() == rightHandle.getBucketCount()) {
            return Optional.of(leftHandle);
        }
        if (!HiveSessionProperties.isOptimizedMismatchedBucketCount(session)) {
            return Optional.empty();
        }

        int largerBucketCount = Math.max(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        int smallerBucketCount = Math.min(leftHandle.getBucketCount(), rightHandle.getBucketCount());
        if (largerBucketCount % smallerBucketCount != 0) {
            // must be evenly divisible
            return Optional.empty();
        }
        if (Integer.bitCount(largerBucketCount / smallerBucketCount) != 1) {
            // ratio must be power of two
            return Optional.empty();
        }

        OptionalInt maxCompatibleBucketCount = min(leftHandle.getMaxCompatibleBucketCount(), rightHandle.getMaxCompatibleBucketCount());
        if (maxCompatibleBucketCount.isPresent() && maxCompatibleBucketCount.getAsInt() < smallerBucketCount) {
            // maxCompatibleBucketCount must be larger than or equal to smallerBucketCount
            // because the current code uses the smallerBucketCount as the common partitioning handle.
            return Optional.empty();
        }

        return Optional.of(new HivePartitioningHandle(
                leftHandle.getBucketingVersion(), // same as rightHandle.getBucketingVersion()
                smallerBucketCount,
                leftHandle.getHiveTypes(),
                maxCompatibleBucketCount));
    }

    private static OptionalInt min(OptionalInt left, OptionalInt right)
    {
        if (!left.isPresent()) {
            return right;
        }
        if (!right.isPresent()) {
            return left;
        }
        return OptionalInt.of(Math.min(left.getAsInt(), right.getAsInt()));
    }

    @Override
    public ConnectorTableHandle makeCompatiblePartitioning(ConnectorSession session, ConnectorTableHandle tableHandle, ConnectorPartitioningHandle partitioningHandle)
    {
        HiveTableHandle hiveTable = (HiveTableHandle) tableHandle;
        HivePartitioningHandle hivePartitioningHandle = (HivePartitioningHandle) partitioningHandle;

        checkArgument(hiveTable.getBucketHandle().isPresent(), "Hive connector only provides alternative layout for bucketed table");
        HiveBucketHandle bucketHandle = hiveTable.getBucketHandle().get();
        ImmutableList<HiveType> bucketTypes = bucketHandle.getColumns().stream().map(HiveColumnHandle::getHiveType).collect(toImmutableList());
        checkArgument(
                hivePartitioningHandle.getHiveTypes().equals(bucketTypes),
                "Types from the new PartitioningHandle (%s) does not match the TableHandle (%s)",
                hivePartitioningHandle.getHiveTypes(),
                bucketTypes);
        int largerBucketCount = Math.max(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        int smallerBucketCount = Math.min(bucketHandle.getTableBucketCount(), hivePartitioningHandle.getBucketCount());
        checkArgument(
                largerBucketCount % smallerBucketCount == 0 && Integer.bitCount(largerBucketCount / smallerBucketCount) == 1,
                "The requested partitioning is not a valid alternative for the table layout");

        return new HiveTableHandle(
                hiveTable.getSchemaName(),
                hiveTable.getTableName(),
                hiveTable.getTableParameters(),
                hiveTable.getPartitionColumns(),
                hiveTable.getPartitions(),
                hiveTable.getCompactEffectivePredicate(),
                hiveTable.getEnforcedConstraint(),
                Optional.of(new HiveBucketHandle(
                        bucketHandle.getColumns(),
                        bucketHandle.getBucketingVersion(),
                        bucketHandle.getTableBucketCount(),
                        hivePartitioningHandle.getBucketCount())),
                hiveTable.getBucketFilter(),
                hiveTable.getAnalyzePartitionValues(),
                hiveTable.getPredicateColumns(),
                hiveTable.getDisjunctCompactEffectivePredicate(),
                hiveTable.isSuitableToPush());
    }

    @VisibleForTesting
    static TupleDomain<ColumnHandle> createPredicate(List<ColumnHandle> partitionColumns, List<HivePartition> partitions)
    {
        if (partitions.isEmpty()) {
            return TupleDomain.none();
        }

        return withColumnDomains(
                partitionColumns.stream()
                        .collect(toMap(identity(), column -> buildColumnDomain(column, partitions))));
    }

    private static Domain buildColumnDomain(ColumnHandle column, List<HivePartition> partitions)
    {
        checkArgument(!partitions.isEmpty(), "partitions cannot be empty");

        boolean hasNull = false;
        List<Object> nonNullValues = new ArrayList<>();
        Type type = null;

        for (HivePartition partition : partitions) {
            NullableValue value = partition.getKeys().get(column);
            if (value == null) {
                throw new PrestoException(HiveErrorCode.HIVE_UNKNOWN_ERROR, format("Partition %s does not have a value for partition column %s", partition, column));
            }

            if (value.isNull()) {
                hasNull = true;
            }
            else {
                nonNullValues.add(value.getValue());
            }

            if (type == null) {
                type = value.getType();
            }
        }

        if (!nonNullValues.isEmpty()) {
            Domain domain = Domain.multipleValues(type, nonNullValues);
            if (hasNull) {
                return domain.union(Domain.onlyNull(type));
            }

            return domain;
        }

        return Domain.onlyNull(type);
    }

    @Override
    public Optional<ConnectorNewTableLayout> getInsertLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        return getInsertTableLayoutInternal(session, table);
    }

    private Optional<ConnectorNewTableLayout> getInsertTableLayoutInternal(ConnectorSession session, Table table)
    {
        if (table.getStorage().getBucketProperty().isPresent()) {
            if (containsTimestampBucketedV2(table.getStorage().getBucketProperty().get(), table)) {
                throw new PrestoException(NOT_SUPPORTED, "Table bucketing version not supported for writing when bucketing on timestamp type");
            }
        }

        Optional<HiveBucketHandle> hiveBucketHandle = HiveBucketing.getHiveBucketHandle(table);
        if (!hiveBucketHandle.isPresent()) {
            // return preferred layout which is partitioned by partition columns
            List<Column> partitionColumns = table.getPartitionColumns();
            if (partitionColumns.isEmpty() || !HiveSessionProperties.isWritePartitionDistributionEnabled(session)) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorNewTableLayout(
                    partitionColumns.stream()
                            .map(Column::getName)
                            .collect(toImmutableList())));
        }
        HiveBucketProperty bucketProperty = table.getStorage().getBucketProperty()
                .orElseThrow(() -> new NoSuchElementException("Bucket property should be set"));
        if (!bucketProperty.getSortedBy().isEmpty() && !HiveSessionProperties.isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                hiveBucketHandle.get().getBucketingVersion(),
                hiveBucketHandle.get().getTableBucketCount(),
                hiveBucketHandle.get().getColumns().stream()
                        .map(HiveColumnHandle::getHiveType)
                        .collect(toList()),
                OptionalInt.of(hiveBucketHandle.get().getTableBucketCount()));
        List<String> partitionColumns = hiveBucketHandle.get().getColumns().stream()
                .map(HiveColumnHandle::getName)
                .collect(toList());
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumns));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getUpdateLayout(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
        SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));

        //When bucketing is not enabled, do the bucketing based on the RowId handle for update/delete
        //In case of partitioned table, include the partition columns also for the parallel partitioning of writes.
        List<Column> tablePartitionColumns = table.getPartitionColumns();
        List<String> partitionColumnNames = new ArrayList<>();
        List<HiveType> partitionColumnTypes = new ArrayList<>();
        tablePartitionColumns.forEach(column -> {
            partitionColumnNames.add(column.getName());
            partitionColumnTypes.add(column.getType());
        });
        partitionColumnNames.add(HiveColumnHandle.UPDATE_ROW_ID_COLUMN_NAME.toLowerCase(ENGLISH));
        partitionColumnTypes.add(HiveColumnHandle.updateRowIdHandle().getHiveType());
        HivePartitioningHandle partitioningHandle = new HivePartitioningHandle(
                BucketingVersion.BUCKETING_V2,
                HiveBucketing.MAX_BUCKET_NUMBER,
                partitionColumnTypes,
                OptionalInt.empty(),
                true);
        return Optional.of(new ConnectorNewTableLayout(partitioningHandle, partitionColumnNames));
    }

    @Override
    public Optional<ConnectorNewTableLayout> getNewTableLayout(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateCsvColumns(tableMetadata);
        Optional<HiveBucketProperty> bucketProperty = HiveTableProperties.getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            // return preferred layout which is partitioned by partition columns
            List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());
            if (partitionedBy.isEmpty() || !HiveSessionProperties.isWritePartitionDistributionEnabled(session)) {
                return Optional.empty();
            }

            return Optional.of(new ConnectorNewTableLayout(partitionedBy));
        }
        if (!bucketProperty.get().getSortedBy().isEmpty() && !HiveSessionProperties.isSortedWritingEnabled(session)) {
            throw new PrestoException(NOT_SUPPORTED, "Writing to bucketed sorted Hive tables is disabled");
        }

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        Map<String, HiveType> hiveTypeMap = tableMetadata.getColumns().stream()
                .collect(toMap(ColumnMetadata::getName, column -> HiveType.toHiveType(typeTranslator, column.getType())));
        return Optional.of(new ConnectorNewTableLayout(
                new HivePartitioningHandle(
                        bucketProperty.get().getBucketingVersion(),
                        bucketProperty.get().getBucketCount(),
                        bucketedBy.stream()
                                .map(hiveTypeMap::get)
                                .collect(toList()),
                        OptionalInt.of(bucketProperty.get().getBucketCount())),
                bucketedBy));
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadataForWrite(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        if (!HiveSessionProperties.isCollectColumnStatisticsOnWrite(session)) {
            return TableStatisticsMetadata.empty();
        }
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, false);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = firstNonNull(getPartitionedBy(tableMetadata.getProperties()), ImmutableList.of());
        return getStatisticsCollectionMetadata(tableMetadata.getColumns(), partitionedBy, true);
    }

    private TableStatisticsMetadata getStatisticsCollectionMetadata(List<ColumnMetadata> columns, List<String> partitionedBy, boolean includeRowCount)
    {
        Set<ColumnStatisticMetadata> columnStatistics = columns.stream()
                .filter(column -> !partitionedBy.contains(column.getName()))
                .filter(column -> !column.isHidden())
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        Set<TableStatisticType> tableStatistics = includeRowCount ? ImmutableSet.of(ROW_COUNT) : ImmutableSet.of();
        return new TableStatisticsMetadata(columnStatistics, tableStatistics, partitionedBy);
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        return getColumnStatisticMetadata(columnMetadata.getName(), metastore.getSupportedColumnStatistics(columnMetadata.getType()));
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(String columnName, Set<ColumnStatisticType> statisticTypes)
    {
        return statisticTypes.stream()
                .map(type -> new ColumnStatisticMetadata(columnName, type))
                .collect(toImmutableList());
    }

    @Override
    public void createRole(ConnectorSession session, String role, Optional<PrestoPrincipal> grantor)
    {
        accessControlMetadata.createRole(session, role, grantor.map(HivePrincipal::from));
    }

    @Override
    public void dropRole(ConnectorSession session, String role)
    {
        accessControlMetadata.dropRole(session, role);
    }

    @Override
    public Set<String> listRoles(ConnectorSession session)
    {
        return accessControlMetadata.listRoles(session);
    }

    @Override
    public Set<RoleGrant> listRoleGrants(ConnectorSession session, PrestoPrincipal principal)
    {
        return ImmutableSet.copyOf(accessControlMetadata.listRoleGrants(session, HivePrincipal.from(principal)));
    }

    @Override
    public void grantRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean withAdminOption, Optional<PrestoPrincipal> grantor)
    {
        accessControlMetadata.grantRoles(session, roles, HivePrincipal.from(grantees), withAdminOption, grantor.map(HivePrincipal::from));
    }

    @Override
    public void revokeRoles(ConnectorSession session, Set<String> roles, Set<PrestoPrincipal> grantees, boolean adminOptionFor, Optional<PrestoPrincipal> grantor)
    {
        accessControlMetadata.revokeRoles(session, roles, HivePrincipal.from(grantees), adminOptionFor, grantor.map(HivePrincipal::from));
    }

    @Override
    public Set<RoleGrant> listApplicableRoles(ConnectorSession session, PrestoPrincipal principal)
    {
        return accessControlMetadata.listApplicableRoles(session, HivePrincipal.from(principal));
    }

    @Override
    public Set<String> listEnabledRoles(ConnectorSession session)
    {
        return accessControlMetadata.listEnabledRoles(session);
    }

    @Override
    public void grantTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.grantTablePrivileges(session, schemaTableName, privileges, HivePrincipal.from(grantee), grantOption);
    }

    @Override
    public void revokeTablePrivileges(ConnectorSession session, SchemaTableName schemaTableName, Set<Privilege> privileges, PrestoPrincipal grantee, boolean grantOption)
    {
        accessControlMetadata.revokeTablePrivileges(session, schemaTableName, privileges, HivePrincipal.from(grantee), grantOption);
    }

    @Override
    public List<GrantInfo> listTablePrivileges(ConnectorSession session, SchemaTablePrefix schemaTablePrefix)
    {
        return accessControlMetadata.listTablePrivileges(session, listTables(session, schemaTablePrefix));
    }

    protected void verifyJvmTimeZone()
    {
        if (!allowCorruptWritesForTesting && !timeZone.equals(DateTimeZone.getDefault())) {
            throw new PrestoException(HiveErrorCode.HIVE_TIMEZONE_MISMATCH, format(
                    "To write Hive data, your JVM timezone must match the Hive storage timezone. Add -Duser.timezone=%s to your JVM arguments.",
                    timeZone.getID()));
        }
    }

    public static HiveStorageFormat extractHiveStorageFormat(Table table)
    {
        StorageFormat storageFormat = table.getStorage().getStorageFormat();
        String outputFormat = storageFormat.getOutputFormat();
        String serde = storageFormat.getSerDe();
        for (HiveStorageFormat format : HiveStorageFormat.values()) {
            if (format.getOutputFormat().equals(outputFormat) && format.getSerDe().equals(serde)) {
                return format;
            }
        }
        throw new PrestoException(HiveErrorCode.HIVE_UNSUPPORTED_FORMAT, format("Output format %s with SerDe %s is not supported", outputFormat, serde));
    }

    protected void validateBucketColumns(ConnectorTableMetadata tableMetadata)
    {
        Optional<HiveBucketProperty> bucketProperty = HiveTableProperties.getBucketProperty(tableMetadata.getProperties());
        if (!bucketProperty.isPresent()) {
            return;
        }
        Set<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toSet());

        List<String> bucketedBy = bucketProperty.get().getBucketedBy();
        if (!allColumns.containsAll(bucketedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Bucketing columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(bucketedBy), ImmutableSet.copyOf(allColumns))));
        }

        List<String> sortedBy = bucketProperty.get().getSortedBy().stream()
                .map(SortingColumn::getColumnName)
                .collect(toImmutableList());
        if (!allColumns.containsAll(sortedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Sorting columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(sortedBy), ImmutableSet.copyOf(allColumns))));
        }
    }

    private static void validatePartitionColumns(ConnectorTableMetadata tableMetadata)
    {
        List<String> partitionedBy = getPartitionedBy(tableMetadata.getProperties());

        List<String> allColumns = tableMetadata.getColumns().stream()
                .map(ColumnMetadata::getName)
                .collect(toList());

        if (!allColumns.containsAll(partitionedBy)) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, format("Partition columns %s not present in schema", Sets.difference(ImmutableSet.copyOf(partitionedBy), ImmutableSet.copyOf(allColumns))));
        }

        if (allColumns.size() == partitionedBy.size()) {
            throw new PrestoException(INVALID_TABLE_PROPERTY, "Table contains only partition columns");
        }

        if (!allColumns.subList(allColumns.size() - partitionedBy.size(), allColumns.size()).equals(partitionedBy)) {
            throw new PrestoException(HiveErrorCode.HIVE_COLUMN_ORDER_MISMATCH, "Partition keys must be the last columns in the table and in the same order as the table properties: " + partitionedBy);
        }
    }

    protected List<HiveColumnHandle> getColumnHandles(ConnectorTableMetadata tableMetadata, Set<String> partitionColumnNames, TypeTranslator typeTranslator)
    {
        validatePartitionColumns(tableMetadata);
        validateBucketColumns(tableMetadata);
        validateCsvColumns(tableMetadata);

        ImmutableList.Builder<HiveColumnHandle> columnHandles = ImmutableList.builder();
        int ordinal = 0;
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            HiveColumnHandle.ColumnType columnType;
            if (partitionColumnNames.contains(column.getName())) {
                columnType = HiveColumnHandle.ColumnType.PARTITION_KEY;
            }
            else if (column.isHidden()) {
                columnType = HiveColumnHandle.ColumnType.SYNTHESIZED;
            }
            else {
                columnType = HiveColumnHandle.ColumnType.REGULAR;
            }
            columnHandles.add(new HiveColumnHandle(
                    column.getName(),
                    HiveType.toHiveType(typeTranslator, column.getType()),
                    column.getType().getTypeSignature(),
                    ordinal,
                    columnType,
                    Optional.ofNullable(column.getComment())));
            ordinal++;
        }

        return columnHandles.build();
    }

    protected void validateCsvColumns(ConnectorTableMetadata tableMetadata)
    {
        if (HiveTableProperties.getHiveStorageFormat(tableMetadata.getProperties()) != HiveStorageFormat.CSV) {
            return;
        }

        Set<String> partitionedBy = ImmutableSet.copyOf(getPartitionedBy(tableMetadata.getProperties()));
        List<ColumnMetadata> unsupportedColumns = tableMetadata.getColumns().stream()
                .filter(columnMetadata -> !partitionedBy.contains(columnMetadata.getName()))
                .filter(columnMetadata -> !columnMetadata.getType().equals(createUnboundedVarcharType()))
                .collect(toImmutableList());

        if (!unsupportedColumns.isEmpty()) {
            String joinedUnsupportedColumns = unsupportedColumns.stream()
                    .map(columnMetadata -> format("%s %s", columnMetadata.getName(), columnMetadata.getType()))
                    .collect(joining(", "));
            throw new PrestoException(NOT_SUPPORTED, "Hive CSV storage format only supports VARCHAR (unbounded). Unsupported columns: " + joinedUnsupportedColumns);
        }
    }

    protected static Function<HiveColumnHandle, ColumnMetadata> columnMetadataGetter(Table table, TypeManager typeManager)
    {
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        table.getPartitionColumns().stream().map(Column::getName).forEach(columnNames::add);
        table.getDataColumns().stream().map(Column::getName).forEach(columnNames::add);
        List<String> allColumnNames = columnNames.build();
        if (allColumnNames.size() > Sets.newHashSet(allColumnNames).size()) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA,
                    format("Hive metadata for table %s is invalid: Table descriptor contains duplicate columns", table.getTableName()));
        }

        List<Column> tableColumns = table.getDataColumns();
        ImmutableMap.Builder<String, Optional<String>> builder = ImmutableMap.builder();
        for (Column field : concat(tableColumns, table.getPartitionColumns())) {
            if (field.getComment().isPresent() && !field.getComment().get().equals("from deserializer")) {
                builder.put(field.getName(), field.getComment());
            }
            else {
                builder.put(field.getName(), Optional.empty());
            }
        }
        // add hidden columns
        builder.put(HiveColumnHandle.PATH_COLUMN_NAME, Optional.empty());
        if (table.getStorage().getBucketProperty().isPresent()) {
            builder.put(HiveColumnHandle.BUCKET_COLUMN_NAME, Optional.empty());
        }

        Map<String, Optional<String>> columnComment = builder.build();

        return handle -> new ColumnMetadata(
                handle.getName(),
                typeManager.getType(handle.getTypeSignature()),
                true,
                columnComment.get(handle.getName()).orElse(null),
                columnExtraInfo(handle.isPartitionKey()),
                handle.isHidden(),
                emptyMap(),
                handle.isRequired());
    }

    @Override
    public void rollback()
    {
        metastore.rollback();
    }

    @Override
    public void commit()
    {
        metastore.commit();
        metastore.submitCleanupTasks();
    }

    @Override
    public void beginQuery(ConnectorSession session)
    {
        metastore.beginQuery(session);
    }

    @Override
    public void cleanupQuery(ConnectorSession session)
    {
        metastore.cleanupQuery(session);
    }

    public static Optional<SchemaTableName> getSourceTableNameFromSystemTable(SchemaTableName tableName)
    {
        return Stream.of(SystemTableHandler.values())
                .filter(handler -> handler.matches(tableName))
                .map(handler -> handler.getSourceTableName(tableName))
                .findAny();
    }

    private static SystemTable createSystemTable(ConnectorTableMetadata metadata, Function<TupleDomain<Integer>, RecordCursor> cursor)
    {
        return new SystemTable()
        {
            @Override
            public Distribution getDistribution()
            {
                return Distribution.SINGLE_COORDINATOR;
            }

            @Override
            public ConnectorTableMetadata getTableMetadata()
            {
                return metadata;
            }

            @Override
            public RecordCursor cursor(ConnectorTransactionHandle transactionHandle, ConnectorSession session, TupleDomain<Integer> constraint)
            {
                return cursor.apply(constraint);
            }
        };
    }

    private enum SystemTableHandler
    {
        PARTITIONS, PROPERTIES;

        private final String suffix;

        SystemTableHandler()
        {
            this.suffix = "$" + name().toLowerCase(ENGLISH);
        }

        boolean matches(SchemaTableName table)
        {
            return table.getTableName().endsWith(suffix) &&
                    (table.getTableName().length() > suffix.length());
        }

        SchemaTableName getSourceTableName(SchemaTableName table)
        {
            return new SchemaTableName(
                    table.getSchemaName(),
                    table.getTableName().substring(0, table.getTableName().length() - suffix.length()));
        }
    }

    private static <T> Optional<T> firstNonNullable(T... values)
    {
        for (T value : values) {
            if (value != null) {
                return Optional.of(value);
            }
        }
        return Optional.empty();
    }

    /**
     * Presto can only cache execution plans for supported connectors.
     * This method overrides {@link ConnectorMetadata} returns true to indicate
     * execution plan caching is enabled for Hive connectors.
     *
     * @param session Presto session
     * @param handle Connector specific table handle
     */
    @Override
    public boolean isExecutionPlanCacheSupported(ConnectorSession session, ConnectorTableHandle handle)
    {
        return true;
    }

    protected void finishInsertOverwrite(ConnectorSession session, HiveInsertTableHandle handle, Table table, PartitionUpdate partitionUpdate, PartitionStatistics partitionStatistics)
    {
        PrincipalPrivileges principalPrivileges = PrincipalPrivileges.fromHivePrivilegeInfos(metastore.listTablePrivileges(handle.getSchemaName(), handle.getTableName(), null));
        // first drop it
        metastore.dropTable(session, handle.getSchemaName(), handle.getTableName());

        // create the table with the new location
        metastore.createTable(session, table, principalPrivileges, Optional.of(partitionUpdate.getWritePath()), false, partitionStatistics);
    }

    protected void finishInsertInNewPartition(ConnectorSession session, HiveInsertTableHandle handle, Table table, Map<String, Type> columnTypes, PartitionUpdate partitionUpdate, Map<List<String>, ComputedStatistics> partitionComputedStatistics, HiveACIDWriteType acidWriteType)
    {
        // insert into new partition or overwrite existing partition
        Partition partition = buildPartitionObject(session, table, partitionUpdate);
        if (!partition.getStorage().getStorageFormat().getInputFormat().equals(handle.getPartitionStorageFormat().getInputFormat()) && HiveSessionProperties.isRespectTableFormat(session)) {
            throw new PrestoException(HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED, "Partition format changed during insert");
        }
        if (partitionUpdate.getUpdateMode() == PartitionUpdate.UpdateMode.OVERWRITE) {
            metastore.dropPartition(session, handle.getSchemaName(), handle.getTableName(), partition.getValues());
        }
        PartitionStatistics partitionStatistics = createPartitionStatistics(
                session,
                partitionUpdate.getStatistics(),
                columnTypes,
                getColumnStatistics(partitionComputedStatistics, partition.getValues()));
        metastore.addPartition(session, handle.getSchemaName(), handle.getTableName(), partition, partitionUpdate.getWritePath(), partitionStatistics, acidWriteType);
    }

    public void setExternalTable(boolean externalTable)
    {
        this.externalTable = externalTable;
    }

    protected void verifyStorageFormatForCatalog(StorageFormat storageFormat)
    {
        requireNonNull(storageFormat, "Storage format is null");

        if (storageFormat.getInputFormat().contains("CarbonInputFormat")) {
            String sf = storageFormat.getInputFormat();
            throw new PrestoException(NOT_SUPPORTED,
                    String.format("Tables with %s are not supported by Hive connector", sf.substring(sf.lastIndexOf(".") + 1)));
        }
    }

    @Override
    public List<ConnectorVacuumTableInfo> getTablesForVacuum()
    {
        if (autoVacuumEnabled) {
            return VacuumEligibleTableCollector.getVacuumTableList(metastore, hdfsEnvironment,
                    vacuumDeltaNumThreshold, vacuumDeltaPercentThreshold, vacuumExecutorService, vacuumCollectorInterval);
        }
        return null;
    }
}
