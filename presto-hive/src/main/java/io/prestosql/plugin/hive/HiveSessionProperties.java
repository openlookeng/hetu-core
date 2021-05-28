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
import io.airlift.units.DataSize;
import io.prestosql.orc.OrcWriteValidation.OrcWriteValidationMode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.session.PropertyMetadata;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.APPEND;
import static io.prestosql.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior.ERROR;
import static io.prestosql.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.dataSizeProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public final class HiveSessionProperties
{
    private static final String BUCKET_EXECUTION_ENABLED = "bucket_execution_enabled";
    private static final String FORCE_LOCAL_SCHEDULING = "force_local_scheduling";
    private static final String INSERT_EXISTING_PARTITIONS_BEHAVIOR = "insert_existing_partitions_behavior";
    private static final String ORC_BLOOM_FILTERS_ENABLED = "orc_bloom_filters_enabled";
    private static final String ORC_MAX_MERGE_DISTANCE = "orc_max_merge_distance";
    private static final String ORC_MAX_BUFFER_SIZE = "orc_max_buffer_size";
    private static final String ORC_STREAM_BUFFER_SIZE = "orc_stream_buffer_size";
    private static final String ORC_TINY_STRIPE_THRESHOLD = "orc_tiny_stripe_threshold";
    private static final String ORC_MAX_READ_BLOCK_SIZE = "orc_max_read_block_size";
    private static final String ORC_LAZY_READ_SMALL_RANGES = "orc_lazy_read_small_ranges";
    private static final String ORC_NESTED_LAZY_ENABLED = "orc_nested_lazy_enabled";
    private static final String ORC_STRING_STATISTICS_LIMIT = "orc_string_statistics_limit";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE = "orc_optimized_writer_validate";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE = "orc_optimized_writer_validate_percentage";
    private static final String ORC_OPTIMIZED_WRITER_VALIDATE_MODE = "orc_optimized_writer_validate_mode";
    private static final String ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE = "orc_optimized_writer_min_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE = "orc_optimized_writer_max_stripe_size";
    private static final String ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS = "orc_optimized_writer_max_stripe_rows";
    private static final String ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY = "orc_optimized_writer_max_dictionary_memory";
    private static final String ORC_FILE_TAIL_CACHE_ENABLED = "orc_file_tail_cache_enabled";
    private static final String ORC_STRIPE_FOOTER_CACHE_ENABLED = "orc_stripe_footer_cache_enabled";
    private static final String ORC_ROW_INDEX_CACHE_ENABLED = "orc_row_index_cache_enabled";
    private static final String ORC_BLOOM_FILTERS_CACHE_ENABLED = "orc_bloom_filters_cache_enabled";
    private static final String ORC_ROW_DATA_CACHE_ENABLED = "orc_row_data_cache_enabled";
    private static final String HIVE_STORAGE_FORMAT = "hive_storage_format";
    private static final String RESPECT_TABLE_FORMAT = "respect_table_format";
    private static final String CREATE_EMPTY_BUCKET_FILES = "create_empty_bucket_files";
    private static final String PARQUET_USE_COLUMN_NAME = "parquet_use_column_names";
    private static final String PARQUET_FAIL_WITH_CORRUPTED_STATISTICS = "parquet_fail_with_corrupted_statistics";
    private static final String PARQUET_MAX_READ_BLOCK_SIZE = "parquet_max_read_block_size";
    private static final String PARQUET_WRITER_BLOCK_SIZE = "parquet_writer_block_size";
    private static final String PARQUET_WRITER_PAGE_SIZE = "parquet_writer_page_size";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String MAX_INITIAL_SPLIT_SIZE = "max_initial_split_size";
    private static final String RCFILE_OPTIMIZED_WRITER_VALIDATE = "rcfile_optimized_writer_validate";
    private static final String SORTED_WRITING_ENABLED = "sorted_writing_enabled";
    private static final String STATISTICS_ENABLED = "statistics_enabled";
    private static final String PARTITION_STATISTICS_SAMPLE_SIZE = "partition_statistics_sample_size";
    private static final String IGNORE_CORRUPTED_STATISTICS = "ignore_corrupted_statistics";
    private static final String COLLECT_COLUMN_STATISTICS_ON_WRITE = "collect_column_statistics_on_write";
    private static final String OPTIMIZE_MISMATCHED_BUCKET_COUNT = "optimize_mismatched_bucket_count";
    private static final String S3_SELECT_PUSHDOWN_ENABLED = "s3_select_pushdown_enabled";
    private static final String TEMPORARY_STAGING_DIRECTORY_ENABLED = "temporary_staging_directory_enabled";
    private static final String TEMPORARY_STAGING_DIRECTORY_PATH = "temporary_staging_directory_path";
    private static final String DYNAMIC_FILTERING_SPLIT_FILTERING = "dynamic_filtering_partition_filtering";
    private static final String DYNAMIC_FILTERING_ROW_FILTERING_THRESHOLD = "dynamic_filtering_filter_rows_threshold";
    private static final String ORC_PREDICATE_PUSHDOWN = "orc_predicate_pushdown_enabled";
    private static final String ORC_DISJUCT_PREDICATE_PUSHDOWN = "orc_disjunct_predicate_pushdown_enabled";
    private static final String ORC_PUSHDOWN_DATACACHE = "orc_pushdown_data_cache_enabled";
    private static final String WRITE_PARTITION_DISTRIBUTION = "write_partition_distribution";
    private static final String METASTORE_WRITE_BATCH_SIZE = "metastore_write_batch_size";

    private final List<PropertyMetadata<?>> sessionProperties;

    public enum InsertExistingPartitionsBehavior
    {
        ERROR,
        APPEND,
        OVERWRITE,
        /**/;

        public static InsertExistingPartitionsBehavior valueOf(String value, boolean immutablePartition)
        {
            InsertExistingPartitionsBehavior enumValue = valueOf(value.toUpperCase(ENGLISH));
            if (immutablePartition) {
                checkArgument(enumValue != APPEND,
                        "Presto is configured to treat Hive partitions as immutable. %s is not allowed to be set to %s",
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR, APPEND);
            }

            return enumValue;
        }
    }

    @Inject
    public HiveSessionProperties(HiveConfig hiveConfig, OrcFileWriterConfig orcFileWriterConfig,
            ParquetFileWriterConfig parquetFileWriterConfig)
    {
        sessionProperties = ImmutableList.of(
                booleanProperty(
                        BUCKET_EXECUTION_ENABLED,
                        "Enable bucket-aware execution: only use a single worker per bucket",
                        hiveConfig.isBucketExecutionEnabled(),
                        false),
                booleanProperty(
                        WRITE_PARTITION_DISTRIBUTION,
                        "Distribute writes based on partition columns",
                        false,
                        false),
                booleanProperty(
                        FORCE_LOCAL_SCHEDULING,
                        "Only schedule splits on workers colocated with data node",
                        hiveConfig.isForceLocalScheduling(),
                        false),
                new PropertyMetadata<>(
                        INSERT_EXISTING_PARTITIONS_BEHAVIOR,
                        "Behavior on insert existing partitions; this session property doesn't control behavior on insert existing unpartitioned table",
                        VARCHAR,
                        InsertExistingPartitionsBehavior.class,
                        hiveConfig.isImmutablePartitions() ? ERROR : APPEND,
                        false,
                        value -> InsertExistingPartitionsBehavior.valueOf((String) value,
                                hiveConfig.isImmutablePartitions()),
                        InsertExistingPartitionsBehavior::toString),
                booleanProperty(
                        ORC_BLOOM_FILTERS_ENABLED,
                        "ORC: Enable bloom filters for predicate pushdown",
                        hiveConfig.isOrcBloomFiltersEnabled(),
                        false),
                dataSizeProperty(
                        ORC_MAX_MERGE_DISTANCE,
                        "ORC: Maximum size of gap between two reads to merge into a single read",
                        hiveConfig.getOrcMaxMergeDistance(),
                        false),
                dataSizeProperty(
                        ORC_MAX_BUFFER_SIZE,
                        "ORC: Maximum size of a single read",
                        hiveConfig.getOrcMaxBufferSize(),
                        false),
                dataSizeProperty(
                        ORC_STREAM_BUFFER_SIZE,
                        "ORC: Size of buffer for streaming reads",
                        hiveConfig.getOrcStreamBufferSize(),
                        false),
                dataSizeProperty(
                        ORC_TINY_STRIPE_THRESHOLD,
                        "ORC: Threshold below which an ORC stripe or file will read in its entirety",
                        hiveConfig.getOrcTinyStripeThreshold(),
                        false),
                dataSizeProperty(
                        ORC_MAX_READ_BLOCK_SIZE,
                        "ORC: Soft max size of Presto blocks produced by ORC reader",
                        hiveConfig.getOrcMaxReadBlockSize(),
                        false),
                booleanProperty(
                        ORC_LAZY_READ_SMALL_RANGES,
                        "Experimental: ORC: Read small file segments lazily",
                        hiveConfig.isOrcLazyReadSmallRanges(),
                        false),
                booleanProperty(
                        ORC_NESTED_LAZY_ENABLED,
                        "Experimental: ORC: Lazily read nested data",
                        true,
                        false),
                dataSizeProperty(
                        ORC_STRING_STATISTICS_LIMIT,
                        "ORC: Maximum size of string statistics; drop if exceeding",
                        orcFileWriterConfig.getStringStatisticsLimit(),
                        false),
                booleanProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: ORC: Force all validation for files",
                        hiveConfig.getOrcWriterValidationPercentage() > 0.0,
                        false),
                new PropertyMetadata<>(
                        ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE,
                        "Experimental: ORC: sample percentage for validation for files",
                        DOUBLE,
                        Double.class,
                        hiveConfig.getOrcWriterValidationPercentage(),
                        false,
                        value -> {
                            double doubleValue = ((Number) value).doubleValue();
                            if (doubleValue < 0.0 || doubleValue > 100.0) {
                                throw new PrestoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s must be between 0.0 and 100.0 inclusive: %s",
                                                ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, doubleValue));
                            }
                            return doubleValue;
                        },
                        value -> value),
                stringProperty(
                        ORC_OPTIMIZED_WRITER_VALIDATE_MODE,
                        "Experimental: ORC: Level of detail in ORC validation",
                        hiveConfig.getOrcWriterValidationMode().toString(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE,
                        "Experimental: ORC: Min stripe size",
                        orcFileWriterConfig.getStripeMinSize(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE,
                        "Experimental: ORC: Max stripe size",
                        orcFileWriterConfig.getStripeMaxSize(),
                        false),
                integerProperty(
                        ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS,
                        "Experimental: ORC: Max stripe row count",
                        orcFileWriterConfig.getStripeMaxRowCount(),
                        false),
                dataSizeProperty(
                        ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY,
                        "Experimental: ORC: Max dictionary memory",
                        orcFileWriterConfig.getDictionaryMaxMemory(),
                        false),
                booleanProperty(
                        ORC_FILE_TAIL_CACHE_ENABLED,
                        "Cache Orc file tail",
                        hiveConfig.isOrcFileTailCacheEnabled(),
                        false),
                booleanProperty(
                        ORC_STRIPE_FOOTER_CACHE_ENABLED,
                        "Cache Orc stripes' footer",
                        hiveConfig.isOrcStripeFooterCacheEnabled(),
                        false),
                booleanProperty(
                        ORC_ROW_INDEX_CACHE_ENABLED,
                        "Cache Orc row index",
                        hiveConfig.isOrcRowIndexCacheEnabled(),
                        false),
                booleanProperty(
                        ORC_BLOOM_FILTERS_CACHE_ENABLED,
                        "Cache Orc bloom filters",
                        hiveConfig.isOrcBloomFiltersCacheEnabled(),
                        false),
                booleanProperty(
                        ORC_ROW_DATA_CACHE_ENABLED,
                        "Cache Orc row data",
                        hiveConfig.isOrcRowDataCacheEnabled(),
                        false),
                stringProperty(
                        HIVE_STORAGE_FORMAT,
                        "Default storage format for new tables or partitions",
                        hiveConfig.getHiveStorageFormat().toString(),
                        false),
                booleanProperty(
                        RESPECT_TABLE_FORMAT,
                        "Write new partitions using table format rather than default storage format",
                        hiveConfig.isRespectTableFormat(),
                        false),
                booleanProperty(
                        CREATE_EMPTY_BUCKET_FILES,
                        "Create empty files for buckets that have no data",
                        hiveConfig.isCreateEmptyBucketFiles(),
                        false),
                booleanProperty(
                        PARQUET_USE_COLUMN_NAME,
                        "Experimental: Parquet: Access Parquet columns using names from the file",
                        hiveConfig.isUseParquetColumnNames(),
                        false),
                booleanProperty(
                        PARQUET_FAIL_WITH_CORRUPTED_STATISTICS,
                        "Parquet: Fail when scanning Parquet files with corrupted statistics",
                        hiveConfig.isFailOnCorruptedParquetStatistics(),
                        false),
                dataSizeProperty(
                        PARQUET_MAX_READ_BLOCK_SIZE,
                        "Parquet: Maximum size of a block to read",
                        hiveConfig.getParquetMaxReadBlockSize(),
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_BLOCK_SIZE,
                        "Parquet: Writer block size",
                        parquetFileWriterConfig.getBlockSize(),
                        false),
                dataSizeProperty(
                        PARQUET_WRITER_PAGE_SIZE,
                        "Parquet: Writer page size",
                        parquetFileWriterConfig.getPageSize(),
                        false),
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        hiveConfig.getMaxSplitSize(),
                        true),
                dataSizeProperty(
                        MAX_INITIAL_SPLIT_SIZE,
                        "Max initial split size",
                        hiveConfig.getMaxInitialSplitSize(),
                        true),
                booleanProperty(
                        RCFILE_OPTIMIZED_WRITER_VALIDATE,
                        "Experimental: RCFile: Validate writer files",
                        hiveConfig.isRcfileWriterValidate(),
                        false),
                booleanProperty(
                        SORTED_WRITING_ENABLED,
                        "Enable writing to bucketed sorted tables",
                        hiveConfig.isSortedWritingEnabled(),
                        false),
                booleanProperty(
                        STATISTICS_ENABLED,
                        "Experimental: Expose table statistics",
                        hiveConfig.isTableStatisticsEnabled(),
                        false),
                integerProperty(
                        PARTITION_STATISTICS_SAMPLE_SIZE,
                        "Maximum sample size of the partitions column statistics",
                        hiveConfig.getPartitionStatisticsSampleSize(),
                        false),
                booleanProperty(
                        IGNORE_CORRUPTED_STATISTICS,
                        "Experimental: Ignore corrupted statistics rather than failing",
                        hiveConfig.isIgnoreCorruptedStatistics(),
                        false),
                booleanProperty(
                        COLLECT_COLUMN_STATISTICS_ON_WRITE,
                        "Experimental: Enables automatic column level statistics collection on write",
                        hiveConfig.isCollectColumnStatisticsOnWrite(),
                        false),
                booleanProperty(
                        OPTIMIZE_MISMATCHED_BUCKET_COUNT,
                        "Experimenal: Enable optimization to avoid shuffle when bucket count is compatible but not the same",
                        hiveConfig.isOptimizeMismatchedBucketCount(),
                        false),
                booleanProperty(
                        S3_SELECT_PUSHDOWN_ENABLED,
                        "S3 Select pushdown enabled",
                        hiveConfig.isS3SelectPushdownEnabled(),
                        false),
                booleanProperty(
                        TEMPORARY_STAGING_DIRECTORY_ENABLED,
                        "Should use temporary staging directory for write operations",
                        hiveConfig.isTemporaryStagingDirectoryEnabled(),
                        false),
                stringProperty(
                        TEMPORARY_STAGING_DIRECTORY_PATH,
                        "Temporary staging directory location",
                        hiveConfig.getTemporaryStagingDirectoryPath(),
                        false),
                integerProperty(
                        METASTORE_WRITE_BATCH_SIZE,
                        "Batch size for requests to HMS for partition and partition statistics write operation",
                        hiveConfig.getMetastoreWriteBatchSize(),
                        false),
                integerProperty(
                        DYNAMIC_FILTERING_ROW_FILTERING_THRESHOLD,
                        "Only enable row filtering with dynamic filter if the filter size is below this threshold",
                        hiveConfig.getDynamicFilteringRowFilteringThreshold(),
                        false),
                booleanProperty(
                        DYNAMIC_FILTERING_SPLIT_FILTERING,
                        "Filter out hive splits early based on partition value using dynamic filter",
                        hiveConfig.isDynamicFilterPartitionFilteringEnabled(),
                        false),
                booleanProperty(
                        ORC_PREDICATE_PUSHDOWN,
                        "Experimental: Consume deterministic predicates(conjucts: AND) for ORC scan.",
                        hiveConfig.isOrcPredicatePushdownEnabled(),
                        false),
                booleanProperty(
                        ORC_DISJUCT_PREDICATE_PUSHDOWN,
                        "Experimental: Consume deterministic predicates(disjucts: OR) for ORC scan.",
                        true,
                        false),
                booleanProperty(
                        ORC_PUSHDOWN_DATACACHE,
                        "Experimental: Enable data cache or result cache with predicate pushdown.",
                        true,
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isBucketExecutionEnabled(ConnectorSession session)
    {
        return session.getProperty(BUCKET_EXECUTION_ENABLED, Boolean.class);
    }

    public static boolean isWritePartitionDistributionEnabled(ConnectorSession session)
    {
        return session.getProperty(WRITE_PARTITION_DISTRIBUTION, Boolean.class);
    }

    public static int getMetastoreWriteBatchSize(ConnectorSession session)
    {
        return session.getProperty(METASTORE_WRITE_BATCH_SIZE, Integer.class);
    }

    public static boolean isForceLocalScheduling(ConnectorSession session)
    {
        return session.getProperty(FORCE_LOCAL_SCHEDULING, Boolean.class);
    }

    public static InsertExistingPartitionsBehavior getInsertExistingPartitionsBehavior(ConnectorSession session)
    {
        return session.getProperty(INSERT_EXISTING_PARTITIONS_BEHAVIOR, InsertExistingPartitionsBehavior.class);
    }

    public static boolean isOrcBloomFiltersEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_BLOOM_FILTERS_ENABLED, Boolean.class);
    }

    public static DataSize getOrcMaxMergeDistance(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_MERGE_DISTANCE, DataSize.class);
    }

    public static DataSize getOrcMaxBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcStreamBufferSize(ConnectorSession session)
    {
        return session.getProperty(ORC_STREAM_BUFFER_SIZE, DataSize.class);
    }

    public static DataSize getOrcTinyStripeThreshold(ConnectorSession session)
    {
        return session.getProperty(ORC_TINY_STRIPE_THRESHOLD, DataSize.class);
    }

    public static DataSize getOrcMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(ORC_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static boolean getOrcLazyReadSmallRanges(ConnectorSession session)
    {
        return session.getProperty(ORC_LAZY_READ_SMALL_RANGES, Boolean.class);
    }

    public static boolean isOrcNestedLazy(ConnectorSession session)
    {
        return session.getProperty(ORC_NESTED_LAZY_ENABLED, Boolean.class);
    }

    public static DataSize getOrcStringStatisticsLimit(ConnectorSession session)
    {
        return session.getProperty(ORC_STRING_STATISTICS_LIMIT, DataSize.class);
    }

    public static boolean isOrcOptimizedWriterValidate(ConnectorSession session)
    {
        boolean validate = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
        double percentage = session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_PERCENTAGE, Double.class);

        checkArgument(percentage >= 0.0 && percentage <= 100.0);

        // session property can disabled validation
        if (!validate) {
            return false;
        }

        // session property can not force validation when sampling is enabled
        // todo change this if session properties support null
        return ThreadLocalRandom.current().nextDouble(100) < percentage;
    }

    public static OrcWriteValidationMode getOrcOptimizedWriterValidateMode(ConnectorSession session)
    {
        return OrcWriteValidationMode.valueOf(
                session.getProperty(ORC_OPTIMIZED_WRITER_VALIDATE_MODE, String.class).toUpperCase(ENGLISH));
    }

    public static DataSize getOrcOptimizedWriterMinStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MIN_STRIPE_SIZE, DataSize.class);
    }

    public static DataSize getOrcOptimizedWriterMaxStripeSize(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_SIZE, DataSize.class);
    }

    public static int getOrcOptimizedWriterMaxStripeRows(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_STRIPE_ROWS, Integer.class);
    }

    public static DataSize getOrcOptimizedWriterMaxDictionaryMemory(ConnectorSession session)
    {
        return session.getProperty(ORC_OPTIMIZED_WRITER_MAX_DICTIONARY_MEMORY, DataSize.class);
    }

    public static boolean isOrcFileTailCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_FILE_TAIL_CACHE_ENABLED, Boolean.class);
    }

    public static boolean isOrcStripeFooterCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_STRIPE_FOOTER_CACHE_ENABLED, Boolean.class);
    }

    public static boolean isOrcRowIndexCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_ROW_INDEX_CACHE_ENABLED, Boolean.class);
    }

    public static boolean isOrcBloomFiltersCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_BLOOM_FILTERS_CACHE_ENABLED, Boolean.class);
    }

    public static boolean isOrcRowDataCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_ROW_DATA_CACHE_ENABLED, Boolean.class);
    }

    public static HiveStorageFormat getHiveStorageFormat(ConnectorSession session)
    {
        return HiveStorageFormat.valueOf(session.getProperty(HIVE_STORAGE_FORMAT, String.class).toUpperCase(ENGLISH));
    }

    public static boolean isRespectTableFormat(ConnectorSession session)
    {
        return session.getProperty(RESPECT_TABLE_FORMAT, Boolean.class);
    }

    public static boolean isCreateEmptyBucketFiles(ConnectorSession session)
    {
        return session.getProperty(CREATE_EMPTY_BUCKET_FILES, Boolean.class);
    }

    public static boolean isUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(PARQUET_USE_COLUMN_NAME, Boolean.class);
    }

    public static boolean isFailOnCorruptedParquetStatistics(ConnectorSession session)
    {
        return session.getProperty(PARQUET_FAIL_WITH_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static DataSize getParquetMaxReadBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_MAX_READ_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterBlockSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_BLOCK_SIZE, DataSize.class);
    }

    public static DataSize getParquetWriterPageSize(ConnectorSession session)
    {
        return session.getProperty(PARQUET_WRITER_PAGE_SIZE, DataSize.class);
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static DataSize getMaxInitialSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_INITIAL_SPLIT_SIZE, DataSize.class);
    }

    public static boolean isRcfileOptimizedWriterValidate(ConnectorSession session)
    {
        return session.getProperty(RCFILE_OPTIMIZED_WRITER_VALIDATE, Boolean.class);
    }

    public static boolean isSortedWritingEnabled(ConnectorSession session)
    {
        return session.getProperty(SORTED_WRITING_ENABLED, Boolean.class);
    }

    public static boolean isS3SelectPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(S3_SELECT_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isStatisticsEnabled(ConnectorSession session)
    {
        return session.getProperty(STATISTICS_ENABLED, Boolean.class);
    }

    public static int getPartitionStatisticsSampleSize(ConnectorSession session)
    {
        int size = session.getProperty(PARTITION_STATISTICS_SAMPLE_SIZE, Integer.class);
        if (size < 1) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than 0: %s", PARTITION_STATISTICS_SAMPLE_SIZE, size));
        }
        return size;
    }

    public static boolean isIgnoreCorruptedStatistics(ConnectorSession session)
    {
        return session.getProperty(IGNORE_CORRUPTED_STATISTICS, Boolean.class);
    }

    public static boolean isCollectColumnStatisticsOnWrite(ConnectorSession session)
    {
        return session.getProperty(COLLECT_COLUMN_STATISTICS_ON_WRITE, Boolean.class);
    }

    public static boolean isOptimizedMismatchedBucketCount(ConnectorSession session)
    {
        return session.getProperty(OPTIMIZE_MISMATCHED_BUCKET_COUNT, Boolean.class);
    }

    public static boolean isTemporaryStagingDirectoryEnabled(ConnectorSession session)
    {
        return session.getProperty(TEMPORARY_STAGING_DIRECTORY_ENABLED, Boolean.class);
    }

    public static int getDynamicFilteringRowFilteringThreshold(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_ROW_FILTERING_THRESHOLD, Integer.class);
    }

    public static boolean isDynamicFilteringSplitFilteringEnabled(ConnectorSession session)
    {
        return session.getProperty(DYNAMIC_FILTERING_SPLIT_FILTERING, Boolean.class);
    }

    public static boolean isOrcPredicatePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_PREDICATE_PUSHDOWN, Boolean.class);
    }

    public static boolean isOrcDisjunctPredicatePushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_DISJUCT_PREDICATE_PUSHDOWN, Boolean.class);
    }

    public static boolean isOrcPushdownDataCacheEnabled(ConnectorSession session)
    {
        return session.getProperty(ORC_PUSHDOWN_DATACACHE, Boolean.class);
    }
}
