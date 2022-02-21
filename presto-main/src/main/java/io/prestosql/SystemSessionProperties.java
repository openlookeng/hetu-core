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
package io.prestosql;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.execution.TaskManagerConfig;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.snapshot.SnapshotConfig;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.session.PropertyMetadata;
import io.prestosql.spi.type.TimeZoneKey;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.analyzer.FeaturesConfig.DynamicFilterDataType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinDistributionType;
import io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import io.prestosql.sql.analyzer.FeaturesConfig.RedistributeWritesType;
import io.prestosql.utils.HetuConfig;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.prestosql.spi.session.PropertyMetadata.booleanProperty;
import static io.prestosql.spi.session.PropertyMetadata.dataSizeProperty;
import static io.prestosql.spi.session.PropertyMetadata.doubleProperty;
import static io.prestosql.spi.session.PropertyMetadata.durationProperty;
import static io.prestosql.spi.session.PropertyMetadata.enumProperty;
import static io.prestosql.spi.session.PropertyMetadata.integerProperty;
import static io.prestosql.spi.session.PropertyMetadata.longProperty;
import static io.prestosql.spi.session.PropertyMetadata.stringProperty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static io.prestosql.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SystemSessionProperties
{
    public static final String OPTIMIZE_HASH_GENERATION = "optimize_hash_generation";
    public static final String JOIN_DISTRIBUTION_TYPE = "join_distribution_type";
    public static final String JOIN_MAX_BROADCAST_TABLE_SIZE = "join_max_broadcast_table_size";
    public static final String DISTRIBUTED_INDEX_JOIN = "distributed_index_join";
    public static final String HASH_PARTITION_COUNT = "hash_partition_count";
    public static final String GROUPED_EXECUTION = "grouped_execution";
    public static final String DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION = "dynamic_schedule_for_grouped_execution";
    public static final String PREFER_STREAMING_OPERATORS = "prefer_streaming_operators";
    public static final String TASK_WRITER_COUNT = "task_writer_count";
    public static final String TASK_CONCURRENCY = "task_concurrency";
    public static final String TASK_SHARE_INDEX_LOADING = "task_share_index_loading";
    public static final String QUERY_MAX_MEMORY = "query_max_memory";
    public static final String QUERY_MAX_TOTAL_MEMORY = "query_max_total_memory";
    public static final String QUERY_MAX_EXECUTION_TIME = "query_max_execution_time";
    public static final String QUERY_MAX_RUN_TIME = "query_max_run_time";
    public static final String RESOURCE_OVERCOMMIT = "resource_overcommit";
    public static final String QUERY_MAX_CPU_TIME = "query_max_cpu_time";
    public static final String QUERY_MAX_STAGE_COUNT = "query_max_stage_count";
    public static final String REDISTRIBUTE_WRITES = "redistribute_writes";
    public static final String TIME_ZONE_ID = "time_zone_id";
    // redistribute writes type session property key
    public static final String REDISTRIBUTE_WRITES_TYPE = "redistribute_writes_type";
    public static final String SCALE_WRITERS = "scale_writers";
    public static final String WRITER_MIN_SIZE = "writer_min_size";
    public static final String PUSH_TABLE_WRITE_THROUGH_UNION = "push_table_write_through_union";
    public static final String EXECUTION_POLICY = "execution_policy";
    public static final String DICTIONARY_AGGREGATION = "dictionary_aggregation";
    public static final String PLAN_WITH_TABLE_NODE_PARTITIONING = "plan_with_table_node_partitioning";
    public static final String SPATIAL_JOIN = "spatial_join";
    public static final String SPATIAL_PARTITIONING_TABLE_NAME = "spatial_partitioning_table_name";
    public static final String COLOCATED_JOIN = "colocated_join";
    public static final String CONCURRENT_LIFESPANS_PER_NODE = "concurrent_lifespans_per_task";
    public static final String REORDER_JOINS = "reorder_joins";
    public static final String JOIN_REORDERING_STRATEGY = "join_reordering_strategy";
    public static final String MAX_REORDERED_JOINS = "max_reordered_joins";
    public static final String SKIP_REORDERING_THRESHOLD = "skip_reordering_threshold";
    public static final String INITIAL_SPLITS_PER_NODE = "initial_splits_per_node";
    public static final String SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL = "split_concurrency_adjustment_interval";
    public static final String OPTIMIZE_METADATA_QUERIES = "optimize_metadata_queries";
    public static final String FAST_INEQUALITY_JOINS = "fast_inequality_joins";
    public static final String QUERY_PRIORITY = "query_priority";
    public static final String SPILL_ENABLED = "spill_enabled";
    public static final String SPILL_ORDER_BY = "spill_order_by";
    public static final String SPILL_NON_BLOCKING_ORDERBY = "spill_non_blocking_orderby";
    public static final String SPILL_WINDOW_OPERATOR = "spill_window_operator";
    public static final String SPILL_OUTER_JOIN_ENABLED = "spill_build_for_outer_join_enabled";
    public static final String INNER_JOIN_SPILL_FILTER_ENABLED = "inner_join_spill_filter_enabled";
    public static final String AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT = "aggregation_operator_unspill_memory_limit";
    public static final String OPTIMIZE_DISTINCT_AGGREGATIONS = "optimize_mixed_distinct_aggregations";
    public static final String ITERATIVE_OPTIMIZER = "iterative_optimizer_enabled";
    public static final String ITERATIVE_OPTIMIZER_TIMEOUT = "iterative_optimizer_timeout";
    public static final String ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID = "enable_forced_exchange_below_group_id";
    public static final String EXCHANGE_COMPRESSION = "exchange_compression";
    public static final String ENABLE_INTERMEDIATE_AGGREGATIONS = "enable_intermediate_aggregations";
    public static final String PUSH_AGGREGATION_THROUGH_JOIN = "push_aggregation_through_join";
    public static final String PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN = "push_partial_aggregation_through_join";
    public static final String PARSE_DECIMAL_LITERALS_AS_DOUBLE = "parse_decimal_literals_as_double";
    public static final String FORCE_SINGLE_NODE_OUTPUT = "force_single_node_output";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE = "filter_and_project_min_output_page_size";
    public static final String FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT = "filter_and_project_min_output_page_row_count";
    public static final String DISTRIBUTED_SORT = "distributed_sort";
    public static final String USE_MARK_DISTINCT = "use_mark_distinct";
    public static final String PREFER_PARTIAL_AGGREGATION = "prefer_partial_aggregation";
    public static final String OPTIMIZE_TOP_N_RANKING_NUMBER = "optimize_top_n_ranking_number";
    public static final String MAX_GROUPING_SETS = "max_grouping_sets";
    public static final String STATISTICS_CPU_TIMER_ENABLED = "statistics_cpu_timer_enabled";
    public static final String ENABLE_STATS_CALCULATOR = "enable_stats_calculator";
    public static final String IGNORE_STATS_CALCULATOR_FAILURES = "ignore_stats_calculator_failures";
    public static final String MAX_DRIVERS_PER_TASK = "max_drivers_per_task";
    public static final String DEFAULT_FILTER_FACTOR_ENABLED = "default_filter_factor_enabled";
    public static final String UNWRAP_CASTS = "unwrap_casts";
    public static final String SKIP_REDUNDANT_SORT = "skip_redundant_sort";
    public static final String PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES = "predicate_pushdown_use_table_properties";
    public static final String WORK_PROCESSOR_PIPELINES = "work_processor_pipelines";
    public static final String ENABLE_DYNAMIC_FILTERING = "enable_dynamic_filtering";
    public static final String QUERY_PUSHDOWN = "query_pushdown";
    public static final String FILTERING_SEMI_JOIN_TO_INNER = "rewrite_filtering_semi_join_to_inner_join";
    public static final String JOIN_ORDER = "join_order";
    public static final String IMPLICIT_CONVERSION = "implicit_conversion";
    public static final String PUSH_LIMIT_THROUGH_UNION = "push_limit_through_union";
    public static final String PUSH_LIMIT_THROUGH_SEMI_JOIN = "push_limit_through_semi_join";
    public static final String PUSH_LIMIT_THROUGH_OUTER_JOIN = "push_limit_through_outer_join";
    public static final String PUSH_LIMIT_DOWN = "push_limit_down";
    public static final String DYNAMIC_FILTERING_MAX_SIZE = "dynamic_filtering_max_size";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_VALUE_COUNT = "dynamic_filtering_max_per_driver_value_count";
    public static final String DYNAMIC_FILTERING_WAIT_TIME = "dynamic_filtering_wait_time";
    public static final String DYNAMIC_FILTERING_DATA_TYPE = "dynamic_filtering_data_type";
    public static final String DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE = "dynamic_filtering_max_per_driver_size";
    public static final String DYNAMIC_FILTERING_BLOOM_FILTER_FPP = "dynamic_filtering_bloom_filter_fpp";
    public static final String ENABLE_EXECUTION_PLAN_CACHE = "enable_execution_plan_cache";
    public static final String ENABLE_CROSS_REGION_DYNAMIC_FILTER = "cross_region_dynamic_filter_enabled";
    public static final String ENABLE_HEURISTICINDEX_FILTER = "heuristicindex_filter_enabled";
    public static final String ENABLE_STAR_TREE_INDEX = "enable_star_tree_index";
    public static final String PUSH_TABLE_THROUGH_SUBQUERY = "push_table_through_subquery";
    public static final String OPTIMIZE_DYNAMIC_FILTER_GENERATION = "optimize_dynamic_filter_generation";
    public static final String TRANSFORM_SELF_JOIN_TO_GROUPBY = "transform_self_join_to_groupby";
    public static final String REUSE_TABLE_SCAN = "reuse_table_scan";
    public static final String SPILL_REUSE_TABLESCAN = "spill_reuse_tablescan";
    public static final String SPILL_THRESHOLD_REUSE_TABLESCAN = "spill_threshold_reuse_tablescan";
    public static final String SORT_BASED_AGGREGATION_ENABLED = "sort_based_aggregation_enabled";
    public static final String PRCNT_DRIVERS_FOR_PARTIAL_AGGR = "prcnt_drivers_for_partial_aggr";
    // CTE Optimization configurations
    public static final String CTE_REUSE_ENABLED = "cte_reuse_enabled";
    public static final String CTE_MAX_QUEUE_SIZE = "cte_max_queue_size";
    public static final String CTE_MAX_PREFETCH_QUEUE_SIZE = "cte_max_prefetch_queue_size";
    public static final String DELETE_TRANSACTIONAL_TABLE_DIRECT = "delete_transactional_table_direct";
    public static final String LIST_BUILT_IN_FUNCTIONS_ONLY = "list_built_in_functions_only";
    // Snapshot related configurations
    public static final String SNAPSHOT_ENABLED = "snapshot_enabled";
    public static final String SNAPSHOT_INTERVAL_TYPE = "snapshot_interval_type";
    public static final String SNAPSHOT_TIME_INTERVAL = "snapshot_time_interval";
    public static final String SNAPSHOT_SPLIT_COUNT_INTERVAL = "snapshot_split_count_interval";
    public static final String SNAPSHOT_MAX_RETRIES = "snapshot_max_retries";
    public static final String SNAPSHOT_RETRY_TIMEOUT = "snapshot_retry_timeout";
    public static final String SKIP_ATTACHING_STATS_WITH_PLAN = "skip_attaching_stats_with_plan";
    public static final String SKIP_NON_APPLICABLE_RULES_ENABLED = "skip_non_applicable_rules_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    public SystemSessionProperties()
    {
        this(new QueryManagerConfig(), new TaskManagerConfig(), new MemoryManagerConfig(), new FeaturesConfig(), new HetuConfig(), new SnapshotConfig());
    }

    @Inject
    public SystemSessionProperties(
            QueryManagerConfig queryManagerConfig,
            TaskManagerConfig taskManagerConfig,
            MemoryManagerConfig memoryManagerConfig,
            FeaturesConfig featuresConfig,
            HetuConfig hetuConfig,
            SnapshotConfig snapshotConfig)
    {
        sessionProperties = ImmutableList.of(
                stringProperty(
                        EXECUTION_POLICY,
                        "Policy used for scheduling query tasks",
                        queryManagerConfig.getQueryExecutionPolicy(),
                        false),
                booleanProperty(
                        QUERY_PUSHDOWN,
                        "Enable sub-query push down for all connectors",
                        featuresConfig.isQueryPushDown(),
                        false),
                booleanProperty(
                        FILTERING_SEMI_JOIN_TO_INNER,
                        "Rewrite semi join in filtering context to inner join",
                        featuresConfig.isRewriteFilteringSemiJoinToInnerJoin(),
                        false),
                booleanProperty(
                        TRANSFORM_SELF_JOIN_TO_GROUPBY,
                        "Transform Self Join to Group BY if possible",
                        featuresConfig.isTransformSelfJoinToGroupby(),
                        false),
                booleanProperty(
                        DELETE_TRANSACTIONAL_TABLE_DIRECT,
                        "Deletes the transactional table/partition directly instead of creating delta",
                        false,
                        false),
                stringProperty(
                        JOIN_ORDER,
                        "Join order in comma separated indexes",
                        "",
                        false),
                booleanProperty(
                        IMPLICIT_CONVERSION,
                        "Enable data type implicit conversion",
                        featuresConfig.isImplicitConversionEnabled(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_DOWN,
                        "Enable limit push down",
                        featuresConfig.isPushLimitDown(),
                        false),
                booleanProperty(
                        OPTIMIZE_HASH_GENERATION,
                        "Compute hash codes for distribution, joins, and aggregations early in query plan",
                        featuresConfig.isOptimizeHashGeneration(),
                        false),
                enumProperty(
                        JOIN_DISTRIBUTION_TYPE,
                        "Join distribution type",
                        JoinDistributionType.class,
                        featuresConfig.getJoinDistributionType(),
                        false),
                dataSizeProperty(
                        JOIN_MAX_BROADCAST_TABLE_SIZE,
                        "Maximum estimated size of a table that can be broadcast when using automatic join type selection",
                        featuresConfig.getJoinMaxBroadcastTableSize(),
                        false),
                booleanProperty(
                        DISTRIBUTED_INDEX_JOIN,
                        "Distribute index joins on join keys instead of executing inline",
                        featuresConfig.isDistributedIndexJoinsEnabled(),
                        false),
                integerProperty(
                        HASH_PARTITION_COUNT,
                        "Number of partitions for distributed joins and aggregations",
                        queryManagerConfig.getInitialHashPartitions(),
                        false),
                booleanProperty(
                        GROUPED_EXECUTION,
                        "Use grouped execution when possible",
                        featuresConfig.isGroupedExecutionEnabled(),
                        false),
                booleanProperty(
                        DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION,
                        "Experimental: Use dynamic schedule for grouped execution when possible",
                        featuresConfig.isDynamicScheduleForGroupedExecutionEnabled(),
                        false),
                booleanProperty(
                        PREFER_STREAMING_OPERATORS,
                        "Prefer source table layouts that produce streaming operators",
                        false,
                        false),
                new PropertyMetadata<>(
                        TASK_WRITER_COUNT,
                        "Default number of local parallel table writer jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getWriterCount(),
                        false,
                        value -> validateValueIsPowerOfTwo(value, TASK_WRITER_COUNT),
                        value -> value),
                booleanProperty(
                        REDISTRIBUTE_WRITES,
                        "Force parallel distributed writes",
                        featuresConfig.isRedistributeWrites(),
                        false),
                stringProperty(
                        TIME_ZONE_ID,
                        "Time Zone Id for the current session",
                        null,
                        value -> {
                            if (value != null) {
                                TimeZoneKey.getTimeZoneKey(value);
                            }
                        },
                        true),
                // redistribute writes type config
                enumProperty(
                        REDISTRIBUTE_WRITES_TYPE,
                        "Parallel distributed writes type",
                        RedistributeWritesType.class,
                        featuresConfig.getRedistributeWritesType(),
                        false),
                booleanProperty(
                        SCALE_WRITERS,
                        "Scale out writers based on throughput (use minimum necessary)",
                        featuresConfig.isScaleWriters(),
                        false),
                dataSizeProperty(
                        WRITER_MIN_SIZE,
                        "Target minimum size of writer output when scaling writers",
                        featuresConfig.getWriterMinSize(),
                        false),
                booleanProperty(
                        PUSH_TABLE_WRITE_THROUGH_UNION,
                        "Parallelize writes when using UNION ALL in queries that write data",
                        featuresConfig.isPushTableWriteThroughUnion(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_THROUGH_UNION,
                        "Push limit through union",
                        featuresConfig.isPushLimitThroughUnion(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_THROUGH_SEMI_JOIN,
                        "Push limit through semijoin",
                        featuresConfig.isPushLimitThroughSemiJoin(),
                        false),
                booleanProperty(
                        PUSH_LIMIT_THROUGH_OUTER_JOIN,
                        "Push limit through outerjoin",
                        featuresConfig.isPushLimitThroughOuterJoin(),
                        false),
                new PropertyMetadata<>(
                        TASK_CONCURRENCY,
                        "Default number of local parallel jobs per worker",
                        BIGINT,
                        Integer.class,
                        taskManagerConfig.getTaskConcurrency(),
                        false,
                        value -> validateValueIsPowerOfTwo(value, TASK_CONCURRENCY),
                        value -> value),
                booleanProperty(
                        TASK_SHARE_INDEX_LOADING,
                        "Share index join lookups and caching within a task",
                        taskManagerConfig.isShareIndexLoading(),
                        false),
                durationProperty(
                        QUERY_MAX_RUN_TIME,
                        "Maximum run time of a query (includes the queueing time)",
                        queryManagerConfig.getQueryMaxRunTime(),
                        false),
                durationProperty(
                        QUERY_MAX_EXECUTION_TIME,
                        "Maximum execution time of a query",
                        queryManagerConfig.getQueryMaxExecutionTime(),
                        false),
                durationProperty(
                        QUERY_MAX_CPU_TIME,
                        "Maximum CPU time of a query",
                        queryManagerConfig.getQueryMaxCpuTime(),
                        false),
                dataSizeProperty(
                        QUERY_MAX_MEMORY,
                        "Maximum amount of distributed memory a query can use",
                        memoryManagerConfig.getMaxQueryMemory(),
                        true),
                dataSizeProperty(
                        QUERY_MAX_TOTAL_MEMORY,
                        "Maximum amount of distributed total memory a query can use",
                        memoryManagerConfig.getMaxQueryTotalMemory(),
                        true),
                booleanProperty(
                        RESOURCE_OVERCOMMIT,
                        "Use resources which are not guaranteed to be available to the query",
                        false,
                        false),
                integerProperty(
                        QUERY_MAX_STAGE_COUNT,
                        "Temporary: Maximum number of stages a query can have",
                        queryManagerConfig.getMaxStageCount(),
                        true),
                booleanProperty(
                        DICTIONARY_AGGREGATION,
                        "Enable optimization for aggregations on dictionaries",
                        featuresConfig.isDictionaryAggregation(),
                        false),
                integerProperty(
                        INITIAL_SPLITS_PER_NODE,
                        "The number of splits each node will run per task, initially",
                        taskManagerConfig.getInitialSplitsPerNode(),
                        false),
                durationProperty(
                        SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL,
                        "Experimental: Interval between changes to the number of concurrent splits per node",
                        taskManagerConfig.getSplitConcurrencyAdjustmentInterval(),
                        false),
                booleanProperty(
                        OPTIMIZE_METADATA_QUERIES,
                        "Enable optimization for metadata queries",
                        featuresConfig.isOptimizeMetadataQueries(),
                        false),
                integerProperty(
                        QUERY_PRIORITY,
                        "The priority of queries. Larger numbers are higher priority",
                        1,
                        false),
                booleanProperty(
                        PLAN_WITH_TABLE_NODE_PARTITIONING,
                        "Experimental: Adapt plan to pre-partitioned tables",
                        true,
                        false),
                booleanProperty(
                        REORDER_JOINS,
                        "(DEPRECATED) Reorder joins to remove unnecessary cross joins. If this is set, join_reordering_strategy will be ignored",
                        null,
                        false),
                enumProperty(
                        JOIN_REORDERING_STRATEGY,
                        "Join reordering strategy",
                        JoinReorderingStrategy.class,
                        featuresConfig.getJoinReorderingStrategy(),
                        false),
                new PropertyMetadata<>(
                        MAX_REORDERED_JOINS,
                        "The maximum number of joins to reorder as one group in cost-based join reordering",
                        BIGINT,
                        Integer.class,
                        featuresConfig.getMaxReorderedJoins(),
                        false,
                        value -> {
                            int intValue = ((Number) requireNonNull(value, "value is null")).intValue();
                            if (intValue < 2) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than or equal to 2: %s", MAX_REORDERED_JOINS, intValue));
                            }
                            return intValue;
                        },
                        value -> value),
                new PropertyMetadata<>(
                        SKIP_REORDERING_THRESHOLD,
                        "Skip reordering joins if the number of joins in the logical plan is greater than this threshold",
                        BIGINT,
                        Integer.class,
                        Integer.MAX_VALUE,
                        false,
                        value -> {
                            int intValue = ((Number) requireNonNull(value, "value is null")).intValue();
                            if (intValue < 0) {
                                throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be greater than or equal to 0: %s", SKIP_REORDERING_THRESHOLD, intValue));
                            }
                            return intValue;
                        },
                        value -> value),
                booleanProperty(
                        FAST_INEQUALITY_JOINS,
                        "Use faster handling of inequality join if it is possible",
                        featuresConfig.isFastInequalityJoins(),
                        false),
                booleanProperty(
                        COLOCATED_JOIN,
                        "Experimental: Use a colocated join when possible",
                        featuresConfig.isColocatedJoinsEnabled(),
                        false),
                booleanProperty(
                        SPATIAL_JOIN,
                        "Use spatial index for spatial join when possible",
                        featuresConfig.isSpatialJoinsEnabled(),
                        false),
                stringProperty(
                        SPATIAL_PARTITIONING_TABLE_NAME,
                        "Name of the table containing spatial partitioning scheme",
                        null,
                        false),
                integerProperty(
                        CONCURRENT_LIFESPANS_PER_NODE,
                        "Experimental: Run a fixed number of groups concurrently for eligible JOINs",
                        featuresConfig.getConcurrentLifespansPerTask(),
                        false),
                new PropertyMetadata<>(
                        SPILL_ENABLED,
                        "Experimental: Enable spilling",
                        BOOLEAN,
                        Boolean.class,
                        featuresConfig.isSpillEnabled(),
                        false,
                        value -> {
                            boolean spillEnabled = (Boolean) value;
                            if (spillEnabled && featuresConfig.getSpillerSpillPaths().isEmpty()) {
                                throw new PrestoException(
                                        INVALID_SESSION_PROPERTY,
                                        format("%s cannot be set to true; no spill paths configured", SPILL_ENABLED));
                            }
                            return spillEnabled;
                        },
                        value -> value),
                booleanProperty(
                        SPILL_ORDER_BY,
                        "Spill in OrderBy if spill_enabled is also set",
                        featuresConfig.isSpillOrderBy(),
                        false),
                booleanProperty(
                        SPILL_NON_BLOCKING_ORDERBY,
                        "Spill orderby in non blocking manner",
                        featuresConfig.isNonBlockingSpill(),
                        false),
                booleanProperty(
                        SPILL_OUTER_JOIN_ENABLED,
                        "Enable build side spill for Right or Full Outer Join",
                        featuresConfig.isSpillBuildForOuterJoinEnabled(),
                        false),
                booleanProperty(
                        INNER_JOIN_SPILL_FILTER_ENABLED,
                        "Enable build side spill matching optimization for Inner Join",
                        featuresConfig.isInnerJoinSpillFilterEnabled(),
                        false),
                booleanProperty(
                        SPILL_WINDOW_OPERATOR,
                        "Spill in WindowOperator if spill_enabled is also set",
                        featuresConfig.isSpillWindowOperator(),
                        false),
                dataSizeProperty(
                        AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT,
                        "Experimental: How much memory can should be allocated per aggragation operator in unspilling process",
                        featuresConfig.getAggregationOperatorUnspillMemoryLimit(),
                        false),
                booleanProperty(
                        OPTIMIZE_DISTINCT_AGGREGATIONS,
                        "Optimize mixed non-distinct and distinct aggregations",
                        featuresConfig.isOptimizeMixedDistinctAggregations(),
                        false),
                booleanProperty(
                        ITERATIVE_OPTIMIZER,
                        "Experimental: enable iterative optimizer",
                        featuresConfig.isIterativeOptimizerEnabled(),
                        false),
                durationProperty(
                        ITERATIVE_OPTIMIZER_TIMEOUT,
                        "Timeout for plan optimization in iterative optimizer",
                        featuresConfig.getIterativeOptimizerTimeout(),
                        false),
                booleanProperty(
                        ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID,
                        "Enable a stats-based rule adding exchanges below GroupId",
                        featuresConfig.isEnableForcedExchangeBelowGroupId(),
                        true),
                booleanProperty(
                        EXCHANGE_COMPRESSION,
                        "Enable compression in exchanges",
                        featuresConfig.isExchangeCompressionEnabled(),
                        false),
                booleanProperty(
                        ENABLE_INTERMEDIATE_AGGREGATIONS,
                        "Enable the use of intermediate aggregations",
                        featuresConfig.isEnableIntermediateAggregations(),
                        false),
                booleanProperty(
                        PUSH_AGGREGATION_THROUGH_JOIN,
                        "Allow pushing aggregations below joins",
                        featuresConfig.isPushAggregationThroughJoin(),
                        false),
                booleanProperty(
                        PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN,
                        "Push partial aggregations below joins",
                        false,
                        false),
                booleanProperty(
                        PARSE_DECIMAL_LITERALS_AS_DOUBLE,
                        "Parse decimal literals as DOUBLE instead of DECIMAL",
                        featuresConfig.isParseDecimalLiteralsAsDouble(),
                        false),
                booleanProperty(
                        FORCE_SINGLE_NODE_OUTPUT,
                        "Force single node output",
                        featuresConfig.isForceSingleNodeOutput(),
                        true),
                dataSizeProperty(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE,
                        "Experimental: Minimum output page size for filter and project operators",
                        featuresConfig.getFilterAndProjectMinOutputPageSize(),
                        false),
                integerProperty(
                        FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT,
                        "Experimental: Minimum output page row count for filter and project operators",
                        featuresConfig.getFilterAndProjectMinOutputPageRowCount(),
                        false),
                booleanProperty(
                        DISTRIBUTED_SORT,
                        "Parallelize sort across multiple nodes",
                        featuresConfig.isDistributedSortEnabled(),
                        false),
                booleanProperty(
                        USE_MARK_DISTINCT,
                        "Implement DISTINCT aggregations using MarkDistinct",
                        featuresConfig.isUseMarkDistinct(),
                        false),
                booleanProperty(
                        PREFER_PARTIAL_AGGREGATION,
                        "Prefer splitting aggregations into partial and final stages",
                        featuresConfig.isPreferPartialAggregation(),
                        false),
                booleanProperty(
                        OPTIMIZE_TOP_N_RANKING_NUMBER,
                        "Use top N row number optimization",
                        featuresConfig.isOptimizeTopNRankingNumber(),
                        false),
                integerProperty(
                        MAX_GROUPING_SETS,
                        "Maximum number of grouping sets in a GROUP BY",
                        featuresConfig.getMaxGroupingSets(),
                        true),
                booleanProperty(
                        STATISTICS_CPU_TIMER_ENABLED,
                        "Experimental: Enable cpu time tracking for automatic column statistics collection on write",
                        taskManagerConfig.isStatisticsCpuTimerEnabled(),
                        false),
                booleanProperty(
                        ENABLE_STATS_CALCULATOR,
                        "Experimental: Enable statistics calculator",
                        featuresConfig.isEnableStatsCalculator(),
                        false),
                new PropertyMetadata<>(
                        MAX_DRIVERS_PER_TASK,
                        "Maximum number of drivers per task",
                        INTEGER,
                        Integer.class,
                        null,
                        false,
                        value -> min(taskManagerConfig.getMaxDriversPerTask(), validateNullablePositiveIntegerValue(value, MAX_DRIVERS_PER_TASK)),
                        object -> object),
                booleanProperty(
                        IGNORE_STATS_CALCULATOR_FAILURES,
                        "Ignore statistics calculator failures",
                        featuresConfig.isIgnoreStatsCalculatorFailures(),
                        false),
                booleanProperty(
                        DEFAULT_FILTER_FACTOR_ENABLED,
                        "use a default filter factor for unknown filters in a filter node",
                        featuresConfig.isDefaultFilterFactorEnabled(),
                        false),
                booleanProperty(
                        ENABLE_CROSS_REGION_DYNAMIC_FILTER,
                        "Enable cross region dynamic filtering",
                        false,
                        true),
                booleanProperty(
                        UNWRAP_CASTS,
                        "Enable optimization to unwrap CAST expression",
                        featuresConfig.isUnwrapCasts(),
                        false),
                booleanProperty(
                        SKIP_REDUNDANT_SORT,
                        "Skip redundant sort operations",
                        featuresConfig.isSkipRedundantSort(),
                        false),
                booleanProperty(
                        PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES,
                        "Use table properties in predicate pushdown",
                        featuresConfig.isPredicatePushdownUseTableProperties(),
                        false),
                booleanProperty(
                        WORK_PROCESSOR_PIPELINES,
                        "Experimental: Use WorkProcessor pipelines",
                        featuresConfig.isWorkProcessorPipelines(),
                        false),
                booleanProperty(
                        ENABLE_DYNAMIC_FILTERING,
                        "Enable dynamic filtering",
                        featuresConfig.isEnableDynamicFiltering(),
                        false),
                durationProperty(
                        DYNAMIC_FILTERING_WAIT_TIME,
                        "Maximum waiting time for dynamic filter to be ready",
                        featuresConfig.getDynamicFilteringWaitTime(),
                        false),
                integerProperty(
                        DYNAMIC_FILTERING_MAX_SIZE,
                        "Maximum number of build-side rows to be collected for each dynamic filter",
                        featuresConfig.getDynamicFilteringMaxSize(),
                        false),
                integerProperty(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_VALUE_COUNT,
                        "Maximum number of build-side rows to be collected for dynamic filtering per-driver",
                        featuresConfig.getDynamicFilteringMaxPerDriverRowCount(),
                        false),
                enumProperty(
                        DYNAMIC_FILTERING_DATA_TYPE,
                        "Data type for the dynamic filter (BLOOM_FILTER or HASHSET)",
                        DynamicFilterDataType.class,
                        featuresConfig.getDynamicFilteringDataType(),
                        false),
                dataSizeProperty(
                        DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE,
                        "Maximum number of bytes to be collected for dynamic filtering per-driver",
                        featuresConfig.getDynamicFilteringMaxPerDriverSize(),
                        false),
                doubleProperty(
                        DYNAMIC_FILTERING_BLOOM_FILTER_FPP,
                        "Expected FPP for BloomFilter which is used in dynamic filtering",
                        featuresConfig.getDynamicFilteringBloomFilterFpp(),
                        false),
                booleanProperty(
                        OPTIMIZE_DYNAMIC_FILTER_GENERATION,
                        "Generate dynamic filters based on the selectivity",
                        true,
                        false),
                booleanProperty(
                        ENABLE_EXECUTION_PLAN_CACHE,
                        "Enable execution plan caching",
                        featuresConfig.isEnableExecutionPlanCache(),
                        false),
                booleanProperty(
                        ENABLE_HEURISTICINDEX_FILTER,
                        "Enable heuristic index filter",
                        hetuConfig.isFilterEnabled(),
                        false),
                booleanProperty(
                        PUSH_TABLE_THROUGH_SUBQUERY,
                        "Allow pushing outer tables into subqueries if there is a join between the two",
                        featuresConfig.isPushTableThroughSubquery(),
                        false),
                booleanProperty(
                        REUSE_TABLE_SCAN,
                        "Reuse data cached by similar table scan",
                        featuresConfig.isReuseTableScanEnabled(),
                        false),
                booleanProperty(
                        SPILL_REUSE_TABLESCAN,
                        "Spill in TableScanOperator and WorkProcessorSourceOperatorAdapter if spill_enabled is also set",
                        featuresConfig.isSpillReuseExchange(),
                        false),
                integerProperty(
                        SPILL_THRESHOLD_REUSE_TABLESCAN,
                        "Spiller Threshold (in MB) for TableScanOperator and WorkProcessorSourceOperatorAdapter for Reuse Exchange",
                        featuresConfig.getSpillOperatorThresholdReuseExchange(),
                        false),
                booleanProperty(
                        CTE_REUSE_ENABLED,
                        "Enabled CTE reuse",
                        featuresConfig.isCteReuseEnabled(),
                        false),
                integerProperty(
                        CTE_MAX_QUEUE_SIZE,
                        "Max queue size to store cte data (for every cte reference)",
                        featuresConfig.getMaxQueueSize(),
                        false),
                integerProperty(
                        CTE_MAX_PREFETCH_QUEUE_SIZE,
                        "Max prefetch queue size",
                        featuresConfig.getMaxPrefetchQueueSize(),
                        false),
                booleanProperty(
                        ENABLE_STAR_TREE_INDEX,
                        "Enable star-tree index",
                        featuresConfig.isEnableStarTreeIndex(),
                        false),
                booleanProperty(
                        LIST_BUILT_IN_FUNCTIONS_ONLY,
                        "Only List built-in functions in SHOW FUNCTIONS",
                        featuresConfig.isListBuiltInFunctionsOnly(),
                        false),
                booleanProperty(
                        SNAPSHOT_ENABLED,
                        "Enable query snapshoting",
                        false,
                        false),
                enumProperty(
                        SNAPSHOT_INTERVAL_TYPE,
                        "Snapshot interval type",
                        SnapshotConfig.IntervalType.class,
                        snapshotConfig.getSnapshotIntervalType(),
                        false),
                durationProperty(
                        SNAPSHOT_TIME_INTERVAL,
                        "Snapshot time interval",
                        snapshotConfig.getSnapshotTimeInterval(),
                        false),
                longProperty(
                        SNAPSHOT_SPLIT_COUNT_INTERVAL,
                        "snapshot split count interval",
                        snapshotConfig.getSnapshotSplitCountInterval(),
                        false),
                longProperty(
                        SNAPSHOT_MAX_RETRIES,
                        "snapshot max retries",
                        snapshotConfig.getSnapshotMaxRetries(),
                        false),
                durationProperty(
                        SNAPSHOT_RETRY_TIMEOUT,
                        "Snapshot retry timeout",
                        snapshotConfig.getSnapshotRetryTimeout(),
                        false),
                booleanProperty(
                        SORT_BASED_AGGREGATION_ENABLED,
                        "Enable sort based aggregation",
                        featuresConfig.isSortBasedAggregationEnabled(),
                        false),
                integerProperty(
                        PRCNT_DRIVERS_FOR_PARTIAL_AGGR,
                        "Sort based aggr, percentage of number of drivers that are used for not finalized values",
                        featuresConfig.getPrcntDriversForPartialAggr(),
                        false),
                booleanProperty(
                        SKIP_ATTACHING_STATS_WITH_PLAN,
                        "Whether to calculate stats and attach with final plan",
                        featuresConfig.isSkipAttachingStatsWithPlan(),
                        false),
                booleanProperty(
                        SKIP_NON_APPLICABLE_RULES_ENABLED,
                        "Whether to skip applying some selected rules based on query pattern",
                        featuresConfig.isSkipNonApplicableRulesEnabled(),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isCrossRegionDynamicFilterEnabled(Session session)
    {
        return session.getSystemProperty(ENABLE_CROSS_REGION_DYNAMIC_FILTER, Boolean.class);
    }

    public static String getExecutionPolicy(Session session)
    {
        return session.getSystemProperty(EXECUTION_POLICY, String.class);
    }

    public static boolean isOptimizeHashGenerationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_HASH_GENERATION, Boolean.class);
    }

    public static boolean isQueryPushDown(Session session)
    {
        return session.getSystemProperty(QUERY_PUSHDOWN, Boolean.class);
    }

    public static boolean isImplicitConversionEnabled(Session session)
    {
        return session.getSystemProperty(IMPLICIT_CONVERSION, Boolean.class);
    }

    public static boolean isRewriteFilteringSemiJoinToInnerJoin(Session session)
    {
        return session.getSystemProperty(FILTERING_SEMI_JOIN_TO_INNER, Boolean.class);
    }

    public static String getJoinOrder(Session session)
    {
        return session.getSystemProperty(JOIN_ORDER, String.class);
    }

    public static boolean isLimitPushDown(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_DOWN, Boolean.class);
    }

    public static JoinDistributionType getJoinDistributionType(Session session)
    {
        return session.getSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.class);
    }

    public static Optional<DataSize> getJoinMaxBroadcastTableSize(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, DataSize.class));
    }

    public static boolean isDistributedIndexJoinEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_INDEX_JOIN, Boolean.class);
    }

    public static int getHashPartitionCount(Session session)
    {
        return session.getSystemProperty(HASH_PARTITION_COUNT, Integer.class);
    }

    public static boolean isGroupedExecutionEnabled(Session session)
    {
        return session.getSystemProperty(GROUPED_EXECUTION, Boolean.class);
    }

    public static boolean isDynamicSchduleForGroupedExecution(Session session)
    {
        return session.getSystemProperty(DYNAMIC_SCHEDULE_FOR_GROUPED_EXECUTION, Boolean.class);
    }

    public static boolean preferStreamingOperators(Session session)
    {
        return session.getSystemProperty(PREFER_STREAMING_OPERATORS, Boolean.class);
    }

    public static int getTaskWriterCount(Session session)
    {
        return session.getSystemProperty(TASK_WRITER_COUNT, Integer.class);
    }

    public static boolean isRedistributeWrites(Session session)
    {
        return session.getSystemProperty(REDISTRIBUTE_WRITES, Boolean.class);
    }

    // redistribute writes type getter
    public static RedistributeWritesType getRedistributeWritesType(Session session)
    {
        return session.getSystemProperty(REDISTRIBUTE_WRITES_TYPE, RedistributeWritesType.class);
    }

    public static boolean isScaleWriters(Session session)
    {
        return session.getSystemProperty(SCALE_WRITERS, Boolean.class);
    }

    public static DataSize getWriterMinSize(Session session)
    {
        return session.getSystemProperty(WRITER_MIN_SIZE, DataSize.class);
    }

    public static boolean isPushTableWriteThroughUnion(Session session)
    {
        return session.getSystemProperty(PUSH_TABLE_WRITE_THROUGH_UNION, Boolean.class);
    }

    public static boolean isPushLimitThroughUnion(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_THROUGH_UNION, Boolean.class);
    }

    public static boolean isPushLimitThroughSemiJoin(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_THROUGH_SEMI_JOIN, Boolean.class);
    }

    public static boolean isPushLimitThroughOuterJoin(Session session)
    {
        return session.getSystemProperty(PUSH_LIMIT_THROUGH_OUTER_JOIN, Boolean.class);
    }

    public static int getTaskConcurrency(Session session)
    {
        return session.getSystemProperty(TASK_CONCURRENCY, Integer.class);
    }

    public static boolean isShareIndexLoading(Session session)
    {
        return session.getSystemProperty(TASK_SHARE_INDEX_LOADING, Boolean.class);
    }

    public static boolean isDictionaryAggregationEnabled(Session session)
    {
        return session.getSystemProperty(DICTIONARY_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeMetadataQueries(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_METADATA_QUERIES, Boolean.class);
    }

    public static DataSize getQueryMaxMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_MEMORY, DataSize.class);
    }

    public static DataSize getQueryMaxTotalMemory(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_TOTAL_MEMORY, DataSize.class);
    }

    public static Duration getQueryMaxRunTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_RUN_TIME, Duration.class);
    }

    public static Duration getQueryMaxExecutionTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_EXECUTION_TIME, Duration.class);
    }

    public static boolean resourceOvercommit(Session session)
    {
        return session.getSystemProperty(RESOURCE_OVERCOMMIT, Boolean.class);
    }

    public static int getQueryMaxStageCount(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_STAGE_COUNT, Integer.class);
    }

    public static boolean planWithTableNodePartitioning(Session session)
    {
        return session.getSystemProperty(PLAN_WITH_TABLE_NODE_PARTITIONING, Boolean.class);
    }

    public static boolean isFastInequalityJoin(Session session)
    {
        return session.getSystemProperty(FAST_INEQUALITY_JOINS, Boolean.class);
    }

    public static JoinReorderingStrategy getJoinReorderingStrategy(Session session)
    {
        Boolean reorderJoins = session.getSystemProperty(REORDER_JOINS, Boolean.class);
        if (reorderJoins != null) {
            if (!reorderJoins) {
                return NONE;
            }
            return ELIMINATE_CROSS_JOINS;
        }
        return session.getSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.class);
    }

    public static int getMaxReorderedJoins(Session session)
    {
        return session.getSystemProperty(MAX_REORDERED_JOINS, Integer.class);
    }

    public static int getSkipReorderingThreshold(Session session)
    {
        return session.getSystemProperty(SKIP_REORDERING_THRESHOLD, Integer.class);
    }

    public static boolean isColocatedJoinEnabled(Session session)
    {
        return session.getSystemProperty(COLOCATED_JOIN, Boolean.class);
    }

    public static boolean isSpatialJoinEnabled(Session session)
    {
        return session.getSystemProperty(SPATIAL_JOIN, Boolean.class);
    }

    public static Optional<String> getSpatialPartitioningTableName(Session session)
    {
        return Optional.ofNullable(session.getSystemProperty(SPATIAL_PARTITIONING_TABLE_NAME, String.class));
    }

    public static OptionalInt getConcurrentLifespansPerNode(Session session)
    {
        Integer result = session.getSystemProperty(CONCURRENT_LIFESPANS_PER_NODE, Integer.class);
        if (result == 0) {
            return OptionalInt.empty();
        }
        else {
            checkArgument(result > 0, "Concurrent lifespans per node must be positive if set to non-zero");
            return OptionalInt.of(result);
        }
    }

    public static int getInitialSplitsPerNode(Session session)
    {
        return session.getSystemProperty(INITIAL_SPLITS_PER_NODE, Integer.class);
    }

    public static int getQueryPriority(Session session)
    {
        Integer priority = session.getSystemProperty(QUERY_PRIORITY, Integer.class);
        checkArgument(priority > 0, "Query priority must be positive");
        return priority;
    }

    public static Duration getSplitConcurrencyAdjustmentInterval(Session session)
    {
        return session.getSystemProperty(SPLIT_CONCURRENCY_ADJUSTMENT_INTERVAL, Duration.class);
    }

    public static Duration getQueryMaxCpuTime(Session session)
    {
        return session.getSystemProperty(QUERY_MAX_CPU_TIME, Duration.class);
    }

    public static boolean isSpillEnabled(Session session)
    {
        return session.getSystemProperty(SPILL_ENABLED, Boolean.class);
    }

    public static boolean isSpillForOuterJoinEnabled(Session session)
    {
        return session.getSystemProperty(SPILL_OUTER_JOIN_ENABLED, Boolean.class);
    }

    public static boolean isInnerJoinSpillFilteringEnabled(Session session)
    {
        return session.getSystemProperty(INNER_JOIN_SPILL_FILTER_ENABLED, Boolean.class);
    }

    public static boolean isNonBlockingSpillOrderby(Session session)
    {
        return session.getSystemProperty(SPILL_NON_BLOCKING_ORDERBY, Boolean.class);
    }

    public static boolean isSpillOrderBy(Session session)
    {
        return session.getSystemProperty(SPILL_ORDER_BY, Boolean.class);
    }

    public static boolean isSpillWindowOperator(Session session)
    {
        return session.getSystemProperty(SPILL_WINDOW_OPERATOR, Boolean.class);
    }

    public static DataSize getAggregationOperatorUnspillMemoryLimit(Session session)
    {
        DataSize memoryLimitForMerge = session.getSystemProperty(AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT, DataSize.class);
        checkArgument(memoryLimitForMerge.toBytes() >= 0, "%s must be positive", AGGREGATION_OPERATOR_UNSPILL_MEMORY_LIMIT);
        return memoryLimitForMerge;
    }

    public static boolean isOptimizeDistinctAggregationEnabled(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DISTINCT_AGGREGATIONS, Boolean.class);
    }

    public static boolean isNewOptimizerEnabled(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER, Boolean.class);
    }

    public static Duration getOptimizerTimeout(Session session)
    {
        return session.getSystemProperty(ITERATIVE_OPTIMIZER_TIMEOUT, Duration.class);
    }

    public static boolean isEnableForcedExchangeBelowGroupId(Session session)
    {
        return session.getSystemProperty(ENABLE_FORCED_EXCHANGE_BELOW_GROUP_ID, Boolean.class);
    }

    public static boolean isExchangeCompressionEnabled(Session session)
    {
        return session.getSystemProperty(EXCHANGE_COMPRESSION, Boolean.class);
    }

    public static boolean isEnableIntermediateAggregations(Session session)
    {
        return session.getSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, Boolean.class);
    }

    public static boolean shouldPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isPushAggregationThroughJoin(Session session)
    {
        return session.getSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, Boolean.class);
    }

    public static boolean isParseDecimalLiteralsAsDouble(Session session)
    {
        return session.getSystemProperty(PARSE_DECIMAL_LITERALS_AS_DOUBLE, Boolean.class);
    }

    public static boolean isForceSingleNodeOutput(Session session)
    {
        return session.getSystemProperty(FORCE_SINGLE_NODE_OUTPUT, Boolean.class);
    }

    public static DataSize getFilterAndProjectMinOutputPageSize(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_SIZE, DataSize.class);
    }

    public static int getFilterAndProjectMinOutputPageRowCount(Session session)
    {
        return session.getSystemProperty(FILTER_AND_PROJECT_MIN_OUTPUT_PAGE_ROW_COUNT, Integer.class);
    }

    public static boolean useMarkDistinct(Session session)
    {
        return session.getSystemProperty(USE_MARK_DISTINCT, Boolean.class);
    }

    public static boolean preferPartialAggregation(Session session)
    {
        return session.getSystemProperty(PREFER_PARTIAL_AGGREGATION, Boolean.class);
    }

    public static boolean isOptimizeTopNRankingNumber(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_TOP_N_RANKING_NUMBER, Boolean.class);
    }

    public static boolean isDistributedSortEnabled(Session session)
    {
        return session.getSystemProperty(DISTRIBUTED_SORT, Boolean.class);
    }

    public static int getMaxGroupingSets(Session session)
    {
        return session.getSystemProperty(MAX_GROUPING_SETS, Integer.class);
    }

    public static OptionalInt getMaxDriversPerTask(Session session)
    {
        Integer value = session.getSystemProperty(MAX_DRIVERS_PER_TASK, Integer.class);
        if (value == null) {
            return OptionalInt.empty();
        }
        return OptionalInt.of(value);
    }

    private static int validateValueIsPowerOfTwo(Object value, String property)
    {
        int intValue = ((Number) requireNonNull(value, "value is null")).intValue();
        if (Integer.bitCount(intValue) != 1) {
            throw new PrestoException(
                    INVALID_SESSION_PROPERTY,
                    format("%s must be a power of 2: %s", property, intValue));
        }
        return intValue;
    }

    private static Integer validateNullablePositiveIntegerValue(Object value, String property)
    {
        return validateIntegerValue(value, property, 1, true);
    }

    private static Integer validateIntegerValue(Object value, String property, int lowerBoundIncluded, boolean allowNull)
    {
        if (value == null && !allowNull) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be non-null", property));
        }

        if (value == null) {
            return null;
        }

        int intValue = ((Number) value).intValue();
        if (intValue < lowerBoundIncluded) {
            throw new PrestoException(INVALID_SESSION_PROPERTY, format("%s must be equal or greater than %s", property, lowerBoundIncluded));
        }
        return intValue;
    }

    public static boolean isStatisticsCpuTimerEnabled(Session session)
    {
        return session.getSystemProperty(STATISTICS_CPU_TIMER_ENABLED, Boolean.class);
    }

    public static boolean isEnableStatsCalculator(Session session)
    {
        return session.getSystemProperty(ENABLE_STATS_CALCULATOR, Boolean.class);
    }

    public static boolean isIgnoreStatsCalculatorFailures(Session session)
    {
        return session.getSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, Boolean.class);
    }

    public static boolean isDefaultFilterFactorEnabled(Session session)
    {
        return session.getSystemProperty(DEFAULT_FILTER_FACTOR_ENABLED, Boolean.class);
    }

    public static boolean isUnwrapCasts(Session session)
    {
        return session.getSystemProperty(UNWRAP_CASTS, Boolean.class);
    }

    public static boolean isSkipRedundantSort(Session session)
    {
        return session.getSystemProperty(SKIP_REDUNDANT_SORT, Boolean.class);
    }

    public static boolean isPredicatePushdownUseTableProperties(Session session)
    {
        return session.getSystemProperty(PREDICATE_PUSHDOWN_USE_TABLE_PROPERTIES, Boolean.class);
    }

    public static boolean isWorkProcessorPipelines(Session session)
    {
        return session.getSystemProperty(WORK_PROCESSOR_PIPELINES, Boolean.class);
    }

    public static boolean isEnableDynamicFiltering(Session session)
    {
        return session.getSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.class);
    }

    public static int getDynamicFilteringMaxSize(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_MAX_SIZE, Integer.class);
    }

    public static int getDynamicFilteringMaxPerDriverValueCount(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_MAX_PER_DRIVER_VALUE_COUNT, Integer.class);
    }

    public static DynamicFilterDataType getDynamicFilteringDataType(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_DATA_TYPE, DynamicFilterDataType.class);
    }

    public static DataSize getDynamicFilteringMaxPerDriverSize(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_MAX_PER_DRIVER_SIZE, DataSize.class);
    }

    public static Duration getDynamicFilteringWaitTime(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_WAIT_TIME, Duration.class);
    }

    public static double getDynamicFilteringBloomFilterFpp(Session session)
    {
        return session.getSystemProperty(DYNAMIC_FILTERING_BLOOM_FILTER_FPP, Double.class);
    }

    public static boolean isOptimizeDynamicFilterGeneration(Session session)
    {
        return session.getSystemProperty(OPTIMIZE_DYNAMIC_FILTER_GENERATION, Boolean.class);
    }

    public static boolean isExecutionPlanCacheEnabled(Session session)
    {
        return session.getSystemProperty(ENABLE_EXECUTION_PLAN_CACHE, Boolean.class);
    }

    public static boolean isHeuristicIndexFilterEnabled(Session session)
    {
        return session.getSystemProperty(ENABLE_HEURISTICINDEX_FILTER, Boolean.class);
    }

    public static boolean shouldEnableTablePushdown(Session session)
    {
        return session.getSystemProperty(PUSH_TABLE_THROUGH_SUBQUERY, Boolean.class);
    }

    public static boolean isReuseTableScanEnabled(Session session)
    {
        return session.getSystemProperty(REUSE_TABLE_SCAN, Boolean.class);
    }

    public static boolean isSpillReuseExchange(Session session)
    {
        return session.getSystemProperty(SPILL_REUSE_TABLESCAN, Boolean.class);
    }

    public static int getSpillOperatorThresholdReuseExchange(Session session)
    {
        return session.getSystemProperty(SPILL_THRESHOLD_REUSE_TABLESCAN, Integer.class);
    }

    public static boolean isCTEReuseEnabled(Session session)
    {
        return session.getSystemProperty(CTE_REUSE_ENABLED, Boolean.class);
    }

    public static int getCteMaxPrefetchQueueSize(Session session)
    {
        return session.getSystemProperty(CTE_MAX_PREFETCH_QUEUE_SIZE, Integer.class);
    }

    public static int getCteMaxQueueSize(Session session)
    {
        return session.getSystemProperty(CTE_MAX_QUEUE_SIZE, Integer.class);
    }

    public static boolean isEnableStarTreeIndex(Session session)
    {
        return session.getSystemProperty(ENABLE_STAR_TREE_INDEX, Boolean.class);
    }

    public static boolean isListBuiltInFunctionsOnly(Session session)
    {
        return session.getSystemProperty(LIST_BUILT_IN_FUNCTIONS_ONLY, Boolean.class);
    }

    public static boolean isSnapshotEnabled(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_ENABLED, Boolean.class);
    }

    public static SnapshotConfig.IntervalType getSnapshotIntervalType(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_INTERVAL_TYPE, SnapshotConfig.IntervalType.class);
    }

    public static Duration getSnapshotTimeInterval(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_TIME_INTERVAL, Duration.class);
    }

    public static long getSnapshotSplitCountInterval(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_SPLIT_COUNT_INTERVAL, Long.class);
    }

    public static long getSnapshotMaxRetries(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_MAX_RETRIES, Long.class);
    }

    public static Duration getSnapshotRetryTimeout(Session session)
    {
        return session.getSystemProperty(SNAPSHOT_RETRY_TIMEOUT, Duration.class);
    }

    public static boolean isSortBasedAggregationEnabled(Session session)
    {
        return session.getSystemProperty(SORT_BASED_AGGREGATION_ENABLED, Boolean.class);
    }

    public static int getPrcntDriversForPartialAggr(Session session)
    {
        return session.getSystemProperty(PRCNT_DRIVERS_FOR_PARTIAL_AGGR, Integer.class);
    }

    public static boolean isSkipAttachingStatsWithPlan(Session session)
    {
        return session.getSystemProperty(SKIP_ATTACHING_STATS_WITH_PLAN, Boolean.class);
    }

    public static boolean isSkipNonApplicableRulesEnabled(Session session)
    {
        return session.getSystemProperty(SKIP_NON_APPLICABLE_RULES_ENABLED, Boolean.class);
    }
}
