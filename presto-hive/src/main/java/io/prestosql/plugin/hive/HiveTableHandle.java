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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.prestosql.plugin.hive.HiveMetadata.STORAGE_FORMAT;
import static java.util.Objects.requireNonNull;

public class HiveTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final Optional<Map<String, String>> tableParameters;
    private final List<HiveColumnHandle> partitionColumns;
    private final Optional<List<HivePartition>> partitions;
    private final TupleDomain<HiveColumnHandle> compactEffectivePredicate;
    private final TupleDomain<ColumnHandle> enforcedConstraint;
    private final Optional<HiveBucketHandle> bucketHandle;
    private final Optional<HiveBucketing.HiveBucketFilter> bucketFilter;
    private final Optional<List<List<String>>> analyzePartitionValues;
    private final Map<String, HiveColumnHandle> predicateColumns;
    private final Optional<List<TupleDomain<HiveColumnHandle>>> disjunctCompactEffectivePredicate;
    private final boolean suitableToPush;
//    private final RowExpression remainingPredicate; //For Complex Expression.

    @JsonCreator
    public HiveTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("compactEffectivePredicate") TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            @JsonProperty("enforcedConstraint") TupleDomain<ColumnHandle> enforcedConstraint,
            @JsonProperty("bucketHandle") Optional<HiveBucketHandle> bucketHandle,
            @JsonProperty("bucketFilter") Optional<HiveBucketing.HiveBucketFilter> bucketFilter,
            @JsonProperty("analyzePartitionValues") Optional<List<List<String>>> analyzePartitionValues,
            @JsonProperty("predicateColumns") Map<String, HiveColumnHandle> predicateColumns,
            @JsonProperty("additionaPredicates") Optional<List<TupleDomain<HiveColumnHandle>>> disjunctCompactEffectivePredicate,
            @JsonProperty("suitableToPush") boolean suitableToPush)
    {
        this(
                schemaName,
                tableName,
                Optional.empty(),
                partitionColumns,
                Optional.empty(),
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                predicateColumns,
                disjunctCompactEffectivePredicate,
                suitableToPush);
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            Map<String, String> tableParameters,
            List<HiveColumnHandle> partitionColumns,
            Optional<HiveBucketHandle> bucketHandle)
    {
        this(
                schemaName,
                tableName,
                Optional.of(tableParameters),
                partitionColumns,
                Optional.empty(),
                TupleDomain.all(),
                TupleDomain.all(),
                bucketHandle,
                Optional.empty(),
                Optional.empty(),
                null,
                Optional.empty(),
                false);
    }

    public HiveTableHandle(
            String schemaName,
            String tableName,
            Optional<Map<String, String>> tableParameters,
            List<HiveColumnHandle> partitionColumns,
            Optional<List<HivePartition>> partitions,
            TupleDomain<HiveColumnHandle> compactEffectivePredicate,
            TupleDomain<ColumnHandle> enforcedConstraint,
            Optional<HiveBucketHandle> bucketHandle,
            Optional<HiveBucketing.HiveBucketFilter> bucketFilter,
            Optional<List<List<String>>> analyzePartitionValues,
            Map<String, HiveColumnHandle> predicateColumns,
            Optional<List<TupleDomain<HiveColumnHandle>>> disjunctCompactEffectivePredicate,
            boolean suitableToPush)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableParameters = requireNonNull(tableParameters, "tableParameters is null").map(ImmutableMap::copyOf);
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.partitions = requireNonNull(partitions, "partitions is null").map(ImmutableList::copyOf);
        this.compactEffectivePredicate = requireNonNull(compactEffectivePredicate, "compactEffectivePredicate is null");
        this.enforcedConstraint = requireNonNull(enforcedConstraint, "enforcedConstraint is null");
        this.bucketHandle = requireNonNull(bucketHandle, "bucketHandle is null");
        this.bucketFilter = requireNonNull(bucketFilter, "bucketFilter is null");
        this.analyzePartitionValues = requireNonNull(analyzePartitionValues, "analyzePartitionValues is null");
        this.predicateColumns = predicateColumns;
        this.disjunctCompactEffectivePredicate = requireNonNull(disjunctCompactEffectivePredicate, "disjunctCompactEffectivePredicate is null");
        this.suitableToPush = suitableToPush;
    }

    public HiveTableHandle withAnalyzePartitionValues(Optional<List<List<String>>> analyzePartitionValues)
    {
        return new HiveTableHandle(
                schemaName,
                tableName,
                tableParameters,
                partitionColumns,
                partitions,
                compactEffectivePredicate,
                enforcedConstraint,
                bucketHandle,
                bucketFilter,
                analyzePartitionValues,
                predicateColumns,
                Optional.empty(),
                suitableToPush);
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    // do not serialize tableParameters as they are not needed on workers
    @JsonIgnore
    public Optional<Map<String, String>> getTableParameters()
    {
        return tableParameters;
    }

    //hetu: add implementation to get qualified name
    @Override
    public String getSchemaPrefixedTableName()
    {
        return String.format("%s.%s", schemaName, tableName);
    }

    //hetu: return true for isFilterSupported for hive connector
    @Override
    public boolean isFilterSupported()
    {
        return true;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    // do not serialize partitions as they are not needed on workers
    @JsonIgnore
    public Optional<List<HivePartition>> getPartitions()
    {
        return partitions;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getCompactEffectivePredicate()
    {
        return compactEffectivePredicate;
    }

    @JsonProperty
    public Map<String, HiveColumnHandle> getPredicateColumns()
    {
        return predicateColumns;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getEnforcedConstraint()
    {
        return enforcedConstraint;
    }

    @JsonProperty
    public Optional<HiveBucketHandle> getBucketHandle()
    {
        return bucketHandle;
    }

    @JsonProperty
    public Optional<HiveBucketing.HiveBucketFilter> getBucketFilter()
    {
        return bucketFilter;
    }

    @JsonProperty
    public Optional<List<List<String>>> getAnalyzePartitionValues()
    {
        return analyzePartitionValues;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public Optional<List<TupleDomain<HiveColumnHandle>>> getDisjunctCompactEffectivePredicate()
    {
        return disjunctCompactEffectivePredicate;
    }

    @JsonProperty
    public boolean isSuitableToPush()
    {
        return suitableToPush;
    }

    /**
     * Hetu execution plan caching functionality requires a method to update
     * {@link ConnectorTableHandle} from a previous execution plan with new info from
     * a new query. This is a workaround for hetu-main module to modify jdbc connector table
     * handles in a generic way without needing access to classes in hetu-hive package or
     * knowledge of connector specific constructor implementations. Connectors must override this
     * method to support execution plan caching.
     *
     * @param oldConnectorTableHandle connector table handle containing information
     * to be passed into a new {@link HiveTableHandle}
     * @return new {@link HiveTableHandle} containing the constraints, limit,
     * and subquery from an old {@link HiveTableHandle}
     */
    @Override
    public ConnectorTableHandle createFrom(ConnectorTableHandle oldConnectorTableHandle)
    {
        HiveTableHandle oldHiveConnectorTableHandle = (HiveTableHandle) oldConnectorTableHandle;
        return new HiveTableHandle(oldHiveConnectorTableHandle.getSchemaName(),
                oldHiveConnectorTableHandle.getTableName(),
                oldHiveConnectorTableHandle.getTableParameters(),
                oldHiveConnectorTableHandle.getPartitionColumns(),
                oldHiveConnectorTableHandle.getPartitions(),
                oldHiveConnectorTableHandle.getCompactEffectivePredicate(),
                oldHiveConnectorTableHandle.getEnforcedConstraint(),
                oldHiveConnectorTableHandle.getBucketHandle(),
                oldHiveConnectorTableHandle.getBucketFilter(),
                oldHiveConnectorTableHandle.getAnalyzePartitionValues(),
                oldHiveConnectorTableHandle.getPredicateColumns(),
                oldHiveConnectorTableHandle.getDisjunctCompactEffectivePredicate(),
                oldHiveConnectorTableHandle.isSuitableToPush());
    }

    @Override
    public boolean hasDisjunctFiltersPushdown()
    {
        return disjunctCompactEffectivePredicate.isPresent()
                && disjunctCompactEffectivePredicate.get().size() > 0;
    }

    private String formatPredicate(Function<Domain, String> printer, TupleDomain<HiveColumnHandle> predicate)
    {
        return predicate.getDomains().get().entrySet().stream()
                .map(filter -> filter.getKey().getColumnName() + " <- " + printer.apply(filter.getValue()))
                .collect(Collectors.joining(" AND ", "{", "}"));
    }

    @Override
    public String getDisjunctFilterConditions(Function<Domain, String> printer)
    {
        if (disjunctCompactEffectivePredicate.isPresent()) {
            return disjunctCompactEffectivePredicate.get().stream()
                    .map(predicate -> "[ " + formatPredicate(printer, predicate) + " ]")
                    .collect(Collectors.joining(" OR "));
        }

        return "";
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveTableHandle that = (HiveTableHandle) o;
        return Objects.equals(schemaName, that.schemaName) &&
                Objects.equals(tableName, that.tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(schemaName).append(":").append(tableName);
        bucketHandle.ifPresent(bucket ->
                builder.append(" bucket=").append(bucket.getReadBucketCount()));
        return builder.toString();
    }

    @Override
    public boolean isDeleteAsInsertSupported()
    {
        return AcidUtils.isTransactionalTable(getTableParameters().get()) && !AcidUtils.isInsertOnlyTable(getTableParameters().get());
    }

    @Override
    public boolean isSuitableForPushdown()
    {
        return this.suitableToPush;
    }

    @Override
    public boolean isTableCacheable()
    {
        return HiveStorageFormat.ORC.getOutputFormat().equals(tableParameters.get().get(STORAGE_FORMAT));
    }

    /**
     * ORC is the only format supported to create heuristic index now
     * We will add more formats in the future.
     */
    @Override
    public boolean isHeuristicIndexSupported()
    {
        return Stream.of(HiveStorageFormat.ORC)
                .anyMatch(storageFormat -> storageFormat.getOutputFormat().equals(tableParameters.get().get(STORAGE_FORMAT)));
    }

    /**
     * Create heuristic index... where predicate = xxx
     * The predicate column only support partition columns
     */
    @Override
    public boolean isPartitionColumn(String column)
    {
        return partitionColumns.stream().map(HiveColumnHandle::getColumnName).collect(Collectors.toSet()).contains(column);
    }

    /* This method checks if reuse table scan can be used*/
    @Override
    public boolean isReuseTableScanSupported()
    {
        return true;
    }
}
