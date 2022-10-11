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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.hetu.core.plugin.iceberg.delete.TrinoDeleteFile;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.SplitWeight;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionSpecParser;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.util.TableScanUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Suppliers.memoize;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.hetu.core.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.hetu.core.plugin.iceberg.IcebergErrorCode.ICEBERG_INVALID_METADATA;
import static io.hetu.core.plugin.iceberg.IcebergSplitManager.ICEBERG_DOMAIN_COMPACTION_THRESHOLD;
import static io.hetu.core.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.hetu.core.plugin.iceberg.IcebergUtil.deserializePartitionValue;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getColumnHandle;
import static io.hetu.core.plugin.iceberg.IcebergUtil.getPartitionKeys;
import static io.hetu.core.plugin.iceberg.IcebergUtil.primitiveFieldTypes;
import static io.hetu.core.plugin.iceberg.TypeConverter.toIcebergType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.iceberg.types.Conversions.fromByteBuffer;

public class IcebergSplitSource
        implements ConnectorSplitSource
{
    private static final ConnectorSplitBatch EMPTY_BATCH = new ConnectorSplitBatch(ImmutableList.of(), false);
    private static final ConnectorSplitBatch NO_MORE_SPLITS_BATCH = new ConnectorSplitBatch(ImmutableList.of(), true);

    private final IcebergTableHandle tableHandle;
    private final TableScan tableScan;
    private final Optional<Long> maxScannedFileSizeInBytes;
    private final Map<Integer, Type.PrimitiveType> fieldIdToType;
    private final DynamicFilter dynamicFilter;
    private final long dynamicFilteringWaitTimeoutMillis;
    private final Stopwatch dynamicFilterWaitStopwatch;
    private final Constraint constraint;
    private final TypeManager typeManager;
    private final Closer closer = Closer.create();
    private final double minimumAssignedSplitWeight;

    private CloseableIterable<FileScanTask> fileScanTaskIterable;
    private CloseableIterator<FileScanTask> fileScanTaskIterator;
    private TupleDomain<IcebergColumnHandle> pushedDownDynamicFilterPredicate;

    private final boolean recordScannedFiles;
    private final ImmutableSet.Builder<DataFile> scannedFiles = ImmutableSet.builder();

    public IcebergSplitSource(
            IcebergTableHandle tableHandle,
            TableScan tableScan,
            Optional<DataSize> maxScannedFileSize,
            DynamicFilter dynamicFilter,
            Duration dynamicFilteringWaitTimeout,
            Constraint constraint,
            TypeManager typeManager,
            boolean recordScannedFiles,
            double minimumAssignedSplitWeight)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.tableScan = requireNonNull(tableScan, "tableScan is null");
        this.maxScannedFileSizeInBytes = requireNonNull(maxScannedFileSize, "maxScannedFileSize is null").map(DataSize::toBytes);
        this.fieldIdToType = primitiveFieldTypes(tableScan.schema());
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.dynamicFilteringWaitTimeoutMillis = requireNonNull(dynamicFilteringWaitTimeout, "dynamicFilteringWaitTimeout is null").toMillis();
        this.dynamicFilterWaitStopwatch = Stopwatch.createStarted();
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.recordScannedFiles = recordScannedFiles;
        this.minimumAssignedSplitWeight = minimumAssignedSplitWeight;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        long timeLeft = dynamicFilteringWaitTimeoutMillis - dynamicFilterWaitStopwatch.elapsed(MILLISECONDS);
        if (dynamicFilter.isAwaitable() && timeLeft > 0) {
            return null;
        }

        if (fileScanTaskIterable == null) {
            // Used to avoid duplicating work if the Dynamic Filter was already pushed down to the Iceberg API
            this.pushedDownDynamicFilterPredicate = dynamicFilter.getCurrentPredicate().transformKeys(IcebergColumnHandle.class::cast);
            TupleDomain<IcebergColumnHandle> fullPredicate = tableHandle.getUnenforcedPredicate()
                    .intersect(pushedDownDynamicFilterPredicate);
            // TODO: (https://github.com/trinodb/trino/issues/9743): Consider removing TupleDomain#simplify
            TupleDomain<IcebergColumnHandle> simplifiedPredicate = fullPredicate.simplify(ICEBERG_DOMAIN_COMPACTION_THRESHOLD);
            if (!simplifiedPredicate.equals(fullPredicate)) {
                // Pushed down predicate was simplified, always evaluate it against individual splits
                this.pushedDownDynamicFilterPredicate = TupleDomain.all();
            }

            TupleDomain<IcebergColumnHandle> effectivePredicate = tableHandle.getEnforcedPredicate()
                    .intersect(simplifiedPredicate);

            if (effectivePredicate.isNone()) {
                finish();
                return completedFuture(NO_MORE_SPLITS_BATCH);
            }

            Expression filterExpression = toIcebergExpression(effectivePredicate);
            this.fileScanTaskIterable = TableScanUtil.splitFiles(
                    tableScan.filter(filterExpression)
                            .includeColumnStats()
                            .planFiles(),
                    tableScan.targetSplitSize());
            closer.register(fileScanTaskIterable);
            this.fileScanTaskIterator = fileScanTaskIterable.iterator();
            closer.register(fileScanTaskIterator);
        }

        TupleDomain<IcebergColumnHandle> dynamicFilterPredicate = dynamicFilter.getCurrentPredicate()
                .transformKeys(IcebergColumnHandle.class::cast);
        if (dynamicFilterPredicate.isNone()) {
            finish();
            return completedFuture(NO_MORE_SPLITS_BATCH);
        }

        Iterator<FileScanTask> fileScanTasks = Iterators.limit(fileScanTaskIterator, maxSize);
        ImmutableList.Builder<ConnectorSplit> splits = ImmutableList.builder();
        while (fileScanTasks.hasNext()) {
            FileScanTask scanTask = fileScanTasks.next();
            if (maxScannedFileSizeInBytes.isPresent() && scanTask.file().fileSizeInBytes() > maxScannedFileSizeInBytes.get()) {
                continue;
            }

            IcebergSplit icebergSplit = toIcebergSplit(scanTask);

            Schema fileSchema = scanTask.spec().schema();
            Map<Integer, Optional<String>> partitionKeys = getPartitionKeys(scanTask);

            Set<IcebergColumnHandle> identityPartitionColumns = partitionKeys.keySet().stream()
                    .map(fieldId -> getColumnHandle(fileSchema.findField(fieldId), typeManager))
                    .collect(toImmutableSet());

            Supplier<Map<ColumnHandle, NullableValue>> partitionValues = memoize(() -> {
                Map<ColumnHandle, NullableValue> bindings = new HashMap<>();
                for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
                    Object partitionValue = deserializePartitionValue(
                            partitionColumn.getType(),
                            partitionKeys.get(partitionColumn.getId()).orElse(null),
                            partitionColumn.getName());
                    NullableValue bindingValue = new NullableValue(partitionColumn.getType(), partitionValue);
                    bindings.put(partitionColumn, bindingValue);
                }
                return bindings;
            });

            if (!dynamicFilterPredicate.isAll() && !dynamicFilterPredicate.equals(pushedDownDynamicFilterPredicate)) {
                if (!partitionMatchesPredicate(
                        identityPartitionColumns,
                        partitionValues,
                        dynamicFilterPredicate)) {
                    continue;
                }
                if (!fileMatchesPredicate(
                        fieldIdToType,
                        dynamicFilterPredicate,
                        scanTask.file().lowerBounds(),
                        scanTask.file().upperBounds(),
                        scanTask.file().nullValueCounts())) {
                    continue;
                }
            }
            if (!partitionMatchesConstraint(identityPartitionColumns, partitionValues, constraint)) {
                continue;
            }
            if (recordScannedFiles) {
                scannedFiles.add(scanTask.file());
            }
            splits.add(icebergSplit);
        }
        return completedFuture(new ConnectorSplitBatch(splits.build(), isFinished()));
    }

    private void finish()
    {
        close();
        this.fileScanTaskIterable = CloseableIterable.empty();
        this.fileScanTaskIterator = CloseableIterator.empty();
    }

    @Override
    public boolean isFinished()
    {
        return fileScanTaskIterator != null && !fileScanTaskIterator.hasNext();
    }

    @Override
    public Optional<List<Object>> getTableExecuteSplitsInfo()
    {
        checkState(isFinished(), "Split source must be finished before TableExecuteSplitsInfo is read");
        if (!recordScannedFiles) {
            return Optional.empty();
        }
        return Optional.of(ImmutableList.copyOf(scannedFiles.build()));
    }

    @Override
    public void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @VisibleForTesting
    static boolean fileMatchesPredicate(
            Map<Integer, Type.PrimitiveType> primitiveTypeForFieldId,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate,
            @Nullable Map<Integer, ByteBuffer> lowerBounds,
            @Nullable Map<Integer, ByteBuffer> upperBounds,
            @Nullable Map<Integer, Long> nullValueCounts)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow(() -> new NoSuchElementException("No value present"));

        for (Map.Entry<IcebergColumnHandle, Domain> domainEntry : domains.entrySet()) {
            IcebergColumnHandle column = domainEntry.getKey();
            Domain domain = domainEntry.getValue();

            int fieldId = column.getId();
            boolean mayContainNulls;
            if (nullValueCounts == null) {
                mayContainNulls = true;
            }
            else {
                Long nullValueCount = nullValueCounts.get(fieldId);
                mayContainNulls = nullValueCount == null || nullValueCount > 0;
            }
            Type type = primitiveTypeForFieldId.get(fieldId);
            Domain statisticsDomain = domainForStatistics(
                    column.getType(),
                    lowerBounds == null ? null : fromByteBuffer(type, lowerBounds.get(fieldId)),
                    upperBounds == null ? null : fromByteBuffer(type, upperBounds.get(fieldId)),
                    mayContainNulls);
            if (!domain.overlaps(statisticsDomain)) {
                return false;
            }
        }
        return true;
    }

    private static Domain domainForStatistics(
            io.prestosql.spi.type.Type type,
            @Nullable Object lowerBound,
            @Nullable Object upperBound,
            boolean mayContainNulls)
    {
        Type icebergType = toIcebergType(type);
        if (lowerBound == null && upperBound == null) {
            return Domain.create(ValueSet.all(type), mayContainNulls);
        }

        Range statisticsRange;
        if (lowerBound != null && upperBound != null) {
            statisticsRange = Range.range(
                    type,
                    convertIcebergValueToTrino(icebergType, lowerBound),
                    true,
                    convertIcebergValueToTrino(icebergType, upperBound),
                    true);
        }
        else if (upperBound != null) {
            statisticsRange = Range.lessThanOrEqual(type, convertIcebergValueToTrino(icebergType, upperBound));
        }
        else {
            statisticsRange = Range.greaterThanOrEqual(type, convertIcebergValueToTrino(icebergType, lowerBound));
        }
        return Domain.create(ValueSet.ofRanges(statisticsRange), mayContainNulls);
    }

    static boolean partitionMatchesConstraint(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            Constraint constraint)
    {
        // We use Constraint just to pass functional predicate here from DistributedExecutionPlanner
        verify(constraint.getSummary().isAll());

        if (!constraint.predicate().isPresent() ||
                intersection(constraint.getPredicateColumns().orElseThrow(() -> new NoSuchElementException("No value present")), identityPartitionColumns).isEmpty()) {
            return true;
        }
        return constraint.predicate().get().test(partitionValues.get());
    }

    @VisibleForTesting
    static boolean partitionMatchesPredicate(
            Set<IcebergColumnHandle> identityPartitionColumns,
            Supplier<Map<ColumnHandle, NullableValue>> partitionValues,
            TupleDomain<IcebergColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }
        Map<IcebergColumnHandle, Domain> domains = dynamicFilterPredicate.getDomains().orElseThrow(() -> new NoSuchElementException("No value present"));

        for (IcebergColumnHandle partitionColumn : identityPartitionColumns) {
            Domain allowedDomain = domains.get(partitionColumn);
            if (allowedDomain != null) {
                if (!allowedDomain.includesNullableValue(partitionValues.get().get(partitionColumn).getValue())) {
                    return false;
                }
            }
        }
        return true;
    }

    private IcebergSplit toIcebergSplit(FileScanTask task)
    {
        return new IcebergSplit(
                hadoopPath(task.file().path().toString()),
                task.start(),
                task.length(),
                task.file().fileSizeInBytes(),
                task.file().recordCount(),
                IcebergFileFormat.fromIceberg(task.file().format()),
                ImmutableList.of(),
                PartitionSpecParser.toJson(task.spec()),
                PartitionData.toJson(task.file().partition()),
                task.deletes().stream()
                        .map(TrinoDeleteFile::copyOf)
                        .collect(toImmutableList()),
                SplitWeight.fromProportion(Math.min(Math.max((double) task.length() / tableScan.targetSplitSize(), minimumAssignedSplitWeight), 1.0)));
    }

    private static String hadoopPath(String path)
    {
        // hack to preserve the original path for S3 if necessary
        Path hadoopPath;
        try {
            hadoopPath = new Path(path);
            if ("s3".equals(hadoopPath.toUri().getScheme()) && !path.equals(hadoopPath.toString())) {
                if (hadoopPath.toUri().getFragment() != null) {
                    throw new PrestoException(ICEBERG_INVALID_METADATA, "Unexpected URI fragment in path: " + path);
                }
                URI uri = URI.create(path);
                return uri + "#" + URLEncoder.encode(uri.getPath(), String.valueOf(UTF_8));
            }
        }
        catch (Exception e) {
            throw new UnsupportedOperationException("hadoopPath error");
        }
        return path;
    }
}
