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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Booleans;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.HivePageSourceProvider.BucketAdaptation;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.plugin.hive.coercions.HiveCoercer;
import io.prestosql.plugin.hive.orc.OrcSelectivePageSource;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterSupplier;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveBucketing.getHiveBucket;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.prestosql.plugin.hive.HiveSessionProperties.getDynamicFilteringRowFilteringThreshold;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.HiveUtil.typedPartitionKey;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(HivePageSource.class);

    private final List<ColumnMapping> columnMappings;
    private final Optional<BucketAdapter> bucketAdapter;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final TypeManager typeManager;
    private final List<Optional<Function<Block, Block>>> coercers;
    private final int rowFilteringThreshold;
    protected boolean eligibleForRowFiltering;

    private final ConnectorPageSource delegate;

    private final List<HivePartitionKey> partitionKeys;
    private final Optional<DynamicFilterSupplier> dynamicFilterSupplier;
    private boolean isSelectiveRead;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate,
            Optional<DynamicFilterSupplier> dynamicFilterSupplier,
            ConnectorSession session,
            List<HivePartitionKey> partitionKeys)
    {
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;
        this.bucketAdapter = bucketAdaptation.map(BucketAdapter::new);

        this.dynamicFilterSupplier = dynamicFilterSupplier;

        this.partitionKeys = partitionKeys;
        this.rowFilteringThreshold = getDynamicFilteringRowFilteringThreshold(session);

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];
        ImmutableList.Builder<Optional<Function<Block, Block>>> coercers = ImmutableList.builder();

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.getCoercionFrom().isPresent()) {
                coercers.add(Optional.of(HiveCoercer.createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType())));
            }
            else {
                coercers.add(Optional.empty());
            }

            if (columnMapping.getKind() == PREFILLED) {
                prefilledValues[columnIndex] = typedPartitionKey(columnMapping.getPrefilledValue(), type, name, hiveStorageTimeZone);
            }
        }
        this.coercers = coercers.build();
        this.isSelectiveRead = delegate instanceof OrcSelectivePageSource;
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            final Map<ColumnHandle, DynamicFilter> dynamicFilters;
            if (dynamicFilterSupplier.isPresent()) {
                dynamicFilters = dynamicFilterSupplier.get().getDynamicFilters();
                // Wait for any dynamic filter
                if (dynamicFilters.isEmpty() && dynamicFilterSupplier.get().isBlocked()) {
                    return null;
                }

                // Close the current PageSource if the partition should be filtered
                if (isPartitionFiltered(partitionKeys, new HashSet(dynamicFilters.values()), typeManager)) {
                    close();
                    return null;
                }
            }
            else {
                dynamicFilters = ImmutableMap.of();
            }

            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            // This part is for filtering using the bloom filter
            // we filter out rows that are not in the bloom filter
            // using the filter rows function
            if (!dynamicFilters.isEmpty()) {
                final Map<Integer, ColumnHandle> eligibleColumns = getEligibleColumnsForRowFiltering(dataPage.getChannelCount(), dynamicFilters);
                if (!eligibleColumns.isEmpty()) {
                    dataPage = filter(dynamicFilters, dataPage, eligibleColumns, types);
                }
            }

            if (bucketAdapter.isPresent()) {
                IntArrayList rowsToKeep = bucketAdapter.get().computeEligibleRowIds(dataPage);
                Block[] adaptedBlocks = new Block[dataPage.getChannelCount()];
                for (int i = 0; i < adaptedBlocks.length; i++) {
                    Block block = dataPage.getBlock(i);
                    if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                        adaptedBlocks[i] = new LazyBlock(rowsToKeep.size(), new RowFilterLazyBlockLoader(dataPage.getBlock(i), rowsToKeep.elements()));
                    }
                    else {
                        adaptedBlocks[i] = block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size());
                    }
                }
                dataPage = new Page(rowsToKeep.size(), adaptedBlocks);
            }

            if (isSelectiveRead) { //FixMe(Rajeev) : Check way to optimize for prefilled fields.
                return dataPage;
            }

            int batchSize = dataPage.getPositionCount();
            List<Block> blocks = new ArrayList<>();
            for (int fieldId = 0; fieldId < columnMappings.size(); fieldId++) {
                ColumnMapping columnMapping = columnMappings.get(fieldId);
                switch (columnMapping.getKind()) {
                    case PREFILLED:
                        blocks.add(RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize));
                        break;
                    case REGULAR:
                    case TRANSACTIONID:
                        Block block = dataPage.getBlock(columnMapping.getIndex());
                        Optional<Function<Block, Block>> coercer = coercers.get(fieldId);
                        if (coercer.isPresent()) {
                            block = new LazyBlock(batchSize, new CoercionLazyBlockLoader(block, coercer.get()));
                        }
                        blocks.add(block);
                        break;
                    case INTERIM:
                        // interim columns don't show up in output
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            return new Page(batchSize, dataPage.getPageMetadata(), blocks.toArray(new Block[0]));
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }

    private Map<Integer, ColumnHandle> getEligibleColumnsForRowFiltering(int channelCount, Map<ColumnHandle, DynamicFilter> dynamicFilters)
    {
        Map<Integer, ColumnHandle> eligibleColumns = new HashMap<>();
        for (int channel = 0; channel < channelCount; channel++) {
            HiveColumnHandle columnHandle = columnMappings.get(channel).getHiveColumnHandle();
            if (!columnHandle.isPartitionKey() && dynamicFilters.containsKey(columnHandle)) {
                if (dynamicFilters.get(columnHandle).getSize() <= rowFilteringThreshold) {
                    eligibleColumns.put(channel, columnHandle);
                }
            }
        }
        return eligibleColumns;
    }

    private static boolean[] filterRows(Map<ColumnHandle, DynamicFilter> dynamicFilters, Page page, Map<Integer, ColumnHandle> eligibleColumns, Type[] types)
    {
        boolean[] result = new boolean[page.getPositionCount()];
        Arrays.fill(result, Boolean.TRUE);
        for (Map.Entry<Integer, ColumnHandle> column : eligibleColumns.entrySet()) {
            final int columnIndex = column.getKey();
            final ColumnHandle columnHandle = column.getValue();
            final DynamicFilter dynamicFilter = dynamicFilters.get(columnHandle);
            final Block block = page.getBlock(columnIndex).getLoadedBlock();
            if (dynamicFilter instanceof BloomFilterDynamicFilter) {
                block.filter(((BloomFilterDynamicFilter) dynamicFilters.get(columnHandle)).getBloomFilterDeserialized(), result);
            }
            else {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    result[i] = result[i] && dynamicFilter.contains(TypeUtils.readNativeValue(types[columnIndex], block, i));
                }
            }
        }
        return result;
    }

    @VisibleForTesting
    public static Page filter(Map<ColumnHandle, DynamicFilter> dynamicFilters, Page page, Map<Integer, ColumnHandle> eligibleColumns, Type[] types)
    {
        boolean[] result = filterRows(dynamicFilters, page, eligibleColumns, types);
        int[] rowsToKeep = toPositions(result);
        // If no row is filtered, no need to create a new page
        if (rowsToKeep.length == page.getPositionCount()) {
            return page;
        }

        Block[] adaptedBlocks = new Block[page.getChannelCount()];
        for (int i = 0; i < adaptedBlocks.length; i++) {
            Block block = page.getBlock(i);
            if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                adaptedBlocks[i] = new LazyBlock(rowsToKeep.length, new RowFilterLazyBlockLoader(page.getBlock(i), rowsToKeep));
            }
            else {
                adaptedBlocks[i] = block.getPositions(rowsToKeep, 0, rowsToKeep.length);
            }
        }
        return new Page(rowsToKeep.length, adaptedBlocks);
    }

    /**
     * Is position 1-based? extract the "true" value positions
     *
     * @param keep Boolean array including which row to keep
     * @return Int array of positions need to be kept
     */
    private static int[] toPositions(boolean[] keep)
    {
        int size = Booleans.countTrue(keep);
        int[] result = new int[size];
        int idx = 0;
        for (int i = 0; i < keep.length; i++) {
            if (keep[i]) {
                result[idx] = i; //position is 1-based
                idx++;
            }
        }
        return result;
    }

    private static final class CoercionLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public void load(LazyBlock lazyBlock)
        {
            if (block == null) {
                return;
            }

            lazyBlock.setBlock(coercer.apply(block.getLoadedBlock()));

            // clear reference to loader to free resources, since load was successful
            block = null;
        }
    }

    private static final class RowFilterLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final int[] rowsToKeep;
        private Block block;

        public RowFilterLazyBlockLoader(Block block, int[] rowsToKeep)
        {
            this.block = requireNonNull(block, "block is null");
            this.rowsToKeep = requireNonNull(rowsToKeep, "rowsToKeep is null");
        }

        @Override
        public void load(LazyBlock lazyBlock)
        {
            if (block == null) {
                return;
            }

            lazyBlock.setBlock(block.getPositions(rowsToKeep, 0, rowsToKeep.length));

            // clear reference to loader to free resources, since load was successful
            block = null;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            RowFilterLazyBlockLoader other = (RowFilterLazyBlockLoader) obj;
            return Arrays.equals(this.rowsToKeep, other.rowsToKeep) &&
                    Objects.equals(this.block, other.block);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(Arrays.hashCode(rowsToKeep), block);
        }
    }

    public static class BucketAdapter
    {
        private final int[] bucketColumns;
        private final BucketingVersion bucketingVersion;
        private final int bucketToKeep;
        private final int tableBucketCount;
        private final int partitionBucketCount; // for sanity check only
        private final List<TypeInfo> typeInfoList;

        public BucketAdapter(BucketAdaptation bucketAdaptation)
        {
            this.bucketColumns = bucketAdaptation.getBucketColumnIndices();
            this.bucketingVersion = bucketAdaptation.getBucketingVersion();
            this.bucketToKeep = bucketAdaptation.getBucketToKeep();
            this.typeInfoList = bucketAdaptation.getBucketColumnHiveTypes().stream()
                    .map(HiveType::getTypeInfo)
                    .collect(toImmutableList());
            this.tableBucketCount = bucketAdaptation.getTableBucketCount();
            this.partitionBucketCount = bucketAdaptation.getPartitionBucketCount();
        }

        public IntArrayList computeEligibleRowIds(Page page)
        {
            IntArrayList ids = new IntArrayList(page.getPositionCount());
            Page bucketColumnsPage = extractColumns(page, bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = getHiveBucket(bucketingVersion, tableBucketCount, typeInfoList, bucketColumnsPage, position);
                if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                    throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format(
                            "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                            bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
                }
                if (bucket == bucketToKeep) {
                    ids.add(position);
                }
            }
            return ids;
        }
    }

    @Override
    public boolean needMergingForPages()
    {
        return isSelectiveRead;
    }
}
