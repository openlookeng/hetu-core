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
import com.google.common.primitives.Booleans;
import io.airlift.log.Logger;
import io.prestosql.plugin.hive.HiveBucketing.BucketingVersion;
import io.prestosql.plugin.hive.HivePageSourceProvider.BucketAdaptation;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.plugin.hive.coercions.DoubleToFloatCoercer;
import io.prestosql.plugin.hive.coercions.FloatToDoubleCoercer;
import io.prestosql.plugin.hive.coercions.IntegerNumberToVarcharCoercer;
import io.prestosql.plugin.hive.coercions.IntegerNumberUpscaleCoercer;
import io.prestosql.plugin.hive.coercions.VarcharToIntegerNumberCoercer;
import io.prestosql.plugin.hive.coercions.VarcharToVarcharCoercer;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.ArrayBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.ColumnarArray;
import io.prestosql.spi.block.ColumnarMap;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.block.DictionaryBlock;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.block.RowBlock;
import io.prestosql.spi.block.RunLengthEncodedBlock;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.spi.type.VarcharType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
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
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveBucketing.getHiveBucket;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.prestosql.plugin.hive.HiveSessionProperties.isDynamicFilteringFilterRows;
import static io.prestosql.plugin.hive.HiveType.HIVE_BYTE;
import static io.prestosql.plugin.hive.HiveType.HIVE_DOUBLE;
import static io.prestosql.plugin.hive.HiveType.HIVE_FLOAT;
import static io.prestosql.plugin.hive.HiveType.HIVE_INT;
import static io.prestosql.plugin.hive.HiveType.HIVE_LONG;
import static io.prestosql.plugin.hive.HiveType.HIVE_SHORT;
import static io.prestosql.plugin.hive.HiveUtil.bigintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.booleanPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.charPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.datePartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.doublePartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.extractStructFieldTypes;
import static io.prestosql.plugin.hive.HiveUtil.floatPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.integerPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.isArrayType;
import static io.prestosql.plugin.hive.HiveUtil.isHiveNull;
import static io.prestosql.plugin.hive.HiveUtil.isMapType;
import static io.prestosql.plugin.hive.HiveUtil.isPartitionFiltered;
import static io.prestosql.plugin.hive.HiveUtil.isRowType;
import static io.prestosql.plugin.hive.HiveUtil.longDecimalPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.shortDecimalPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.smallintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.timestampPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.tinyintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.varcharPartitionKey;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToDecimalCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToDoubleCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDecimalToRealCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createDoubleToDecimalCoercer;
import static io.prestosql.plugin.hive.coercions.DecimalCoercers.createRealToDecimalCoercer;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.block.ColumnarArray.toColumnarArray;
import static io.prestosql.spi.block.ColumnarMap.toColumnarMap;
import static io.prestosql.spi.block.ColumnarRow.toColumnarRow;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
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
    private boolean eligibleForRowFiltering;

    private final ConnectorPageSource delegate;

    private Map<ColumnHandle, DynamicFilter> dynamicFilters;
    private final List<HivePartitionKey> partitionKeys;

    private final Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier;
    private final ConnectorSession session;
    private int numberOfGlobalDynamicFilters;
    private int numberOfLocalDynamicFilters;
    private final long waitUntil;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate,
            Supplier<Map<ColumnHandle, DynamicFilter>> dynamicFilterSupplier,
            ConnectorSession session,
            List<HivePartitionKey> partitionKeys)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;
        this.bucketAdapter = bucketAdaptation.map(BucketAdapter::new);

        this.session = session;

        this.dynamicFilterSupplier = dynamicFilterSupplier;

        this.partitionKeys = partitionKeys;
        //FIXME: KEN: BIG ASSUMPTION, assuming system time is synchronized across the cluster
        this.waitUntil = session.getStartTime() + session.getDynamicFilteringWaitTime().toMillis();
        this.eligibleForRowFiltering = isDynamicFilteringFilterRows(session);

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
                coercers.add(Optional.of(createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType())));
            }
            else {
                coercers.add(Optional.empty());
            }

            if (columnMapping.getKind() == PREFILLED) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                Object prefilledValue;
                if (isHiveNull(bytes)) {
                    prefilledValue = null;
                }
                else if (type.equals(BOOLEAN)) {
                    prefilledValue = booleanPartitionKey(columnValue, name);
                }
                else if (type.equals(BIGINT)) {
                    prefilledValue = bigintPartitionKey(columnValue, name);
                }
                else if (type.equals(INTEGER)) {
                    prefilledValue = integerPartitionKey(columnValue, name);
                }
                else if (type.equals(SMALLINT)) {
                    prefilledValue = smallintPartitionKey(columnValue, name);
                }
                else if (type.equals(TINYINT)) {
                    prefilledValue = tinyintPartitionKey(columnValue, name);
                }
                else if (type.equals(REAL)) {
                    prefilledValue = floatPartitionKey(columnValue, name);
                }
                else if (type.equals(DOUBLE)) {
                    prefilledValue = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    prefilledValue = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    prefilledValue = charPartitionKey(columnValue, name, type);
                }
                else if (type.equals(DATE)) {
                    prefilledValue = datePartitionKey(columnValue, name);
                }
                else if (type.equals(TIMESTAMP)) {
                    prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }

                prefilledValues[columnIndex] = prefilledValue;
            }
        }
        this.coercers = coercers.build();
    }

    private static Function<Block, Block> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());

        if (toType instanceof VarcharType && fromType instanceof VarcharType) {
            return new VarcharToVarcharCoercer((VarcharType) fromType, (VarcharType) toType);
        }
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer<>(fromType, (VarcharType) toType);
        }
        if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer<>((VarcharType) fromType, toType);
        }
        if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer<>(fromType, toType);
        }
        if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }
        if (fromHiveType.equals(HIVE_DOUBLE) && toHiveType.equals(HIVE_FLOAT)) {
            return new DoubleToFloatCoercer();
        }
        if (fromType instanceof DecimalType && toType instanceof DecimalType) {
            return createDecimalToDecimalCoercer((DecimalType) fromType, (DecimalType) toType);
        }
        if (fromType instanceof DecimalType && toType == DOUBLE) {
            return createDecimalToDoubleCoercer((DecimalType) fromType);
        }
        if (fromType instanceof DecimalType && toType == REAL) {
            return createDecimalToRealCoercer((DecimalType) fromType);
        }
        if (fromType == DOUBLE && toType instanceof DecimalType) {
            return createDoubleToDecimalCoercer((DecimalType) toType);
        }
        if (fromType == REAL && toType instanceof DecimalType) {
            return createRealToDecimalCoercer((DecimalType) toType);
        }
        if (isArrayType(fromType) && isArrayType(toType)) {
            return new ListCoercer(typeManager, fromHiveType, toHiveType);
        }
        if (isMapType(fromType) && isMapType(toType)) {
            return new MapCoercer(typeManager, fromHiveType, toHiveType);
        }
        if (isRowType(fromType) && isRowType(toType)) {
            return new StructCoercer(typeManager, fromHiveType, toHiveType);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
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

    private Map<ColumnHandle, DynamicFilter> getDynamicFilters()
    {
        if (dynamicFilterSupplier == null) {
            return ImmutableMap.of();
        }
        Map<ColumnHandle, DynamicFilter> dynamicFilters = dynamicFilterSupplier.get();
        // TODO: Remove this logic when row filtering decision can be made in optimizers
        numberOfGlobalDynamicFilters = 0;
        numberOfLocalDynamicFilters = 0;
        dynamicFilters.entrySet().stream().forEach(entry -> {
            if (entry.getValue().getType() == DynamicFilter.Type.GLOBAL) {
                numberOfGlobalDynamicFilters += 1;
            }
            else {
                numberOfLocalDynamicFilters += 1;
            }
        });
        return dynamicFilters;
    }

    /**
     * this method is design to wait until a specific time when there are expected dynamic filters
     * The wait until time is relative to the SQL query start time
     */
    private boolean needToWait()
    {
        //assuming starttime is global indicting the time query execution started
        //assuming system time is accurate enough to ms level
        return System.currentTimeMillis() <= waitUntil;
    }

    @Override
    public Page getNextPage()
    {
        try {
            dynamicFilters = getDynamicFilters();
            if (dynamicFilterSupplier != null) {
                // Wait for any dynamic filter
                if (dynamicFilters.isEmpty() && needToWait()) {
                    return null;
                }

                // Close the current PageSource if the partition should be filtered
                if (isPartitionFiltered(partitionKeys, new HashSet(dynamicFilters.values()), typeManager)) {
                    close();
                    return null;
                }
            }

            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            // This part is for filtering using the bloom filter
            // we filter out rows that are not in the bloom filter
            // using the filter rows function
            if ((numberOfGlobalDynamicFilters > 0 && numberOfLocalDynamicFilters == 0) || eligibleForRowFiltering) {
                if (!dynamicFilters.isEmpty()) {
                    dataPage = filter(dynamicFilters, dataPage);
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
            return new Page(batchSize, blocks.toArray(new Block[0]));
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

    private Page filter(Map<ColumnHandle, DynamicFilter> dynamicFilters, Page page)
    {
        boolean[] result = new boolean[page.getPositionCount()];
        Arrays.fill(result, Boolean.TRUE);

        Map<Integer, ColumnHandle> eligibleColumns = new HashMap<>();
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            HiveColumnHandle columnHandle = columnMappings.get(channel).getHiveColumnHandle();
            if (dynamicFilters.containsKey(columnHandle)) {
                eligibleColumns.put(channel, columnHandle);
            }
        }

        for (Map.Entry<Integer, ColumnHandle> column : eligibleColumns.entrySet()) {
            DynamicFilter dynamicFilter = dynamicFilters.get(column.getValue());
            Block block = page.getBlock(column.getKey()).getLoadedBlock();
            if (dynamicFilter instanceof BloomFilterDynamicFilter) {
                block.filter(((BloomFilterDynamicFilter) dynamicFilters.get(column.getValue())).getBloomFilterDeserialized(), result);
            }
            else {
                for (int i = 0; i < block.getPositionCount(); i++) {
                    result[i] = result[i] && dynamicFilter.contains(TypeUtils.readNativeValue(types[column.getKey()], block, i));
                }
            }
        }

        Block[] adaptedBlocks = new Block[page.getChannelCount()];
        int[] rowsToKeep = toPositions(result);
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
     * @param keep
     * @return
     */
    private int[] toPositions(boolean[] keep)
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

    private static class ListCoercer
            implements Function<Block, Block>
    {
        private final Function<Block, Block> elementCoercer;

        public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = fromElementHiveType.equals(toElementHiveType) ? null : createCoercer(typeManager, fromElementHiveType, toElementHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            if (elementCoercer == null) {
                return block;
            }
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.apply(arrayBlock.getElementsBlock());
            boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
            int[] offsets = new int[arrayBlock.getPositionCount() + 1];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                valueIsNull[i] = arrayBlock.isNull(i);
                offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
            }
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
        }
    }

    private static class MapCoercer
            implements Function<Block, Block>
    {
        private final Type toType;
        private final Function<Block, Block> keyCoercer;
        private final Function<Block, Block> valueCoercer;

        public MapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            this.toType = requireNonNull(toHiveType, "toHiveType is null").getType(typeManager);
            HiveType fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            HiveType toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.keyCoercer = fromKeyHiveType.equals(toKeyHiveType) ? null : createCoercer(typeManager, fromKeyHiveType, toKeyHiveType);
            this.valueCoercer = fromValueHiveType.equals(toValueHiveType) ? null : createCoercer(typeManager, fromValueHiveType, toValueHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarMap mapBlock = toColumnarMap(block);
            Block keysBlock = keyCoercer == null ? mapBlock.getKeysBlock() : keyCoercer.apply(mapBlock.getKeysBlock());
            Block valuesBlock = valueCoercer == null ? mapBlock.getValuesBlock() : valueCoercer.apply(mapBlock.getValuesBlock());
            boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
            int[] offsets = new int[mapBlock.getPositionCount() + 1];
            for (int i = 0; i < mapBlock.getPositionCount(); i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return ((MapType) toType).createBlockFromKeyValue(Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
        }
    }

    private static class StructCoercer
            implements Function<Block, Block>
    {
        private final List<Optional<Function<Block, Block>>> coercers;
        private final Block[] nullBlocks;

        public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
            List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
            ImmutableList.Builder<Optional<Function<Block, Block>>> coercers = ImmutableList.builder();
            this.nullBlocks = new Block[toFieldTypes.size()];
            for (int i = 0; i < toFieldTypes.size(); i++) {
                if (i >= fromFieldTypes.size()) {
                    nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                    coercers.add(Optional.empty());
                }
                else if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                    coercers.add(Optional.of(createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i))));
                }
                else {
                    coercers.add(Optional.empty());
                }
            }
            this.coercers = coercers.build();
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarRow rowBlock = toColumnarRow(block);
            Block[] fields = new Block[coercers.size()];
            int[] ids = new int[rowBlock.getField(0).getPositionCount()];
            for (int i = 0; i < coercers.size(); i++) {
                Optional<Function<Block, Block>> coercer = coercers.get(i);
                if (coercer.isPresent()) {
                    fields[i] = coercer.get().apply(rowBlock.getField(i));
                }
                else if (i < rowBlock.getFieldCount()) {
                    fields[i] = rowBlock.getField(i);
                }
                else {
                    fields[i] = new DictionaryBlock(nullBlocks[i], ids);
                }
            }
            boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                valueIsNull[i] = rowBlock.isNull(i);
            }
            return RowBlock.fromFieldBlocks(valueIsNull.length, Optional.of(valueIsNull), fields);
        }
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
}
