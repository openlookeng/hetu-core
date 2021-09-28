/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.operator;

import com.google.common.primitives.Booleans;
import io.airlift.log.Logger;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.spi.Page;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.statestore.StateStoreProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static io.prestosql.statestore.StateStoreConstants.CROSS_LAYER_DYNAMIC_FILTER;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTERS;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static io.prestosql.statestore.StateStoreConstants.QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING;
import static java.util.Objects.requireNonNull;

public class BloomFilterUtils
{
    private static final Logger LOGGER = Logger.get(BloomFilterUtils.class);

    private BloomFilterUtils() {}

    /**
     * update bloom filter from dynamicFilterCacheManger,
     * and if the table is a dc table, should create a new bloomFilter for the dc table, and put the new filter into state-store
     *
     * @param queryIdOptional queryId
     * @param isDcTable is this table a dc connector table
     * @param stateStoreProviderOptional stateStoreProvider
     * @param tableScanNodeOptional tableScanNode
     * @param dynamicFilterCacheManagerOptional dynamicFilterCacheManager
     * @param bloomFiltersBackup bloom filters backup, to check whether the bloom filter in stateStore has been updated
     * @param bloomFilters bloom filters
     */
    public static void updateBloomFilter(Optional<QueryId> queryIdOptional,
                                         boolean isDcTable,
                                         Optional<StateStoreProvider> stateStoreProviderOptional,
                                         Optional<TableScanNode> tableScanNodeOptional,
                                         Optional<DynamicFilterCacheManager> dynamicFilterCacheManagerOptional,
                                         Map<String, byte[]> bloomFiltersBackup,
                                         Map<Integer, BloomFilter> bloomFilters)
    {
        if (!queryIdOptional.isPresent() || !stateStoreProviderOptional.isPresent() || !tableScanNodeOptional.isPresent() || !dynamicFilterCacheManagerOptional.isPresent()) {
            return;
        }

        Map<String, Set<String>> mapping = dynamicFilterCacheManagerOptional.get().getMapping(queryIdOptional.get().getId() + QUERY_COLUMN_NAME_TO_SYMBOL_MAPPING);
        Map<String, byte[]> bloomFiltersFromStateStoreCache = dynamicFilterCacheManagerOptional.get().getBloomFitler(queryIdOptional.get().getId() + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);

        for (Map.Entry<String, byte[]> entry : bloomFiltersFromStateStoreCache.entrySet()) {
            // check the bloom filter exist in backup
            if (bloomFiltersBackup.keySet().contains(entry.getKey())) {
                // if the bloom filter is the same as the backup, just continue
                // maybe the bloom filter has been merged, so have to check it
                if (Arrays.equals(bloomFiltersBackup.get(entry.getKey()), entry.getValue())) {
                    continue;
                }
            }

            bloomFiltersBackup.put(entry.getKey(), Arrays.copyOf(entry.getValue(), entry.getValue().length));

            int index = -1;

            // query statement column -> outputs symbols
            Set<String> columnToSymbols = mapping.get(entry.getKey());
            if (columnToSymbols != null && columnToSymbols.size() > 0) {
                List<Symbol> outputSymbols = tableScanNodeOptional.get().getOutputSymbols();
                for (int i = 0; i < outputSymbols.size(); i++) {
                    // find the position of the output symbol
                    if (columnToSymbols.contains(outputSymbols.get(i).getName())) {
                        index = i;
                        break;
                    }
                }
            }

            if (index < 0) {
                continue;
            }
            // put the bloom filter into bloomFilters
            try (ByteArrayInputStream input = new ByteArrayInputStream(bloomFiltersBackup.get(entry.getKey()))) {
                bloomFilters.put(index, BloomFilter.readFrom(input));
            }
            catch (IOException e) {
                // ignore the bloomFilter if broken
                LOGGER.warn("queryId:%s, update BloomFilter error, cause : %s", queryIdOptional.get(), e.getMessage());
            }

            // if this tableScanNode.table is a dc connector table, we should push the bloomFilter to next cluster
            if (isDcTable) {
                // filter keyName is DataCenterColumnHandle.getName(), filter value is entry.getValue()
                ColumnHandle columnHandle = tableScanNodeOptional.get().getAssignments().get(tableScanNodeOptional.get().getOutputSymbols().get(index));
                StateMap<String, Map<String, byte[]>> crossRegionDynamicFilters = (StateMap<String, Map<String, byte[]>>) stateStoreProviderOptional.get()
                        .getStateStore().getOrCreateStateCollection(CROSS_REGION_DYNAMIC_FILTERS, StateCollection.Type.MAP);
                Map<String, byte[]> newBloomFilterForNextDCCluster = dynamicFilterCacheManagerOptional.get().getBloomFitler(queryIdOptional.get().getId() + CROSS_LAYER_DYNAMIC_FILTER);
                if (newBloomFilterForNextDCCluster == null) {
                    newBloomFilterForNextDCCluster = new HashMap<>();
                }
                newBloomFilterForNextDCCluster.put(columnHandle.getColumnName(), entry.getValue());

                crossRegionDynamicFilters.put(queryIdOptional.get().getId() + CROSS_LAYER_DYNAMIC_FILTER, newBloomFilterForNextDCCluster);
            }
        }
    }

    /**
     * get cross region dynamic filter from dynamicFilterCacheManager for next cluster
     *
     * @param dynamicFilterCacheManager cache dynamic filter manager
     * @param queryId running query id
     * @param tableScanNode table
     * @return
     */
    public static Supplier<List<Map<ColumnHandle, DynamicFilter>>> getCrossRegionDynamicFilterSupplier(DynamicFilterCacheManager dynamicFilterCacheManager, String queryId, TableScanNode tableScanNode)
    {
        if (queryId == null || tableScanNode == null || dynamicFilterCacheManager == null) {
            return null;
        }

        Set<String> tableColumnNames = new HashSet<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : tableScanNode.getAssignments().entrySet()) {
            tableColumnNames.add(entry.getValue().getColumnName());
        }

        return () -> {
            List<Map<ColumnHandle, DynamicFilter>> result = new ArrayList<>();
            Map<String, byte[]> newBloomFilterStateStoreCache = dynamicFilterCacheManager.getBloomFitler(queryId + CROSS_LAYER_DYNAMIC_FILTER);
            if (newBloomFilterStateStoreCache == null) {
                return result;
            }
            Map<ColumnHandle, DynamicFilter> dynamicFilters = new HashMap<>();
            for (Map.Entry<String, byte[]> entry : newBloomFilterStateStoreCache.entrySet()) {
                if (tableColumnNames.contains(entry.getKey())) {
                    ColumnHandle columnHandle = new ColumnHandle() {
                        @Override
                        public String getColumnName()
                        {
                            return entry.getKey();
                        }
                    };
                    DynamicFilter newDynamicFilter = new BloomFilterDynamicFilter("", columnHandle, entry.getValue(), DynamicFilter.Type.GLOBAL);
                    dynamicFilters.put(columnHandle, newDynamicFilter);
                    result.add(dynamicFilters);
                }
            }
            return result;
        };
    }

    /**
     * filter page by BloomFilter
     *
     * @param page source data page
     * @param bloomFilterMap bloom filter map
     * @return return filtered page
     */
    public static Page filter(Page page, Map<Integer, BloomFilter> bloomFilterMap)
    {
        boolean[] result = new boolean[page.getPositionCount()];
        Arrays.fill(result, Boolean.TRUE);
        for (Map.Entry<Integer, BloomFilter> entry : bloomFilterMap.entrySet()) {
            int columnIndex = entry.getKey();
            Block block = page.getBlock(columnIndex).getLoadedBlock();
            block.filter(entry.getValue(), result);
        }

        Block[] adaptedBlocks = new Block[page.getChannelCount()];
        int[] rowsToKeep = toPositions(result);
        if (rowsToKeep.length == page.getPositionCount()) {
            return page;
        }

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
     * Is position 0-based? extract the "true" value positions
     *
     * @param keep
     * @return
     */
    private static int[] toPositions(boolean[] keep)
    {
        int size = Booleans.countTrue(keep);
        int[] result = new int[size];
        int idx = 0;
        for (int i = 0; i < keep.length; i++) {
            if (keep[i]) {
                result[idx] = i; //position is 0-based
                idx++;
            }
        }
        return result;
    }

    private static final class RowFilterLazyBlockLoader<T>
            implements LazyBlockLoader<T>
    {
        private final int[] rowsToKeep;
        private Block block;

        public RowFilterLazyBlockLoader(Block block, int[] rowsToKeep)
        {
            this.block = requireNonNull(block, "block is null");
            this.rowsToKeep = requireNonNull(rowsToKeep, "rowsToKeep is null");
        }

        @Override
        public void load(LazyBlock<T> lazyBlock)
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
}
