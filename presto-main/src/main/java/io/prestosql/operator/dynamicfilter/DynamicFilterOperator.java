/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package io.prestosql.operator.dynamicfilter;

import com.google.common.primitives.Booleans;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.statestore.StateStoreProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class DynamicFilterOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final String queryId;
    private final List<Symbol> symbols;
    private final StateStoreProvider stateStoreProvider;
    private final Map<Integer, Type> columnTypes = new HashMap<>();
    private final Optional<List<String>> columns;
    private boolean finished;
    private Page currentPage;
    private Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();
    private final ScheduledExecutorService queryBloomFilter = newSingleThreadScheduledExecutor(threadsNamed("dynamic-filter"));

    public DynamicFilterOperator(OperatorContext operatorContext, String queryId, List<Symbol> symbols, TypeProvider typeProvider, StateStoreProvider stateStoreProvider, Optional<List<String>> columns)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryId = queryId;
        this.stateStoreProvider = stateStoreProvider;
        this.symbols = symbols;
        this.columns = columns;

        // Map column types to index
        for (int i = 0; i < symbols.size(); i++) {
            Type type = typeProvider.get(symbols.get(i));
            columnTypes.put(i, type);
        }

        // Initialize the local bloomFilter collection
        if (stateStoreProvider.getStateStore() != null) {
            stateStoreProvider.getStateStore().createStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION, StateCollection.Type.MAP);
        }
        this.queryBloomFilter.scheduleWithFixedDelay(
                () -> {
                    updateBloomFilter();
                }, 50, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && currentPage == null;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        if (bloomFilterMap.isEmpty()) {
            updateBloomFilter();
        }

        if (!bloomFilterMap.isEmpty()) {
            currentPage = filter(page);
        }
        else {
            currentPage = page;
        }
    }

    @Override
    public Page getOutput()
    {
        Page page = currentPage;
        currentPage = null;
        return page;
    }

    @Override
    public void finish()
    {
        finished = true;
        this.queryBloomFilter.shutdownNow();
    }

    @Override
    public synchronized void close()
    {
        this.queryBloomFilter.shutdownNow();
    }

    @Override
    public boolean isFinished()
    {
        return finished && currentPage == null;
    }

    private Page filter(Page page)
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

    private void updateBloomFilter()
    {
        try {
            if (stateStoreProvider.getStateStore() == null) {
                this.queryBloomFilter.shutdownNow();
                return;
            }
            // get bloom filter from hazelcast using queryId
            StateMap<String, byte[]> bloomFilters = (StateMap<String, byte[]>) stateStoreProvider.getStateStore().getStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);
            if (bloomFilters == null || bloomFilters.size() <= bloomFilterMap.size()) {
                return;
            }

            for (int i = 0; i < symbols.size(); i++) {
                String columnName;
                // if symbols is not null, we use columns to get columnName
                if (columns.isPresent()) {
                    columnName = columns.get().get(i);
                }
                else {
                    columnName = symbols.get(i).getName();
                }
                if (bloomFilters.containsKey(columnName) && !bloomFilterMap.containsKey(i)) {
                    // Deserialize new bloomfilters
                    try (ByteArrayInputStream input = new ByteArrayInputStream(bloomFilters.get(columnName))) {
                        bloomFilterMap.put(i, BloomFilter.readFrom(input));
                    }
                    catch (IOException e) {
                        // ignore the bloomfilter if broken
                    }
                }
            }
        }
        catch (Throwable e) {
            // ignore any exception and error
        }
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
                result[idx] = i; //position is 0-based
                idx++;
            }
        }
        return result;
    }

    public static class DynamicFilterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final String queryId;
        private final StateStoreProvider stateStoreProvider;
        private final List<Symbol> symbols;
        private final TypeProvider typeProvider;
        private final Optional<List<String>> columns;

        public DynamicFilterOperatorFactory(int operatorId, PlanNodeId planNodeId, String queryId, List<Symbol> symbols, TypeProvider typeProvider, StateStoreProvider stateStoreProvider, Optional<List<String>> columns)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.symbols = requireNonNull(symbols, "symbols is null");
            this.typeProvider = requireNonNull(typeProvider, "typeProvider is null");
            this.stateStoreProvider = stateStoreProvider;
            this.columns = columns;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterOperator.class.getSimpleName());
            return new DynamicFilterOperator(operatorContext, queryId, symbols, typeProvider, stateStoreProvider, columns);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DynamicFilterOperatorFactory(operatorId, planNodeId, queryId, symbols, typeProvider, stateStoreProvider, columns);
        }
    }

    private final class RowFilterLazyBlockLoader<T>
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
    }
}
