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

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
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
import io.prestosql.spi.type.TypeUtils;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.statestore.StateStoreProvider;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static java.util.Objects.requireNonNull;

public class DynamicFilterOperator
        implements Operator
{
    public static class DynamicFilterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final String queryId;
        private final StateStoreProvider stateStoreProvider;
        private final List<Symbol> symbols;
        private final TypeProvider typeProvider;

        public DynamicFilterOperatorFactory(int operatorId, PlanNodeId planNodeId, String queryId, List<Symbol> symbols, TypeProvider typeProvider, StateStoreProvider stateStoreProvider)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.symbols = requireNonNull(symbols, "symbols is null");
            this.typeProvider = requireNonNull(typeProvider, "typeProvider is null");
            this.stateStoreProvider = stateStoreProvider;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, DynamicFilterOperator.class.getSimpleName());
            return new DynamicFilterOperator(operatorContext, queryId, symbols, typeProvider, stateStoreProvider);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DynamicFilterOperatorFactory(operatorId, planNodeId, queryId, symbols, typeProvider, stateStoreProvider);
        }
    }

    private final OperatorContext operatorContext;
    private boolean finished;
    private Page currentPage;
    private final String queryId;
    private final List<Symbol> symbols;
    private final StateStoreProvider stateStoreProvider;
    private final Map<Integer, Type> columnTypes = new HashMap<>();
    private Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();

    public DynamicFilterOperator(OperatorContext operatorContext, String queryId, List<Symbol> symbols, TypeProvider typeProvider, StateStoreProvider stateStoreProvider)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryId = queryId;
        this.stateStoreProvider = stateStoreProvider;
        this.symbols = symbols;

        // Map column types to index
        for (int i = 0; i < symbols.size(); i++) {
            Type type = typeProvider.get(symbols.get(i));
            columnTypes.put(i, type);
        }

        // Initialize the local bloomFilter collection
        if (stateStoreProvider.getStateStore() != null) {
            stateStoreProvider.getStateStore().createStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION, StateCollection.Type.MAP);
        }
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

        // get bloom filter from hazelcast using queryId
        if (stateStoreProvider.getStateStore() != null) {
            StateMap<String, byte[]> bloomFilters = (StateMap<String, byte[]>) stateStoreProvider.getStateStore().getStateCollection(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);
            if (bloomFilters != null && bloomFilters.size() > bloomFilterMap.size()) {
                for (int i = 0; i < symbols.size(); i++) {
                    Symbol symbol = symbols.get(i);
                    if (bloomFilters.containsKey(symbol.getName()) && !bloomFilterMap.containsKey(i)) {
                        // Deserialize new bloomfilters
                        try (ByteArrayInputStream input = new ByteArrayInputStream(bloomFilters.get(symbol.getName()))) {
                            bloomFilterMap.put(i, BloomFilter.readFrom(input, Funnels.stringFunnel(Charset.defaultCharset())));
                        }
                        catch (IOException e) {
                            // ignore the bloomfilter if broken
                        }
                    }
                }
            }
        }

        if (!bloomFilterMap.isEmpty()) {
            IntArrayList rowsToKeep = filterRows(page);
            Block[] adaptedBlocks = new Block[page.getChannelCount()];
            for (int i = 0; i < adaptedBlocks.length; i++) {
                Block block = page.getBlock(i);
                if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                    adaptedBlocks[i] = new LazyBlock(rowsToKeep.size(), new RowFilterLazyBlockLoader(page.getBlock(i), rowsToKeep));
                }
                else {
                    adaptedBlocks[i] = block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size());
                }
            }
            currentPage = new Page(rowsToKeep.size(), adaptedBlocks);
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
    }

    @Override
    public boolean isFinished()
    {
        return finished && currentPage == null;
    }

    private IntArrayList filterRows(Page page)
    {
        IntArrayList ids = new IntArrayList(page.getPositionCount());

        for (int position = 0; position < page.getPositionCount(); position++) {
            boolean shouldKeep = true;
            for (Map.Entry<Integer, BloomFilter> entry : bloomFilterMap.entrySet()) {
                int columnIndex = entry.getKey();

                if (columnIndex >= page.getChannelCount()) {
                    // Index out of array range
                    continue;
                }

                Block block = page.getBlock(columnIndex).getLoadedBlock();
                String nativeValue = TypeUtils.readNativeValueForDynamicFilter(columnTypes.get(columnIndex), block, position);

                if (nativeValue != null && !entry.getValue().mightContain(nativeValue)) {
                    shouldKeep = false;
                    break;
                }
            }
            if (shouldKeep) {
                ids.add(position);
            }
        }

        return ids;
    }

    private final class RowFilterLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private Block block;
        private final IntArrayList rowsToKeep;

        public RowFilterLazyBlockLoader(Block block, IntArrayList rowsToKeep)
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

            lazyBlock.setBlock(block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size()));

            // clear reference to loader to free resources, since load was successful
            block = null;
        }
    }
}
