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

import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.operator.BloomFilterUtils;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.Operator;
import io.prestosql.operator.OperatorContext;
import io.prestosql.operator.OperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.planner.TypeProvider;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.prestosql.statestore.StateStoreConstants.CROSS_REGION_DYNAMIC_FILTER_COLLECTION;
import static java.util.Objects.requireNonNull;

public class CrossRegionDynamicFilterOperator
        implements Operator
{
    private final OperatorContext operatorContext;
    private final String queryId;
    private final List<Symbol> symbols;
    private final DynamicFilterCacheManager dynamicFilterCacheManager;
    private final List<String> columns;
    private boolean finished;
    private Page currentPage;
    private Map<Integer, BloomFilter> bloomFilterMap = new HashMap<>();

    public CrossRegionDynamicFilterOperator(OperatorContext operatorContext, String queryId, List<Symbol> symbols, TypeProvider typeProvider, DynamicFilterCacheManager dynamicFilterCacheManager, List<String> columns)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.queryId = queryId;
        this.dynamicFilterCacheManager = dynamicFilterCacheManager;
        this.symbols = symbols;
        this.columns = columns;
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

        updateBloomFilter();

        if (!bloomFilterMap.isEmpty()) {
            currentPage = BloomFilterUtils.filter(page, bloomFilterMap);
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

    private void updateBloomFilter()
    {
        try {
            // get bloom filter from hazelcast using queryId
            Map<String, byte[]> bloomFilters = this.dynamicFilterCacheManager.getBloomFitler(queryId + CROSS_REGION_DYNAMIC_FILTER_COLLECTION);
            if (bloomFilters == null || bloomFilters.size() <= bloomFilterMap.size()) {
                return;
            }

            for (int i = 0; i < symbols.size(); i++) {
                String columnName;
                // if symbols is not null, we use columns to get columnName
                if (columns != null && columns.size() > 0) {
                    columnName = columns.get(i);
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

    public static class CrossRegionDynamicFilterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final String queryId;
        private final DynamicFilterCacheManager dynamicFilterCacheManager;
        private final List<Symbol> symbols;
        private final TypeProvider typeProvider;
        private final List<String> columns;

        public CrossRegionDynamicFilterOperatorFactory(int operatorId, PlanNodeId planNodeId, String queryId, List<Symbol> symbols, TypeProvider typeProvider, DynamicFilterCacheManager dynamicFilterCacheManager, List<String> columns)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
            this.symbols = requireNonNull(symbols, "symbols is null");
            this.typeProvider = requireNonNull(typeProvider, "typeProvider is null");
            this.dynamicFilterCacheManager = dynamicFilterCacheManager;
            this.columns = columns;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, CrossRegionDynamicFilterOperator.class.getSimpleName());
            return new CrossRegionDynamicFilterOperator(operatorContext, queryId, symbols, typeProvider, dynamicFilterCacheManager, columns);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CrossRegionDynamicFilterOperatorFactory(operatorId, planNodeId, queryId, symbols, typeProvider, dynamicFilterCacheManager, columns);
        }
    }
}
