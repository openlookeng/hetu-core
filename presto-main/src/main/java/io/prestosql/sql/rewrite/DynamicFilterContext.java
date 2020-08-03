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
package io.prestosql.sql.rewrite;

import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * this is a query specific dynamic filter context design to avoid any duplication of calculation
 */
public class DynamicFilterContext
{
    private final StateStore stateStore;
    private final List<DynamicFilters.Descriptor> descriptors;
    private Map<String, String> filterIds = new HashMap<>();

    public DynamicFilterContext(Map<Symbol, ColumnHandle> columns, List<DynamicFilters.Descriptor> descriptors, StateStoreProvider stateStoreProvider)
    {
        this.descriptors = descriptors;
        this.stateStore = stateStoreProvider.getStateStore();

        initFilterIds();
    }

    /**
     * for a specific set of descriptor, the id for a column is unique
     *
     * @param column
     * @return
     */
    public String getId(Symbol column)
    {
        return filterIds.get(column.getName());
    }

    private void initFilterIds()
    {
        for (DynamicFilters.Descriptor dynamicFilter : descriptors) {
            if (dynamicFilter.getInput() instanceof SymbolReference) {
                String colName = ((SymbolReference) dynamicFilter.getInput()).getName();
                filterIds.putIfAbsent(colName, dynamicFilter.getId());
            }
            else {
                List<Symbol> symbolList = SymbolsExtractor.extractAll(dynamicFilter.getInput());
                for (Symbol symbol : symbolList) {
                    //FIXME: KEN: is it possible to override?
                    filterIds.putIfAbsent(symbol.getName(), dynamicFilter.getId());
                }
            }
        }
    }

    /**
     * Returns a state store guaranteed to be initialized
     *
     * @return
     */
    public StateStore getStateStore()
    {
        return stateStore;
    }
}
