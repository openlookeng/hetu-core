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
package io.prestosql.sql.rewrite;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ListMultimap;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.SymbolsExtractor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * this is a query specific dynamic filter context design to avoid any duplication of calculation
 */
public class DynamicFilterContext
{
    private final List<List<DynamicFilters.Descriptor>> descriptors;
    private List<ListMultimap<String, String>> filterIdsList = new ArrayList<>();
    private List<Map<String, Optional<RowExpression>>> filtersList = new ArrayList<>();

    public DynamicFilterContext(List<List<DynamicFilters.Descriptor>> descriptors, Map<Integer, Symbol> layOut)
    {
        this.descriptors = descriptors;

        initFilterIds(layOut);
    }

    /**
     * @return size of the disjuncts list
     */
    public int getDisjunctSize()
    {
        return descriptors.size();
    }

    /**
     * @return List of Ids of dynamic filters for all dynamic filters
     */
    public List<ListMultimap<String, String>> getFilterIdsList()
    {
        return filterIdsList;
    }

    /**
     * For a specific set of descriptor, the id for a column is unique
     *
     * @param column column symbol
     * @param index index of the disjunct
     * @return List pf Id of dynamic filter for the column
     */
    public List<String> getId(Symbol column, int index)
    {
        return filterIdsList.get(index).get(column.getName());
    }

    public Optional<RowExpression> getFilter(String id, int index)
    {
        return filtersList.get(index).getOrDefault(id, Optional.empty());
    }

    /**
     * Get id for all the dynamic filters
     *
     * @return Set of dynamic filter ids
     */
    public Set<String> getFilterIds(int index)
    {
        return ImmutableSet.copyOf(filterIdsList.get(index).values());
    }

    private void initFilterIds(Map<Integer, Symbol> layOut)
    {
        for (List<DynamicFilters.Descriptor> descriptorList : descriptors) {
            ListMultimap<String, String> filterIds = ArrayListMultimap.create();
            Map<String, Optional<RowExpression>> filters = new HashMap<>();
            for (DynamicFilters.Descriptor dynamicFilter : descriptorList) {
                if (dynamicFilter.getInput() instanceof VariableReferenceExpression) {
                    String colName = ((VariableReferenceExpression) dynamicFilter.getInput()).getName();
                    filterIds.put(colName, dynamicFilter.getId());
                }
                else {
                    List<Symbol> symbolList = SymbolsExtractor.extractAll(dynamicFilter.getInput(), layOut);
                    for (Symbol symbol : symbolList) {
                        //FIXME: KEN: is it possible to override?
                        filterIds.put(symbol.getName(), dynamicFilter.getId());
                    }
                }
                if (dynamicFilter.getFilter().isPresent()) {
                    filters.put(dynamicFilter.getId(), dynamicFilter.getFilter());
                }
            }
            filterIdsList.add(filterIds);
            filtersList.add(filters);
        }
    }
}
