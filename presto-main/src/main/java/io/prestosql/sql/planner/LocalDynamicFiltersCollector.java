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
package io.prestosql.sql.planner;

import io.airlift.log.Logger;
import io.hetu.core.common.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.DynamicFilterUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LocalDynamicFiltersCollector
{
    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    private Map<Symbol, DynamicFilter> localFilters = new HashMap<>();
    private Map<Symbol, Set> predicates = new HashMap<>();
    private Set<Symbol> globalFilters = new HashSet<>();
    private StateStoreProvider stateStoreProvider;
    private static final Logger LOG = Logger.get(LocalDynamicFiltersCollector.class);
    private Map<String, DynamicFilter> cachedGlobalDynamicFilters = new HashMap<>();
    private boolean initialized;

    /**
     * Constructor for the LocalDynamicFiltersCollector
     */
    LocalDynamicFiltersCollector() {}

    synchronized void intersectDynamicFilter(Map<Symbol, Set> predicate)
    {
        for (Map.Entry<Symbol, Set> entry : predicate.entrySet()) {
            if (entry.getValue().size() == 1 && entry.getValue().contains("GLOBAL")) {
                globalFilters.add(entry.getKey());
                continue;
            }

            if (!predicates.containsKey(entry.getKey())) {
                predicates.put(entry.getKey(), entry.getValue());
                continue;
            }

            Set predicateSet = predicates.get(entry.getKey());
            Set newValues = entry.getValue();
            for (Object value : newValues) {
                predicateSet.add(value);
            }
        }
    }

    /**
     * This function returns the bloom filters fetched from the state store. To prevent excessive reads from state store,
     * it caches fetched bloom filters for re-use
     *
     * @param tableScan TableScanNode that has DynamicFilter applied
     * @param dynamicFilters DynamicFilter id and expressions
     * @param queryId ID of the query
     * @return ColumnHandle to DynamicFilter mapping that contains any DynamicFilter that are ready for use
     */
    synchronized Map<ColumnHandle, DynamicFilter> getDynamicFilters(TableScanNode tableScan, List<DynamicFilters.Descriptor> dynamicFilters, String queryId)
    {
        if (!initialized && stateStoreProvider.getStateStore() != null) {
            stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.MERGEMAP, StateCollection.Type.MAP);
            stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.DFTYPEMAP, StateCollection.Type.MAP);
            initialized = true;
        }

        Map<Symbol, ColumnHandle> assignments = tableScan.getAssignments();
        // Skips symbols irrelevant to this table scan node.
        Map<ColumnHandle, DynamicFilter> result = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            final Symbol columnSymbol = entry.getKey();
            final ColumnHandle columnHandle = entry.getValue();
            final String filterId = getFilterId(columnSymbol, dynamicFilters);

            // Try to get dynamic filter from local cache first
            final String key = DynamicFilterUtils.createKey(filterId, queryId);
            DynamicFilter cachedDynamicFilter = cachedGlobalDynamicFilters.get(key);
            if (cachedDynamicFilter != null) {
                result.put(columnHandle, cachedDynamicFilter);
                continue;
            }

            // Global dynamic filters
            if (filterId != null && stateStoreProvider.getStateStore() != null) {
                String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, filterId, queryId);
                String type = (String) ((StateMap) stateStoreProvider.getStateStore()
                        .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).get(typeKey);
                if (type != null) {
                    if (type.equals(DynamicFilterUtils.HASHSETTYPEGLOBAL)) {
                        Set filter = (Set) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP))
                                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
                        if (filter != null) {
                            DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), filter, DynamicFilter.Type.GLOBAL);
                            LOG.debug("got new HashSet DynamicFilter from state store: " + filterId + " " + filter.size());
                            cachedGlobalDynamicFilters.putIfAbsent(key, dynamicFilter);
                            result.put(columnHandle, dynamicFilter);
                            continue;
                        }
                    }
                    else if (type.equals(DynamicFilterUtils.BLOOMFILTERTYPEGLOBAL)) {
                        byte[] serializedBloomFilter = (byte[]) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP))
                                .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
                        if (serializedBloomFilter != null) {
                            DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), serializedBloomFilter, DynamicFilter.Type.GLOBAL);
                            LOG.debug("got new BloomFilter DynamicFilter from state store: " + filterId + " " + dynamicFilter.getSize());
                            cachedGlobalDynamicFilters.putIfAbsent(key, dynamicFilter);
                            result.put(columnHandle, dynamicFilter);
                            continue;
                        }
                    }
                }
            }

            // Local dynamic filters
            if (!localFilters.containsKey(columnSymbol) && predicates.containsKey(columnSymbol)) {
                DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, columnHandle, predicates.get(columnSymbol), DynamicFilter.Type.LOCAL);
                localFilters.put(columnSymbol, dynamicFilter);
            }

            if (localFilters.containsKey(columnSymbol)) {
                result.put(columnHandle, localFilters.get(columnSymbol));
            }
        }
        return result;
    }

    private static String getFilterId(Symbol column, List<DynamicFilters.Descriptor> dynamicFilters)
    {
        for (DynamicFilters.Descriptor dynamicFilter : dynamicFilters) {
            if (dynamicFilter.getInput() instanceof SymbolReference) {
                if (column.getName().equals(((SymbolReference) dynamicFilter.getInput()).getName())) {
                    return dynamicFilter.getId();
                }
            }
            else {
                List<Symbol> symbolList = SymbolsExtractor.extractAll(dynamicFilter.getInput());
                for (Symbol symbol : symbolList) {
                    if (column.getName().equals(symbol.getName())) {
                        return dynamicFilter.getId();
                    }
                }
            }
        }
        return null;
    }

    public void setStateStoreProvider(StateStoreProvider stateStoreProvider)
    {
        this.stateStoreProvider = stateStoreProvider;
    }
}
