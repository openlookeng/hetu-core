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
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter.DataType;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.DynamicFilterUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.HASHSET;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static io.prestosql.spi.statestore.StateCollection.Type.MAP;
import static io.prestosql.utils.DynamicFilterUtils.getDynamicFilterDataType;

public class LocalDynamicFiltersCollector
{
    private StateStoreProvider stateStoreProvider;
    private final FeaturesConfig.DynamicFilterDataType dynamicFilterDataType;
    private static final Logger LOG = Logger.get(LocalDynamicFiltersCollector.class);

    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    private Map<Symbol, Set> predicates = new ConcurrentHashMap<>();
    private Map<String, DynamicFilter> cachedDynamicFilters = new ConcurrentHashMap<>();

    /**
     * Constructor for the LocalDynamicFiltersCollector
     */
    LocalDynamicFiltersCollector(FeaturesConfig.DynamicFilterDataType dataType)
    {
        this.dynamicFilterDataType = dataType;
    }

    void intersectDynamicFilter(Map<Symbol, Set> predicate)
    {
        for (Map.Entry<Symbol, Set> entry : predicate.entrySet()) {
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
    Map<ColumnHandle, DynamicFilter> getDynamicFilters(TableScanNode tableScan, List<DynamicFilters.Descriptor> dynamicFilters, String queryId)
    {
        final StateStore stateStore = stateStoreProvider.getStateStore();

        Map<Symbol, ColumnHandle> assignments = tableScan.getAssignments();
        // Skips symbols irrelevant to this table scan node.
        Map<ColumnHandle, DynamicFilter> result = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            final Symbol columnSymbol = entry.getKey();
            final ColumnHandle columnHandle = entry.getValue();
            final String filterId = getFilterId(columnSymbol, dynamicFilters);

            // Try to get dynamic filter from local cache first
            final String key = DynamicFilterUtils.createKey(filterId, queryId);
            DynamicFilter cachedDynamicFilter = cachedDynamicFilters.get(key);
            if (cachedDynamicFilter != null) {
                result.put(columnHandle, cachedDynamicFilter);
                continue;
            }

            // Global dynamic filters
            if (filterId != null && stateStore != null) {
                StateMap filterMap = (StateMap) stateStore.getOrCreateStateCollection(DynamicFilterUtils.MERGEMAP, MAP);
                Object mergedFilter = filterMap.get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
                if (mergedFilter != null) {
                    DataType dataType = getDynamicFilterDataType(GLOBAL, dynamicFilterDataType);
                    if (dataType == HASHSET) {
                        Set hashSetFilter = (Set) mergedFilter;
                        DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), hashSetFilter, GLOBAL);
                        LOG.debug("got new HashSet DynamicFilter from state store: " + filterId + " " + hashSetFilter.size());
                        cachedDynamicFilters.putIfAbsent(key, dynamicFilter);
                        result.put(columnHandle, dynamicFilter);
                        continue;
                    }
                    else if (dataType == BLOOM_FILTER) {
                        byte[] serializedBloomFilter = (byte[]) mergedFilter;
                        DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), serializedBloomFilter, GLOBAL);
                        LOG.debug("got new BloomFilter DynamicFilter from state store: " + filterId + " " + dynamicFilter.getSize());
                        cachedDynamicFilters.putIfAbsent(key, dynamicFilter);
                        result.put(columnHandle, dynamicFilter);
                        continue;
                    }
                }
            }

            // Local dynamic filters
            if (predicates.containsKey(columnSymbol)) {
                DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, columnHandle, predicates.get(columnSymbol), LOCAL);
                cachedDynamicFilters.put(key, dynamicFilter);
                result.put(columnHandle, dynamicFilter);
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
