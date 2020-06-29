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
import io.airlift.slice.Slice;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.predicate.TupleDomain;
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
    private TupleDomain<Symbol> predicate;
    private Map<Symbol, DynamicFilter> localFilters = new HashMap<>();
    private Map<Symbol, Set<String>> predicates = new HashMap<>();
    private Set<Symbol> globalFilters = new HashSet<>();
    private StateStoreProvider stateStoreProvider;
    private static final Logger LOG = Logger.get(LocalDynamicFiltersCollector.class);
    private Map<String, DynamicFilter> cachedGlobalDynamicFilters = new HashMap<>();
    private boolean initialized;

    /**
     * Constructor for the LocalDynamicFiltersCollector
     */
    LocalDynamicFiltersCollector()
    {
        this.predicate = TupleDomain.all();
    }

    synchronized void intersectBloomFilter(Map<Symbol, Set<String>> predicate)
    {
        for (Map.Entry<Symbol, Set<String>> entry : predicate.entrySet()) {
            if (entry.getValue().size() == 1 && entry.getValue().contains("GLOBAL")) {
                globalFilters.add(entry.getKey());
                continue;
            }

            if (!predicates.containsKey(entry.getKey())) {
                predicates.put(entry.getKey(), entry.getValue());
                continue;
            }

            Set<String> predicateSet = predicates.get(entry.getKey());
            Set<String> newValues = entry.getValue();
            for (String value : newValues) {
                predicateSet.add(value);
            }
        }
    }

    synchronized void intersect(TupleDomain<Symbol> predicate)
    {
        if (predicate.isAll()) {
            return;
        }
        this.predicate = this.predicate.intersect(predicate);
    }

    /***
     * This function returns the bloom filters fetched from the state store. To prevent excessive reads from state store,
     * it caches fetched bloom filters for re-use
     * @param tableScan
     * @param dynamicFilters
     * @param queryId
     * @return
     */
    synchronized Map<ColumnHandle, DynamicFilter> getBloomFilters(TableScanNode tableScan, List<DynamicFilters.Descriptor> dynamicFilters, String queryId)
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
            String filterId = getFilterId(entry.getKey(), dynamicFilters);
            String key = DynamicFilterUtils.createKey(filterId, queryId);
            DynamicFilter localcache = cachedGlobalDynamicFilters.get(key);
            if (localcache != null) {
                result.put(entry.getValue(), localcache);
            }
            else {
                boolean readFromStateStore = false;
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
                                LOG.info("got new HSDF from state store: " + filterId + " " + filter.size());
                                cachedGlobalDynamicFilters.putIfAbsent(key, dynamicFilter);
                                result.put(entry.getValue(), dynamicFilter);
                                readFromStateStore = true;
                            }
                        }
                        else if (type.equals(DynamicFilterUtils.BLOOMFILTERTYPEGLOBAL)) {
                            byte[] serializedBloomFilter = (byte[]) ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP))
                                    .get(DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId));
                            if (serializedBloomFilter != null) {
                                DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), serializedBloomFilter, DynamicFilter.Type.GLOBAL);
                                LOG.info("got new BFDF from state store: " + filterId + " " + dynamicFilter.getSize());
                                cachedGlobalDynamicFilters.putIfAbsent(key, dynamicFilter);
                                result.put(entry.getValue(), dynamicFilter);
                                readFromStateStore = true;
                            }
                        }
                    }
                }
                if (!readFromStateStore) {
                    if (!localFilters.containsKey(entry.getKey()) && predicates.containsKey(entry.getKey())) {
                        HashSet<String> valueSet = new HashSet<>();
                        for (Object value : predicates.get(entry.getKey())) {
                            String val;
                            if (value instanceof Slice) {
                                val = new String(((Slice) value).getBytes());
                            }
                            else {
                                val = String.valueOf(value);
                            }
                            valueSet.add(val);
                        }
                        DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, entry.getValue(), valueSet, DynamicFilter.Type.LOCAL);
                        localFilters.put(entry.getKey(), dynamicFilter);
                    }

                    if (localFilters.containsKey(entry.getKey())) {
                        result.put(entry.getValue(), localFilters.get(entry.getKey()));
                    }
                }
            }
        }
        return result;
    }

    private String getFilterId(Symbol column, List<DynamicFilters.Descriptor> dynamicFilters)
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
