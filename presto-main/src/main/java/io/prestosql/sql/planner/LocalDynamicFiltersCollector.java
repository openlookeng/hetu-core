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
import io.prestosql.Session;
import io.prestosql.dynamicfilter.DynamicFilterStateStoreListener;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.rewrite.DynamicFilterContext;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.DynamicFilterUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.SystemSessionProperties.getDynamicFilteringDataType;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static java.util.Objects.requireNonNull;

public class LocalDynamicFiltersCollector
{
    public static final Logger LOG = Logger.get(LocalDynamicFiltersCollector.class);
    private StateStoreProvider stateStoreProvider;
    private StateMap mergedDynamicFilters;
    private DynamicFilterStateStoreListener stateStoreListeners;
    private final FeaturesConfig.DynamicFilterDataType dynamicFilterDataType;
    private DynamicFilterContext context;

    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    private Map<Symbol, Set> predicates = new ConcurrentHashMap<>();
    private Map<String, DynamicFilter> cachedDynamicFilters = new ConcurrentHashMap<>();
    private final String queryId;

    /**
     * Constructor for the LocalDynamicFiltersCollector
     */
    LocalDynamicFiltersCollector(TaskContext taskContext, StateStoreProvider stateStoreProvider)
    {
        requireNonNull(taskContext, "taskContext is null");
        Session session = taskContext.getSession();
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStoreProvider is null");
        this.dynamicFilterDataType = getDynamicFilteringDataType(session);
        this.queryId = session.getQueryId().getId();

        // add StateStoreListeners and remove them when task finishes
        // only when dynamic filtering is enabled
        if (isEnableDynamicFiltering(session) && stateStoreProvider.getStateStore() != null) {
            addStateStoreListeners();
            taskContext.onTaskFinished(this::removeStateStoreListeners);
        }
    }

    void initContext(Map<Symbol, ColumnHandle> columns, List<DynamicFilters.Descriptor> descriptors)
    {
        if (context == null) {
            context = new DynamicFilterContext(columns, descriptors, stateStoreProvider);
        }
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
     * @return ColumnHandle to DynamicFilter mapping that contains any DynamicFilter that are ready for use
     */
    Map<ColumnHandle, DynamicFilter> getDynamicFilters(TableScanNode tableScan)
    {
        Map<Symbol, ColumnHandle> assignments = tableScan.getAssignments();
        // Skips symbols irrelevant to this table scan node.
        Map<ColumnHandle, DynamicFilter> result = new HashMap<>();
        for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
            final Symbol columnSymbol = entry.getKey();
            final ColumnHandle columnHandle = entry.getValue();
            final String filterId = context.getId(columnSymbol);
            if (filterId == null) {
                continue;
            }

            // Try to get dynamic filter from local cache first
            DynamicFilter cachedDynamicFilter = cachedDynamicFilters.get(filterId);
            if (cachedDynamicFilter != null) {
                cachedDynamicFilter.setColumnHandle(columnHandle);
                result.put(columnHandle, cachedDynamicFilter);
                continue;
            }

            // Local dynamic filters
            if (predicates.containsKey(columnSymbol)) {
                DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, columnHandle, predicates.get(columnSymbol), LOCAL);
                cachedDynamicFilters.put(filterId, dynamicFilter);
                result.put(columnHandle, dynamicFilter);
            }
        }
        return result;
    }

    public void addStateStoreListeners()
    {
        this.stateStoreListeners = new DynamicFilterStateStoreListener(cachedDynamicFilters, queryId, dynamicFilterDataType);
        mergedDynamicFilters = stateStoreProvider.getStateStore().createStateMap(DynamicFilterUtils.MERGEMAP);
        mergedDynamicFilters.addEntryListener(stateStoreListeners);
        LOG.debug("Added listeners: " + stateStoreListeners);
    }

    private void removeStateStoreListeners(Boolean queryFinished)
    {
        if (mergedDynamicFilters != null) {
            mergedDynamicFilters.removeEntryListener(stateStoreListeners);
            LOG.debug("Removed listener: " + stateStoreListeners);
        }
    }
}
