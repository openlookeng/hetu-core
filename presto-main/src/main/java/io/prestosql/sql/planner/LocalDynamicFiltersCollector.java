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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.connector.DataCenterUtility;
import io.prestosql.dynamicfilter.DynamicFilterCacheManager;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.TaskContext;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.rewrite.DynamicFilterContext;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Predicate;

import static io.prestosql.SystemSessionProperties.isCrossRegionDynamicFilterEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.dynamicfilter.DynamicFilterCacheManager.createCacheKey;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static io.prestosql.statestore.StateStoreConstants.CROSS_LAYER_DYNAMIC_FILTER;
import static java.util.Objects.requireNonNull;

public class LocalDynamicFiltersCollector
{
    public static final Logger LOG = Logger.get(LocalDynamicFiltersCollector.class);
    private DynamicFilterContext context;
    private Optional<Metadata> metadataOptional;

    /**
     * May contains domains for dynamic filters for different table scans
     * (e.g. in case of co-located joins).
     */
    private final Map<String, Set<?>> predicates = new ConcurrentHashMap<>();
    private final Map<String, DynamicFilter> cachedDynamicFilters = new ConcurrentHashMap<>();
    private final DynamicFilterCacheManager dynamicFilterCacheManager;
    private final String queryId;
    private final TaskId taskId;
    private final Session session;

    /**
     * Constructor for the LocalDynamicFiltersCollector
     */
    LocalDynamicFiltersCollector(TaskContext taskContext, Optional<Metadata> metadataOptional, DynamicFilterCacheManager dynamicFilterCacheManager)
    {
        requireNonNull(taskContext, "taskContext is null");
        session = taskContext.getSession();
        this.queryId = session.getQueryId().getId();
        this.taskId = taskContext.getTaskId();
        this.dynamicFilterCacheManager = requireNonNull(dynamicFilterCacheManager, "dynamicFilterCacheManager is null");
        this.metadataOptional = metadataOptional;

        if (isEnableDynamicFiltering(session)) {
            taskContext.onTaskFinished(this::removeDynamicFilter);
        }
    }

    void initContext(List<List<DynamicFilters.Descriptor>> descriptors, Map<Integer, Symbol> layOut)
    {
        if (context == null) {
            context = new DynamicFilterContext(descriptors, layOut);
        }
    }

    void intersectDynamicFilter(Map<String, Set> predicate)
    {
        for (Map.Entry<String, Set> entry : predicate.entrySet()) {
            if (!predicates.containsKey(entry.getKey())) {
                predicates.put(entry.getKey(), entry.getValue());
                continue;
            }

            Set predicateSet = predicates.get(entry.getKey());
            Set newValues = entry.getValue();
            predicateSet.addAll(newValues);
        }
    }

    /**
     * This function returns the bloom filters fetched from the state store. To prevent excessive reads from state store,
     * it caches fetched bloom filters for re-use
     *
     * @param tableScan TableScanNode that has DynamicFilter applied
     * @return ColumnHandle to DynamicFilter mapping that contains any DynamicFilter that are ready for use
     */
    List<Map<ColumnHandle, DynamicFilter>> getDynamicFilters(TableScanNode tableScan)
    {
        Map<Symbol, ColumnHandle> assignments = tableScan.getAssignments();
        // Skips symbols irrelevant to this table scan node.
        Set<String> columnNames = new HashSet<>();
        List<Map<ColumnHandle, DynamicFilter>> resultList = new ArrayList<>();
        for (int i = 0; i < context.getDisjunctSize(); i++) {
            Map<ColumnHandle, DynamicFilter> result = new HashMap<ColumnHandle, DynamicFilter>();
            for (Map.Entry<Symbol, ColumnHandle> entry : assignments.entrySet()) {
                final Symbol columnSymbol = entry.getKey();
                final ColumnHandle columnHandle = entry.getValue();
                try {
                    columnNames.add(columnHandle.getColumnName());
                }
                catch (NotImplementedException e) {
                    // ignore this exception, maybe some implementation class not implement the default method.
                }

                final List<String> filterIds = context.getId(columnSymbol, i);
                if (filterIds == null || filterIds.isEmpty()) {
                    continue;
                }

                for (String filterId : filterIds) {
                    // Try to get dynamic filter from local cache first
                    String cacheKey = createCacheKey(filterId, queryId);
                    DynamicFilter cachedDynamicFilter = cachedDynamicFilters.get(filterId);
                    if (cachedDynamicFilter == null) {
                        cachedDynamicFilter = dynamicFilterCacheManager.getDynamicFilter(cacheKey);
                    }

                    if (cachedDynamicFilter != null) {
                        //Combine multiple dynamic filters for same column handle
                        DynamicFilter dynamicFilter = result.get(columnHandle);
                        //Same dynamic filter might be referred in multiple table scans for different columns due multi table joins.
                        //So clone before setting the columnHandle to avoid race in setting the columnHandle.
                        cachedDynamicFilter = cachedDynamicFilter.clone();
                        cachedDynamicFilter.setColumnHandle(columnHandle);
                        if (dynamicFilter == null) {
                            dynamicFilter = cachedDynamicFilter;
                        }
                        else {
                            dynamicFilter = DynamicFilterFactory.combine(columnHandle, dynamicFilter, cachedDynamicFilter);
                        }
                        dynamicFilter.setColumnHandle(columnHandle);
                        result.put(columnHandle, dynamicFilter);
                        continue;
                    }

                    // Local dynamic filters
                    if (predicates.containsKey(filterId)) {
                        Optional<RowExpression> filter = context.getFilter(filterId, i);
                        Optional<Predicate<List>> filterPredicate = DynamicFilters.createDynamicFilterPredicate(filter);
                        DynamicFilter dynamicFilter = DynamicFilterFactory.create(filterId, columnHandle, predicates.get(filterId), LOCAL, filterPredicate, filter);
                        cachedDynamicFilters.put(filterId, dynamicFilter);
                        result.put(columnHandle, dynamicFilter);
                    }
                }
            }
            if (!result.isEmpty()) {
                resultList.add(result);
            }
        }

        if (isCrossRegionDynamicFilterEnabled(session)) {
            if (!metadataOptional.isPresent()) {
                return resultList;
            }

            // check the tableScan is a dc connector table,if a dc table, should consider push down the cross region bloom filter to next cluster
            if (!DataCenterUtility.isDCCatalog(metadataOptional.get(), tableScan.getTable().getCatalogName().getCatalogName())) {
                return resultList;
            }
            // stateMap, key is dc-connector-table column name, value is bloomFilter bytes
            Map<String, byte[]> newBloomFilterFromStateStoreCache = dynamicFilterCacheManager.getBloomFitler(session.getQueryId().getId() + CROSS_LAYER_DYNAMIC_FILTER);

            if (newBloomFilterFromStateStoreCache == null) {
                return resultList;
            }

            // check tableScan contains the stateMap.key, if contains, should push the filter to next cluster
            for (Map.Entry<String, byte[]> entry : newBloomFilterFromStateStoreCache.entrySet()) {
                if (!columnNames.contains(entry.getKey())) {
                    continue;
                }

                ColumnHandle columnHandle = new ColumnHandle() {
                    @Override
                    public String getColumnName()
                    {
                        return entry.getKey();
                    }
                };

                BloomFilterDynamicFilter newBloomDynamicFilter = new BloomFilterDynamicFilter("", columnHandle, entry.getValue(), GLOBAL);

                for (Map<ColumnHandle, DynamicFilter> result : resultList) {
                    if (result.keySet().contains(entry.getKey())) {
                        DynamicFilter existsFilter = result.get(entry.getKey());
                        if (existsFilter instanceof BloomFilterDynamicFilter) {
                            BloomFilter existsBloomFilter = ((BloomFilterDynamicFilter) existsFilter).getBloomFilterDeserialized();
                            existsBloomFilter.merge(newBloomDynamicFilter.getBloomFilterDeserialized());
                            DynamicFilter newDynamicFilter = new BloomFilterDynamicFilter(existsFilter.getFilterId(), columnHandle, existsBloomFilter, GLOBAL);
                            result.put(columnHandle, newDynamicFilter);
                        }
                    }
                    else {
                        result.put(columnHandle, newBloomDynamicFilter);
                    }
                }
            }
        }

        if (resultList.size() != context.getDisjunctSize()) {
            return ImmutableList.of();
        }
        return resultList;
    }

    public boolean checkTableIsDcTable(TableScanNode tableScanNode)
    {
        if (metadataOptional.isPresent()) {
            // check the tableScan is a dc connector table
            if (DataCenterUtility.isDCCatalog(metadataOptional.get(), tableScanNode.getTable().getCatalogName().getCatalogName())) {
                return true;
            }
        }
        return false;
    }

    @VisibleForTesting
    public void removeDynamicFilter(Boolean taskFinished)
    {
        if (context != null) {
            for (int i = 0; i < context.getDisjunctSize(); i++) {
                for (String filterId : context.getFilterIds(i)) {
                    dynamicFilterCacheManager.removeDynamicFilter(createCacheKey(filterId, queryId), taskId);
                }
            }
        }
    }
}
