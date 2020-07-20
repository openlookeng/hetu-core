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
package io.prestosql.dynamicfilter;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.hetu.core.common.dynamicfilter.BloomFilterDynamicFilter;
import io.hetu.core.common.dynamicfilter.HashSetDynamicFilter;
import io.hetu.core.common.util.BloomFilter;
import io.prestosql.execution.StageStateMachine;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.DynamicFilterUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType;
import static io.prestosql.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static java.util.Objects.requireNonNull;

public class DynamicFilterService
{
    private static final Logger log = Logger.get(DynamicFilterService.class);
    private final ScheduledExecutorService filterMergeExecutor;
    private static final int THREAD_POOL_SIZE = 3;
    private static final int UPDATE_INTERVAL = 20;
    private ScheduledFuture<?> backgroundTask;
    private boolean initialized;

    private Map<String, Map<String, DynamicFilterRegistryInfo>> dynamicFilters = new ConcurrentHashMap<>();
    private Map<String, CopyOnWriteArrayList<String>> dynamicFiltersToWorker = new ConcurrentHashMap<>();
    private static Map<String, Map<String, DynamicFilter>> cachedDynamicFilters = new HashMap<>();

    private final StateStoreProvider stateStoreProvider;

    /**
     * Dynamic Filter Service constructor
     *
     * @param stateStoreProvider the State Store
     */
    @Inject
    public DynamicFilterService(StateStoreProvider stateStoreProvider)
    {
        requireNonNull(stateStoreProvider, "State store is null");
        this.stateStoreProvider = stateStoreProvider;
        this.filterMergeExecutor = Executors.newScheduledThreadPool(THREAD_POOL_SIZE, threadsNamed("dynamic-filter-service-%s"));
    }

    /**
     * Starting the Dynamic Filter Service
     */
    @PostConstruct
    public void start()
    {
        checkState(backgroundTask == null, "Dynamic filter merger already started");
        backgroundTask = filterMergeExecutor.scheduleWithFixedDelay(() -> {
            try {
                if (this.stateStoreProvider.getStateStore() != null) {
                    mergeDynamicFilters();
                }
            }
            catch (NullPointerException e) {
                log.error("Error updating query states: " + e.getMessage());
            }
        }, UPDATE_INTERVAL, UPDATE_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Stopping the Dynamic Filter Service
     */
    @PreDestroy
    public void stop()
    {
        filterMergeExecutor.shutdownNow();
    }

    /**
     * Global Dynamic Filter merging, periodically looks for dynamic filters that can be merged and merges them
     */
    private void mergeDynamicFilters()
    {
        for (Map.Entry<String, Map<String, DynamicFilterRegistryInfo>> outerEntry : dynamicFilters.entrySet()) {
            String queryId = outerEntry.getKey();
            for (Map.Entry<String, DynamicFilterRegistryInfo> entry : outerEntry.getValue().entrySet()) {
                String filterId = entry.getKey();
                Symbol column = entry.getValue().getSymbol();
                String filterKey = DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId);

                if (hasMergeCondition(filterId, queryId)) {
                    Collection<Object> results = ((StateSet) stateStoreProvider.getStateStore()
                            .getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId))).getAll();

                    String typeKey = DynamicFilterUtils.createKey(DynamicFilterUtils.TYPEPREFIX, entry.getKey(), queryId);
                    String type = (String) ((StateMap) stateStoreProvider.getStateStore()
                            .getStateCollection(DynamicFilterUtils.DFTYPEMAP)).get(typeKey);
                    if (type != null) {
                        if (type.equals(DynamicFilterUtils.BLOOMFILTERTYPEGLOBAL) || type.equals(DynamicFilterUtils.BLOOMFILTERTYPELOCAL)) {
                            BloomFilter mergedFilter = mergeBloomFilters(results);
                            if (mergedFilter == null || mergedFilter.expectedFpp() > DynamicFilterUtils.BLOOM_FILTER_EXPECTED_FPP) {
                                if (mergedFilter == null) {
                                    log.error("could not merge dynamic filter");
                                }
                                else {
                                    log.info("FPP too high: " + mergedFilter.approximateElementCount());
                                }
                                clearPartialResults(filterId, queryId);
                                return;
                            }

                            if (!cachedDynamicFilters.containsKey(queryId)) {
                                cachedDynamicFilters.put(queryId, new ConcurrentHashMap<>());
                            }

                            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                                mergedFilter.writeTo(out);
                                byte[] filter = out.toByteArray();
                                cachedDynamicFilters.get(queryId).put(filterId, new BloomFilterDynamicFilter(filterKey, null, filter, DynamicFilter.Type.GLOBAL));
                                ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP)).put(filterKey, filter);
                                // remove the filter so we don't need to monitor it anymore
                                outerEntry.getValue().remove(filterId);
                                log.debug("Merged successfully dynamic filter id: "
                                        + filterId + "-" + queryId + " type: " + type + ", column: " + column + ", item count: "
                                        + mergedFilter.approximateElementCount() + ", fpp: " + mergedFilter.expectedFpp());
                            }
                            catch (IOException e) {
                                log.error(e);
                            }
                            finally {
                                clearPartialResults(filterId, queryId);
                            }
                        }
                        else if (type.equals(DynamicFilterUtils.HASHSETTYPEGLOBAL) || type.equals(DynamicFilterUtils.HASHSETTYPELOCAL)) {
                            Set merged = mergeHashSets(results);
                            if (merged == null) {
                                log.error("could not merge dynamic filter");
                                clearPartialResults(filterId, queryId);
                                return;
                            }
                            if (!cachedDynamicFilters.containsKey(queryId)) {
                                cachedDynamicFilters.put(queryId, new ConcurrentHashMap<>());
                            }
                            cachedDynamicFilters.get(queryId).put(filterId, new HashSetDynamicFilter(filterKey, null, merged, DynamicFilter.Type.GLOBAL));
                            ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP)).put(filterKey, merged);
                            // remove the filter so we don't need to monitor it anymore
                            outerEntry.getValue().remove(filterId);
                            log.info("Merged successfully dynamic filter id using stringsets: " + entry.getKey() + "-" + queryId + " type: " + type + ", column: " + entry.getValue() + ", item count: " + merged.size());
                            clearPartialResults(filterId, queryId);
                        }
                    }
                }
            }
        }
    }

    private BloomFilter mergeBloomFilters(Collection partialBloomFilters)
    {
        BloomFilter mergedFilter = null;
        for (Object partialBloomFilter : partialBloomFilters) {
            try {
                BloomFilter deserializedBloomFilter = BloomFilter.readFrom(new ByteArrayInputStream((byte[]) partialBloomFilter));
                if (mergedFilter == null) {
                    mergedFilter = deserializedBloomFilter;
                }
                else {
                    mergedFilter.merge(deserializedBloomFilter);
                }
            }
            catch (IOException e) {
                mergedFilter = null;
                log.error(e);
                break;
            }
        }
        return mergedFilter;
    }

    private Set mergeHashSets(Collection<Object> results)
    {
        Set merged = new HashSet<>();
        for (Object o : results) {
            Set s = (Set) o;
            merged.addAll(s);
        }
        return merged;
    }

    private boolean hasMergeCondition(String filterkey, String queryId)
    {
        int registeredNum = 0;
        int workersNum = 0;
        int finishedNum = 0;

        StateCollection temp = stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterkey, queryId));
        if (temp != null) {
            registeredNum = temp.size();
        }
        temp = stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterkey, queryId));
        if (temp != null) {
            finishedNum = temp.size();
        }
        temp = stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterkey, queryId));
        if (temp != null) {
            workersNum = temp.size();
        }

        if (registeredNum > 0 &&
                registeredNum == finishedNum && workersNum > 0 && workersNum == dynamicFiltersToWorker.get(filterkey + "-" + queryId).size()) {
            return true;
        }
        return false;
    }

    /**
     * Registering tasks for global dynamic filters
     *
     * @param node the Join node from the logical plan
     * @param taskIds set of task Ids
     * @param workers set of workers
     * @param stateMachine the state machine
     */
    public void registerTasks(JoinNode node, Set<TaskId> taskIds, Set<InternalNode> workers, StageStateMachine stateMachine)
    {
        if (taskIds.isEmpty() || stateStoreProvider.getStateStore() == null) {
            return;
        }
        String queryId = stateMachine.getSession().getQueryId().toString();
        if (!initialized) {
            stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.MERGEMAP, StateCollection.Type.MAP);
            stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.DFTYPEMAP, StateCollection.Type.MAP);
            initialized = true;
        }

        for (Map.Entry<String, Symbol> entry : node.getDynamicFilters().entrySet()) {
            Symbol buildSymbol = node.getCriteria().get(0).getRight();
            if (entry.getValue().getName().equals(buildSymbol.getName())) {
                String filterId = entry.getKey();
                stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId), StateCollection.Type.SET);
                stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId), StateCollection.Type.SET);
                stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId), StateCollection.Type.SET);
                stateStoreProvider.getStateStore().createStateCollection(DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId), StateCollection.Type.SET);

                dynamicFilters.putIfAbsent(queryId, new ConcurrentHashMap<>());
                Map<String, DynamicFilterRegistryInfo> filters = dynamicFilters.get(queryId);
                filters.put(filterId, extractDynamicFilterRegistryInfo(node));

                dynamicFiltersToWorker.putIfAbsent(filterId + "-" + queryId, new CopyOnWriteArrayList<>());
                CopyOnWriteArrayList workersSet = dynamicFiltersToWorker.get(filterId + "-" + queryId);
                workersSet.addAll(workers.stream().map(x -> x.getNodeIdentifier()).collect(Collectors.toSet()));

                log.debug("registerTasks source " + filterId + " filters:" + filters + ", workers: " + workers.stream().map(x -> x.getNodeIdentifier()).collect(Collectors.joining(",")) +
                        ", taskIds: " + taskIds.stream().map(TaskId::toString).collect(Collectors.joining(",")));
            }
        }
    }

    /**
     * Clear dynamic filter tasks and data created for a query
     *
     * @param queryId query id for dynamic filter tasks and data to cleanup
     */
    public void clearDynamicFiltersForQuery(String queryId)
    {
        // Clear registered dynamic filter tasks
        Map<String, DynamicFilterRegistryInfo> filters = dynamicFilters.get(queryId);
        if (filters != null) {
            for (Entry<String, DynamicFilterRegistryInfo> entry : filters.entrySet()) {
                String filterId = entry.getKey();
                clearPartialResults(entry.getKey(), queryId);
                dynamicFiltersToWorker.remove(filterId + "-" + queryId);

                // Clear cached dynamic filters in state store
                String filterKey = DynamicFilterUtils.createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId);
                ((StateMap) stateStoreProvider.getStateStore().getStateCollection(DynamicFilterUtils.MERGEMAP)).remove(filterKey);
            }
        }
        dynamicFilters.remove(queryId);

        // Clear cached dynamic filters locally
        cachedDynamicFilters.remove(queryId);
    }

    /**
     * Cleaning local cache and state store cache
     *
     * @param filterId, part of the id for dynamic filter that will be cleaned
     * @param queryId, query id for part of the id of dynamic filter that will be cleaned
     */
    private void clearPartialResults(String filterId, String queryId)
    {
        StateStore stateStore = stateStoreProvider.getStateStore();
        if (stateStore != null) {
            // Todo: also remove the state store collections
            clearStatesInStateStore(stateStore, DynamicFilterUtils.createKey(DynamicFilterUtils.REGISTERPREFIX, filterId, queryId));
            clearStatesInStateStore(stateStore, DynamicFilterUtils.createKey(DynamicFilterUtils.FINISHREFIX, filterId, queryId));
            clearStatesInStateStore(stateStore, DynamicFilterUtils.createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId));
            clearStatesInStateStore(stateStore, DynamicFilterUtils.createKey(DynamicFilterUtils.WORKERSPREFIX, filterId, queryId));
        }
    }

    private static void clearStatesInStateStore(StateStore stateStore, String stateCollectionName)
    {
        StateCollection states = stateStore.getStateCollection(stateCollectionName);
        if (states != null) {
            states.destroy();
        }
        stateStore.removeStateCollection(stateCollectionName);
    }

    /**
     * Create a supplier that supplies available dynamic filters for a query
     * based on dynamic filter descriptor created in logical plan
     * dynamic filter can be available at any time
     *
     * @param queryId query id of the query
     * @param dynamicFilters dynamic filter descriptors from logical plan
     * @param columnHandles column handles of the table to be scanned
     * @return supplier that may contain a set of dynamic filters
     */
    public static Supplier<Set<DynamicFilter>> getDynamicFilterSupplier(QueryId queryId, List<DynamicFilters.Descriptor> dynamicFilters, Map<Symbol, ColumnHandle> columnHandles)
    {
        Map<String, ColumnHandle> sourceColumnHandles = extractSourceExpressionSymbols(dynamicFilters)
                .entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> columnHandles.get(entry.getValue())));
        return () -> {
            ImmutableSet.Builder<DynamicFilter> builder = ImmutableSet.builder();

            if (sourceColumnHandles.isEmpty() || !cachedDynamicFilters.containsKey(queryId.getId())) {
                return builder.build();
            }

            Map<String, DynamicFilter> cachedDynamicFiltersForQuery = cachedDynamicFilters.get(queryId.getId());
            if (cachedDynamicFiltersForQuery.isEmpty()) {
                return builder.build();
            }

            for (DynamicFilters.Descriptor dynamicFilterDescriptor : dynamicFilters) {
                String filterId = dynamicFilterDescriptor.getId();
                if (cachedDynamicFiltersForQuery.containsKey(filterId) && sourceColumnHandles.containsKey(filterId)) {
                    ColumnHandle column = sourceColumnHandles.get(filterId);
                    DynamicFilter df = cachedDynamicFiltersForQuery.get(filterId).clone();
                    df.setColumnHandle(column);
                    builder.add(df);
                }
            }
            return builder.build();
        };
    }

    private static Map<String, Symbol> extractSourceExpressionSymbols(List<DynamicFilters.Descriptor> dynamicFilters)
    {
        ImmutableMap.Builder<String, Symbol> resultBuilder = ImmutableMap.builder();
        for (DynamicFilters.Descriptor descriptor : dynamicFilters) {
            Expression expression = descriptor.getInput();
            if (!(expression instanceof SymbolReference)) {
                continue;
            }
            resultBuilder.put(descriptor.getId(), Symbol.from(expression));
        }
        return resultBuilder.build();
    }

    private static DynamicFilterRegistryInfo extractDynamicFilterRegistryInfo(JoinNode node)
    {
        Symbol symbol = node.getCriteria().get(0).getLeft();
        DistributionType joinType = node.getDistributionType().orElse(PARTITIONED);

        if (joinType == PARTITIONED) {
            return new DynamicFilterRegistryInfo(symbol, GLOBAL);
        }
        else {
            return new DynamicFilterRegistryInfo(symbol, LOCAL);
        }
    }

    static class DynamicFilterRegistryInfo
    {
        private Symbol symbol;
        private Type type;

        public DynamicFilterRegistryInfo(Symbol symbol, Type type)
        {
            this.symbol = symbol;
            this.type = type;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Type getType()
        {
            return type;
        }
    }
}
