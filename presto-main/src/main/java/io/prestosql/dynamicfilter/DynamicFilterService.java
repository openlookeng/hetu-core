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
import io.prestosql.Session;
import io.prestosql.execution.StageStateMachine;
import io.prestosql.execution.TaskId;
import io.prestosql.metadata.InternalNode;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter.DataType;
import io.prestosql.spi.dynamicfilter.DynamicFilterFactory;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statestore.StateCollection;
import io.prestosql.spi.statestore.StateMap;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.statestore.StateStoreProvider;
import io.prestosql.utils.DynamicFilterUtils;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringDataType;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.HASHSET;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.GLOBAL;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.Type.LOCAL;
import static io.prestosql.spi.statestore.StateCollection.Type.MAP;
import static io.prestosql.spi.statestore.StateCollection.Type.SET;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static io.prestosql.utils.DynamicFilterUtils.findFilterNodeInStage;
import static io.prestosql.utils.DynamicFilterUtils.getDynamicFilterDataType;
import static java.util.Objects.requireNonNull;

public class DynamicFilterService
{
    private static final Logger log = Logger.get(DynamicFilterService.class);
    private final ScheduledExecutorService filterMergeExecutor;
    private static final int THREAD_POOL_SIZE = 1;
    private static final int MERGE_DYNAMIC_FILTER_INTERVAL = 1;
    private ScheduledFuture<?> backgroundTask;

    private final Map<String, Map<String, DynamicFilterRegistryInfo>> dynamicFilters = new ConcurrentHashMap<>();
    private final Map<String, CopyOnWriteArraySet<TaskId>> dynamicFiltersToTask = new ConcurrentHashMap<>();
    private static final Map<String, Map<String, DynamicFilter>> cachedDynamicFilters = new HashMap<>();
    private final List<String> finishedQuery = Collections.synchronizedList(new ArrayList<>());

    private final StateStoreProvider stateStoreProvider;

    /**
     * Dynamic Filter Service constructor
     *
     * @param stateStoreProvider the State Store
     */
    @Inject
    public DynamicFilterService(StateStoreProvider stateStoreProvider)
    {
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "StateStoreProvider is null");
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
                    removeFinishedQuery();
                }
            }
            catch (Exception e) {
                log.error("Error merging Dynamic Filters: " + e.getMessage());
            }
        }, 0, MERGE_DYNAMIC_FILTER_INTERVAL, TimeUnit.MILLISECONDS);
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
        final StateStore stateStore = stateStoreProvider.getStateStore();
        for (Map.Entry<String, Map<String, DynamicFilterRegistryInfo>> queryToDynamicFiltersEntry : dynamicFilters.entrySet()) {
            final String queryId = queryToDynamicFiltersEntry.getKey();
            if (!cachedDynamicFilters.containsKey(queryId)) {
                cachedDynamicFilters.put(queryId, new ConcurrentHashMap<>());
            }
            Map<String, DynamicFilter> cachedDynamicFiltersForQuery = cachedDynamicFilters.get(queryId);
            StateMap mergedDynamicFilters = (StateMap) stateStore.getOrCreateStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS, MAP);

            for (Map.Entry<String, DynamicFilterRegistryInfo> columnToDynamicFilterEntry : queryToDynamicFiltersEntry.getValue().entrySet()) {
                if (columnToDynamicFilterEntry.getValue().isMerged()) {
                    continue;
                }

                final String filterId = columnToDynamicFilterEntry.getKey();
                final Type filterType = columnToDynamicFilterEntry.getValue().getType();
                final DataType filterDataType = columnToDynamicFilterEntry.getValue().getDataType();
                final Optional<Predicate<List>> dfFilter = columnToDynamicFilterEntry.getValue().getFilter();
                final Symbol column = columnToDynamicFilterEntry.getValue().getSymbol();
                final String filterKey = createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId);

                if (!hasMergeCondition(filterId, queryId)) {
                    continue;
                }

                Collection<Object> results = ((StateSet) stateStore.getStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId))).getAll();
                try {
                    DynamicFilter mergedFilter;
                    if (filterDataType == BLOOM_FILTER) {
                        BloomFilter mergedBloomFilter = mergeBloomFilters(results);
                        if (mergedBloomFilter.expectedFpp() > DynamicFilterUtils.BLOOM_FILTER_EXPECTED_FPP) {
                            throw new PrestoException(GENERIC_INTERNAL_ERROR, "FPP too high: " + mergedBloomFilter.approximateElementCount());
                        }
                        mergedFilter = new BloomFilterDynamicFilter(filterKey, null, mergedBloomFilter, filterType);

                        if (filterType == GLOBAL) {
                            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                                mergedBloomFilter.writeTo(out);
                                byte[] filter = out.toByteArray();
                                mergedDynamicFilters.put(filterKey, filter);
                            }
                        }
                    }
                    else if (filterDataType == HASHSET) {
                        Set mergedSet = mergeHashSets(results);
                        mergedFilter = DynamicFilterFactory.create(filterKey, null, mergedSet, filterType, dfFilter);

                        if (filterType == GLOBAL) {
                            mergedDynamicFilters.put(filterKey, mergedSet);
                        }
                    }
                    else {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unsupported filter data type: " + filterDataType);
                    }

                    log.debug("Merged successfully dynamic filter id: "
                            + filterId + "-" + queryId + " type: " + filterDataType
                            + ", column: " + column + ", item count: " + mergedFilter.getSize());
                    cachedDynamicFiltersForQuery.put(filterId, mergedFilter);
                }
                catch (IOException | PrestoException e) {
                    log.warn("Could not merge dynamic filter: " + e.getLocalizedMessage());
                }
                finally {
                    // for each dynamic filter we only try to merge it once
                    columnToDynamicFilterEntry.getValue().setMerged();
                }
            }
        }
    }

    private void removeFinishedQuery()
    {
        List<String> handledQuery = new ArrayList<>();
        StateMap mergedStateCollection = (StateMap) stateStoreProvider.getStateStore().getOrCreateStateCollection(DynamicFilterUtils.MERGED_DYNAMIC_FILTERS, MAP);
        // Clear registered dynamic filter tasks
        for (String queryId : finishedQuery) {
            Map<String, DynamicFilterRegistryInfo> filters = dynamicFilters.get(queryId);
            if (filters != null) {
                for (Entry<String, DynamicFilterRegistryInfo> entry : filters.entrySet()) {
                    String filterId = entry.getKey();
                    clearPartialResults(filterId, queryId);
                    if (entry.getValue().isMerged()) {
                        String filterKey = createKey(DynamicFilterUtils.FILTERPREFIX, filterId, queryId);
                        mergedStateCollection.remove(filterKey);
                    }
                }
            }
            dynamicFilters.remove(queryId);

            cachedDynamicFilters.remove(queryId);
            handledQuery.add(queryId);
        }
        finishedQuery.removeAll(handledQuery);
    }

    private static BloomFilter mergeBloomFilters(Collection<Object> partialBloomFilters)
            throws IOException
    {
        BloomFilter mergedFilter = null;
        for (Object partialBloomFilter : partialBloomFilters) {
            BloomFilter deserializedBloomFilter = BloomFilter.readFrom(new ByteArrayInputStream((byte[]) partialBloomFilter));
            if (mergedFilter == null) {
                mergedFilter = deserializedBloomFilter;
            }
            else {
                mergedFilter.merge(deserializedBloomFilter);
            }
        }
        return mergedFilter;
    }

    private static Set<?> mergeHashSets(Collection<Object> results)
            throws IOException
    {
        Set<?> merged = new HashSet<>();
        for (Object o : results) {
            if (!(o instanceof Set)) {
                throw new IOException("Partial HashSet DynamicFilter is invalid.");
            }
            merged.addAll((Set) o);
        }
        return merged;
    }

    private boolean hasMergeCondition(String filterKey, String queryId)
    {
        int finishedDynamicFilterNumber = 0;
        final StateStore stateStore = stateStoreProvider.getStateStore();

        StateCollection temp = stateStore.getStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterKey, queryId));
        if (temp != null) {
            finishedDynamicFilterNumber = temp.size();
        }

        return finishedDynamicFilterNumber > 0 && finishedDynamicFilterNumber == dynamicFiltersToTask.get(filterKey + "-" + queryId).size();
    }

    /**
     * Registering tasks for global dynamic filters
     *
     * @param node the Join node from the logical plan
     * @param taskIds set of task Ids
     * @param workers set of workers
     * @param stateMachine the state machine
     */
    public void registerTasks(PlanNode node, Set<TaskId> taskIds, Set<InternalNode> workers, StageStateMachine stateMachine)
    {
        if (taskIds.isEmpty() || stateStoreProvider.getStateStore() == null) {
            return;
        }
        if (node instanceof JoinNode) {
            JoinNode joinNode = (JoinNode) node;
            List<JoinNode.EquiJoinClause> criterias = joinNode.getCriteria();
            if (!criterias.isEmpty()) {
                registerTasksHelper(node, criterias.get(0).getRight(), joinNode.getDynamicFilters(), taskIds, workers, stateMachine);
            }
            else {
                log.warn("registerTasks is empty");
            }
        //    registerTasksHelper(node, (joinNode.getCriteria().isEmpty() ? null : joinNode.getCriteria().get(0).getRight()), joinNode.getDynamicFilters(), taskIds, workers, stateMachine);
        }
        else if (node instanceof SemiJoinNode) {
            SemiJoinNode semiJoinNode = (SemiJoinNode) node;
            if (semiJoinNode.getDynamicFilterId().isPresent()) {
                registerTasksHelper(node, semiJoinNode.getFilteringSourceJoinSymbol(), Collections.singletonMap(semiJoinNode.getDynamicFilterId().get(), semiJoinNode.getFilteringSourceJoinSymbol()), taskIds, workers, stateMachine);
            }
        }
    }

    private void registerTasksHelper(PlanNode node, Symbol buildSymbol, Map<String, Symbol> dynamicFiltersMap, Set<TaskId> taskIds, Set<InternalNode> workers, StageStateMachine stateMachine)
    {
        final StateStore stateStore = stateStoreProvider.getStateStore();
        String queryId = stateMachine.getSession().getQueryId().toString();
        for (Map.Entry<String, Symbol> entry : dynamicFiltersMap.entrySet()) {
            Symbol buildSymbolToCheck = buildSymbol != null ? buildSymbol : node.getOutputSymbols().contains(entry.getValue()) ? entry.getValue() : null;
            if (buildSymbolToCheck != null && entry.getValue().getName().equals(buildSymbol.getName())) {
                String filterId = entry.getKey();
                stateStore.createStateCollection(createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId), SET);
                stateStore.createStateCollection(createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId), SET);
                dynamicFilters.putIfAbsent(queryId, new ConcurrentHashMap<>());
                Map<String, DynamicFilterRegistryInfo> filters = dynamicFilters.get(queryId);
                if (node instanceof JoinNode) {
                    filters.put(filterId, extractDynamicFilterRegistryInfo((JoinNode) node, stateMachine.getSession(), filterId));
                }
                else if (node instanceof SemiJoinNode) {
                    filters.put(filterId, extractDynamicFilterRegistryInfo((SemiJoinNode) node, stateMachine.getSession()));
                }
                dynamicFiltersToTask.putIfAbsent(filterId + "-" + queryId, new CopyOnWriteArraySet<>());
                CopyOnWriteArraySet<TaskId> taskSet = dynamicFiltersToTask.get(filterId + "-" + queryId);
                taskSet.addAll(taskIds);
                log.debug("registerTasks source " + filterId + " filters:" + filters + ", workers: "
                        + workers.stream().map(x -> x.getNodeIdentifier()).collect(Collectors.joining(",")) +
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
        finishedQuery.add(queryId);
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
            clearStatesInStateStore(stateStore, createKey(DynamicFilterUtils.PARTIALPREFIX, filterId, queryId));
            clearStatesInStateStore(stateStore, createKey(DynamicFilterUtils.TASKSPREFIX, filterId, queryId));
        }
        dynamicFiltersToTask.remove(filterId + "-" + queryId);
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
            RowExpression expression = descriptor.getInput();

            // Extract the column symbols from CAST expressions
            while (expression instanceof CallExpression) {
                expression = ((CallExpression) expression).getArguments().get(0);
            }

            if (!(expression instanceof VariableReferenceExpression)) {
                continue;
            }
            resultBuilder.put(descriptor.getId(), new Symbol(((VariableReferenceExpression) expression).getName()));
        }
        return resultBuilder.build();
    }

    private static DynamicFilterRegistryInfo extractDynamicFilterRegistryInfo(JoinNode node, Session session, String filterId)
    {
        Symbol symbol = node.getCriteria().isEmpty() ? null : node.getCriteria().get(0).getLeft();
        List<FilterNode> filterNodes = findFilterNodeInStage(node);

        if (filterNodes.isEmpty()) {
            return new DynamicFilterRegistryInfo(symbol, GLOBAL, session, Optional.empty());
        }
        else {
            Optional<Predicate<List>> filterPredicate = Optional.empty();
            if (symbol == null) {
                //Symbol is not found in Join Node. It must have been pushed down to filters.
                for (FilterNode filter : filterNodes) {
                    DynamicFilters.ExtractResult extractResult = DynamicFilters.extractDynamicFilters(filter.getPredicate());
                    List<DynamicFilters.Descriptor> dynamicConjuncts = extractResult.getDynamicConjuncts();
                    for (DynamicFilters.Descriptor desc : dynamicConjuncts) {
                        if (desc.getId().equals(filterId)) {
                            checkArgument(desc.getInput() instanceof VariableReferenceExpression, "Expression not symbol reference");
                            symbol = new Symbol(((VariableReferenceExpression) desc.getInput()).getName());
                            if (desc.getFilter().isPresent()) {
                                filterPredicate = DynamicFilters.createDynamicFilterPredicate(desc.getFilter());
                            }
                            break;
                        }
                    }
                    if (symbol != null) {
                        break;
                    }
                }
                if (symbol == null) {
                    throw new IllegalStateException("DynamicFilter symbol not found to register");
                }
            }
            return new DynamicFilterRegistryInfo(symbol, LOCAL, session, filterPredicate);
        }
    }

    private static DynamicFilterRegistryInfo extractDynamicFilterRegistryInfo(SemiJoinNode node, Session session)
    {
        Symbol symbol = node.getFilteringSourceJoinSymbol();
        List<FilterNode> filterNodes = findFilterNodeInStage(node);

        if (filterNodes.isEmpty()) {
            return new DynamicFilterRegistryInfo(symbol, GLOBAL, session, Optional.empty());
        }
        else {
            return new DynamicFilterRegistryInfo(symbol, LOCAL, session, Optional.empty());
        }
    }

    private static class DynamicFilterRegistryInfo
    {
        private final Symbol symbol;
        private final Type type;
        private final DataType dataType;
        private boolean isMerged;
        private Optional<Predicate<List>> filter;

        public DynamicFilterRegistryInfo(Symbol symbol, Type type, Session session, Optional<Predicate<List>> filter)
        {
            this.symbol = symbol;
            this.type = type;
            this.dataType = getDynamicFilterDataType(type, getDynamicFilteringDataType(session));
            this.isMerged = false;
            this.filter = filter;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Type getType()
        {
            return type;
        }

        public DataType getDataType()
        {
            return dataType;
        }

        public boolean isMerged()
        {
            return isMerged;
        }

        public void setMerged()
        {
            this.isMerged = true;
        }

        public Optional<Predicate<List>> getFilter()
        {
            return filter;
        }
    }
}
