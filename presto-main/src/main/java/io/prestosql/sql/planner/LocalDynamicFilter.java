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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.operator.DynamicFilterSourceOperator;
import io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statestore.StateSet;
import io.prestosql.spi.statestore.StateStore;
import io.prestosql.spi.util.BloomFilter;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.statestore.StateStoreProvider;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringBloomFilterFpp;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringDataType;
import static io.prestosql.spi.dynamicfilter.BloomFilterDynamicFilter.convertBloomFilterToByteArray;
import static io.prestosql.spi.dynamicfilter.DynamicFilter.DataType.BLOOM_FILTER;
import static io.prestosql.spi.statestore.StateCollection.Type.SET;
import static io.prestosql.sql.DynamicFilters.Descriptor;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.utils.DynamicFilterUtils.PARTIALPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.TASKSPREFIX;
import static io.prestosql.utils.DynamicFilterUtils.createKey;
import static io.prestosql.utils.DynamicFilterUtils.findFilterNodeInStage;
import static io.prestosql.utils.DynamicFilterUtils.getDynamicFilterDataType;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class LocalDynamicFilter
{
    private static final Logger log = Logger.get(LocalDynamicFilter.class);

    // Mapping from dynamic filter ID to its probe symbols.
    private final Multimap<String, Symbol> probeSymbols;
    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<String, Integer> buildChannels;
    private final SettableFuture<TupleDomain<Symbol>> resultFuture;
    private final DynamicFilter.Type type;
    // If any partial dynamic filter is discarded due to too large
    private boolean isIncomplete;
    private SettableFuture<Map<String, Set>> dynamicFilterResultFuture;
    // Number of partitions left to be processed.
    private int partitionsLeft;
    // The resulting predicate for local dynamic filtering.
    private Map<String, Set> result = new HashMap<>();

    private FeaturesConfig.DynamicFilterDataType dynamicFilterDataType;
    private final double bloomFilterFpp;
    private final StateStoreProvider stateStoreProvider;
    private final TaskId taskId;
    private Map<String, DynamicFilterSourceOperator.Channel> channels = new HashMap<>();

    public LocalDynamicFilter(Multimap<String, Symbol> probeSymbols, Map<String, Integer> buildChannels, int partitionCount, DynamicFilter.Type type, Session session,
            TaskId taskId, StateStoreProvider stateStoreProvider)
    {
        this(probeSymbols, buildChannels, partitionCount, type, getDynamicFilteringDataType(session),
                getDynamicFilteringBloomFilterFpp(session), taskId, stateStoreProvider);
    }

    public LocalDynamicFilter(Multimap<String, Symbol> probeSymbols, Map<String, Integer> buildChannels, int partitionCount,
                              DynamicFilter.Type filterType, FeaturesConfig.DynamicFilterDataType dataType,
                              double bloomFilterFpp, TaskId taskId, StateStoreProvider stateStoreProvider)
    {
        this.probeSymbols = requireNonNull(probeSymbols, "probeSymbols is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(probeSymbols.keySet().equals(buildChannels.keySet()), "probeSymbols and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();
        this.dynamicFilterResultFuture = SettableFuture.create();

        this.partitionsLeft = partitionCount;

        this.type = filterType;
        this.dynamicFilterDataType = requireNonNull(dataType, "dynamic filter data type is null");
        this.bloomFilterFpp = bloomFilterFpp;
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.stateStoreProvider = requireNonNull(stateStoreProvider, "stateStore is null");
    }

    public static Optional<LocalDynamicFilter> create(JoinNode planNode, int partitionCount, Session session, TaskId taskId, StateStoreProvider stateStoreProvider)
    {
        Set<String> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        // Mapping from probe-side dynamic filters' IDs to their matching probe symbols.
        Multimap<String, Symbol> localProbeSymbols = MultimapBuilder.treeKeys().arrayListValues().build();
        PlanNode buildNode;
        DynamicFilter.Type localType = DynamicFilter.Type.LOCAL;

        List<FilterNode> filterNodes = findFilterNodeInStage(planNode);
        if (filterNodes.isEmpty()) {
            buildNode = planNode.getRight();
            mapProbeSymbolsFromCriteria(planNode.getDynamicFilters(), localProbeSymbols, planNode.getCriteria());
            localType = DynamicFilter.Type.GLOBAL;
        }
        else {
            buildNode = planNode.getRight();
            for (FilterNode filterNode : filterNodes) {
                mapProbeSymbols(filterNode.getPredicate(), joinDynamicFilters, localProbeSymbols);
            }
        }

        final List<Symbol> buildSideSymbols = buildNode.getOutputSymbols();

        Map<String, Integer> localBuildChannels = planNode
                .getDynamicFilters()
                .entrySet()
                .stream()
                // Skip build channels that don't match local probe dynamic filters.
                .filter(entry -> localProbeSymbols.containsKey(entry.getKey()))
                .collect(toMap(
                        // Dynamic filter ID
                        entry -> entry.getKey(),
                        // Build-side channel index
                        entry -> {
                            Symbol buildSymbol = entry.getValue();
                            int buildChannelIndex = buildSideSymbols.indexOf(buildSymbol);
                            verify(buildChannelIndex >= 0);
                            return buildChannelIndex;
                        }));

        if (localBuildChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LocalDynamicFilter(localProbeSymbols, localBuildChannels, partitionCount, localType, session, taskId, stateStoreProvider));
    }

    public static Optional<LocalDynamicFilter> create(SemiJoinNode semiJoinNode, Session session, TaskId taskId, StateStoreProvider stateStoreProvider)
    {
        if (!semiJoinNode.getDynamicFilterId().isPresent()) {
            return Optional.empty();
        }
        String dynamicFilterId = semiJoinNode.getDynamicFilterId().get();
        DynamicFilter.Type localType;
        List<FilterNode> localFilterNodeWithThisDynamicFiltering = findFilterNodeInStage(semiJoinNode);
        if (localFilterNodeWithThisDynamicFiltering.isEmpty()) {
            localType = DynamicFilter.Type.GLOBAL;
        }
        else {
            localType = DynamicFilter.Type.LOCAL;
        }
        Multimap<String, Symbol> probeSymbolMultiMap = ImmutableMultimap.of(dynamicFilterId, semiJoinNode.getSourceJoinSymbol());
        Map<String, Integer> localChannels = ImmutableMap.of(dynamicFilterId, semiJoinNode.getFilteringSource().getOutputSymbols().indexOf(semiJoinNode.getFilteringSourceJoinSymbol()));
        return Optional.of(new LocalDynamicFilter(probeSymbolMultiMap, localChannels, 1, localType, session, taskId, stateStoreProvider));
    }

    private static void mapProbeSymbols(RowExpression predicate, Set<String> joinDynamicFilters, Multimap<String, Symbol> probeSymbols)
    {
        DynamicFilters.ExtractResult extractResult = extractDynamicFilters(predicate);
        for (Descriptor descriptor : extractResult.getDynamicConjuncts()) {
            if (descriptor.getInput() instanceof VariableReferenceExpression) {
                // Add descriptors that match the local dynamic filter (from the current join node).
                if (joinDynamicFilters.contains(descriptor.getId())) {
                    Symbol probeSymbol = new Symbol(((VariableReferenceExpression) descriptor.getInput()).getName());
                    log.debug("Adding dynamic filter %s: %s", descriptor, probeSymbol);
                    probeSymbols.put(descriptor.getId(), probeSymbol);
                }
            }
        }
    }

    // For partitioned join we are not able to get probe side symbols from filter node, so get it from criteria
    private static void mapProbeSymbolsFromCriteria(Map<String, Symbol> joinDynamicFilters, Multimap<String, Symbol> probeSymbols, List<JoinNode.EquiJoinClause> criteria)
    {
        for (JoinNode.EquiJoinClause joinClause : criteria) {
            for (Map.Entry<String, Symbol> filter : joinDynamicFilters.entrySet()) {
                if (joinClause.getRight().getName().equals(filter.getValue().getName())) {
                    probeSymbols.put(filter.getKey(), joinClause.getLeft());
                }
            }
        }
    }

    /**
     * The results from each operator is added to the filters. Each operator should call this only once
     *
     * @param values each item in the array represents a column. each column contains a set of values
     */
    public synchronized void addOperatorResult(Map<DynamicFilterSourceOperator.Channel, Set> values)
    {
        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        partitionsLeft--;
        verify(partitionsLeft >= 0);

        if (values == null) {
            isIncomplete = true;
        }
        else if (!isIncomplete) {
            values.forEach((key, value) -> {
                result.putIfAbsent(key.getFilterId(), new HashSet<>());
                Set set = result.get(key.getFilterId());
                set.addAll(value);
                channels.put(key.getFilterId(), key);
            });
        }

        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        if (partitionsLeft == 0) {
            // No more partitions are left to be processed.
            Map<String, Set> dynamicFilterResult = new HashMap<>();
            if (!isIncomplete) {
                for (Map.Entry<String, Set> entry : result.entrySet()) {
                    dynamicFilterResult.put(entry.getKey(), entry.getValue());
                }
                try {
                    addPartialFilterToStateStore();
                }
                catch (RuntimeException e) {
                    log.warn("Cannot add partial filter to state store with following message: " + e.getMessage());
                }
            }
            dynamicFilterResultFuture.set(dynamicFilterResult);
        }
    }

    private void addPartialFilterToStateStore()
    {
        StateStore stateStore = stateStoreProvider.getStateStore();
        if (stateStore == null) {
            return;
        }

        DynamicFilter.DataType dataType = getDynamicFilterDataType(type, dynamicFilterDataType);
        for (Map.Entry<String, Set> filter : result.entrySet()) {
            DynamicFilterSourceOperator.Channel channel = channels.get(filter.getKey());
            Set filterValues = filter.getValue();
            String filterId = channel.getFilterId();
            String key = createKey(PARTIALPREFIX, filterId, channel.getQueryId());

            if (dataType == BLOOM_FILTER) {
                byte[] finalOutput = convertBloomFilterToByteArray(createBloomFilterFromSet(channel, filterValues, bloomFilterFpp));
                if (finalOutput != null) {
                    ((StateSet) stateStore.getOrCreateStateCollection(key, SET)).add(finalOutput);
                }
            }
            else {
                ((StateSet) stateStore.getOrCreateStateCollection(key, SET)).add(filterValues);
            }
            ((StateSet) stateStore.getOrCreateStateCollection(createKey(TASKSPREFIX, filterId, channel.getQueryId()), SET)).add(taskId.toString());
            log.debug("creating new " + dataType + " dynamic filter for size of: " + result.size() + ", key: " + key + ", taskId: " + taskId);
        }
    }

    private BloomFilter createBloomFilterFromSet(DynamicFilterSourceOperator.Channel channel, Set values, double bloomFilterFpp)
    {
        BloomFilter bloomFilter = new BloomFilter(BloomFilterDynamicFilter.DEFAULT_DYNAMIC_FILTER_SIZE, bloomFilterFpp);
        if (channel.getType().getJavaType() == long.class) {
            for (Object value : values) {
                long lv = (Long) value;
                bloomFilter.add(lv);
            }
        }
        else if (channel.getType().getJavaType() == double.class) {
            for (Object value : values) {
                double lv = (Double) value;
                bloomFilter.add(lv);
            }
        }
        else if (channel.getType().getJavaType() == Slice.class) {
            for (Object value : values) {
                bloomFilter.add((Slice) value);
            }
        }
        else {
            for (Object value : values) {
                bloomFilter.add(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
            }
        }
        return bloomFilter;
    }

    public Map<String, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    public ListenableFuture<Map<String, Set>> getDynamicFilterResultFuture()
    {
        return dynamicFilterResultFuture;
    }

    public Consumer<Map<DynamicFilterSourceOperator.Channel, Set>> getValueConsumer()
    {
        return this::addOperatorResult;
    }

    public DynamicFilter.Type getType()
    {
        return type;
    }
}
