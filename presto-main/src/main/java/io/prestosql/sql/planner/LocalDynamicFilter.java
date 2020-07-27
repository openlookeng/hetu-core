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
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.log.Logger;
import io.prestosql.spi.dynamicfilter.DynamicFilter;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

import static com.google.common.base.Verify.verify;
import static io.prestosql.sql.DynamicFilters.Descriptor;
import static io.prestosql.utils.DynamicFilterUtils.findFilterNodeInStage;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

public class LocalDynamicFilter
{
    private static final Logger log = Logger.get(LocalDynamicFilter.class);

    // Mapping from dynamic filter ID to its probe symbols.
    private final Multimap<String, Symbol> probeSymbols;

    private boolean isIncomplete;
    // Mapping from dynamic filter ID to its build channel indices.
    private final Map<String, Integer> buildChannels;

    private final SettableFuture<TupleDomain<Symbol>> resultFuture;

    // The resulting predicate for local dynamic filtering.
    private TupleDomain<String> result;

    private SettableFuture<Map<Symbol, Set>> hashSetResultFuture;

    // Number of partitions left to be processed.
    private int partitionsLeft;

    private Map<String, Set> domainResult = new HashMap<>();

    private final DynamicFilter.Type type;

    public LocalDynamicFilter(Multimap<String, Symbol> probeSymbols, Map<String, Integer> buildChannels, int partitionCount, DynamicFilter.Type type)
    {
        this.probeSymbols = requireNonNull(probeSymbols, "probeSymbols is null");
        this.buildChannels = requireNonNull(buildChannels, "buildChannels is null");
        verify(probeSymbols.keySet().equals(buildChannels.keySet()), "probeSymbols and buildChannels must have same keys");

        this.resultFuture = SettableFuture.create();
        this.hashSetResultFuture = SettableFuture.create();

        this.result = TupleDomain.none();
        this.partitionsLeft = partitionCount;

        this.type = type;

        this.isIncomplete = false;
    }

    private synchronized void addPartition(TupleDomain<String> tupleDomain)
    {
        if (type == DynamicFilter.Type.GLOBAL) {
            Map<Symbol, Set> bloomFilterResult = new HashMap<>();
            if (isIncomplete) {
                hashSetResultFuture.set(bloomFilterResult);
                return;
            }
        }

        // Called concurrently by each DynamicFilterSourceOperator instance (when collection is over).
        partitionsLeft -= 1;
        verify(partitionsLeft >= 0);

        if (!tupleDomain.getDomains().isPresent()) {
            return;
        }

        if (tupleDomain.isAll()) {
            isIncomplete = true;
        }

        Map<String, Domain> domains = tupleDomain.getDomains().get();
        domains.forEach((key, value) -> {
            if (!domainResult.containsKey(key)) {
                domainResult.put(key, new HashSet<>());
            }
            Set set = domainResult.get(key);
            for (Range range : value.getValues().getRanges().getOrderedRanges()) {
                Object obj = range.getSingleValue();
                set.add(obj);
            }
        });

        // NOTE: may result in a bit more relaxed constraint if there are multiple columns and multiple rows.
        // See the comment at TupleDomain::columnWiseUnion() for more details.
        if (partitionsLeft == 0) {
            // No more partitions are left to be processed.
            Map<Symbol, Set> bloomFilterResult = new HashMap<>();
            if (isIncomplete) {
                hashSetResultFuture.set(bloomFilterResult);
                return;
            }
            for (Map.Entry<String, Set> entry : domainResult.entrySet()) {
                for (Symbol probeSymbol : probeSymbols.get(entry.getKey())) {
                    if (!bloomFilterResult.containsKey(probeSymbol)) {
                        bloomFilterResult.put(probeSymbol, new HashSet<>());
                    }
                    bloomFilterResult.put(probeSymbol, entry.getValue());
                }
            }
            hashSetResultFuture.set(bloomFilterResult);
        }
    }

    private TupleDomain<Symbol> convertTupleDomain(TupleDomain<String> result)
    {
        if (result.isNone()) {
            return TupleDomain.none();
        }
        // Convert the predicate to use probe symbols (instead dynamic filter IDs).
        // Note that in case of a probe-side union, a single dynamic filter may match multiple probe symbols.
        ImmutableMap.Builder<Symbol, Domain> builder = ImmutableMap.builder();
        for (Map.Entry<String, Domain> entry : result.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            // Store all matching symbols for each build channel index.
            for (Symbol probeSymbol : probeSymbols.get(entry.getKey())) {
                builder.put(probeSymbol, domain);
            }
        }
        return TupleDomain.withColumnDomains(builder.build());
    }

    public static Optional<LocalDynamicFilter> create(JoinNode planNode, int partitionCount)
    {
        Set<String> joinDynamicFilters = planNode.getDynamicFilters().keySet();
        // Mapping from probe-side dynamic filters' IDs to their matching probe symbols.
        Multimap<String, Symbol> probeSymbols = MultimapBuilder.treeKeys().arrayListValues().build();
        PlanNode buildNode;
        DynamicFilter.Type type = DynamicFilter.Type.LOCAL;

        List<FilterNode> filterNodes = findFilterNodeInStage(planNode);
        if (filterNodes.isEmpty()) {
            buildNode = planNode.getRight();
            mapProbeSymbolsFromCriteria(planNode.getDynamicFilters(), probeSymbols, planNode.getCriteria());
            type = DynamicFilter.Type.GLOBAL;
        }
        else {
            buildNode = planNode.getRight();
            for (FilterNode filterNode : filterNodes) {
                mapProbeSymbols(filterNode.getPredicate(), joinDynamicFilters, probeSymbols);
            }
        }

        final List<Symbol> buildSideSymbols = buildNode.getOutputSymbols();

        Map<String, Integer> buildChannels = planNode
                .getDynamicFilters()
                .entrySet()
                .stream()
                // Skip build channels that don't match local probe dynamic filters.
                .filter(entry -> probeSymbols.containsKey(entry.getKey()))
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

        if (buildChannels.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new LocalDynamicFilter(probeSymbols, buildChannels, partitionCount, type));
    }

    private static void mapProbeSymbols(Expression predicate, Set<String> joinDynamicFilters, Multimap<String, Symbol> probeSymbols)
    {
        DynamicFilters.ExtractResult extractResult = DynamicFilters.extractDynamicFilters(predicate);
        for (Descriptor descriptor : extractResult.getDynamicConjuncts()) {
            if (descriptor.getInput() instanceof SymbolReference) {
                // Add descriptors that match the local dynamic filter (from the current join node).
                if (joinDynamicFilters.contains(descriptor.getId())) {
                    Symbol probeSymbol = Symbol.from(descriptor.getInput());
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

    public Map<String, Integer> getBuildChannels()
    {
        return buildChannels;
    }

    public ListenableFuture<TupleDomain<Symbol>> getResultFuture()
    {
        return resultFuture;
    }

    public ListenableFuture<Map<Symbol, Set>> getDynamicFilterResultFuture()
    {
        return hashSetResultFuture;
    }

    public Consumer<TupleDomain<String>> getTupleDomainConsumer()
    {
        return this::addPartition;
    }

    public DynamicFilter.Type getType()
    {
        return type;
    }
}
