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
package io.prestosql.sql.planner.assertions;

import com.google.common.base.Joiner;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.Expression;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static java.util.Objects.requireNonNull;

public class DynamicFilterMatcher
        implements Matcher
{
    // LEFT_SYMBOL -> RIGHT_SYMBOL
    private final Map<SymbolAlias, SymbolAlias> expectedDynamicFilters;
    private final Map<String, String> joinExpectedMappings;
    private final Map<String, String> filterExpectedMappings;
    private final Optional<Expression> expectedStaticFilter;

    private JoinNode joinNode;
    private SymbolAliases symbolAliases;
    private FilterNode filterNode;

    public DynamicFilterMatcher(Map<SymbolAlias, SymbolAlias> expectedDynamicFilters, Optional<Expression> expectedStaticFilter)
    {
        this.expectedDynamicFilters = requireNonNull(expectedDynamicFilters, "expectedDynamicFilters is null");
        this.joinExpectedMappings = expectedDynamicFilters.values().stream()
                .collect(toImmutableMap(rightSymbol -> rightSymbol.toString() + "_alias", SymbolAlias::toString));
        this.filterExpectedMappings = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().toString() + "_alias"));
        this.expectedStaticFilter = requireNonNull(expectedStaticFilter, "expectedStaticFilter is null");
    }

    public MatchResult match(JoinNode joinNode, SymbolAliases symbolAliases)
    {
        checkState(this.joinNode == null, "joinNode must be null at this point");
        this.joinNode = joinNode;
        this.symbolAliases = symbolAliases;
        return new MatchResult(match());
    }

    public MatchResult match(FilterNode filterNode, Metadata metadata, Session session, SymbolAliases symbolAliases)
    {
        checkState(this.filterNode == null, "filterNode must be null at this point");
        this.filterNode = filterNode;
        this.symbolAliases = symbolAliases;

        FunctionResolution functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), functionResolution, metadata.getFunctionAndTypeManager());
        boolean staticFilterMatches = expectedStaticFilter.map(filter -> {
            RowExpressionVerifier verifier = new RowExpressionVerifier(symbolAliases, metadata, session, filterNode.getOutputSymbols());
            RowExpression staticFilter = logicalRowExpressions.combineConjuncts(extractDynamicFilters(filterNode.getPredicate()).getStaticConjuncts());
            return verifier.process(filter, staticFilter);
        }).orElse(true);

        return new MatchResult(match() && staticFilterMatches);
    }

    private boolean match()
    {
        checkState(symbolAliases != null, "symbolAliases is null");

        // both nodes must be provided to do the matching
        if (filterNode == null || joinNode == null) {
            return true;
        }

        List<DynamicFilters.Descriptor> dynamicConjuncts = extractDynamicFilters(filterNode.getPredicate()).getDynamicConjuncts();
        Map<String, Symbol> idToProbeSymbolMap = dynamicConjuncts.stream()
                .collect(toImmutableMap(DynamicFilters.Descriptor::getId, filter -> new Symbol(((VariableReferenceExpression) filter.getInput()).getName())));
        Map<String, Symbol> idToBuildSymbolMap = joinNode.getDynamicFilters();

        if (idToProbeSymbolMap == null) {
            return false;
        }

        if (idToProbeSymbolMap.size() != expectedDynamicFilters.size()) {
            return false;
        }

        Map<Symbol, Symbol> actual = new HashMap<>();
        for (Map.Entry<String, Symbol> idToProbeSymbol : idToProbeSymbolMap.entrySet()) {
            String id = idToProbeSymbol.getKey();
            Symbol probe = idToProbeSymbol.getValue();
            Symbol build = idToBuildSymbolMap.get(id);
            if (build == null) {
                return false;
            }
            actual.put(probe, build);
        }

        Map<Symbol, Symbol> expected = expectedDynamicFilters.entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().toSymbol(symbolAliases), entry -> entry.getValue().toSymbol(symbolAliases)));

        return expected.equals(actual);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof FilterNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof FilterNode)) {
            return new MatchResult(false);
        }
        return match((FilterNode) node, metadata, session, symbolAliases);
    }

    public Map<String, String> getJoinExpectedMappings()
    {
        return joinExpectedMappings;
    }

    @Override
    public String toString()
    {
        String predicate = Joiner.on(" AND ")
                .join(filterExpectedMappings.entrySet().stream()
                        .map(entry -> entry.getKey() + " = " + entry.getValue())
                        .collect(toImmutableList()));
        return toStringHelper(this)
                .add("dynamicPredicate", predicate)
                .add("staticPredicate", expectedStaticFilter)
                .toString();
    }
}
