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

package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import io.prestosql.Session;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.BuiltInFunctionHandle;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.JoinNode.EquiJoinClause;
import io.prestosql.spi.plan.JoinOnAggregationNode;
import io.prestosql.spi.plan.JoinOnAggregationNode.JoinInternalAggregation;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeId;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.analyzer.TypeSignatureProvider;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.SystemSessionProperties.isMergePartialAggregationWithJoin;
import static io.prestosql.SystemSessionProperties.isSnapshotEnabled;
import static io.prestosql.SystemSessionProperties.isSpillEnabled;
import static io.prestosql.spi.plan.AggregationNode.Aggregation;
import static io.prestosql.spi.plan.AggregationNode.Step.PARTIAL;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.iterative.rule.MergePartialAggregationWithJoin.GroupJoinAggregationFunction.SUPPORTED_FUNCTIONS;
import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class MergePartialAggregationWithJoin
        implements Rule<AggregationNode>
{
    static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(MergePartialAggregationWithJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

    private final Metadata metadata;

    public MergePartialAggregationWithJoin(Metadata metadata)
    {
        this.metadata = metadata;
    }

    static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations
        if (aggregationNode.isStreamable()) {
            return false;
        }

        if (aggregationNode.getHashSymbol().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isMergePartialAggregationWithJoin(session)
                && !isSpillEnabled(session)
                && !isSnapshotEnabled(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);

        if (joinNode.getType() != JoinNode.Type.INNER) {
            return Result.empty();
        }

        // Merge AggregationNode within JoinNode
        return checkAndApplyRule(aggregationNode, context, joinNode);
    }

    protected Result checkAndApplyRule(AggregationNode aggregationNode, Context context, JoinNode joinNode)
    {
        // Verify Aggregations against Whitelist functions
        if (!checkAggregationsInWhitelist(aggregationNode.getAggregations())) {
            return Result.empty();
        }
        if (aggregationKeysSameAsJoinKeysOrSuperWithPresentInStart(aggregationNode.getGroupingKeys(), joinNode.getCriteria(), joinNode)) {
            return Result.ofPlanNode(mergePartialAggregationWithJoin(aggregationNode, joinNode, context));
        }

        if (isJoinKeysSubsetOfAggrGroupKeys(aggregationNode.getGroupingKeys(), joinNode.getCriteria())) {
            return Result.ofPlanNode(mergePartialAggregationWithJoin(aggregationNode, joinNode, context));
        }

        return Result.empty();
    }

    private boolean checkAggregationsInWhitelist(Map<Symbol, Aggregation> aggregations)
    {
        for (Map.Entry<Symbol, Aggregation> entry : aggregations.entrySet()) {
            if (!SUPPORTED_FUNCTIONS.contains(entry.getValue().getFunctionCall().getDisplayName().toLowerCase(Locale.ROOT))) {
                return false;
            }
        }
        return true;
    }

    private JoinInternalAggregation replaceAggregationSource(
            PlanNodeId id,
            AggregationNode aggregation,
            Map<Symbol, AggregationNode.Aggregation> aggregations,
            PlanNode source,
            List<Symbol> groupingKeys)
    {
        return new JoinInternalAggregation(
                id,
                source,
                aggregations,
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashSymbol(),
                aggregation.getGroupIdSymbol(),
                aggregation.getAggregationType(),
                aggregation.getFinalizeSymbol());
    }

    private PlanNode mergePartialAggregationWithJoin(AggregationNode aggregationNode, JoinNode child, Context context)
    {
        // Divide AggregationNode into left and right AggregationNode
        Set<Symbol> joinLeftChildSymbols = ImmutableSet.copyOf(child.getLeft().getOutputSymbols());
        List<Symbol> leftGroupingKeys = getPushedDownGroupingSet(aggregationNode, joinLeftChildSymbols,
                intersection(getJoinRequiredSymbols(child), joinLeftChildSymbols));
        Map<Symbol, Aggregation> leftAggregations = getAggregationsMatchingSymbols(
                aggregationNode.getAggregations(), joinLeftChildSymbols);

        Set<Symbol> joinRightChildSymbols = ImmutableSet.copyOf(child.getRight().getOutputSymbols());
        List<Symbol> rightGroupingKeys = getPushedDownGroupingSet(aggregationNode, joinRightChildSymbols,
                intersection(getJoinRequiredSymbols(child), joinRightChildSymbols));
        Map<Symbol, Aggregation> rightAggregations = getAggregationsMatchingSymbols(
                aggregationNode.getAggregations(), joinRightChildSymbols);
        Map<Symbol, Aggregation> commonAggregations = getCommonAggregations(aggregationNode.getAggregations());
        if (leftAggregations.size() > 0) {
            leftAggregations.putAll(commonAggregations);
        }
        else if (rightAggregations.size() > 0) {
            rightAggregations.putAll(commonAggregations);
        }
        else {
            leftAggregations.putAll(commonAggregations);
        }
        String aggFunction = "count";
        FunctionHandle functionHandle = metadata.getFunctionAndTypeManager().lookupFunction(aggFunction,
                TypeSignatureProvider.fromTypeSignatures());
        CallExpression countExpr = new CallExpression(
                aggFunction,
                functionHandle,
                BIGINT,
                ImmutableList.of());
        Aggregation count = new Aggregation(
                countExpr,
                ImmutableList.of(),
                false,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
        Symbol countSymbolLeft = context.getSymbolAllocator().newSymbol(countExpr.getDisplayName(), BIGINT, null);
        Symbol countSymbolRight = context.getSymbolAllocator().newSymbol(countExpr.getDisplayName(), BIGINT, null);

        ImmutableMap.Builder<Symbol, Aggregation> newLeftAggrs = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Aggregation> newRightAggrs = ImmutableMap.builder();
        Map<Symbol, Aggregation> aggrOnLeftAggregations = getNewAggrOnAggr(leftAggregations, context, newLeftAggrs);
        Map<Symbol, Aggregation> aggrOnRightAggregations = getNewAggrOnAggr(rightAggregations, context, newRightAggrs);
        JoinInternalAggregation left = replaceAggregationSource(child.getId(),
                aggregationNode,
                newLeftAggrs.put(countSymbolLeft, count).build(),
                child.getLeft(),
                leftGroupingKeys);
        JoinInternalAggregation right = replaceAggregationSource(child.getId(),
                aggregationNode,
                newRightAggrs.put(countSymbolRight, count).build(),
                child.getRight(),
                rightGroupingKeys);

        JoinInternalAggregation aggrOnLeft = replaceAggregationSource(child.getId(),
                aggregationNode,
                aggrOnLeftAggregations,
                left,
                leftGroupingKeys);
        JoinInternalAggregation aggrOnRight = replaceAggregationSource(child.getId(),
                aggregationNode,
                aggrOnRightAggregations,
                right,
                rightGroupingKeys);

        ImmutableList.Builder<Symbol> outputSymbols = ImmutableList.builder();
        outputSymbols.addAll(aggrOnLeft.getOutputSymbols()).addAll(aggrOnRight.getOutputSymbols());
        JoinOnAggregationNode newJoinNode = new JoinOnAggregationNode(child.getId(),
                child.getType(),
                child.getCriteria(),
                child.getFilter(),
                child.getLeftHashSymbol(),
                child.getRightHashSymbol(),
                child.getDistributionType(),
                child.isSpillable(),
                child.getDynamicFilters(),
                left,
                right,
                aggrOnLeft,
                aggrOnRight,
                outputSymbols.build());
        return restrictOutputs(context.getIdAllocator(),
                newJoinNode,
                ImmutableSet.copyOf(aggregationNode.getOutputSymbols()),
                true,
                context.getSymbolAllocator().getTypes()).orElse(newJoinNode);
    }

    private Map<Symbol, Aggregation> getNewAggrOnAggr(Map<Symbol, Aggregation> aggregations,
            Context context,
            Builder<Symbol, Aggregation> origAggrsNewRef)
    {
        String aggFunctionSum = "sum";

        ImmutableMap.Builder<Symbol, Aggregation> newAggregations = ImmutableMap.builder();
        aggregations.forEach((symbol, aggregation) -> {
            CallExpression expression = aggregation.getFunctionCall();
            CallExpression newExpr = null;

            // Define a new symbol for original expression
            TypeSignature origAggrReturnTypeSig = ((BuiltInFunctionHandle) expression.getFunctionHandle()).getSignature().getReturnType();
            Type origAggrReturnType = metadata.getFunctionAndTypeManager().getType(origAggrReturnTypeSig);
            Symbol newSymbol = null;

            if (expression.getDisplayName().equals("count")) {
                FunctionHandle newFunctionHandle = metadata.getFunctionAndTypeManager().lookupFunction(aggFunctionSum,
                        TypeSignatureProvider.fromTypeSignatures(origAggrReturnTypeSig));

                // newSymbol is going to point to origExpression, so using its returnType during allocation
                newSymbol = context.getSymbolAllocator().newSymbol(expression.getDisplayName(), origAggrReturnType, null);
                newExpr = new CallExpression(aggFunctionSum,
                        newFunctionHandle,
                        expression.getType(),
                        ImmutableList.of(new VariableReferenceExpression(newSymbol.getName(),
                                origAggrReturnType)));
            }
            else {
                FunctionHandle newFunctionHandle = metadata.getFunctionAndTypeManager().lookupFunction(expression.getDisplayName(),
                        TypeSignatureProvider.fromTypeSignatures(origAggrReturnTypeSig));

                // newSymbol is going to point to origExpression, so using its returnType during allocation
                newSymbol = context.getSymbolAllocator().newSymbol(expression.getDisplayName(), expression.getType(), null);
                newExpr = new CallExpression(expression.getDisplayName(),
                        newFunctionHandle,
                        expression.getType(),
                        ImmutableList.of(new VariableReferenceExpression(newSymbol.getName(),
                                expression.getType())));
            }
            context.getSymbolAllocator().updateSymbolType(symbol, newExpr.getType());

            Aggregation newAggr = new Aggregation(
                    newExpr,
                    newExpr.getArguments(),
                    aggregation.isDistinct(),
                    aggregation.getFilter(),
                    aggregation.getOrderingScheme(),
                    aggregation.getMask());
            newAggregations.put(symbol, newAggr);
            origAggrsNewRef.put(newSymbol, aggregation);
        });
        return newAggregations.build();
    }

    private Map<Symbol, Aggregation> getAggregationsMatchingSymbols(
            Map<Symbol, Aggregation> aggregations, Set<Symbol> symbols)
    {
        return aggregations.entrySet().stream()
                .filter(entry -> {
                    List<Symbol> aggrSymbolList = SymbolsExtractor.extractAll(entry.getValue());
                    return aggrSymbolList.size() == 1 && symbols.contains(aggrSymbolList.get(0));
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map<Symbol, Aggregation> getCommonAggregations(Map<Symbol, Aggregation> aggregations)
    {
        return aggregations.entrySet().stream()
                .filter(entry -> {
                    List<Symbol> aggrSymbolList = SymbolsExtractor.extractAll(entry.getValue());
                    return aggrSymbolList.size() == 0;
                })
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Set<Symbol> getJoinRequiredSymbols(JoinNode node)
    {
        return Streams.concat(
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(JoinNode.EquiJoinClause::getRight),
                        node.getFilter().map(SymbolsExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                        node.getLeftHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                        node.getRightHashSymbol().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream())
                .collect(toImmutableSet());
    }

    private List<Symbol> getPushedDownGroupingSet(AggregationNode aggregation, Set<Symbol> availableSymbols,
            Set<Symbol> requiredJoinSymbols)
    {
        List<Symbol> groupingSet = aggregation.getGroupingKeys();

        // keep symbols that are directly from the join's child (availableSymbols)
        ImmutableList.Builder<Symbol> newGrpKeys = ImmutableList.builder();
        List<Symbol> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableSymbols::contains)
                .collect(Collectors.toList());

        // add missing required join symbols to grouping set
        Set<Symbol> existingSymbols = new HashSet<>(pushedDownGroupingSet);
        requiredJoinSymbols.stream()
                .filter(existingSymbols::add)
                .forEach(newGrpKeys::add);
        // New Grp Keys - First have Join Keys and then Non-Join Grp Keys
        newGrpKeys.addAll(pushedDownGroupingSet);
        return newGrpKeys.build();
    }

    private boolean aggregationKeysSameAsJoinKeysOrSuperWithPresentInStart(List<Symbol> groupingKeys, List<EquiJoinClause> criteria, JoinNode joinNode)
    {
        // Check all join keys are part of grouping keys
        // and Grouping keys which are not in Join keys, should belong to only one side
        if (criteria.size() < 1) {
            return false;
        }
        if (criteria.size() > groupingKeys.size()) {
            return false;
        }
        Set<Symbol> grpKeySet = new HashSet<>(groupingKeys.subList(0, criteria.size()));
        for (JoinNode.EquiJoinClause joinClause : criteria) {
            if (grpKeySet.contains(joinClause.getLeft())) {
                grpKeySet.remove(joinClause.getLeft());
            }
            else if (grpKeySet.contains(joinClause.getRight())) {
                grpKeySet.remove(joinClause.getRight());
            }
            else {
                return false;
            }
        }
        if (criteria.size() == groupingKeys.size()) {
            return true;
        }
        Set<Symbol> remainingGrpKeys = new HashSet<>(groupingKeys.subList(criteria.size(), groupingKeys.size()));
        if (allSymbolsOn(remainingGrpKeys, joinNode.getLeft().getOutputSymbols())) {
            return true;
        }
        else if (allSymbolsOn(remainingGrpKeys, joinNode.getRight().getOutputSymbols())) {
            return true;
        }
        return false;
    }

    private static boolean allSymbolsOn(Set<Symbol> grpKeySet, List<Symbol> symbols)
    {
        return new HashSet<>(symbols).containsAll(grpKeySet);
    }

    private boolean isJoinKeysSubsetOfAggrGroupKeys(List<Symbol> groupingKeys, List<EquiJoinClause> criteria)
    {
        // Check all join keys are part of grouping keys(Any Place)
        // and Grouping keys which are not in Join keys can belong to any side
        if (criteria.size() < 1) {
            return false;
        }
        if (criteria.size() > groupingKeys.size()) {
            return false;
        }
        Set<Symbol> grpKeySet = new HashSet<>(groupingKeys);
        for (JoinNode.EquiJoinClause joinClause : criteria) {
            if (grpKeySet.contains(joinClause.getLeft())) {
                grpKeySet.remove(joinClause.getLeft());
            }
            else if (grpKeySet.contains(joinClause.getRight())) {
                grpKeySet.remove(joinClause.getRight());
            }
            else {
                return false;
            }
        }
        // Exact match between group by and Join
        if (criteria.size() == groupingKeys.size()) {
            return true;
        }
        return true;
    }

    public static enum GroupJoinAggregationFunction
    {
        SUM("sum"),
        COUNT("count"),
        AVG("avg"),
        MIN("min"),
        MAX("max"),
        STDDEV_SAMP("stddev_samp"),
        STDDEV("stddev");

        private final String name;

        public static final Set<String> SUPPORTED_FUNCTIONS = Stream.of(GroupJoinAggregationFunction.values()).map(GroupJoinAggregationFunction::getName).collect(Collectors.toSet());

        GroupJoinAggregationFunction(String name)
        {
            this.name = name;
        }

        public String getName()
        {
            return this.name;
        }

        public String toString()
        {
            return this.name;
        }
    }
}
