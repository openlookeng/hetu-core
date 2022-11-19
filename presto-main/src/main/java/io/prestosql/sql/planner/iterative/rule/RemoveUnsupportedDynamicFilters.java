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
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.PlanNodeStatsEstimate;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.expressions.RowExpressionRewriter;
import io.prestosql.expressions.RowExpressionTreeRewriter;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.statistics.Estimate;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.optimizations.PlanOptimizer;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.InternalPlanVisitor;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.SystemSessionProperties.getDynamicFilteringMaxSize;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.SystemSessionProperties.isOptimizeDynamicFilterGeneration;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.extractAllPredicates;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.expressions.LogicalRowExpressions.extractDisjuncts;
import static io.prestosql.expressions.LogicalRowExpressions.extractPredicates;
import static io.prestosql.operator.JoinUtils.getJoinDynamicFilters;
import static io.prestosql.operator.JoinUtils.getSemiJoinDynamicFilterId;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilters;
import static io.prestosql.sql.DynamicFilters.getDescriptor;
import static io.prestosql.sql.DynamicFilters.isDynamicFilter;
import static io.prestosql.sql.planner.plan.ChildReplacer.replaceChildren;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * Dynamic filters are supported only right after TableScan and only if the subtree is on the probe side of some downstream join node
 * Dynamic filters are removed from JoinNode if there is no consumer for it on probe side
 * Dynamic filters are removed from JoinNode and TableScan when the selectivity of dynamic filter is too high
 */
public class RemoveUnsupportedDynamicFilters
        implements PlanOptimizer
{
    private static final double DEFAULT_GENERATE_SELECTIVITY_THRESHOLD = 0.5D;
    private static final double DEFAULT_REMOVE_SELECTIVITY_THRESHOLD = 0.01D;

    private final Metadata metadata;
    private final StatsCalculator statsCalculator;
    private StatsProvider statsProvider;
    private Set<String> removedDynamicFilterIds;
    private final LogicalRowExpressions logicalRowExpressions;

    public RemoveUnsupportedDynamicFilters(Metadata metadata, StatsCalculator statsCalculator)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        this.removedDynamicFilterIds = new HashSet<>();
        this.statsProvider = new CachingStatsProvider(statsCalculator, session, planSymbolAllocator.getTypes());
        PlanWithConsumedDynamicFilters result = plan.accept(new RemoveUnsupportedDynamicFilters.Rewriter(session, metadata, removedDynamicFilterIds), ImmutableSet.of());
        return SimplePlanRewriter.rewriteWith(new RemoveFilterVisitor(removedDynamicFilterIds, this.metadata), result.getNode(), null);
    }

    private class Rewriter
            extends InternalPlanVisitor<PlanWithConsumedDynamicFilters, Set<String>>
    {
        private final Metadata metadata;
        private final Session session;
        private final Set<String> removedDynamicFilterIds;

        public Rewriter(Session session, Metadata metadata, Set<String> removedDynamicFilterIds)
        {
            this.session = session;
            this.metadata = metadata;
            this.removedDynamicFilterIds = removedDynamicFilterIds;
        }

        @Override
        public PlanWithConsumedDynamicFilters visitPlan(PlanNode node, Set<String> allowedDynamicFilterIds)
        {
            List<PlanWithConsumedDynamicFilters> children = node.getSources().stream()
                    .map(source -> source.accept(this, allowedDynamicFilterIds))
                    .collect(toImmutableList());

            PlanNode result = replaceChildren(
                    node,
                    children.stream()
                            .map(PlanWithConsumedDynamicFilters::getNode)
                            .collect(toList()));

            Set<String> consumedDynamicFilterIds = children.stream()
                    .map(PlanWithConsumedDynamicFilters::getConsumedDynamicFilterIds)
                    .flatMap(Set::stream)
                    .collect(toImmutableSet());

            return new PlanWithConsumedDynamicFilters(result, consumedDynamicFilterIds);
        }

        @Override
        public PlanWithConsumedDynamicFilters visitCTEScan(CTEScanNode node, Set<String> allowedDynamicFilterIds)
        {
            ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder().addAll(allowedDynamicFilterIds);
            collectFromCTEScanNode(node.getPredicate().isPresent() ? node.getPredicate().get() : TRUE_CONSTANT, builder);
            Set<String> allowedDynamicFilterIdsFromCTE = builder.build();
            return visitPlan(node, allowedDynamicFilterIdsFromCTE);
        }

        @Override
        public PlanWithConsumedDynamicFilters visitJoin(JoinNode node, Set<String> allowedDynamicFilterIds)
        {
            Map<String, Symbol> currentJoinDynamicFilters = getJoinDynamicFilters(node);
            ImmutableSet.Builder<String> builder = ImmutableSet.<String>builder().addAll(allowedDynamicFilterIds);
            if (!isOptimizeDynamicFilterGeneration(session) || (isOptimizeDynamicFilterGeneration(session) && !hasHighSelectivity(node.getRight()))) {
                builder.addAll(currentJoinDynamicFilters.keySet());
            }
            ImmutableSet<String> allowedDynamicFilterIdsProbeSide = builder.build();

            PlanWithConsumedDynamicFilters leftResult = node.getLeft().accept(this, allowedDynamicFilterIdsProbeSide);
            Set<String> consumedProbeSide = leftResult.getConsumedDynamicFilterIds();
            Map<String, Symbol> dynamicFilters = currentJoinDynamicFilters.entrySet().stream()
                    .filter(entry -> consumedProbeSide.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            PlanWithConsumedDynamicFilters rightResult = node.getRight().accept(this, allowedDynamicFilterIds);
            Set<String> consumed = new HashSet<>(rightResult.getConsumedDynamicFilterIds());
            Set<String> unsupportedDynamicFiltersFromBuildSide = intersection(currentJoinDynamicFilters.keySet(), consumed);
            if (!unsupportedDynamicFiltersFromBuildSide.isEmpty()) {
                removedDynamicFilterIds.addAll(unsupportedDynamicFiltersFromBuildSide);
            }
            consumed.addAll(consumedProbeSide);
            consumed.removeAll(dynamicFilters.keySet());

            PlanNode left = leftResult.getNode();
            PlanNode right = rightResult.getNode();
            Set<String> removedIdsFromJoin = node.getDynamicFilters().keySet().stream().filter(id -> !dynamicFilters.containsKey(id)).collect(Collectors.toSet());
            removedDynamicFilterIds.addAll(removedIdsFromJoin);
            if (!left.equals(node.getLeft()) || !right.equals(node.getRight()) || !dynamicFilters.equals(currentJoinDynamicFilters)) {
                return new PlanWithConsumedDynamicFilters(new JoinNode(
                        node.getId(),
                        node.getType(),
                        left,
                        right,
                        node.getCriteria(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        // When there was no dynamic filter in join, it should remain empty
                        node.getDynamicFilters().isEmpty() ? ImmutableMap.of() : dynamicFilters),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitSemiJoin(SemiJoinNode node, Set<String> allowedDynamicFilterIds)
        {
            Optional<String> dynamicFilterIdOptional = getSemiJoinDynamicFilterId(node);

            if (!dynamicFilterIdOptional.isPresent()) {
                return visitPlan(node, allowedDynamicFilterIds);
            }
            String dynamicFilterId = dynamicFilterIdOptional.get();

            Set<String> allowedDynamicFilterIdsSourceSide = ImmutableSet.<String>builder()
                    .add(dynamicFilterId)
                    .addAll(allowedDynamicFilterIds)
                    .build();
            PlanWithConsumedDynamicFilters sourceResult = node.getSource().accept(this, allowedDynamicFilterIdsSourceSide);
            PlanWithConsumedDynamicFilters filteringSourceResult = node.getFilteringSource().accept(this, allowedDynamicFilterIds);

            Set<String> consumed = new HashSet<>(filteringSourceResult.getConsumedDynamicFilterIds());
            consumed.addAll(sourceResult.getConsumedDynamicFilterIds());
            Optional<String> newFilterId;
            if (consumed.contains(dynamicFilterId)) {
                consumed.remove(dynamicFilterId);
                newFilterId = Optional.of(dynamicFilterId);
            }
            else {
                newFilterId = Optional.empty();
                removedDynamicFilterIds.add(node.getDynamicFilterId().get());
            }

            PlanNode newSource = sourceResult.getNode();
            PlanNode newFilteringSource = filteringSourceResult.getNode();
            if (!newSource.equals(node.getSource())
                    || !newFilteringSource.equals(node.getFilteringSource())
                    || !newFilterId.equals(dynamicFilterIdOptional)) {
                return new PlanWithConsumedDynamicFilters(new SemiJoinNode(
                        node.getId(),
                        newSource,
                        newFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        // When there was no dynamic filter in semi-join, it should remain empty
                        node.getDynamicFilterId().isPresent() ? newFilterId : Optional.empty()),
                        ImmutableSet.copyOf(consumed));
            }
            return new PlanWithConsumedDynamicFilters(node, ImmutableSet.copyOf(consumed));
        }

        @Override
        public PlanWithConsumedDynamicFilters visitFilter(FilterNode node, Set<String> allowedDynamicFilterIds)
        {
            PlanWithConsumedDynamicFilters result = node.getSource().accept(this, allowedDynamicFilterIds);

            RowExpression original = node.getPredicate();
            ImmutableSet.Builder<String> consumedDynamicFilterIds = ImmutableSet.<String>builder()
                    .addAll(result.getConsumedDynamicFilterIds());

            PlanNode source = result.getNode();
            RowExpression modified;
            if (source instanceof TableScanNode) {
                // Keep only small table
                DynamicFilters.ExtractResult extractResult = extractDynamicFilters(original);
                // If there are no allowed dynamic filter passed from join so nothing to be removed hence no need to check stats
                if (isOptimizeDynamicFilterGeneration(session) && !allowedDynamicFilterIds.isEmpty() && !highSelectivity(node)) {
                    modified = removeDynamicFilters(original, ImmutableSet.of(), consumedDynamicFilterIds);
                    extractResult.getDynamicConjuncts().forEach(descriptor -> removedDynamicFilterIds.add(descriptor.getId()));
                }
                // Keep only allowed dynamic filters
                else {
                    if (isCTEReuseEnabled(session)) {
                        modified = removeDynamicFiltersForCTE(original, allowedDynamicFilterIds, consumedDynamicFilterIds);
                    }
                    else {
                        modified = removeDynamicFilters(original, allowedDynamicFilterIds, consumedDynamicFilterIds);
                    }
                }
            }
            else {
                modified = removeAllDynamicFilters(original);
            }

            if (TRUE_CONSTANT.equals(modified)) {
                return new PlanWithConsumedDynamicFilters(source, consumedDynamicFilterIds.build());
            }

            if (!original.equals(modified) || source != node.getSource()) {
                return new PlanWithConsumedDynamicFilters(new FilterNode(node.getId(), source, modified),
                        consumedDynamicFilterIds.build());
            }

            return new PlanWithConsumedDynamicFilters(node, consumedDynamicFilterIds.build());
        }

        private boolean hasHighSelectivity(PlanNode node)
        {
            Optional<PlanNode> buildSideTableScanNode = Optional.empty();
            if (node instanceof ProjectNode) {
                return hasHighSelectivity(((ProjectNode) node).getSource());
            }

            if (node instanceof ExchangeNode && node.getSources().size() == 1) {
                return hasHighSelectivity(node.getSources().get(0));
            }

            // Only handle the case that build side of JoinNode is0
            // TableScanNode or FilterNode above TableScanNode
            // as the estimates will be more accurate
            Optional<RowExpression> predicates = Optional.empty();
            if (node instanceof TableScanNode) {
                buildSideTableScanNode = Optional.of(node);
                predicates = ((TableScanNode) buildSideTableScanNode.get()).getPredicate();
            }

            if (node instanceof FilterNode) {
                PlanNode sourceNode = ((FilterNode) node).getSource();
                if (sourceNode instanceof TableScanNode) {
                    buildSideTableScanNode = Optional.of(sourceNode);
                    predicates = Optional.of(((FilterNode) node).getPredicate());
                }
            }

            if (buildSideTableScanNode.isPresent()) {
                // If there is dynamic filters applied on the build side,
                // the selectivity cannot be easily calculated,
                // thus we assume it's not high selectivity
                if (predicates.isPresent()) {
                    long numDynamicFilters = extractAllPredicates(predicates.get()).stream().filter(DynamicFilters::isDynamicFilter).count();
                    if (numDynamicFilters > 0) {
                        return false;
                    }
                }

                Estimate totalRowCount = metadata.getTableStatistics(session, ((TableScanNode) buildSideTableScanNode.get()).getTable(), Constraint.alwaysTrue(), true).getRowCount();
                PlanNodeStatsEstimate filteredStats = statsProvider.getStats(node);

                if (!filteredStats.isOutputRowCountUnknown() && !totalRowCount.isUnknown()) {
                    // If filtered row count is too big, no need to create Dynamic Filter
                    if (filteredStats.getOutputRowCount() > getDynamicFilteringMaxSize(session)) {
                        return true;
                    }
                }
            }

            return false;
        }

        private boolean highSelectivity(FilterNode node)
        {
            Estimate totalRowCount = metadata.getTableStatistics(session, ((TableScanNode) node.getSource()).getTable(), Constraint.alwaysTrue(), true).getRowCount();
            PlanNodeStatsEstimate filteredStats = statsProvider.getStats(node);

            if (!filteredStats.isOutputRowCountUnknown() && !totalRowCount.isUnknown()) {
                // If filtered row count is too big, no need to create Dynamic Filter
                if (filteredStats.getOutputRowCount() > getDynamicFilteringMaxSize(session)) {
                    return true;
                }

                // If selectivity too low, no need to create Dynamic Filter
                double selectivity = filteredStats.getOutputRowCount() / totalRowCount.getValue();
                return selectivity > DEFAULT_REMOVE_SELECTIVITY_THRESHOLD;
            }
            else {
                return true;
            }
        }

        private RowExpression removeDynamicFilters(RowExpression expression, Set<String> allowedDynamicFilterIds, ImmutableSet.Builder<String> consumedDynamicFilterIds)
        {
            return logicalRowExpressions.combineConjuncts(extractConjuncts(expression)
                    .stream()
                    .map(this::removeNestedDynamicFilters)
                    .filter(conjunct ->
                            getDescriptor(conjunct)
                                    .map(descriptor -> {
                                        if (descriptor.getInput() instanceof VariableReferenceExpression &&
                                                allowedDynamicFilterIds.contains(descriptor.getId())) {
                                            consumedDynamicFilterIds.add(descriptor.getId());
                                            return true;
                                        }
                                        return false;
                                    }).orElse(true))
                    .collect(toImmutableList()));
        }

        private RowExpression removeDynamicFiltersForCTE(RowExpression expression, Set<String> allowedDynamicFilterIds, ImmutableSet.Builder<String> consumedDynamicFilterIds)
        {
            if (expression instanceof SpecialForm) {
                switch (((SpecialForm) expression).getForm()) {
                    case AND:
                        return logicalRowExpressions.combineConjuncts(extractConjuncts(expression)
                                .stream()
                                .map(exp -> removeDynamicFiltersForCTE(exp, allowedDynamicFilterIds, consumedDynamicFilterIds))
                                .collect(Collectors.toList()));
                    case OR:
                        return logicalRowExpressions.combineDisjuncts(extractDisjuncts(expression)
                                .stream()
                                .map(exp -> removeDynamicFiltersForCTE(exp, allowedDynamicFilterIds, consumedDynamicFilterIds))
                                .collect(Collectors.toList()));
                    default:
                        return expression;
                }
            }
            return removeDynamicFilters(expression, allowedDynamicFilterIds, consumedDynamicFilterIds);
        }

        private void collectFromCTEScanNode(RowExpression predicate, ImmutableSet.Builder<String> builder)
        {
            if (predicate instanceof SpecialForm && (((SpecialForm) predicate).getForm() == OR || ((SpecialForm) predicate).getForm() == AND)) {
                collectFromCTEScanNode(((SpecialForm) predicate).getArguments().get(0), builder);
                collectFromCTEScanNode(((SpecialForm) predicate).getArguments().get(1), builder);
            }
            else if (predicate instanceof CallExpression && getDescriptor(predicate).isPresent()) {
                builder.add(getDescriptor(predicate).get().getId());
            }
            else {
                return;
            }
        }

        private RowExpression removeAllDynamicFilters(RowExpression expression)
        {
            RowExpression rewrittenExpression = removeNestedDynamicFilters(expression);
            DynamicFilters.ExtractResult extractResult = extractDynamicFilters(rewrittenExpression);
            if (extractResult.getDynamicConjuncts().isEmpty()) {
                return rewrittenExpression;
            }
            return logicalRowExpressions.combineConjuncts(extractResult.getStaticConjuncts());
        }

        private RowExpression removeNestedDynamicFilters(RowExpression expression)
        {
            return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
            {
                @Override
                public RowExpression rewriteSpecialForm(SpecialForm node, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
                {
                    if (node.getForm() != AND || node.getForm() != OR) {
                        return node;
                    }
                    SpecialForm rewrittenNode = treeRewriter.defaultRewrite(node, context);
                    boolean modified = (node != rewrittenNode);
                    ImmutableList.Builder<RowExpression> expressionBuilder = ImmutableList.builder();
                    if (isDynamicFilter(rewrittenNode.getArguments().get(0))) {
                        expressionBuilder.add(TRUE_CONSTANT);
                        modified = true;
                    }
                    else {
                        expressionBuilder.add(rewrittenNode.getArguments().get(0));
                    }

                    if (isDynamicFilter(rewrittenNode.getArguments().get(1))) {
                        expressionBuilder.add(TRUE_CONSTANT);
                        modified = true;
                    }
                    else {
                        expressionBuilder.add(rewrittenNode.getArguments().get(1));
                    }

                    if (!modified) {
                        return node;
                    }
                    return logicalRowExpressions.combinePredicates(node.getForm(), expressionBuilder.build());
                }
            }, expression);
        }
    }

    private static class PlanWithConsumedDynamicFilters
    {
        private final PlanNode node;
        private final Set<String> consumedDynamicFilterIds;

        PlanWithConsumedDynamicFilters(PlanNode node, Set<String> consumedDynamicFilterIds)
        {
            this.node = node;
            this.consumedDynamicFilterIds = ImmutableSet.copyOf(consumedDynamicFilterIds);
        }

        PlanNode getNode()
        {
            return node;
        }

        Set<String> getConsumedDynamicFilterIds()
        {
            return consumedDynamicFilterIds;
        }
    }

    private static class RemoveFilterVisitor
            extends SimplePlanRewriter<Void>
    {
        private final Set<String> removedDynamicFilterIds;
        private final LogicalRowExpressions logicalRowExpressions;

        public RemoveFilterVisitor(Set<String> removedDynamicFilterIds, Metadata metadata)
        {
            this.removedDynamicFilterIds = removedDynamicFilterIds;
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            PlanNode source = context.rewrite(node.getSource());
            RowExpression original = node.getPredicate();
            RowExpression modified;
            if (source instanceof TableScanNode) {
                modified = removedDynamicFilterIdsForFilter(original);
            }
            else {
                modified = original;
            }
            return new FilterNode(node.getId(), source, modified);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            PlanNode leftSource = context.rewrite(node.getLeft());
            PlanNode rightSource = context.rewrite(node.getRight());
            Map<String, Symbol> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .filter(entry -> !removedDynamicFilterIds.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !dynamicFilters.equals(node.getDynamicFilters())) {
                return new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        node.getCriteria(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        node.getDistributionType(),
                        node.isSpillable(),
                        dynamicFilters);
            }
            return node;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode filteringSource = context.rewrite(node.getFilteringSource());
            PlanNode source = context.rewrite(node.getSource());
            Optional<String> dynamicFilters = node.getDynamicFilterId()
                    .filter(entry -> !removedDynamicFilterIds.contains(entry));
            if (filteringSource != node.getFilteringSource() ||
                    source != node.getSource() ||
                    !dynamicFilters.equals(node.getDynamicFilterId())) {
                return new SemiJoinNode(
                        node.getId(),
                        node.getSource(),
                        node.getFilteringSource(),
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        dynamicFilters);
            }
            return node;
        }

        private RowExpression removedDynamicFilterIdsForFilter(RowExpression expression)
        {
            RowExpression modified;
            if (expression instanceof SpecialForm) {
                switch (((SpecialForm) expression).getForm()) {
                    case AND:
                        return logicalRowExpressions.combineConjuncts(extractPredicates(AND, expression)
                                .stream()
                                .map(this::removedDynamicFilterIdsForFilter)
                                .collect(Collectors.toList()));
                    case OR:
                        RowExpression modifiedDisjunct = logicalRowExpressions.combineDisjuncts(extractPredicates(OR, expression)
                                .stream()
                                .map(this::removedDynamicFilterIdsForFilter)
                                .collect(Collectors.toList()));
                        if (modifiedDisjunct.equals(TRUE_CONSTANT)) {
                            extractAllPredicates(expression).stream().filter(conjunct -> getDescriptor(conjunct).isPresent())
                                    .forEach(conjunct -> removedDynamicFilterIds.add(getDescriptor(conjunct).get().getId()));
                        }
                        return modifiedDisjunct;
                    default:
                        return expression;
                }
            }
            else {
                modified = logicalRowExpressions.combineConjuncts(extractConjuncts(expression)
                            .stream()
                            .filter(conjunct -> getDescriptor(conjunct)
                            .map(descriptor -> !removedDynamicFilterIds.contains(descriptor.getId()))
                            .orElse(true)).collect(toImmutableList()));
            }
            return modified;
        }
    }
}
