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
package io.prestosql.sql.planner.optimizations;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.cost.CachingCostProvider;
import io.prestosql.cost.CachingStatsProvider;
import io.prestosql.cost.CostComparator;
import io.prestosql.cost.CostProvider;
import io.prestosql.cost.PlanCostEstimate;
import io.prestosql.cost.StatsProvider;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.expressions.RowExpressionNodeInliner;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupIdNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.MarkDistinctNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.RowExpressionEqualityInference;
import io.prestosql.sql.planner.RowExpressionInterpreter;
import io.prestosql.sql.planner.RowExpressionPredicateExtractor;
import io.prestosql.sql.planner.RowExpressionVariableInliner;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.VariablesExtractor;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Memo;
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.SemiJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.SortNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.relational.Expressions;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.sql.relational.RowExpressionOptimizer;
import io.prestosql.type.InternalTypeManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.filter;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.SystemSessionProperties.isEnableDynamicFiltering;
import static io.prestosql.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static io.prestosql.expressions.LogicalRowExpressions.extractAllPredicates;
import static io.prestosql.expressions.LogicalRowExpressions.extractConjuncts;
import static io.prestosql.expressions.LogicalRowExpressions.extractDisjuncts;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.plan.JoinNode.DistributionType.PARTITIONED;
import static io.prestosql.spi.plan.JoinNode.DistributionType.REPLICATED;
import static io.prestosql.spi.plan.JoinNode.Type.FULL;
import static io.prestosql.spi.plan.JoinNode.Type.INNER;
import static io.prestosql.spi.plan.JoinNode.Type.LEFT;
import static io.prestosql.spi.plan.JoinNode.Type.RIGHT;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.DynamicFilters.createDynamicFilterRowExpression;
import static io.prestosql.sql.DynamicFilters.extractDynamicFilterExpression;
import static io.prestosql.sql.DynamicFilters.extractStaticFilters;
import static io.prestosql.sql.DynamicFilters.getDescriptor;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.prestosql.sql.planner.PlanOptimizers.CostCalculationHandle;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toSymbol;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReference;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReferenceMap;
import static io.prestosql.sql.planner.VariableReferenceSymbolConverter.toVariableReferences;
import static io.prestosql.sql.planner.plan.AssignmentUtils.identityAssignments;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.sql.relational.Expressions.uniqueSubExpressions;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class PredicatePushDown
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final CostCalculationHandle costCalculationHandle;
    private final boolean useTableProperties;
    private final boolean dynamicFiltering;
    private final boolean pushdownForCTE;

    public PredicatePushDown(Metadata metadata, TypeAnalyzer typeAnalyzer, CostCalculationHandle costCalculationHandle, boolean useTableProperties, boolean dynamicFiltering, boolean pushdownForCTE)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.costCalculationHandle = costCalculationHandle;
        this.useTableProperties = useTableProperties;
        this.dynamicFiltering = dynamicFiltering;
        this.pushdownForCTE = pushdownForCTE;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, TypeProvider types, PlanSymbolAllocator planSymbolAllocator, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(session, "session is null");
        requireNonNull(types, "types is null");
        requireNonNull(idAllocator, "idAllocator is null");

        RowExpressionPredicateExtractor predicateExtractor = new RowExpressionPredicateExtractor(new RowExpressionDomainTranslator(metadata), metadata, planSymbolAllocator, useTableProperties);
        Memo memo = new Memo(idAllocator, plan);
        Lookup lookup = Lookup.from(planNode -> Stream.of(memo.resolve(planNode)));

        StatsProvider statsProvider = new CachingStatsProvider(costCalculationHandle.getStatsCalculator(), Optional.of(memo), lookup, session, planSymbolAllocator.getTypes(), true);
        CostProvider costProvider = new CachingCostProvider(costCalculationHandle.getCostCalculator(), statsProvider, Optional.of(memo), session, planSymbolAllocator.getTypes());
        FilterPushdownForCTEHandler filterPushdownForCTEHandler = new FilterPushdownForCTEHandler(pushdownForCTE, costProvider, costCalculationHandle.getCostComparator());
        Rewriter rewriter = new Rewriter(planSymbolAllocator, idAllocator, metadata, predicateExtractor, typeAnalyzer, session, dynamicFiltering, filterPushdownForCTEHandler);
        PlanNode rewrittenNode = SimplePlanRewriter.rewriteWith(rewriter, plan, TRUE_CONSTANT);
        if (rewriter.isSecondTraverseRequired()) {
            return SimplePlanRewriter.rewriteWith(rewriter, rewrittenNode, TRUE_CONSTANT);
        }

        return rewrittenNode;
    }

    private static class Rewriter
            extends SimplePlanRewriter<RowExpression>
    {
        private static final Logger LOG = Logger.get(PredicatePushDown.Rewriter.class);

        private final PlanSymbolAllocator planSymbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;
        private final RowExpressionPredicateExtractor effectivePredicateExtractor;
        private final Session session;
        private final ExpressionEquivalence expressionEquivalence;
        private final RowExpressionDeterminismEvaluator determinismEvaluator;
        private final LogicalRowExpressions logicalRowExpressions;
        private final TypeManager typeManager;
        private final boolean dynamicFiltering;
        private final boolean pushdownForCTE;
        private boolean isCteNodeAlreadyVisited;
        private final CostProvider costProvider;
        private final CostComparator costComparator;
        private final Map<Integer, List<RowExpression>> cteFilterMap;
        private final Map<Integer, List<Symbol>> cteExprMap;
        private final Map<Integer, PlanCostEstimate> cteCostMap;
        private final Double pushdownCostScaleFactor = 1.5;

        private Rewriter(
                PlanSymbolAllocator planSymbolAllocator,
                PlanNodeIdAllocator idAllocator,
                Metadata metadata,
                RowExpressionPredicateExtractor effectivePredicateExtractor,
                TypeAnalyzer typeAnalyzer,
                Session session,
                boolean dynamicFiltering, FilterPushdownForCTEHandler filterPushdownForCTEHandler)
        {
            this.planSymbolAllocator = requireNonNull(planSymbolAllocator, "variableAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.effectivePredicateExtractor = requireNonNull(effectivePredicateExtractor, "effectivePredicateExtractor is null");
            this.session = requireNonNull(session, "session is null");
            this.expressionEquivalence = new ExpressionEquivalence(metadata, typeAnalyzer);
            this.determinismEvaluator = new RowExpressionDeterminismEvaluator(metadata);
            this.logicalRowExpressions = new LogicalRowExpressions(determinismEvaluator, new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
            this.typeManager = new InternalTypeManager(metadata.getFunctionAndTypeManager());
            this.dynamicFiltering = dynamicFiltering;
            this.pushdownForCTE = filterPushdownForCTEHandler.isPushdownForCTE();
            this.costProvider = filterPushdownForCTEHandler.getCostProvider();
            this.costComparator = filterPushdownForCTEHandler.getCostComparator();
            this.cteFilterMap = new HashMap<>();
            this.cteExprMap = new HashMap<>();
            this.cteCostMap = new HashMap<>();
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<RowExpression> context)
        {
            PlanNode rewrittenNode = context.defaultRewrite(node, TRUE_CONSTANT);
            if (!context.get().equals(TRUE_CONSTANT)) {
                // Drop in a FilterNode b/c we cannot push our predicate down any further
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, context.get());
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitCTEScan(CTEScanNode node, RewriteContext<RowExpression> context)
        {
            if (!pushdownForCTE) {
                return visitPlan(node, context);
            }
            Integer commonCTERefNum = node.getCommonCTERefNum();
            //collect static filters in a map in first traversal for pushing down later
            if (!isCteNodeAlreadyVisited) {
                if (context.get() == null) {
                    return context.defaultRewrite(node, context.get());
                }
                RowExpression expression = context.get();
                CTEScanNode rewrittenPlan = node;
                if (!dynamicFiltering) {
                    rewrittenPlan = (CTEScanNode) context.defaultRewrite(node, context.get());
                }
                RowExpression dynamicConjuncts = logicalRowExpressions.filterConjuncts(context.get(), s -> getDescriptor(s).isPresent());
                if (cteFilterMap.get(commonCTERefNum) == null) {
                    List<RowExpression> expressions = new ArrayList<>();
                    cteExprMap.put(commonCTERefNum, node.getOutputSymbols());
                    if (dynamicFiltering) {
                        expressions.add(dynamicConjuncts);
                        cteFilterMap.put(commonCTERefNum, expressions);
                        return node;
                    }
                    else {
                        expressions.add(expression);
                        cteFilterMap.put(commonCTERefNum, expressions);
                        cteCostMap.put(commonCTERefNum, getCostForSubtree(rewrittenPlan.getSource()));
                    }
                }
                else {
                    if (dynamicFiltering) {
                        List<RowExpression> existing = cteFilterMap.get(commonCTERefNum);
                        // store the union of filter for pushdown later
                        existing.add(rewriteExpression(node, dynamicConjuncts));
                        cteFilterMap.put(commonCTERefNum, existing);
                        return node;
                    }
                    else {
                        List<RowExpression> existing = cteFilterMap.get(commonCTERefNum);
                        // store the union of filter for pushdown later
                        existing.add(rewriteExpression(node, expression));
                        cteFilterMap.put(commonCTERefNum, existing);

                        PlanCostEstimate existingCost = cteCostMap.get(commonCTERefNum);
                        PlanCostEstimate currentCost = getCostForSubtree(rewrittenPlan);
                        computeTotalCost(existingCost, currentCost, node);
                    }
                }
                PlanNode filterNode = new FilterNode(idAllocator.getNextId(), node, context.get());
                return filterNode;
            }
            // add a filter node above cte node for static conjuncts on second traversal and pushdown their union
            if (cteFilterMap.get(node.getCommonCTERefNum()) != null) {
                if (!dynamicFiltering && costProvider.getCost(node).hasUnknownComponents() && !(!node.getSource().getSources().isEmpty() && node.getSource().getSources().get(0) instanceof WindowNode) &&
                        cteFilterMap.get(node.getCommonCTERefNum()).stream().filter(exp -> !exp.equals(TRUE_CONSTANT)).findFirst().isPresent()) {
                    // remove CTE node if there is no stats
                    return ((CTEScanNode) context.defaultRewrite(node, context.get())).getSource();
                }
                RowExpression rewrittenExpression = rewriteExpression(node, logicalRowExpressions.combineDisjuncts(cteFilterMap.get(commonCTERefNum)));
                CTEScanNode rewrittenNode = (CTEScanNode) context.defaultRewrite(node, rewrittenExpression);
                if (dynamicFiltering) {
                    rewrittenNode = new CTEScanNode(rewrittenNode.getId(), rewrittenNode.getSource(), rewrittenNode.getOutputSymbols(), Optional.of(rewrittenExpression),
                                        rewrittenNode.getCteRefName(), rewrittenNode.getConsumerPlans(), rewrittenNode.getCommonCTERefNum());
                    return rewrittenNode;
                }
                PlanNode filterNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, context.get());
                //pushdown disjuncts if cost is better
                if (cteFilterMap.get(node.getCommonCTERefNum()).contains(TRUE_CONSTANT) || !compareCosts(cteCostMap.get(node.getCommonCTERefNum()), getCostForSubtree(rewrittenNode.getSource()), node)) {
                    return filterNode;
                }
                else {
                    // remove CTE node if cost is not optimal
                    rewrittenNode = ((CTEScanNode) context.defaultRewrite(node, context.get()));
                    return rewrittenNode.getSource();
                }
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode node, RewriteContext<RowExpression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Map<VariableReferenceExpression, VariableReferenceExpression> outputsToInputs = new HashMap<>();
                for (int index = 0; index < node.getInputs().get(i).size(); index++) {
                    outputsToInputs.put(
                            toVariableReference(node.getOutputSymbols().get(index), planSymbolAllocator.getTypes()),
                            toVariableReference(node.getInputs().get(i).get(index), planSymbolAllocator.getTypes()));
                }

                RowExpression sourcePredicate = RowExpressionVariableInliner.inlineVariables(outputsToInputs, context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new ExchangeNode(
                        node.getId(),
                        node.getType(),
                        node.getScope(),
                        node.getPartitioningScheme(),
                        builder.build(),
                        node.getInputs(),
                        node.getOrderingScheme(),
                        node.getAggregationType());
            }

            return node;
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<RowExpression> context)
        {
            // TODO: This could be broader. We can push down conjucts if they are constant for all rows in a window partition.
            // The simplest way to guarantee this is if the conjucts are deterministic functions of the partitioning variables.
            // This can leave out cases where they're both functions of some set of common expressions and the partitioning
            // function is injective, but that's a rare case. The majority of window nodes are expected to be partitioned by
            // pre-projected variables.
            Predicate<RowExpression> isSupported = conjunct ->
                    determinismEvaluator.isDeterministic(conjunct) &&
                            SymbolsExtractor.extractUnique(conjunct).stream().allMatch(node.getPartitionBy()::contains);

            Map<Boolean, List<RowExpression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(isSupported));

            PlanNode rewrittenNode = context.defaultRewrite(node, logicalRowExpressions.combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, logicalRowExpressions.combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<RowExpression> context)
        {
            Set<VariableReferenceExpression> deterministicVariables = node.getAssignments().entrySet().stream()
                    .filter(entry -> determinismEvaluator.isDeterministic(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .map(symbol -> toVariableReference(symbol, planSymbolAllocator.getTypes()))
                    .collect(Collectors.toSet());

            Predicate<RowExpression> deterministic = conjunct -> deterministicVariables.containsAll(VariablesExtractor.extractUnique(conjunct));

            Map<Boolean, List<RowExpression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(deterministic));

            // Push down conjuncts from the inherited predicate that only depend on deterministic assignments with
            // certain limitations.
            List<RowExpression> deterministicConjuncts = conjuncts.get(true);

            // We partition the expressions in the deterministicConjuncts into two lists, and only inline the
            // expressions that are in the inlining targets list.
            Map<Boolean, List<RowExpression>> inlineConjuncts = deterministicConjuncts.stream()
                    .collect(Collectors.partitioningBy(expression -> isInliningCandidate(expression, node)));

            List<RowExpression> inlinedDeterministicConjuncts = inlineConjuncts.get(true).stream()
                    .map(entry -> RowExpressionVariableInliner.inlineVariables(toVariableReferenceMap(node.getAssignments().getMap(), planSymbolAllocator.getTypes()), entry))
                    .collect(Collectors.toList());

            PlanNode rewrittenNode = context.defaultRewrite(node, logicalRowExpressions.combineConjuncts(inlinedDeterministicConjuncts));

            // All deterministic conjuncts that contains non-inlining targets, and non-deterministic conjuncts,
            // if any, will be in the filter node.
            List<RowExpression> nonInliningConjuncts = inlineConjuncts.get(false);
            nonInliningConjuncts.addAll(conjuncts.get(false));

            if (!nonInliningConjuncts.isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, logicalRowExpressions.combineConjuncts(nonInliningConjuncts));
            }

            return rewrittenNode;
        }

        private boolean isInliningCandidate(RowExpression expression, ProjectNode node)
        {
            if (isCteNodeAlreadyVisited) {
                return true;
            }
            // TryExpressions should not be pushed down. However they are now being handled as lambda
            // passed to a FunctionCall now and should not affect predicate push down. So we want to make
            // sure the conjuncts are not TryExpressions.

            verify(uniqueSubExpressions(expression)
                    .stream()
                    .noneMatch(subExpression -> subExpression instanceof CallExpression &&
                            (((CallExpression) subExpression).getDisplayName()).contains(TryFunction.NAME)));

            // candidate symbols for inlining are
            //   1. references to simple constants
            //   2. references to complex expressions that appear only once
            // which come from the node, as opposed to an enclosing scope.
            Set<VariableReferenceExpression> childOutputSet = ImmutableSet.copyOf(toVariableReferences(node.getOutputSymbols(), planSymbolAllocator.getTypes()));
            Map<VariableReferenceExpression, Long> dependencies = VariablesExtractor.extractAll(expression).stream()
                    .filter(childOutputSet::contains)
                    .collect(Collectors.groupingBy(identity(), Collectors.counting()));

            AtomicInteger maxOccurance = new AtomicInteger(1);
            if (expression instanceof CallExpression) {
                CallExpression callExpression = (CallExpression) expression;
                if (callExpression.getDisplayName().contains(DynamicFilters.Function.NAME)
                        && callExpression.getFilter().isPresent()) {
                    maxOccurance.set(3);
                }
            }

            return dependencies.entrySet().stream()
                    .allMatch(entry -> entry.getValue() == maxOccurance.get() || node.getAssignments().get(toSymbol(entry.getKey())) instanceof ConstantExpression);
        }

        @Override
        public PlanNode visitGroupId(GroupIdNode node, RewriteContext<RowExpression> context)
        {
            Map<VariableReferenceExpression, VariableReferenceExpression> commonGroupingVariableMapping = node.getGroupingColumns().entrySet().stream()
                    .filter(entry -> node.getCommonGroupingColumns().contains(entry.getKey()))
                    .collect(Collectors.toMap(entry -> toVariableReference(entry.getKey(), planSymbolAllocator.getTypes()), entry -> toVariableReference(entry.getValue(), planSymbolAllocator.getTypes())));

            Predicate<RowExpression> pushdownEligiblePredicate = conjunct -> VariablesExtractor.extractUnique(conjunct).stream()
                    .allMatch(commonGroupingVariableMapping.keySet()::contains);

            Map<Boolean, List<RowExpression>> conjuncts = extractConjuncts(context.get()).stream().collect(Collectors.partitioningBy(pushdownEligiblePredicate));

            // Push down conjuncts from the inherited predicate that apply to common grouping symbols
            PlanNode rewrittenNode = context.defaultRewrite(node, RowExpressionVariableInliner.inlineVariables(commonGroupingVariableMapping, logicalRowExpressions.combineConjuncts(conjuncts.get(true))));

            // All other conjuncts, if any, will be in the filter node.
            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, logicalRowExpressions.combineConjuncts(conjuncts.get(false)));
            }

            return rewrittenNode;
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<RowExpression> context)
        {
            Set<VariableReferenceExpression> pushDownableVariables = ImmutableSet.copyOf(toVariableReferences(node.getDistinctSymbols(), planSymbolAllocator.getTypes()));
            Map<Boolean, List<RowExpression>> conjuncts = extractConjuncts(context.get()).stream()
                    .collect(Collectors.partitioningBy(conjunct -> pushDownableVariables.containsAll(VariablesExtractor.extractUnique(conjunct))));

            PlanNode rewrittenNode = context.defaultRewrite(node, logicalRowExpressions.combineConjuncts(conjuncts.get(true)));

            if (!conjuncts.get(false).isEmpty()) {
                rewrittenNode = new FilterNode(idAllocator.getNextId(), rewrittenNode, logicalRowExpressions.combineConjuncts(conjuncts.get(false)));
            }
            return rewrittenNode;
        }

        @Override
        public PlanNode visitSort(SortNode node, RewriteContext<RowExpression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitUnion(UnionNode node, RewriteContext<RowExpression> context)
        {
            boolean modified = false;
            ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                Map<VariableReferenceExpression, VariableReferenceExpression> sourceVariable = node.sourceSymbolMap(i).entrySet().stream()
                        .collect(Collectors.toMap(entry -> toVariableReference(entry.getKey(), planSymbolAllocator.getTypes()), entry -> toVariableReference(entry.getValue(), planSymbolAllocator.getTypes())));
                RowExpression sourcePredicate = RowExpressionVariableInliner.inlineVariables(sourceVariable, context.get());
                PlanNode source = node.getSources().get(i);
                PlanNode rewrittenSource = context.rewrite(source, sourcePredicate);
                if (rewrittenSource != source) {
                    modified = true;
                }
                builder.add(rewrittenSource);
            }

            if (modified) {
                return new UnionNode(node.getId(), builder.build(), node.getSymbolMapping(), node.getOutputSymbols());
            }

            return node;
        }

        @Deprecated
        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<RowExpression> context)
        {
            PlanNode rewrittenPlan = context.rewrite(node.getSource(), logicalRowExpressions.combineConjuncts(node.getPredicate(), context.get()));
            if (dynamicFiltering && isFilterNodeAboveCTEScan(node)) {
                return new FilterNode(idAllocator.getNextId(), rewrittenPlan, node.getPredicate());
            }
            // handles CTEScanNode rewrite before exchanges are added
            if (node.getSource() instanceof CTEScanNode) {
                return rewrittenPlan;
            }
            if (!(rewrittenPlan instanceof FilterNode)) {
                return rewrittenPlan;
            }

            FilterNode rewrittenFilterNode = (FilterNode) rewrittenPlan;
            if (!areExpressionsEquivalent(rewrittenFilterNode.getPredicate(), node.getPredicate())
                    || node.getSource() != rewrittenFilterNode.getSource()) {
                return rewrittenPlan;
            }

            return node;
        }

        @Override
        public PlanNode visitJoin(JoinNode inputNode, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();

            // See if we can rewrite outer joins in terms of a plain inner join
            JoinNode node = tryNormalizeToOuterToInnerJoin(inputNode, inheritedPredicate);

            RowExpression leftEffectivePredicate = effectivePredicateExtractor.extract(node.getLeft(), session);
            RowExpression rightEffectivePredicate = effectivePredicateExtractor.extract(node.getRight(), session);
            RowExpression joinPredicate = extractJoinPredicate(node);

            RowExpression leftPredicate;
            RowExpression rightPredicate;
            RowExpression postJoinPredicate;
            RowExpression newJoinPredicate;

            List<VariableReferenceExpression> nodeLeftOutput = toVariableReferences(node.getLeft().getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeRightOutput = toVariableReferences(node.getRight().getOutputSymbols(), planSymbolAllocator.getTypes());

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(node,
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            nodeLeftOutput);
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            nodeLeftOutput);
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case RIGHT:
                    OuterJoinPushDownResult rightOuterJoinPushDownResult = processLimitedOuterJoin(inheritedPredicate,
                            rightEffectivePredicate,
                            leftEffectivePredicate,
                            joinPredicate,
                            nodeRightOutput);
                    leftPredicate = rightOuterJoinPushDownResult.getInnerJoinPredicate();
                    rightPredicate = rightOuterJoinPushDownResult.getOuterJoinPredicate();
                    postJoinPredicate = rightOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = rightOuterJoinPushDownResult.getJoinPredicate();
                    break;
                case FULL:
                    leftPredicate = TRUE_CONSTANT;
                    rightPredicate = TRUE_CONSTANT;
                    postJoinPredicate = inheritedPredicate;
                    newJoinPredicate = joinPredicate;
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            // TODO: find a better way to directly optimize FALSE LITERAL in join predicate
            if (newJoinPredicate.equals(FALSE_CONSTANT)) {
                newJoinPredicate = buildEqualsExpression(metadata.getFunctionAndTypeManager(), constant(0L, BIGINT), constant(1L, BIGINT));
            }

            PlanNode output = node;

            // Create identity projections for all existing symbols
            Assignments.Builder leftProjections = Assignments.builder()
                    .putAll(identityAssignments(planSymbolAllocator.getTypes(), node.getLeft().getOutputSymbols()));

            Assignments.Builder rightProjections = Assignments.builder()
                    .putAll(identityAssignments(planSymbolAllocator.getTypes(), node.getRight().getOutputSymbols()));

            // Create new projections for the new join clauses
            List<JoinNode.EquiJoinClause> equiJoinClauses = new ArrayList<>();
            ImmutableList.Builder<RowExpression> joinFilterBuilder = ImmutableList.builder();
            for (RowExpression conjunct : extractConjuncts(newJoinPredicate)) {
                if (joinEqualityExpression(nodeLeftOutput).test(conjunct)) {
                    boolean alignedComparison = Iterables.all(VariablesExtractor.extractUnique(getLeft(conjunct)), in(nodeLeftOutput));
                    RowExpression leftExpression = (alignedComparison) ? getLeft(conjunct) : getRight(conjunct);
                    RowExpression rightExpression = (alignedComparison) ? getRight(conjunct) : getLeft(conjunct);

                    VariableReferenceExpression leftVariable = variableForExpression(leftExpression);
                    if (!nodeLeftOutput.contains(leftVariable)) {
                        leftProjections.put(toSymbol(leftVariable), leftExpression);
                    }

                    VariableReferenceExpression rightVariable = variableForExpression(rightExpression);
                    if (!nodeRightOutput.contains(rightVariable)) {
                        rightProjections.put(toSymbol(rightVariable), rightExpression);
                    }

                    equiJoinClauses.add(new JoinNode.EquiJoinClause(toSymbol(leftVariable), toSymbol(rightVariable)));
                }
                else {
                    joinFilterBuilder.add(conjunct);
                }
            }

            Optional<RowExpression> newJoinFilter = Optional.of(logicalRowExpressions.combineConjuncts(joinFilterBuilder.build()));
            if (newJoinFilter.get() == TRUE_CONSTANT) {
                newJoinFilter = Optional.empty();
            }

            //extract expression to be pushed down to tablescan and leverage dynamic filter for filtering
            DynamicFiltersResult dynamicFiltersResult = createDynamicFilters(node, equiJoinClauses, newJoinFilter, session, idAllocator);
            Map<String, Symbol> dynamicFilters;
            if (isCteNodeAlreadyVisited && !node.getDynamicFilters().isEmpty()) {
                dynamicFilters = node.getDynamicFilters();
                List<RowExpression> predicates = getDynFilterPredicatesFromProbeSide(node.getLeft(), dynamicFilters);
                //the result leftPredicate will have the dynamic filter predicate 'AND' to it.
                leftPredicate = logicalRowExpressions.combineConjuncts(leftPredicate, logicalRowExpressions.combineConjuncts(predicates));
            }
            else {
                dynamicFilters = dynamicFiltersResult.getDynamicFilters();
                //the result leftPredicate will have the dynamic filter predicate 'AND' to it.
                leftPredicate = logicalRowExpressions.combineConjuncts(leftPredicate, logicalRowExpressions.combineConjuncts(dynamicFiltersResult.getPredicates()));
            }

            PlanNode leftSource;
            PlanNode rightSource;
            boolean equiJoinClausesUnmodified = ImmutableSet.copyOf(equiJoinClauses).equals(ImmutableSet.copyOf(node.getCriteria()));
            if (!equiJoinClausesUnmodified) {
                leftSource = context.rewrite(new ProjectNode(idAllocator.getNextId(), node.getLeft(), leftProjections.build()), leftPredicate);
                rightSource = context.rewrite(new ProjectNode(idAllocator.getNextId(), node.getRight(), rightProjections.build()), rightPredicate);
            }
            else {
                leftSource = context.rewrite(node.getLeft(), leftPredicate);
                rightSource = context.rewrite(node.getRight(), rightPredicate);
            }

            if (node.getType() == INNER && newJoinFilter.isPresent() && equiJoinClauses.isEmpty()) {
                // if we do not have any equi conjunct we do not pushdown non-equality condition into
                // inner join, so we plan execution as nested-loops-join followed by filter instead
                // hash join.
                // todo: remove the code when we have support for filter function in nested loop join
                postJoinPredicate = logicalRowExpressions.combineConjuncts(postJoinPredicate, newJoinFilter.get());
                newJoinFilter = Optional.empty();
            }

            boolean filtersEquivalent =
                    newJoinFilter.isPresent() == node.getFilter().isPresent() &&
                            (!newJoinFilter.isPresent() || areExpressionsEquivalent(newJoinFilter.get(), node.getFilter().get()));

            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !filtersEquivalent ||
                    !dynamicFilters.equals(node.getDynamicFilters()) ||
                    !equiJoinClausesUnmodified) {
                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                // if the distribution type is already set, make sure that changes from PredicatePushDown
                // don't make the join node invalid.
                Optional<JoinNode.DistributionType> distributionType = node.getDistributionType();
                if (node.getDistributionType().isPresent()) {
                    if (node.getType().mustPartition()) {
                        distributionType = Optional.of(PARTITIONED);
                    }
                    if (node.getType().mustReplicate(equiJoinClauses)) {
                        distributionType = Optional.of(REPLICATED);
                    }
                }

                output = new JoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        equiJoinClauses,
                        ImmutableList.<Symbol>builder()
                                .addAll(leftSource.getOutputSymbols())
                                .addAll(rightSource.getOutputSymbols())
                                .build(),
                        newJoinFilter,
                        node.getLeftHashSymbol(),
                        node.getRightHashSymbol(),
                        distributionType,
                        node.isSpillable(),
                        dynamicFilters);
            }

            if (!postJoinPredicate.equals(TRUE_CONSTANT)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }

            if (!node.getOutputSymbols().equals(output.getOutputSymbols())) {
                output = new ProjectNode(idAllocator.getNextId(), output, identityAssignments(planSymbolAllocator.getTypes(), node.getOutputSymbols()));
            }

            return output;
        }

        private DynamicFiltersResult createDynamicFilters(JoinNode node, List<JoinNode.EquiJoinClause> equiJoinClauses,
                Optional<RowExpression> newJoinFilter, Session session,
                PlanNodeIdAllocator idAllocator)
        {
            Map<String, Symbol> dynamicFilters = ImmutableMap.of();
            List<RowExpression> predicates = ImmutableList.of();
            if ((node.getType() == INNER || node.getType() == RIGHT) && isEnableDynamicFiltering(session) && dynamicFiltering) {
                // New equiJoinClauses could potentially not contain symbols used in current dynamic filters.
                // Since we use PredicatePushdown to push dynamic filters themselves,
                // instead of separate ApplyDynamicFilters rule we derive dynamic filters within PredicatePushdown itself.
                // Even if equiJoinClauses.equals(node.getCriteria), current dynamic filters may not match equiJoinClauses
                ImmutableMap.Builder<String, Symbol> dynamicFiltersBuilder = ImmutableMap.builder();
                ImmutableList.Builder<RowExpression> predicatesBuilder = ImmutableList.builder();
                for (JoinNode.EquiJoinClause clause : equiJoinClauses) {
                    Symbol probeSymbol = clause.getLeft();
                    Symbol buildSymbol = clause.getRight();
                    String id = idAllocator.getNextId().toString();
                    predicatesBuilder.add(createDynamicFilterRowExpression(session, metadata, typeManager, id, planSymbolAllocator.getTypes().get(probeSymbol), toSymbolReference(probeSymbol), Optional.empty()));
                    dynamicFiltersBuilder.put(id, buildSymbol);
                }
                if (newJoinFilter.isPresent() && (!newJoinFilter.get().equals(TRUE_CONSTANT) || newJoinFilter.get().equals(FALSE_CONSTANT))) {
                    RowExpression joinFilter = newJoinFilter.get();
                    List<RowExpression> expressions = LogicalRowExpressions.extractConjuncts(joinFilter);
                    Set<Symbol> usedSymbols = new HashSet<>();
                    for (RowExpression expression : expressions) {
                        if (expression instanceof CallExpression) {
                            CallExpression call = (CallExpression) expression;
                            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
                            if (functionMetadata.getOperatorType().isPresent() && functionMetadata.getOperatorType().get().isDynamicFilterComparisonOperator()) {
                                if (call.getArguments().stream().allMatch(VariableReferenceExpression.class::isInstance)) {
                                    if (call.getArguments().get(0).getType() != BIGINT) {
                                        continue;
                                    }

                                    Symbol probeSymbol = null;
                                    Symbol buildSymbol = null;
                                    Symbol left = new Symbol(((VariableReferenceExpression) call.getArguments().get(0)).getName());
                                    Symbol right = new Symbol(((VariableReferenceExpression) call.getArguments().get(1)).getName());

                                    Optional<RowExpression> filter = Optional.empty();
                                    if (node.getLeft().getOutputSymbols().contains(left) && node.getRight().getOutputSymbols().contains(right)) {
                                        probeSymbol = left;
                                        buildSymbol = right;
                                        OperatorType type = functionMetadata.getOperatorType().get();
                                        //To skip dependency checker, both arguments are same.
                                        List<RowExpression> arguments = new ArrayList<>();
                                        arguments.add(call.getArguments().get(0));
                                        arguments.add(call.getArguments().get(1));

                                        filter = Optional.of(new CallExpression(type.name(), call.getFunctionHandle(), call.getType(), arguments, Optional.empty()));
                                    }
                                    else if (node.getRight().getOutputSymbols().contains(left) && node.getLeft().getOutputSymbols().contains(right)) {
                                        probeSymbol = right;
                                        buildSymbol = left;

                                        filter = Optional.of(logicalRowExpressions.flipOperatorFunctionWithOneVarOneConstant(call));
                                    }

                                    if (probeSymbol == null || usedSymbols.contains(buildSymbol) || usedSymbols.contains(probeSymbol)) {
                                        continue;
                                    }

                                    usedSymbols.add(buildSymbol);
                                    usedSymbols.add(probeSymbol);

                                    String id = idAllocator.getNextId().toString();
                                    predicatesBuilder.add(createDynamicFilterRowExpression(session, metadata, typeManager, id, planSymbolAllocator.getTypes().get(probeSymbol), toSymbolReference(probeSymbol), filter));
                                    dynamicFiltersBuilder.put(id, buildSymbol);
                                }
                            }
                        }
                    }
                }
                dynamicFilters = dynamicFiltersBuilder.build();
                predicates = predicatesBuilder.build();
            }
            return new DynamicFiltersResult(dynamicFilters, predicates);
        }

        private static class DynamicFiltersResult
        {
            private final Map<String, Symbol> dynamicFilters;
            private final List<RowExpression> predicates;

            public DynamicFiltersResult(Map<String, Symbol> dynamicFilters, List<RowExpression> predicates)
            {
                this.dynamicFilters = dynamicFilters;
                this.predicates = predicates;
            }

            public Map<String, Symbol> getDynamicFilters()
            {
                return dynamicFilters;
            }

            public List<RowExpression> getPredicates()
            {
                return predicates;
            }
        }

        private static RowExpression getLeft(RowExpression expression)
        {
            checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
            return ((CallExpression) expression).getArguments().get(0);
        }

        private static RowExpression getRight(RowExpression expression)
        {
            checkArgument(expression instanceof CallExpression && ((CallExpression) expression).getArguments().size() == 2, "must be binary call expression");
            return ((CallExpression) expression).getArguments().get(1);
        }

        @Override
        public PlanNode visitSpatialJoin(SpatialJoinNode inputNode, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();
            SpatialJoinNode node = inputNode;

            List<VariableReferenceExpression> nodeLeftOutput = toVariableReferences(node.getLeft().getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeRightOutput = toVariableReferences(node.getRight().getOutputSymbols(), planSymbolAllocator.getTypes());

            // See if we can rewrite left join in terms of a plain inner join
            if (node.getType() == SpatialJoinNode.Type.LEFT && canConvertOuterToInner(nodeRightOutput, inheritedPredicate)) {
                node = new SpatialJoinNode(
                        node.getId(),
                        SpatialJoinNode.Type.INNER,
                        node.getLeft(),
                        node.getRight(),
                        node.getOutputSymbols(),
                        node.getFilter(),
                        node.getLeftPartitionSymbol(),
                        node.getRightPartitionSymbol(),
                        node.getKdbTree());
            }

            RowExpression leftEffectivePredicate = effectivePredicateExtractor.extract(node.getLeft(), session);
            RowExpression rightEffectivePredicate = effectivePredicateExtractor.extract(node.getRight(), session);
            RowExpression joinPredicate = node.getFilter();

            RowExpression leftPredicate;
            RowExpression rightPredicate;
            RowExpression postJoinPredicate;
            RowExpression newJoinPredicate;

            switch (node.getType()) {
                case INNER:
                    InnerJoinPushDownResult innerJoinPushDownResult = processInnerJoin(node,
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            nodeLeftOutput);
                    leftPredicate = innerJoinPushDownResult.getLeftPredicate();
                    rightPredicate = innerJoinPushDownResult.getRightPredicate();
                    postJoinPredicate = innerJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = innerJoinPushDownResult.getJoinPredicate();
                    break;
                case LEFT:
                    OuterJoinPushDownResult leftOuterJoinPushDownResult = processLimitedOuterJoin(
                            inheritedPredicate,
                            leftEffectivePredicate,
                            rightEffectivePredicate,
                            joinPredicate,
                            nodeLeftOutput);
                    leftPredicate = leftOuterJoinPushDownResult.getOuterJoinPredicate();
                    rightPredicate = leftOuterJoinPushDownResult.getInnerJoinPredicate();
                    postJoinPredicate = leftOuterJoinPushDownResult.getPostJoinPredicate();
                    newJoinPredicate = leftOuterJoinPushDownResult.getJoinPredicate();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported spatial join type: " + node.getType());
            }

            newJoinPredicate = simplifyExpression(newJoinPredicate);
            verify(!newJoinPredicate.equals(FALSE_CONSTANT), "Spatial join predicate is missing");

            PlanNode leftSource = context.rewrite(node.getLeft(), leftPredicate);
            PlanNode rightSource = context.rewrite(node.getRight(), rightPredicate);

            PlanNode output = node;
            if (leftSource != node.getLeft() ||
                    rightSource != node.getRight() ||
                    !areExpressionsEquivalent(newJoinPredicate, joinPredicate)) {
                // Create identity projections for all existing symbols
                Assignments.Builder leftProjections = Assignments.builder()
                        .putAll(identityAssignments(planSymbolAllocator.getTypes(), node.getLeft().getOutputSymbols()));

                Assignments.Builder rightProjections = Assignments.builder()
                        .putAll(identityAssignments(planSymbolAllocator.getTypes(), node.getRight().getOutputSymbols()));

                leftSource = new ProjectNode(idAllocator.getNextId(), leftSource, leftProjections.build());
                rightSource = new ProjectNode(idAllocator.getNextId(), rightSource, rightProjections.build());

                output = new SpatialJoinNode(
                        node.getId(),
                        node.getType(),
                        leftSource,
                        rightSource,
                        node.getOutputSymbols(),
                        newJoinPredicate,
                        node.getLeftPartitionSymbol(),
                        node.getRightPartitionSymbol(),
                        node.getKdbTree());
            }

            if (!postJoinPredicate.equals(TRUE_CONSTANT)) {
                output = new FilterNode(idAllocator.getNextId(), output, postJoinPredicate);
            }

            return output;
        }

        private VariableReferenceExpression variableForExpression(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return (VariableReferenceExpression) expression;
            }

            Symbol symbol = planSymbolAllocator.newSymbol(expression);

            return toVariableReference(symbol, planSymbolAllocator.getTypes());
        }

        private OuterJoinPushDownResult processLimitedOuterJoin(RowExpression inputInheritedPredicate, RowExpression inputOuterEffectivePredicate, RowExpression inputInnerEffectivePredicate, RowExpression inputJoinPredicate, Collection<VariableReferenceExpression> outerVariables)
        {
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(inputOuterEffectivePredicate), in(outerVariables)), "inputOuterEffectivePredicate must only contain variables from outerVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(inputInnerEffectivePredicate), not(in(outerVariables))), "inputInnerEffectivePredicate must not contain variables from outerVariables");

            ImmutableList.Builder<RowExpression> outerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> innerPushdownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> postJoinConjuncts = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> joinConjuncts = ImmutableList.builder();
            RowExpression inheritedPredicate = inputInheritedPredicate;

            // Strip out non-deterministic conjuncts
            postJoinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(determinismEvaluator::isDeterministic)));
            inheritedPredicate = logicalRowExpressions.filterDeterministicConjuncts(inheritedPredicate);

            RowExpression outerEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(inputOuterEffectivePredicate);
            RowExpression innerEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(inputInnerEffectivePredicate);
            RowExpression joinPredicate = inputJoinPredicate;
            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(determinismEvaluator::isDeterministic)));
            joinPredicate = logicalRowExpressions.filterDeterministicConjuncts(joinPredicate);

            // Generate equality inferences
            RowExpressionEqualityInference inheritedInference = createEqualityInference(inheritedPredicate);
            RowExpressionEqualityInference outerInference = createEqualityInference(inheritedPredicate, outerEffectivePredicate);

            RowExpressionEqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(in(outerVariables));
            RowExpression outerOnlyInheritedEqualities = logicalRowExpressions.combineConjuncts(equalityPartition.getScopeEqualities());
            RowExpressionEqualityInference potentialNullSymbolInference = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, innerEffectivePredicate, joinPredicate);

            // See if we can push inherited predicates down
            for (RowExpression conjunct : nonInferrableConjuncts(inheritedPredicate)) {
                RowExpression outerRewritten = outerInference.rewriteExpression(conjunct, in(outerVariables));
                if (outerRewritten != null) {
                    outerPushdownConjuncts.add(outerRewritten);

                    // A conjunct can only be pushed down into an inner side if it can be rewritten in terms of the outer side
                    RowExpression innerRewritten = potentialNullSymbolInference.rewriteExpression(outerRewritten, not(in(outerVariables)));
                    if (innerRewritten != null) {
                        innerPushdownConjuncts.add(innerRewritten);
                    }
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }
            // Add the equalities from the inferences back in
            outerPushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            // See if we can push down any outer effective predicates to the inner side
            for (RowExpression conjunct : nonInferrableConjuncts(outerEffectivePredicate)) {
                RowExpression rewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)));
                if (rewritten != null) {
                    innerPushdownConjuncts.add(rewritten);
                }
            }

            // See if we can push down join predicates to the inner side
            for (RowExpression conjunct : nonInferrableConjuncts(joinPredicate)) {
                RowExpression innerRewritten = potentialNullSymbolInference.rewriteExpression(conjunct, not(in(outerVariables)));
                if (innerRewritten != null) {
                    innerPushdownConjuncts.add(innerRewritten);
                }
                else {
                    joinConjuncts.add(conjunct);
                }
            }

            // Push outer and join equalities into the inner side. For example:
            // SELECT * FROM nation LEFT OUTER JOIN region ON nation.regionkey = region.regionkey and nation.name = region.name WHERE nation.name = 'blah'

            RowExpressionEqualityInference potentialNullSymbolInferenceWithoutInnerInferred = createEqualityInference(outerOnlyInheritedEqualities, outerEffectivePredicate, joinPredicate);
            innerPushdownConjuncts.addAll(potentialNullSymbolInferenceWithoutInnerInferred.generateEqualitiesPartitionedBy(not(in(outerVariables))).getScopeEqualities());

            // TODO: we can further improve simplifying the equalities by considering other relationships from the outer side
            RowExpressionEqualityInference.EqualityPartition joinEqualityPartition = createEqualityInference(joinPredicate).generateEqualitiesPartitionedBy(not(in(outerVariables)));
            innerPushdownConjuncts.addAll(joinEqualityPartition.getScopeEqualities());
            joinConjuncts.addAll(joinEqualityPartition.getScopeComplementEqualities())
                    .addAll(joinEqualityPartition.getScopeStraddlingEqualities());

            return new OuterJoinPushDownResult(logicalRowExpressions.combineConjuncts(outerPushdownConjuncts.build()),
                    logicalRowExpressions.combineConjuncts(innerPushdownConjuncts.build()),
                    logicalRowExpressions.combineConjuncts(joinConjuncts.build()),
                    logicalRowExpressions.combineConjuncts(postJoinConjuncts.build()));
        }

        private static class OuterJoinPushDownResult
        {
            private final RowExpression outerJoinPredicate;
            private final RowExpression innerJoinPredicate;
            private final RowExpression joinPredicate;
            private final RowExpression postJoinPredicate;

            private OuterJoinPushDownResult(RowExpression outerJoinPredicate, RowExpression innerJoinPredicate, RowExpression joinPredicate, RowExpression postJoinPredicate)
            {
                this.outerJoinPredicate = outerJoinPredicate;
                this.innerJoinPredicate = innerJoinPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private RowExpression getOuterJoinPredicate()
            {
                return outerJoinPredicate;
            }

            private RowExpression getInnerJoinPredicate()
            {
                return innerJoinPredicate;
            }

            public RowExpression getJoinPredicate()
            {
                return joinPredicate;
            }

            private RowExpression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private InnerJoinPushDownResult processInnerJoin(PlanNode node, RowExpression inputInheritedPredicate, RowExpression inputLeftEffectivePredicate, RowExpression inputRightEffectivePredicate, RowExpression inputJoinPredicate, Collection<VariableReferenceExpression> leftVariables)
        {
            RowExpression leftEffectivePredicate = inputLeftEffectivePredicate;
            RowExpression rightEffectivePredicate = inputRightEffectivePredicate;
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(leftEffectivePredicate), in(leftVariables)), "leftEffectivePredicate must only contain variables from leftVariables");
            checkArgument(Iterables.all(VariablesExtractor.extractUnique(rightEffectivePredicate), not(in(leftVariables))), "rightEffectivePredicate must not contain variables from leftVariables");

            ImmutableList.Builder<RowExpression> leftPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> rightPushDownConjuncts = ImmutableList.builder();
            ImmutableList.Builder<RowExpression> joinConjuncts = ImmutableList.builder();
            RowExpression inheritedPredicate = inputInheritedPredicate;
            RowExpression joinPredicate = inputJoinPredicate;

            // Strip out non-deterministic conjuncts
            joinConjuncts.addAll(filter(extractConjuncts(inheritedPredicate), not(determinismEvaluator::isDeterministic)));
            inheritedPredicate = logicalRowExpressions.filterDeterministicConjuncts(inheritedPredicate);

            joinConjuncts.addAll(filter(extractConjuncts(joinPredicate), not(determinismEvaluator::isDeterministic)));
            joinPredicate = logicalRowExpressions.filterDeterministicConjuncts(joinPredicate);

            leftEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(leftEffectivePredicate);
            rightEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(rightEffectivePredicate);

            // Generate equality inferences
            RowExpressionEqualityInference allInference = new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(inheritedPredicate, leftEffectivePredicate, rightEffectivePredicate, joinPredicate)
                    .build();
            RowExpressionEqualityInference allInferenceWithoutLeftInferred = new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(inheritedPredicate, rightEffectivePredicate, joinPredicate)
                    .build();
            RowExpressionEqualityInference allInferenceWithoutRightInferred = new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(inheritedPredicate, leftEffectivePredicate, joinPredicate)
                    .build();

            List<RowExpression> disjuncts = extractDisjuncts(inheritedPredicate);
            if (isCTEReuseEnabled(session) && isUnionofDynamicFilters(inheritedPredicate)) {
                RowExpression combinedLeftPushDownDisjuncts = FALSE_CONSTANT;
                RowExpression combinedRightPushDownDisjuncts = FALSE_CONSTANT;
                for (RowExpression disjunct : disjuncts) {
                    List<RowExpression> leftPushDownDisjuncts = new ArrayList<>();
                    List<RowExpression> rightPushDownDisjuncts = new ArrayList<>();

                    // Sort through conjuncts in inheritedPredicate that were not used for inference
                    for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(disjunct)) {
                        RowExpression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftVariables));
                        if (leftRewrittenConjunct != null) {
                            leftPushDownDisjuncts.add(leftRewrittenConjunct);
                        }

                        RowExpression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftVariables)));
                        if (rightRewrittenConjunct != null) {
                            rightPushDownDisjuncts.add(rightRewrittenConjunct);
                        }

                        // Drop predicate after join only if unable to push down to either side
                        if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                            joinConjuncts.add(conjunct);
                        }
                    }
                    combinedLeftPushDownDisjuncts = logicalRowExpressions.combineDisjuncts(asList(combinedLeftPushDownDisjuncts, logicalRowExpressions.combineConjuncts(leftPushDownDisjuncts)));
                    combinedRightPushDownDisjuncts = logicalRowExpressions.combineDisjuncts(asList(combinedRightPushDownDisjuncts, logicalRowExpressions.combineConjuncts(rightPushDownDisjuncts)));
                }
                leftPushDownConjuncts.add(combinedLeftPushDownDisjuncts);
                rightPushDownConjuncts.add(combinedRightPushDownDisjuncts);
            }

            else {
                // Sort through conjuncts in inheritedPredicate that were not used for inference
                for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(inheritedPredicate)) {
                    RowExpression leftRewrittenConjunct = allInference.rewriteExpression(conjunct, in(leftVariables));
                    if (leftRewrittenConjunct != null) {
                        leftPushDownConjuncts.add(leftRewrittenConjunct);
                    }

                    RowExpression rightRewrittenConjunct = allInference.rewriteExpression(conjunct, not(in(leftVariables)));
                    if (rightRewrittenConjunct != null) {
                        rightPushDownConjuncts.add(rightRewrittenConjunct);
                    }

                    // Drop predicate after join only if unable to push down to either side
                    if (leftRewrittenConjunct == null && rightRewrittenConjunct == null) {
                        joinConjuncts.add(conjunct);
                    }
                }
            }

            // See if we can push the right effective predicate to the left side
            for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(rightEffectivePredicate)) {
                RowExpression rewritten = allInference.rewriteExpression(conjunct, in(leftVariables));
                if (rewritten != null) {
                    leftPushDownConjuncts.add(rewritten);
                }
            }

            // See if we can push the left effective predicate to the right side
            for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(leftEffectivePredicate)) {
                // do not push down dynamic filters here
                if (!(node instanceof JoinNode && getDescriptor(conjunct).isPresent() && ((JoinNode) node).getDynamicFilters().keySet().contains(getDescriptor(conjunct).get().getId()))) {
                    RowExpression rewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)));
                    if (rewritten != null) {
                        rightPushDownConjuncts.add(rewritten);
                    }
                }
            }

            // See if we can push any parts of the join predicates to either side
            for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(joinPredicate)) {
                RowExpression leftRewritten = allInference.rewriteExpression(conjunct, in(leftVariables));
                if (leftRewritten != null) {
                    leftPushDownConjuncts.add(leftRewritten);
                }

                RowExpression rightRewritten = allInference.rewriteExpression(conjunct, not(in(leftVariables)));
                if (rightRewritten != null) {
                    rightPushDownConjuncts.add(rightRewritten);
                }

                if (leftRewritten == null && rightRewritten == null) {
                    joinConjuncts.add(conjunct);
                }
            }

            // Add equalities from the inference back in
            leftPushDownConjuncts.addAll(allInferenceWithoutLeftInferred.generateEqualitiesPartitionedBy(in(leftVariables)).getScopeEqualities());
            rightPushDownConjuncts.addAll(allInferenceWithoutRightInferred.generateEqualitiesPartitionedBy(not(in(leftVariables))).getScopeEqualities());
            joinConjuncts.addAll(allInference.generateEqualitiesPartitionedBy(in(leftVariables)::apply).getScopeStraddlingEqualities()); // scope straddling equalities get dropped in as part of the join predicate

            return new Rewriter.InnerJoinPushDownResult(
                    logicalRowExpressions.combineConjuncts(leftPushDownConjuncts.build()),
                    logicalRowExpressions.combineConjuncts(rightPushDownConjuncts.build()),
                    logicalRowExpressions.combineConjuncts(joinConjuncts.build()), TRUE_CONSTANT);
        }

        private static class InnerJoinPushDownResult
        {
            private final RowExpression leftPredicate;
            private final RowExpression rightPredicate;
            private final RowExpression joinPredicate;
            private final RowExpression postJoinPredicate;

            private InnerJoinPushDownResult(RowExpression leftPredicate, RowExpression rightPredicate, RowExpression joinPredicate, RowExpression postJoinPredicate)
            {
                this.leftPredicate = leftPredicate;
                this.rightPredicate = rightPredicate;
                this.joinPredicate = joinPredicate;
                this.postJoinPredicate = postJoinPredicate;
            }

            private RowExpression getLeftPredicate()
            {
                return leftPredicate;
            }

            private RowExpression getRightPredicate()
            {
                return rightPredicate;
            }

            private RowExpression getJoinPredicate()
            {
                return joinPredicate;
            }

            private RowExpression getPostJoinPredicate()
            {
                return postJoinPredicate;
            }
        }

        private RowExpression extractJoinPredicate(JoinNode joinNode)
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                builder.add(toRowExpression(equiJoinClause));
            }
            joinNode.getFilter().ifPresent(builder::add);
            return logicalRowExpressions.combineConjuncts(builder.build());
        }

        private RowExpression toRowExpression(JoinNode.EquiJoinClause equiJoinClause)
        {
            return buildEqualsExpression(metadata.getFunctionAndTypeManager(), toVariableReference(equiJoinClause.getLeft(), planSymbolAllocator.getTypes()),
                    toVariableReference(equiJoinClause.getRight(), planSymbolAllocator.getTypes()));
        }

        private JoinNode tryNormalizeToOuterToInnerJoin(JoinNode node, RowExpression inheritedPredicate)
        {
            checkArgument(EnumSet.of(INNER, RIGHT, LEFT, FULL).contains(node.getType()), "Unsupported join type: %s", node.getType());

            if (node.getType() == INNER) {
                return node;
            }

            List<VariableReferenceExpression> nodeLeftOutput = toVariableReferences(node.getLeft().getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> nodeRightOutput = toVariableReferences(node.getRight().getOutputSymbols(), planSymbolAllocator.getTypes());

            if (node.getType() == JoinNode.Type.FULL) {
                boolean canConvertToLeftJoin = canConvertOuterToInner(nodeLeftOutput, inheritedPredicate);
                boolean canConvertToRightJoin = canConvertOuterToInner(nodeRightOutput, inheritedPredicate);
                if (!canConvertToLeftJoin && !canConvertToRightJoin) {
                    return node;
                }
                if (canConvertToLeftJoin && canConvertToRightJoin) {
                    return new JoinNode(node.getId(), INNER,
                            node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(),
                            node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters());
                }
                else {
                    return new JoinNode(node.getId(), canConvertToLeftJoin ? LEFT : RIGHT,
                            node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(),
                            node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters());
                }
            }

            if (node.getType() == LEFT && !canConvertOuterToInner(nodeRightOutput, inheritedPredicate) ||
                    node.getType() == RIGHT && !canConvertOuterToInner(nodeLeftOutput, inheritedPredicate)) {
                return node;
            }
            return new JoinNode(node.getId(), INNER,
                    node.getLeft(), node.getRight(), node.getCriteria(), node.getOutputSymbols(), node.getFilter(),
                    node.getLeftHashSymbol(), node.getRightHashSymbol(), node.getDistributionType(), node.isSpillable(), node.getDynamicFilters());
        }

        private boolean canConvertOuterToInner(List<VariableReferenceExpression> innerVariablesForOuterJoin, RowExpression inheritedPredicate)
        {
            Set<VariableReferenceExpression> innerVariables = ImmutableSet.copyOf(innerVariablesForOuterJoin);
            for (RowExpression conjunct : extractConjuncts(inheritedPredicate)) {
                if (determinismEvaluator.isDeterministic(conjunct)) {
                    // Ignore a conjunct for this test if we can not deterministically get responses from it
                    RowExpression response = nullInputEvaluator(innerVariables, conjunct);
                    if (response == null || Expressions.isNull(response) || FALSE_CONSTANT.equals(response)) {
                        // If there is a single conjunct that returns FALSE or NULL given all NULL inputs for the inner side symbols of an outer join
                        // then this conjunct removes all effects of the outer join, and effectively turns this into an equivalent of an inner join.
                        // So, let's just rewrite this join as an INNER join
                        return true;
                    }
                }
            }
            return false;
        }

        // Temporary implementation for joins because the SimplifyExpressions optimizers can not run properly on join clauses
        private RowExpression simplifyExpression(RowExpression expression)
        {
            return new RowExpressionOptimizer(metadata).optimize(expression, RowExpressionInterpreter.Level.SERIALIZABLE, session.toConnectorSession());
        }

        private boolean areExpressionsEquivalent(RowExpression leftExpression, RowExpression rightExpression)
        {
            return expressionEquivalence.areExpressionsEquivalent(simplifyExpression(leftExpression), simplifyExpression(rightExpression));
        }

        //Evaluates an expression's response to binding the specified input symbols to NULL
        private RowExpression nullInputEvaluator(final Collection<VariableReferenceExpression> nullSymbols, RowExpression inputExpression)
        {
            RowExpression expression = RowExpressionNodeInliner.replaceExpression(inputExpression, nullSymbols.stream()
                    .collect(Collectors.toMap(identity(), variable -> constantNull(variable.getType()))));
            return new RowExpressionOptimizer(metadata).optimize(expression, RowExpressionInterpreter.Level.OPTIMIZED, session.toConnectorSession());
        }

        private Predicate<RowExpression> joinEqualityExpression(final Collection<VariableReferenceExpression> leftVariables)
        {
            return expression -> {
                // At this point in time, our join predicates need to be deterministic
                if (determinismEvaluator.isDeterministic(expression) && isOperation(expression, EQUAL)) {
                    Set<VariableReferenceExpression> variables1 = VariablesExtractor.extractUnique(getLeft(expression));
                    Set<VariableReferenceExpression> variables2 = VariablesExtractor.extractUnique(getRight(expression));
                    if (variables1.isEmpty() || variables2.isEmpty()) {
                        return false;
                    }
                    return (Iterables.all(variables1, in(leftVariables)) && Iterables.all(variables2, not(in(leftVariables)))) ||
                            (Iterables.all(variables2, in(leftVariables)) && Iterables.all(variables1, not(in(leftVariables))));
                }
                return false;
            };
        }

        private boolean isOperation(RowExpression expression, OperatorType type)
        {
            if (expression instanceof CallExpression) {
                Optional<OperatorType> operatorType = metadata.getFunctionAndTypeManager().getFunctionMetadata(((CallExpression) expression).getFunctionHandle()).getOperatorType();
                if (operatorType.isPresent()) {
                    return operatorType.get().equals(type);
                }
            }
            return false;
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();
            if (!extractConjuncts(inheritedPredicate).contains(toVariableReference(node.getSemiJoinOutput(), planSymbolAllocator.getTypes()))) {
                return visitNonFilteringSemiJoin(node, context);
            }
            return visitFilteringSemiJoin(node, context);
        }

        private PlanNode visitNonFilteringSemiJoin(SemiJoinNode node, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();
            List<RowExpression> sourceConjuncts = new ArrayList<>();
            List<RowExpression> postJoinConjuncts = new ArrayList<>();

            // TODO: see if there are predicates that can be inferred from the semi join output

            PlanNode rewrittenFilteringSource = context.defaultRewrite(node.getFilteringSource(), TRUE_CONSTANT);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            RowExpressionEqualityInference inheritedInference = new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(inheritedPredicate)
                    .build();
            for (RowExpression conjunct : new RowExpressionEqualityInference.Builder(metadata, typeManager).nonInferrableConjuncts(inheritedPredicate)) {
                RowExpression rewrittenConjunct = inheritedInference.rewriteExpressionAllowNonDeterministic(conjunct, in(toVariableReferences(node.getSource().getOutputSymbols(), planSymbolAllocator.getTypes())));
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Add the inherited equality predicates back in
            RowExpressionEqualityInference.EqualityPartition equalityPartition = inheritedInference.generateEqualitiesPartitionedBy(
                    in(toVariableReferences(node.getSource().getOutputSymbols(), planSymbolAllocator.getTypes()))::apply);
            sourceConjuncts.addAll(equalityPartition.getScopeEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postJoinConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), logicalRowExpressions.combineConjuncts(sourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource()) {
                output = new SemiJoinNode(node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        Optional.empty());
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, logicalRowExpressions.combineConjuncts(postJoinConjuncts));
            }
            return output;
        }

        private PlanNode visitFilteringSemiJoin(SemiJoinNode node, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();
            RowExpression deterministicInheritedPredicate = logicalRowExpressions.filterDeterministicConjuncts(inheritedPredicate);
            RowExpression sourceEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getSource(), session));
            RowExpression filteringSourceEffectivePredicate = logicalRowExpressions.filterDeterministicConjuncts(effectivePredicateExtractor.extract(node.getFilteringSource(), session));
            RowExpression joinExpression = buildEqualsExpression(metadata.getFunctionAndTypeManager(),
                    toVariableReference(node.getSourceJoinSymbol(), planSymbolAllocator.getTypes()),
                    toVariableReference(node.getFilteringSourceJoinSymbol(), planSymbolAllocator.getTypes()));

            List<VariableReferenceExpression> sourceVariables = toVariableReferences(node.getSource().getOutputSymbols(), planSymbolAllocator.getTypes());
            List<VariableReferenceExpression> filteringSourceVariables = toVariableReferences(node.getFilteringSource().getOutputSymbols(), planSymbolAllocator.getTypes());

            List<RowExpression> sourceConjuncts = new ArrayList<>();
            List<RowExpression> filteringSourceConjuncts = new ArrayList<>();
            List<RowExpression> postJoinConjuncts = new ArrayList<>();

            // Generate equality inferences
            RowExpressionEqualityInference allInference = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, filteringSourceEffectivePredicate, joinExpression);
            RowExpressionEqualityInference allInferenceWithoutSourceInferred = createEqualityInference(deterministicInheritedPredicate, filteringSourceEffectivePredicate, joinExpression);
            RowExpressionEqualityInference allInferenceWithoutFilteringSourceInferred = createEqualityInference(deterministicInheritedPredicate, sourceEffectivePredicate, joinExpression);

            // Push inheritedPredicates down to the source if they don't involve the semi join output
            for (RowExpression conjunct : nonInferrableConjuncts(inheritedPredicate)) {
                RowExpression rewrittenConjunct = allInference.rewriteExpressionAllowNonDeterministic(conjunct, in(sourceVariables));
                // Since each source row is reflected exactly once in the output, ok to push non-deterministic predicates down
                if (rewrittenConjunct != null) {
                    sourceConjuncts.add(rewrittenConjunct);
                }
                else {
                    postJoinConjuncts.add(conjunct);
                }
            }

            // Push inheritedPredicates down to the filtering source if possible
            for (RowExpression conjunct : nonInferrableConjuncts(deterministicInheritedPredicate)) {
                RowExpression rewrittenConjunct = allInference.rewriteExpression(conjunct, in(filteringSourceVariables));
                // We cannot push non-deterministic predicates to filtering side. Each filtering side row have to be
                // logically reevaluated for each source row.
                if (rewrittenConjunct != null) {
                    filteringSourceConjuncts.add(rewrittenConjunct);
                }
            }

            // move effective predicate conjuncts source <-> filter
            // See if we can push the filtering source effective predicate to the source side
            for (RowExpression conjunct : nonInferrableConjuncts(filteringSourceEffectivePredicate)) {
                RowExpression rewritten = allInference.rewriteExpression(conjunct, in(sourceVariables));
                if (rewritten != null) {
                    sourceConjuncts.add(rewritten);
                }
            }

            // See if we can push the source effective predicate to the filtering soruce side
            for (RowExpression conjunct : nonInferrableConjuncts(sourceEffectivePredicate)) {
                RowExpression rewritten = allInference.rewriteExpression(conjunct, in(filteringSourceVariables));
                if (rewritten != null) {
                    filteringSourceConjuncts.add(rewritten);
                }
            }

            // Add equalities from the inference back in
            sourceConjuncts.addAll(allInferenceWithoutSourceInferred.generateEqualitiesPartitionedBy(in(sourceVariables)).getScopeEqualities());
            filteringSourceConjuncts.addAll(allInferenceWithoutFilteringSourceInferred.generateEqualitiesPartitionedBy(in(filteringSourceVariables)).getScopeEqualities());

            // Add dynamic filtering predicate
            Optional<String> dynamicFilterId = node.getDynamicFilterId();
            if (!dynamicFilterId.isPresent() && isEnableDynamicFiltering(session) && dynamicFiltering) {
                dynamicFilterId = Optional.of(idAllocator.getNextId().toString());
                Symbol sourceSymbol = node.getSourceJoinSymbol();

                sourceConjuncts.add(createDynamicFilterRowExpression(session, metadata, typeManager, dynamicFilterId.get(), planSymbolAllocator.getTypes().get(sourceSymbol), SymbolUtils.toSymbolReference(sourceSymbol), Optional.empty()));
            }

            PlanNode rewrittenSource = context.rewrite(node.getSource(), logicalRowExpressions.combineConjuncts(sourceConjuncts));
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), logicalRowExpressions.combineConjuncts(filteringSourceConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource() || rewrittenFilteringSource != node.getFilteringSource() || !dynamicFilterId.equals(node.getDynamicFilterId())) {
                output = new SemiJoinNode(
                        node.getId(),
                        rewrittenSource,
                        rewrittenFilteringSource,
                        node.getSourceJoinSymbol(),
                        node.getFilteringSourceJoinSymbol(),
                        node.getSemiJoinOutput(),
                        node.getSourceHashSymbol(),
                        node.getFilteringSourceHashSymbol(),
                        node.getDistributionType(),
                        dynamicFilterId);
            }
            if (!postJoinConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, logicalRowExpressions.combineConjuncts(postJoinConjuncts));
            }
            return output;
        }

        private Iterable<RowExpression> nonInferrableConjuncts(RowExpression inheritedPredicate)
        {
            return new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .nonInferrableConjuncts(inheritedPredicate);
        }

        private RowExpressionEqualityInference createEqualityInference(RowExpression... expressions)
        {
            return new RowExpressionEqualityInference.Builder(metadata, typeManager)
                    .addEqualityInference(expressions)
                    .build();
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<RowExpression> context)
        {
            if (node.hasEmptyGroupingSet()) {
                // TODO: in case of grouping sets, we should be able to push the filters over grouping keys below the aggregation
                // and also preserve the filter above the aggregation if it has an empty grouping set
                return visitPlan(node, context);
            }

            RowExpression inheritedPredicate = context.get();

            RowExpressionEqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<RowExpression> pushdownConjuncts = new ArrayList<>();
            List<RowExpression> postAggregationConjuncts = new ArrayList<>();

            List<VariableReferenceExpression> groupingKeyVariables = toVariableReferences(node.getGroupingKeys(), planSymbolAllocator.getTypes());

            // Strip out non-deterministic conjuncts
            postAggregationConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(determinismEvaluator::isDeterministic))));
            inheritedPredicate = logicalRowExpressions.filterDeterministicConjuncts(inheritedPredicate);

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (RowExpression conjunct : nonInferrableConjuncts(inheritedPredicate)) {
                if (node.getGroupIdSymbol().isPresent() && SymbolsExtractor.extractUnique(conjunct).contains(node.getGroupIdSymbol().get())) {
                    // aggregation operator synthesizes outputs for group ids corresponding to the global grouping set (i.e., ()), so we
                    // need to preserve any predicates that evaluate the group id to run after the aggregation
                    // TODO: we should be able to infer if conditions on grouping() correspond to global grouping sets to determine whether
                    // we need to do this for each specific case
                    postAggregationConjuncts.add(conjunct);
                    continue;
                }

                RowExpression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(groupingKeyVariables));
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postAggregationConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            RowExpressionEqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(groupingKeyVariables)::apply);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postAggregationConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), logicalRowExpressions.combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new AggregationNode(node.getId(),
                        rewrittenSource,
                        node.getAggregations(),
                        node.getGroupingSets(),
                        ImmutableList.of(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol(),
                        node.getAggregationType(),
                        node.getFinalizeSymbol());
            }
            if (!postAggregationConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, logicalRowExpressions.combineConjuncts(postAggregationConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<RowExpression> context)
        {
            RowExpression inheritedPredicate = context.get();

            RowExpressionEqualityInference equalityInference = createEqualityInference(inheritedPredicate);

            List<RowExpression> pushdownConjuncts = new ArrayList<>();
            List<RowExpression> postUnnestConjuncts = new ArrayList<>();

            // Strip out non-deterministic conjuncts
            postUnnestConjuncts.addAll(ImmutableList.copyOf(filter(extractConjuncts(inheritedPredicate), not(determinismEvaluator::isDeterministic))));
            inheritedPredicate = logicalRowExpressions.filterDeterministicConjuncts(inheritedPredicate);

            List<VariableReferenceExpression> nodeReplicate = toVariableReferences(node.getReplicateSymbols(), planSymbolAllocator.getTypes());

            // Sort non-equality predicates by those that can be pushed down and those that cannot
            for (RowExpression conjunct : nonInferrableConjuncts(inheritedPredicate)) {
                RowExpression rewrittenConjunct = equalityInference.rewriteExpression(conjunct, in(nodeReplicate));
                if (rewrittenConjunct != null) {
                    pushdownConjuncts.add(rewrittenConjunct);
                }
                else {
                    postUnnestConjuncts.add(conjunct);
                }
            }

            // Add the equality predicates back in
            RowExpressionEqualityInference.EqualityPartition equalityPartition = equalityInference.generateEqualitiesPartitionedBy(in(nodeReplicate)::apply);
            pushdownConjuncts.addAll(equalityPartition.getScopeEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeComplementEqualities());
            postUnnestConjuncts.addAll(equalityPartition.getScopeStraddlingEqualities());

            PlanNode rewrittenSource = context.rewrite(node.getSource(), logicalRowExpressions.combineConjuncts(pushdownConjuncts));

            PlanNode output = node;
            if (rewrittenSource != node.getSource()) {
                output = new UnnestNode(node.getId(), rewrittenSource, node.getReplicateSymbols(), node.getUnnestSymbols(), node.getOrdinalitySymbol());
            }
            if (!postUnnestConjuncts.isEmpty()) {
                output = new FilterNode(idAllocator.getNextId(), output, logicalRowExpressions.combineConjuncts(postUnnestConjuncts));
            }
            return output;
        }

        @Override
        public PlanNode visitSample(SampleNode node, RewriteContext<RowExpression> context)
        {
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<RowExpression> context)
        {
            RowExpression predicate = simplifyExpression(context.get());
            if (isCteNodeAlreadyVisited) {
                predicate = rewriteExpressionForDynamicFilterUnion(predicate);
            }

            if (!TRUE_CONSTANT.equals(predicate)) {
                return new FilterNode(idAllocator.getNextId(), node, predicate);
            }

            return node;
        }

        @Override
        public PlanNode visitAssignUniqueId(AssignUniqueId node, RewriteContext<RowExpression> context)
        {
            Set<VariableReferenceExpression> predicateVariables = VariablesExtractor.extractUnique(context.get());
            checkState(!predicateVariables.contains(toVariableReference(node.getIdColumn(), planSymbolAllocator.getTypes())), "UniqueId in predicate is not yet supported");
            return context.defaultRewrite(node, context.get());
        }

        private static CallExpression buildEqualsExpression(FunctionAndTypeManager functionAndTypeManager, RowExpression left, RowExpression right)
        {
            return call(EQUAL.name(), functionAndTypeManager.resolveOperatorFunctionHandle(EQUAL, fromTypes(left.getType(), right.getType())), BOOLEAN, left, right);
        }

        private boolean isSecondTraverseRequired()
        {
            isCteNodeAlreadyVisited = !cteFilterMap.isEmpty();
            return isCteNodeAlreadyVisited;
        }

        private PlanCostEstimate getCostForSubtree(PlanNode subTree)
        {
            return costProvider.getCost(subTree);
        }

        private void computeTotalCost(PlanCostEstimate existing, PlanCostEstimate current, CTEScanNode node)
        {
            try {
                if (costComparator.compare(session, existing, current) > 0) {
                    cteCostMap.put(node.getCommonCTERefNum(), current);
                }
            }
            catch (IllegalArgumentException e) {
                LOG.info("Cannot compare unknown costs");
            }
        }

        private boolean compareCosts(PlanCostEstimate left, PlanCostEstimate right, CTEScanNode node)
        {
            if (left == null) {
                return true;
            }
            try {
                return left.getCpuCost() < (right.getCpuCost() * pushdownCostScaleFactor / (cteFilterMap.get(node.getCommonCTERefNum()).size()));
            }
            catch (IllegalArgumentException e) {
                System.out.println(e.getMessage());
            }
            return true;
        }

        private RowExpression rewriteExpression(PlanNode node, RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return new VariableReferenceExpression(getOriginalSymbolRefFromChild(node, ((VariableReferenceExpression) expression).getName()), expression.getType());
            }
            else if (expression instanceof SpecialForm) {
                return new SpecialForm(((SpecialForm) expression).getForm(), expression.getType(), ((SpecialForm) expression).getArguments().stream().map(exp ->
                        rewriteExpression(node, exp)).collect(Collectors.toList()));
            }
            else if (expression instanceof CallExpression) {
                return new CallExpression(((CallExpression) expression).getDisplayName(), ((CallExpression) expression).getFunctionHandle(), expression.getType(), ((CallExpression) expression).getArguments().stream().map(exp ->
                        rewriteExpression(node, exp)).collect(Collectors.toList()));
            }
            return expression;
        }

        private String getOriginalSymbolRefFromChild(PlanNode node, String columnName)
        {
            if (isCteNodeAlreadyVisited) {
                int symbolIndex = cteExprMap.get(((CTEScanNode) node).getCommonCTERefNum()).indexOf(new Symbol(columnName));
                return node.getOutputSymbols().get(symbolIndex).getName();
            }
            else {
                int symbolIndex = node.getOutputSymbols().indexOf(new Symbol(columnName));
                return cteExprMap.get(((CTEScanNode) node).getCommonCTERefNum()).get(symbolIndex).getName();
            }
        }

        private boolean isUnionofDynamicFilters(RowExpression expression)
        {
            if (expression instanceof SpecialForm && ((SpecialForm) expression).getForm().equals(OR)) {
                for (RowExpression argument : ((SpecialForm) expression).getArguments()) {
                    List<RowExpression> conjuncts = extractConjuncts(argument);
                    return conjuncts.stream().anyMatch(exp -> getDescriptor(exp).isPresent());
                }
            }
            return false;
        }

        private List<RowExpression> getDynFilterPredicatesFromProbeSide(PlanNode node, Map<String, Symbol> dynamicFilters)
        {
            List<RowExpression> dynamicFilterPredicates = new ArrayList<>();
            if (node instanceof FilterNode) {
                for (Map.Entry<String, Symbol> dynamicFilter : dynamicFilters.entrySet()) {
                    List<RowExpression> predicates = extractAllPredicates(((FilterNode) node).getPredicate()).stream().filter(exp -> getDescriptor(exp).isPresent() &&
                            getDescriptor(exp).get().getId().equals(dynamicFilter.getKey())).collect(Collectors.toList());
                    dynamicFilterPredicates.addAll(predicates);
                }
                return dynamicFilterPredicates;
            }
            else {
                node.getSources().stream().forEach(source -> dynamicFilterPredicates.addAll(getDynFilterPredicatesFromProbeSide(source, dynamicFilters)));
            }
            return dynamicFilterPredicates;
        }

        private RowExpression rewriteExpressionForDynamicFilterUnion(RowExpression expression)
        {
            Optional<RowExpression> staticFilterExpression = extractStaticFilters(Optional.of(expression), metadata);
            RowExpression dynamicFilterExpression = extractDynamicFilterExpression(expression, metadata);
            List<RowExpression> conjuncts = extractConjuncts(dynamicFilterExpression);
            if (!(dynamicFilterExpression instanceof SpecialForm) || ((SpecialForm) dynamicFilterExpression).getForm() != OR) {
                RowExpression unionSubtree = null;
                RowExpression finalUnionTree = FALSE_CONSTANT;
                List<RowExpression> conjunctsSubtree = new ArrayList<>();
                for (RowExpression conjunct : conjuncts) {
                    if (conjunct instanceof SpecialForm && ((SpecialForm) conjunct).getForm() == OR) {
                        unionSubtree = conjunct;
                    }
                    else {
                        conjunctsSubtree.add(conjunct);
                    }
                }
                if (unionSubtree != null) {
                    for (RowExpression disjunct : extractDisjuncts(unionSubtree)) {
                        RowExpression rewrittenDisjunct = logicalRowExpressions.combineConjuncts(disjunct, logicalRowExpressions.combineConjuncts(conjunctsSubtree));
                        finalUnionTree = logicalRowExpressions.combineDisjuncts(asList(rewrittenDisjunct, finalUnionTree));
                    }
                    if (staticFilterExpression.isPresent()) {
                        return logicalRowExpressions.combineConjuncts(asList(staticFilterExpression.get(), finalUnionTree));
                    }
                    else {
                        return finalUnionTree;
                    }
                }
            }
            return expression;
        }

        private boolean isFilterNodeAboveCTEScan(FilterNode filterNode)
        {
            PlanNode source = filterNode.getSource();
            while (source instanceof ProjectNode || source instanceof ExchangeNode) {
                if (source.getSources().get(0) instanceof CTEScanNode) {
                    return true;
                }
                source = source.getSources().get(0);
            }
            return false;
        }
    }

    private class FilterPushdownForCTEHandler
    {
        private final boolean pushdownForCTE;
        private final CostProvider costProvider;
        private final CostComparator costComparator;

        public FilterPushdownForCTEHandler(boolean pushdownForCTE, CostProvider costProvider, CostComparator costComparator)
        {
            this.pushdownForCTE = pushdownForCTE;
            this.costProvider = costProvider;
            this.costComparator = costComparator;
        }

        public boolean isPushdownForCTE()
        {
            return pushdownForCTE;
        }

        public CostProvider getCostProvider()
        {
            return costProvider;
        }

        public CostComparator getCostComparator()
        {
            return costComparator;
        }
    }
}
