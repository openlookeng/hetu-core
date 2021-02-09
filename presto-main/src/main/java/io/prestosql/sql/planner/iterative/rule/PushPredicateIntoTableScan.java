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

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.TableLayoutResult;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.predicate.NullableValue;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionUtils;
import io.prestosql.sql.DynamicFilters;
import io.prestosql.sql.planner.ExpressionDomainTranslator;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.LookupSymbolResolver;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.RowExpressionInterpreter;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.VariableResolver;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionDomainTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NullLiteral;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.metadata.TableLayoutResult.computeEnforced;
import static io.prestosql.spi.sql.RowExpressionUtils.TRUE_CONSTANT;
import static io.prestosql.sql.ExpressionUtils.combineConjuncts;
import static io.prestosql.sql.ExpressionUtils.extractDisjuncts;
import static io.prestosql.sql.ExpressionUtils.filterDeterministicConjuncts;
import static io.prestosql.sql.ExpressionUtils.filterNonDeterministicConjuncts;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static java.util.Objects.requireNonNull;

/**
 * These rules should not be run after AddExchanges so as not to overwrite the TableLayout
 * chosen by AddExchanges
 */
public class PushPredicateIntoTableScan
        implements Rule<FilterNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Logger log = Logger.get(PushPredicateIntoTableScan.class);

    private static final Pattern<FilterNode> PATTERN = filter().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;
    private final TypeAnalyzer typeAnalyzer;
    private final ExpressionDomainTranslator domainTranslator;
    private final boolean pushPartitionsOnly;

    public PushPredicateIntoTableScan(Metadata metadata, TypeAnalyzer typeAnalyzer, boolean pushPartitionsOnly)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeAnalyzer = requireNonNull(typeAnalyzer, "typeAnalyzer is null");
        this.domainTranslator = new ExpressionDomainTranslator(new LiteralEncoder(metadata));
        this.pushPartitionsOnly = pushPartitionsOnly;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(FilterNode filterNode, Captures captures, Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        Optional<PlanNode> rewritten = pushPredicateIntoTableScan(
                tableScan,
                filterNode.getPredicate(),
                false,
                context.getSession(),
                context.getSymbolAllocator().getTypes(),
                context.getIdAllocator(),
                context.getSymbolAllocator(),
                metadata,
                typeAnalyzer,
                domainTranslator,
                pushPartitionsOnly);

        if (!rewritten.isPresent() || arePlansSame(filterNode, tableScan, rewritten.get())) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }

    private boolean arePlansSame(FilterNode filter, TableScanNode tableScan, PlanNode rewritten)
    {
        if (!(rewritten instanceof FilterNode)) {
            return false;
        }

        FilterNode rewrittenFilter = (FilterNode) rewritten;
        if (!Objects.equals(filter.getPredicate(), rewrittenFilter.getPredicate())) {
            return false;
        }

        if (!(rewrittenFilter.getSource() instanceof TableScanNode)) {
            return false;
        }

        TableScanNode rewrittenTableScan = (TableScanNode) rewrittenFilter.getSource();

        return Objects.equals(tableScan.getEnforcedConstraint(), rewrittenTableScan.getEnforcedConstraint());
    }

    /**
     * @param predicate can be a RowExpression or an OriginalExpression. The method will handle both cases.
     * Once Expression is migrated to RowExpression in PickTableLayout, the method should only support RowExpression.
     */
    public static Optional<PlanNode> pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            PlanSymbolAllocator planSymbolAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            ExpressionDomainTranslator domainTranslator,
            boolean pushPartitionsOnly)
    {
        if (isExpression(predicate)) {
            return PushPredicateIntoTableScan.pushFilterIntoTableScan(node, castToExpression(predicate), pruneWithPredicateExpression, session, types, idAllocator, metadata, typeAnalyzer, domainTranslator, pushPartitionsOnly);
        }
        return pushPredicateIntoTableScan(node, predicate, pruneWithPredicateExpression, session, idAllocator, planSymbolAllocator, metadata, new RowExpressionDomainTranslator(metadata), pushPartitionsOnly);
    }

    /**
     * For RowExpression {@param predicate}
     */
    private static Optional<PlanNode> pushPredicateIntoTableScan(
            TableScanNode node,
            RowExpression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            PlanNodeIdAllocator idAllocator,
            PlanSymbolAllocator planSymbolAllocator,
            Metadata metadata,
            RowExpressionDomainTranslator domainTranslator,
            boolean pushPartitionsOnly)
    {
        // don't include non-deterministic predicates
        LogicalRowExpressions logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata));
        RowExpression deterministicPredicate = logicalRowExpressions.filterDeterministicConjuncts(predicate);
        RowExpressionDomainTranslator.ExtractionResult<VariableReferenceExpression> decomposedPredicate =
                domainTranslator.fromPredicate(session.toConnectorSession(), deterministicPredicate);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(variableName -> node.getAssignments().get(new Symbol(variableName.getName())))
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Set<ColumnHandle> allColumnHandles = new HashSet<>();
        assignments.keySet().stream().forEach(allColumnHandles::add);

        Constraint constraint;
        List<Constraint> disjunctConstraints = ImmutableList.of();

        if (!pushPartitionsOnly) {
            List<RowExpression> orSet = RowExpressionUtils.extractDisjuncts(decomposedPredicate.getRemainingExpression());
            List<RowExpressionDomainTranslator.ExtractionResult<VariableReferenceExpression>> disjunctPredicates = orSet.stream()
                    .map(e -> domainTranslator.fromPredicate(session.toConnectorSession(), e))
                    .collect(Collectors.toList());

            /* Check if any Branch yeild all records; then no need to process OR branches */
            if (!disjunctPredicates.stream().anyMatch(e -> e.getTupleDomain().isAll())) {
                List<TupleDomain<ColumnHandle>> orDomains = disjunctPredicates.stream()
                        .map(er -> er.getTupleDomain().transform(variableName -> node.getAssignments().get(new Symbol(variableName.getName()))))
                        .collect(Collectors.toList());

                disjunctConstraints = orDomains.stream()
                        .filter(d -> !d.isAll() && !d.isNone())
                        .map(d -> new Constraint(d))
                        .collect(Collectors.toList());
            }
        }

        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluatorForRowExpression evaluator = new LayoutConstraintEvaluatorForRowExpression(
                    metadata,
                    session,
                    node.getAssignments(),
                    RowExpressionUtils.combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(column -> {
                                if (assignments.size() == 0 || assignments.getOrDefault(column, null) == null) {
                                    return null;
                                }
                                else {
                                    return new VariableReferenceExpression(assignments.getOrDefault(column, null).getName(),
                                            planSymbolAllocator.getSymbols().get(assignments.getOrDefault(column, null)));
                                }
                            }))));
            constraint = new Constraint(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain);
        }
        TableHandle newTable;
        TupleDomain<ColumnHandle> remainingFilter;
        if (!metadata.usesLegacyTableLayouts(session, node.getTable())) {
            if (newDomain.isNone()) {
                // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
                // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
                // to turn the subtree into a Values node
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, node.getTable(), constraint, disjunctConstraints, allColumnHandles, pushPartitionsOnly);

            if (!result.isPresent()) {
                return Optional.empty();
            }

            newTable = result.get().getHandle();

            if (metadata.getTableProperties(session, newTable).getPredicate().isNone()) {
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            remainingFilter = result.get().getRemainingFilter();
        }
        else {
            Optional<TableLayoutResult> layout = metadata.getLayout(
                    session,
                    node.getTable(),
                    constraint,
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (!layout.isPresent() || layout.get().getTableProperties().getPredicate().isNone()) {
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            newTable = layout.get().getNewTableHandle();
            remainingFilter = layout.get().getUnenforcedConstraint();
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                newTable,
                node.getOutputSymbols(),
                node.getAssignments(),
                computeEnforced(newDomain, remainingFilter),
                Optional.of(deterministicPredicate),
                node.getStrategy(),
                node.getReuseTableScanMappingId(),
                0,
                node.isForDelete());

        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        RowExpression resultingPredicate;
        if (remainingFilter.isAll() && newTable.getConnectorHandle().hasDisjunctFiltersPushdown()) {
            resultingPredicate = RowExpressionUtils.combineConjuncts(
                    domainTranslator.toPredicate(remainingFilter.transform(assignments::get), planSymbolAllocator.getSymbols()),
                    logicalRowExpressions.filterNonDeterministicConjuncts(predicate));
        }
        else {
            resultingPredicate = RowExpressionUtils.combineConjuncts(
                    domainTranslator.toPredicate(remainingFilter.transform(assignments::get), planSymbolAllocator.getSymbols()),
                    logicalRowExpressions.filterNonDeterministicConjuncts(predicate),
                    decomposedPredicate.getRemainingExpression());
        }

        if (!TRUE_CONSTANT.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(idAllocator.getNextId(), tableScan, resultingPredicate));
        }
        return Optional.of(tableScan);
    }

    public static Optional<PlanNode> pushFilterIntoTableScan(
            TableScanNode node,
            Expression predicate,
            boolean pruneWithPredicateExpression,
            Session session,
            TypeProvider types,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata,
            TypeAnalyzer typeAnalyzer,
            ExpressionDomainTranslator domainTranslator,
            boolean pushPartitionsOnly)
    {
        // don't include non-deterministic predicates
        Expression deterministicPredicate = filterDeterministicConjuncts(predicate);

        ExpressionDomainTranslator.ExtractionResult decomposedPredicate = ExpressionDomainTranslator.fromPredicate(
                metadata,
                session,
                deterministicPredicate,
                types);

        TupleDomain<ColumnHandle> newDomain = decomposedPredicate.getTupleDomain()
                .transform(node.getAssignments()::get)
                .intersect(node.getEnforcedConstraint());

        Map<ColumnHandle, Symbol> assignments = ImmutableBiMap.copyOf(node.getAssignments()).inverse();
        Set<ColumnHandle> allColumnHandles = new HashSet<>();
        assignments.keySet().stream().forEach(allColumnHandles::add);

        Constraint constraint;
        List<Constraint> disjunctConstraints = ImmutableList.of();

        if (!pushPartitionsOnly) {
            List<Expression> orSet = extractDisjuncts(decomposedPredicate.getRemainingExpression());
            List<ExpressionDomainTranslator.ExtractionResult> disjunctPredicates = orSet.stream()
                    .map(e -> ExpressionDomainTranslator.fromPredicate(metadata, session, e, types))
                    .collect(Collectors.toList());

            /* Check if any Branch yeild all records; then no need to process OR branches */
            if (!disjunctPredicates.stream().anyMatch(e -> e.getTupleDomain().isAll())) {
                List<TupleDomain<ColumnHandle>> orDomains = disjunctPredicates.stream()
                        .map(er -> er.getTupleDomain().transform(node.getAssignments()::get))
                        .collect(Collectors.toList());

                disjunctConstraints = orDomains.stream()
                        .filter(d -> !d.isAll() && !d.isNone())
                        .map(d -> new Constraint(d))
                        .collect(Collectors.toList());
            }
        }

        if (pruneWithPredicateExpression) {
            LayoutConstraintEvaluator evaluator = new LayoutConstraintEvaluator(
                    metadata,
                    typeAnalyzer,
                    session,
                    types,
                    node.getAssignments(),
                    combineConjuncts(
                            deterministicPredicate,
                            // Simplify the tuple domain to avoid creating an expression with too many nodes,
                            // which would be expensive to evaluate in the call to isCandidate below.
                            domainTranslator.toPredicate(newDomain.simplify().transform(assignments::get))));
            constraint = new Constraint(newDomain, evaluator::isCandidate);
        }
        else {
            // Currently, invoking the expression interpreter is very expensive.
            // TODO invoke the interpreter unconditionally when the interpreter becomes cheap enough.
            constraint = new Constraint(newDomain);
        }

        TableHandle newTable;
        TupleDomain<ColumnHandle> remainingFilter;
        if (!metadata.usesLegacyTableLayouts(session, node.getTable())) {
            if (newDomain.isNone()) {
                // TODO: DomainTranslator.fromPredicate can infer that the expression is "false" in some cases (TupleDomain.none()).
                // This should move to another rule that simplifies the filter using that logic and then rely on RemoveTrivialFilters
                // to turn the subtree into a Values node
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            Optional<ConstraintApplicationResult<TableHandle>> result = metadata.applyFilter(session, node.getTable(), constraint, disjunctConstraints, allColumnHandles, pushPartitionsOnly);

            if (!result.isPresent()) {
                return Optional.empty();
            }

            newTable = result.get().getHandle();

            if (metadata.getTableProperties(session, newTable).getPredicate().isNone()) {
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            remainingFilter = result.get().getRemainingFilter();
        }
        else {
            Optional<TableLayoutResult> layout = metadata.getLayout(
                    session,
                    node.getTable(),
                    constraint,
                    Optional.of(node.getOutputSymbols().stream()
                            .map(node.getAssignments()::get)
                            .collect(toImmutableSet())));

            if (!layout.isPresent() || layout.get().getTableProperties().getPredicate().isNone()) {
                return Optional.of(new ValuesNode(idAllocator.getNextId(), node.getOutputSymbols(), ImmutableList.of()));
            }

            newTable = layout.get().getNewTableHandle();
            remainingFilter = layout.get().getUnenforcedConstraint();
        }

        TableScanNode tableScan = new TableScanNode(
                node.getId(),
                newTable,
                node.getOutputSymbols(),
                node.getAssignments(),
                computeEnforced(newDomain, remainingFilter),
                Optional.of(castToRowExpression(deterministicPredicate)),
                node.getStrategy(),
                node.getReuseTableScanMappingId(),
                0,
                node.isForDelete());

        // The order of the arguments to combineConjuncts matters:
        // * Unenforced constraints go first because they can only be simple column references,
        //   which are not prone to logic errors such as out-of-bound access, div-by-zero, etc.
        // * Conjuncts in non-deterministic expressions and non-TupleDomain-expressible expressions should
        //   retain their original (maybe intermixed) order from the input predicate. However, this is not implemented yet.
        // * Short of implementing the previous bullet point, the current order of non-deterministic expressions
        //   and non-TupleDomain-expressible expressions should be retained. Changing the order can lead
        //   to failures of previously successful queries.
        Expression resultingPredicate;
        if (remainingFilter.isAll() && newTable.getConnectorHandle().hasDisjunctFiltersPushdown()) {
            resultingPredicate = combineConjuncts(
                    domainTranslator.toPredicate(remainingFilter.transform(assignments::get)),
                    filterNonDeterministicConjuncts(predicate));
        }
        else {
            resultingPredicate = combineConjuncts(
                    domainTranslator.toPredicate(remainingFilter.transform(assignments::get)),
                    filterNonDeterministicConjuncts(predicate),
                    decomposedPredicate.getRemainingExpression());
        }

        if (!TRUE_LITERAL.equals(resultingPredicate)) {
            return Optional.of(new FilterNode(idAllocator.getNextId(), tableScan, castToRowExpression(resultingPredicate)));
        }

        return Optional.of(tableScan);
    }

    private static class LayoutConstraintEvaluator
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final ExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;
        private static final String DYNAMIC_FUNCTION_NAME = DynamicFilters.Function.NAME;

        public LayoutConstraintEvaluator(Metadata metadata, TypeAnalyzer typeAnalyzer, Session session, TypeProvider types, Map<Symbol, ColumnHandle> assignments, Expression expression)
        {
            this.assignments = assignments;

            evaluator = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, typeAnalyzer.getTypes(session, types, expression));
            arguments = SymbolsExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }

            if (evaluator.getExpression().toString().contains(DYNAMIC_FUNCTION_NAME)) {
                return true;
            }

            LookupSymbolResolver inputs = new LookupSymbolResolver(assignments, bindings);

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            if (Boolean.FALSE.equals(optimized) || optimized == null || optimized instanceof NullLiteral) {
                return false;
            }

            return true;
        }
    }

    private static class LayoutConstraintEvaluatorForRowExpression
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final RowExpressionInterpreter evaluator;
        private final Set<ColumnHandle> arguments;

        public LayoutConstraintEvaluatorForRowExpression(Metadata metadata, Session session, Map<Symbol, ColumnHandle> assignments, RowExpression expression)
        {
            this.assignments = assignments;

            evaluator = new RowExpressionInterpreter(expression, metadata, session.toConnectorSession(), OPTIMIZED);
            arguments = SymbolsExtractor.extractUnique(expression).stream()
                    .map(assignments::get)
                    .collect(toImmutableSet());
        }

        private boolean isCandidate(Map<ColumnHandle, NullableValue> bindings)
        {
            if (intersection(bindings.keySet(), arguments).isEmpty()) {
                return true;
            }
            LookupVariableResolver inputs = new LookupVariableResolver(assignments, bindings, variable -> variable);

            // Skip pruning if evaluation fails in a recoverable way. Failing here can cause
            // spurious query failures for partitions that would otherwise be filtered out.
            Object optimized = TryFunction.evaluate(() -> evaluator.optimize(inputs), true);

            // If any conjuncts evaluate to FALSE or null, then the whole predicate will never be true and so the partition should be pruned
            return !Boolean.FALSE.equals(optimized) && optimized != null && (!(optimized instanceof ConstantExpression) || !((ConstantExpression) optimized).isNull());
        }
    }

    private static class LookupVariableResolver
            implements VariableResolver
    {
        private final Map<Symbol, ColumnHandle> assignments;
        private final Map<ColumnHandle, NullableValue> bindings;
        // Use Object type to let interpreters consume the result
        // TODO: use RowExpression once the Expression-to-RowExpression is done
        private final Function<VariableReferenceExpression, Object> missingBindingSupplier;

        public LookupVariableResolver(
                Map<Symbol, ColumnHandle> assignments,
                Map<ColumnHandle, NullableValue> bindings,
                Function<VariableReferenceExpression, Object> missingBindingSupplier)
        {
            this.assignments = requireNonNull(assignments, "assignments is null");
            this.bindings = ImmutableMap.copyOf(requireNonNull(bindings, "bindings is null"));
            this.missingBindingSupplier = requireNonNull(missingBindingSupplier, "missingBindingSupplier is null");
        }

        @Override
        public Object getValue(VariableReferenceExpression variable)
        {
            ColumnHandle column = assignments.get(new Symbol(variable.getName()));
            checkArgument(column != null, "Missing column assignment for %s", variable);

            if (!bindings.containsKey(column)) {
                return missingBindingSupplier.apply(variable);
            }

            return bindings.get(column).getValue();
        }
    }
}
