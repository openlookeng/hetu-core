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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.EnforceSingleRowNode;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.ExistsPredicate;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.NotExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.QuantifiedComparisonExpression;
import io.prestosql.sql.tree.QuantifiedComparisonExpression.Quantifier;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.SubqueryExpression;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.util.MorePredicates;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.sql.analyzer.SemanticExceptions.notSupportedException;
import static io.prestosql.sql.planner.ExpressionNodeInliner.replaceExpression;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.ComparisonExpression.Operator.EQUAL;
import static io.prestosql.sql.util.AstUtils.nodeContains;
import static java.util.Objects.requireNonNull;

class SubqueryPlanner
{
    private final Analysis analysis;
    private final PlanSymbolAllocator planSymbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final Session session;
    private final Map<QualifiedName, Integer> namedSubPlan;
    private final UniqueIdAllocator uniqueIdAllocator;

    SubqueryPlanner(
            Analysis analysis,
            PlanSymbolAllocator planSymbolAllocator,
            PlanNodeIdAllocator idAllocator,
            Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap,
            Metadata metadata,
            Session session,
            Map<QualifiedName, Integer> namedSubPlan,
            UniqueIdAllocator uniqueIdAllocator)
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(planSymbolAllocator, "symbolAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(session, "session is null");

        this.analysis = analysis;
        this.planSymbolAllocator = planSymbolAllocator;
        this.idAllocator = idAllocator;
        this.lambdaDeclarationToSymbolMap = lambdaDeclarationToSymbolMap;
        this.metadata = metadata;
        this.session = session;
        this.namedSubPlan = namedSubPlan;
        this.uniqueIdAllocator = uniqueIdAllocator;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        PlanBuilder planBuilder = builder;
        for (Expression expression : expressions) {
            planBuilder = handleSubqueries(planBuilder, expression, node, true);
        }
        return planBuilder;
    }

    public PlanBuilder handleUncorrelatedSubqueries(PlanBuilder builder, Collection<Expression> expressions, Node node)
    {
        PlanBuilder planBuilder = builder;
        for (Expression expression : expressions) {
            planBuilder = handleSubqueries(planBuilder, expression, node, false);
        }
        return planBuilder;
    }

    public PlanBuilder handleSubqueries(PlanBuilder builder, Expression expression, Node node)
    {
        return handleSubqueries(builder, expression, node, true);
    }

    private PlanBuilder handleSubqueries(PlanBuilder inputBuilder, Expression expression, Node node, boolean correlationAllowed)
    {
        PlanBuilder builder = inputBuilder;
        builder = appendInPredicateApplyNodes(builder, collectInPredicateSubqueries(expression, node), correlationAllowed, node);
        builder = appendScalarSubqueryApplyNodes(builder, collectScalarSubqueries(expression, node), correlationAllowed);
        builder = appendExistsSubqueryApplyNodes(builder, collectExistsSubqueries(expression, node), correlationAllowed);
        builder = appendQuantifiedComparisonApplyNodes(builder, collectQuantifiedComparisonSubqueries(expression, node), correlationAllowed, node);
        return builder;
    }

    public Set<InPredicate> collectInPredicateSubqueries(Expression expression, Node node)
    {
        return analysis.getInPredicateSubqueries(node)
                .stream()
                .filter(inPredicate -> nodeContains(expression, inPredicate.getValueList()))
                .collect(toImmutableSet());
    }

    public Set<SubqueryExpression> collectScalarSubqueries(Expression expression, Node node)
    {
        return analysis.getScalarSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<ExistsPredicate> collectExistsSubqueries(Expression expression, Node node)
    {
        return analysis.getExistsSubqueries(node)
                .stream()
                .filter(subquery -> nodeContains(expression, subquery))
                .collect(toImmutableSet());
    }

    public Set<QuantifiedComparisonExpression> collectQuantifiedComparisonSubqueries(Expression expression, Node node)
    {
        return analysis.getQuantifiedComparisonSubqueries(node)
                .stream()
                .filter(quantifiedComparison -> nodeContains(expression, quantifiedComparison.getSubquery()))
                .collect(toImmutableSet());
    }

    private PlanBuilder appendInPredicateApplyNodes(PlanBuilder subPlan, Set<InPredicate> inPredicates, boolean correlationAllowed, Node node)
    {
        PlanBuilder planBuilder = subPlan;
        for (InPredicate inPredicate : inPredicates) {
            planBuilder = appendInPredicateApplyNode(planBuilder, inPredicate, correlationAllowed, node);
        }
        return planBuilder;
    }

    private PlanBuilder appendInPredicateApplyNode(PlanBuilder inputSubPlan, InPredicate inPredicate, boolean correlationAllowed, Node node)
    {
        PlanBuilder subPlan = inputSubPlan;
        if (subPlan.canTranslate(inPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        subPlan = handleSubqueries(subPlan, inPredicate.getValue(), node);

        subPlan = subPlan.appendProjections(ImmutableList.of(inPredicate.getValue()), planSymbolAllocator, idAllocator);

        checkState(inPredicate.getValueList() instanceof SubqueryExpression);
        SubqueryExpression valueListSubquery = (SubqueryExpression) inPredicate.getValueList();
        SubqueryExpression uncoercedValueListSubquery = uncoercedSubquery(valueListSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedValueListSubquery);

        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(valueListSubquery), planSymbolAllocator, idAllocator);
        SymbolReference valueList = toSymbolReference(subqueryPlan.translate(valueListSubquery));

        Symbol rewrittenValue = subPlan.translate(inPredicate.getValue());
        InPredicate inPredicateSubqueryExpression = new InPredicate(toSymbolReference(rewrittenValue), valueList);
        Symbol inPredicateSubquerySymbol = planSymbolAllocator.newSymbol(inPredicateSubqueryExpression, BOOLEAN);

        subPlan.getTranslations().put(inPredicate, inPredicateSubquerySymbol);

        return appendApplyNode(subPlan, inPredicate, subqueryPlan.getRoot(), Assignments.of(inPredicateSubquerySymbol, castToRowExpression(inPredicateSubqueryExpression)), correlationAllowed);
    }

    private PlanBuilder appendScalarSubqueryApplyNodes(PlanBuilder inputBuilder, Set<SubqueryExpression> scalarSubqueries, boolean correlationAllowed)
    {
        PlanBuilder builder = inputBuilder;
        for (SubqueryExpression scalarSubquery : scalarSubqueries) {
            builder = appendScalarSubqueryApplyNode(builder, scalarSubquery, correlationAllowed);
        }
        return builder;
    }

    private PlanBuilder appendScalarSubqueryApplyNode(PlanBuilder subPlan, SubqueryExpression scalarSubquery, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(scalarSubquery)) {
            // given subquery is already appended
            return subPlan;
        }

        List<Expression> coercions = coercionsFor(scalarSubquery);

        SubqueryExpression uncoercedScalarSubquery = uncoercedSubquery(scalarSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedScalarSubquery);
        subqueryPlan = subqueryPlan.withNewRoot(new EnforceSingleRowNode(idAllocator.getNextId(), subqueryPlan.getRoot()));
        subqueryPlan = subqueryPlan.appendProjections(coercions, planSymbolAllocator, idAllocator);

        Symbol uncoercedScalarSubquerySymbol = subqueryPlan.translate(uncoercedScalarSubquery);
        subPlan.getTranslations().put(uncoercedScalarSubquery, uncoercedScalarSubquerySymbol);

        for (Expression coercion : coercions) {
            Symbol coercionSymbol = subqueryPlan.translate(coercion);
            subPlan.getTranslations().put(coercion, coercionSymbol);
        }

        // The subquery's EnforceSingleRowNode always produces a row, so the join is effectively INNER
        return appendLateralJoin(subPlan, subqueryPlan, scalarSubquery.getQuery(), correlationAllowed, LateralJoinNode.Type.INNER, TRUE_LITERAL);
    }

    public PlanBuilder appendLateralJoin(PlanBuilder subPlan, PlanBuilder subqueryPlan, Query query, boolean correlationAllowed, LateralJoinNode.Type type, Expression filterCondition)
    {
        PlanNode subqueryNode = subqueryPlan.getRoot();
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(query, "Correlated subquery in given context");
        }
        subqueryNode = replaceExpressionsWithSymbols(subqueryNode, correlation);

        return new PlanBuilder(
                subPlan.copyTranslations(),
                new LateralJoinNode(
                        idAllocator.getNextId(),
                        subPlan.getRoot(),
                        subqueryNode,
                        ImmutableList.copyOf(SymbolsExtractor.extractUnique(correlation.values())),
                        type,
                        filterCondition,
                        query));
    }

    private PlanBuilder appendExistsSubqueryApplyNodes(PlanBuilder builder, Set<ExistsPredicate> existsPredicates, boolean correlationAllowed)
    {
        PlanBuilder planBuilder = builder;
        for (ExistsPredicate existsPredicate : existsPredicates) {
            planBuilder = appendExistSubqueryApplyNode(planBuilder, existsPredicate, correlationAllowed);
        }
        return planBuilder;
    }

    /**
     * Exists is modeled as:
     * <pre>
     *     - Project($0 > 0)
     *       - Aggregation(COUNT(*))
     *         - Limit(1)
     *           -- subquery
     * </pre>
     */
    private PlanBuilder appendExistSubqueryApplyNode(PlanBuilder subPlan, ExistsPredicate existsPredicate, boolean correlationAllowed)
    {
        if (subPlan.canTranslate(existsPredicate)) {
            // given subquery is already appended
            return subPlan;
        }

        PlanBuilder subqueryPlan = createPlanBuilder(existsPredicate.getSubquery());

        PlanNode subqueryPlanRoot = subqueryPlan.getRoot();
        if (isAggregationWithEmptyGroupBy(subqueryPlanRoot)) {
            subPlan.getTranslations().put(existsPredicate, TRUE_LITERAL);
            return subPlan;
        }

        // add an explicit projection that removes all columns
        PlanNode subqueryNode = new ProjectNode(idAllocator.getNextId(), subqueryPlan.getRoot(), Assignments.of());

        Symbol exists = planSymbolAllocator.newSymbol("exists", BOOLEAN);
        subPlan.getTranslations().put(existsPredicate, exists);
        ExistsPredicate rewrittenExistsPredicate = new ExistsPredicate(TRUE_LITERAL);
        return appendApplyNode(
                subPlan,
                existsPredicate.getSubquery(),
                subqueryNode,
                Assignments.of(exists, castToRowExpression(rewrittenExistsPredicate)),
                correlationAllowed);
    }

    private PlanBuilder appendQuantifiedComparisonApplyNodes(PlanBuilder subPlan, Set<QuantifiedComparisonExpression> quantifiedComparisons, boolean correlationAllowed, Node node)
    {
        PlanBuilder planBuilder = subPlan;
        for (QuantifiedComparisonExpression quantifiedComparison : quantifiedComparisons) {
            planBuilder = appendQuantifiedComparisonApplyNode(planBuilder, quantifiedComparison, correlationAllowed, node);
        }
        return planBuilder;
    }

    private PlanBuilder appendQuantifiedComparisonApplyNode(PlanBuilder inputSubPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed, Node node)
    {
        PlanBuilder subPlan = inputSubPlan;
        if (subPlan.canTranslate(quantifiedComparison)) {
            // given subquery is already appended
            return subPlan;
        }

        switch (quantifiedComparison.getOperator()) {
            case EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed);
                    case ANY:
                    case SOME:
                        // A = ANY B <=> A IN B
                        InPredicate inPredicate = new InPredicate(quantifiedComparison.getValue(), quantifiedComparison.getSubquery());
                        subPlan = appendInPredicateApplyNode(subPlan, inPredicate, correlationAllowed, node);
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.translate(inPredicate));
                        return subPlan;
                }
                break;

            case NOT_EQUAL:
                switch (quantifiedComparison.getQuantifier()) {
                    case ALL:
                        // A <> ALL B <=> !(A IN B) <=> !(A = ANY B)
                        QuantifiedComparisonExpression rewrittenAny = new QuantifiedComparisonExpression(
                                EQUAL,
                                Quantifier.ANY,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAny = new NotExpression(rewrittenAny);
                        // "A <> ALL B" is equivalent to "NOT (A = ANY B)" so add a rewrite for the initial quantifiedComparison to notAny
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAny));
                        // now plan "A = ANY B" part by calling ourselves for rewrittenAny
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAny, correlationAllowed, node);
                    case ANY:
                    case SOME:
                        // A <> ANY B <=> min B <> max B || A <> min B <=> !(min B = max B && A = min B) <=> !(A = ALL B)
                        QuantifiedComparisonExpression rewrittenAll = new QuantifiedComparisonExpression(
                                EQUAL,
                                QuantifiedComparisonExpression.Quantifier.ALL,
                                quantifiedComparison.getValue(),
                                quantifiedComparison.getSubquery());
                        Expression notAll = new NotExpression(rewrittenAll);
                        // "A <> ANY B" is equivalent to "NOT (A = ALL B)" so add a rewrite for the initial quantifiedComparison to notAll
                        subPlan.getTranslations().put(quantifiedComparison, subPlan.getTranslations().rewrite(notAll));
                        // now plan "A = ALL B" part by calling ourselves for rewrittenAll
                        return appendQuantifiedComparisonApplyNode(subPlan, rewrittenAll, correlationAllowed, node);
                }
                break;

            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                return planQuantifiedApplyNode(subPlan, quantifiedComparison, correlationAllowed);
        }
        // all cases are checked, so this exception should never be thrown
        throw new IllegalArgumentException(
                String.format("Unexpected quantified comparison: '%s %s'", quantifiedComparison.getOperator().getValue(), quantifiedComparison.getQuantifier()));
    }

    private PlanBuilder planQuantifiedApplyNode(PlanBuilder inputSubPlan, QuantifiedComparisonExpression quantifiedComparison, boolean correlationAllowed)
    {
        PlanBuilder subPlan = inputSubPlan;
        subPlan = subPlan.appendProjections(ImmutableList.of(quantifiedComparison.getValue()), planSymbolAllocator, idAllocator);

        checkState(quantifiedComparison.getSubquery() instanceof SubqueryExpression);
        SubqueryExpression quantifiedSubquery = (SubqueryExpression) quantifiedComparison.getSubquery();

        SubqueryExpression uncoercedQuantifiedSubquery = uncoercedSubquery(quantifiedSubquery);
        PlanBuilder subqueryPlan = createPlanBuilder(uncoercedQuantifiedSubquery);
        subqueryPlan = subqueryPlan.appendProjections(ImmutableList.of(quantifiedSubquery), planSymbolAllocator, idAllocator);

        QuantifiedComparisonExpression coercedQuantifiedComparison = new QuantifiedComparisonExpression(
                quantifiedComparison.getOperator(),
                quantifiedComparison.getQuantifier(),
                toSymbolReference(subPlan.translate(quantifiedComparison.getValue())),
                toSymbolReference(subqueryPlan.translate(quantifiedSubquery)));

        Symbol coercedQuantifiedComparisonSymbol = planSymbolAllocator.newSymbol(coercedQuantifiedComparison, BOOLEAN);
        subPlan.getTranslations().put(quantifiedComparison, coercedQuantifiedComparisonSymbol);

        return appendApplyNode(
                subPlan,
                quantifiedComparison.getSubquery(),
                subqueryPlan.getRoot(),
                Assignments.of(coercedQuantifiedComparisonSymbol, castToRowExpression(coercedQuantifiedComparison)),
                correlationAllowed);
    }

    private static boolean isAggregationWithEmptyGroupBy(PlanNode planNode)
    {
        return searchFrom(planNode)
                .recurseOnlyWhen(MorePredicates.isInstanceOfAny(ProjectNode.class))
                .where(AggregationNode.class::isInstance)
                .findFirst()
                .map(AggregationNode.class::cast)
                .map(aggregation -> aggregation.getGroupingKeys().isEmpty())
                .orElse(false);
    }

    /**
     * Implicit coercions are added when mapping an expression to symbol in {@link TranslationMap}. Coercions
     * for expression are obtained from {@link Analysis} by identity comparison. Create a copy of subquery
     * in order to get a subquery expression that does not have any coercion assigned to it {@link Analysis}.
     */
    private SubqueryExpression uncoercedSubquery(SubqueryExpression subquery)
    {
        return new SubqueryExpression(subquery.getQuery());
    }

    private List<Expression> coercionsFor(Expression expression)
    {
        return analysis.getCoercions().keySet().stream()
                .map(NodeRef::getNode)
                .filter(coercionExpression -> coercionExpression.equals(expression))
                .collect(toImmutableList());
    }

    private PlanBuilder appendApplyNode(
            PlanBuilder inputSubPlan,
            Node subquery,
            PlanNode subqueryNode,
            Assignments subqueryAssignments,
            boolean correlationAllowed)
    {
        PlanBuilder subPlan = inputSubPlan;
        Map<Expression, Expression> correlation = extractCorrelation(subPlan, subqueryNode);
        if (!correlationAllowed && !correlation.isEmpty()) {
            throw notSupportedException(subquery, "Correlated subquery in given context");
        }
        subPlan = subPlan.appendProjections(correlation.keySet(), planSymbolAllocator, idAllocator);
        PlanNode planNode = replaceExpressionsWithSymbols(subqueryNode, correlation);

        TranslationMap translations = subPlan.copyTranslations();
        PlanNode root = subPlan.getRoot();
        return new PlanBuilder(translations,
                new ApplyNode(idAllocator.getNextId(),
                        root,
                        planNode,
                        subqueryAssignments,
                        ImmutableList.copyOf(SymbolsExtractor.extractUnique(correlation.values())),
                        subquery));
    }

    private Map<Expression, Expression> extractCorrelation(PlanBuilder subPlan, PlanNode subquery)
    {
        Set<Expression> missingReferences = extractOuterColumnReferences(subquery);
        ImmutableMap.Builder<Expression, Expression> correlation = ImmutableMap.builder();
        for (Expression missingReference : missingReferences) {
            // missing reference expression can be solved within current subPlan,
            // or within outer plans in case of multiple nesting levels of subqueries.
            tryResolveMissingExpression(subPlan, missingReference)
                    .ifPresent(symbolReference -> correlation.put(missingReference, symbolReference));
        }
        return correlation.build();
    }

    /**
     * Checks if give reference expression can resolved within given plan.
     */
    private static Optional<Expression> tryResolveMissingExpression(PlanBuilder subPlan, Expression expression)
    {
        Expression rewritten = subPlan.rewrite(expression);
        if (rewritten != expression) {
            return Optional.of(rewritten);
        }
        return Optional.empty();
    }

    private PlanBuilder createPlanBuilder(Node node)
    {
        RelationPlan relationPlan = new RelationPlanner(analysis, planSymbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session, namedSubPlan, uniqueIdAllocator)
                .process(node, null);
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToSymbolMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the FROM clause directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        if (node instanceof Expression && relationPlan.getFieldMappings().size() == 1) {
            translations.put((Expression) node, getOnlyElement(relationPlan.getFieldMappings()));
        }

        return new PlanBuilder(translations, relationPlan.getRoot());
    }

    /**
     * @return a set of reference expressions which cannot be resolved within this plan. For plan representing:
     * SELECT a, b FROM (VALUES 1) T(a). It will return a set containing single expression reference to 'b'.
     */
    private Set<Expression> extractOuterColumnReferences(PlanNode planNode)
    {
        // at this point all the column references are already rewritten to SymbolReference
        // when reference expression is not rewritten that means it cannot be satisfied within given PlanNode
        // see that TranslationMap only resolves (local) fields in current scope
        return ExpressionExtractor.extractExpressions(planNode).stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .flatMap(expression -> extractColumnReferences(expression, analysis.getColumnReferences()).stream())
                .collect(toImmutableSet());
    }

    private static Set<Expression> extractColumnReferences(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<Expression> expressionColumnReferences = ImmutableSet.builder();
        new ColumnReferencesExtractor(columnReferences).process(expression, expressionColumnReferences);
        return expressionColumnReferences.build();
    }

    private PlanNode replaceExpressionsWithSymbols(PlanNode planNode, Map<Expression, Expression> mapping)
    {
        if (mapping.isEmpty()) {
            return planNode;
        }

        return SimplePlanRewriter.rewriteWith(new ExpressionReplacer(idAllocator, mapping), planNode, null);
    }

    private static class ColumnReferencesExtractor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableSet.Builder<Expression>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private ColumnReferencesExtractor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<Expression> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(node);
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<Expression> builder)
        {
            builder.add(node);
            return null;
        }
    }

    private static class ExpressionReplacer
            extends SimplePlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final Map<Expression, Expression> mapping;

        public ExpressionReplacer(PlanNodeIdAllocator idAllocator, Map<Expression, Expression> mapping)
        {
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.mapping = requireNonNull(mapping, "mapping is null");
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            ProjectNode rewrittenNode = (ProjectNode) context.defaultRewrite(node);

            Assignments assignments = AssignmentUtils.rewrite(rewrittenNode.getAssignments(), expression -> replaceExpression(expression, mapping));

            return new ProjectNode(idAllocator.getNextId(), rewrittenNode.getSource(), assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> context)
        {
            FilterNode rewrittenNode = (FilterNode) context.defaultRewrite(node);
            return new FilterNode(idAllocator.getNextId(),
                    rewrittenNode.getSource(),
                    castToRowExpression(replaceExpression(castToExpression(rewrittenNode.getPredicate()), mapping)));
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            ValuesNode rewrittenNode = (ValuesNode) context.defaultRewrite(node);
            List<List<RowExpression>> rewrittenRows = rewrittenNode.getRows().stream()
                    .map(row -> row.stream()
                            .map(column -> castToRowExpression(replaceExpression(castToExpression(column), mapping)))
                            .collect(toImmutableList()))
                    .collect(toImmutableList());
            return new ValuesNode(
                    idAllocator.getNextId(),
                    rewrittenNode.getOutputSymbols(),
                    rewrittenRows);
        }
    }
}
