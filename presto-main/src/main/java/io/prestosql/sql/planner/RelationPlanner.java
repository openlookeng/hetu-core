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
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.UnmodifiableIterator;
import io.airlift.log.Logger;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.metadata.TableHandle;
import io.prestosql.spi.operator.ReuseExchangeOperator;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.ExceptNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.IntersectNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.PlanNodeIdAllocator;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.UnionNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.RelationId;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.planner.plan.CTEScanNode;
import io.prestosql.sql.planner.plan.JoinNodeUtils;
import io.prestosql.sql.planner.plan.LateralJoinNode;
import io.prestosql.sql.planner.plan.SampleNode;
import io.prestosql.sql.planner.plan.UnnestNode;
import io.prestosql.sql.tree.AliasedRelation;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.CoalesceExpression;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.Except;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.Intersect;
import io.prestosql.sql.tree.Join;
import io.prestosql.sql.tree.JoinCriteria;
import io.prestosql.sql.tree.JoinUsing;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.Lateral;
import io.prestosql.sql.tree.NaturalJoin;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.Query;
import io.prestosql.sql.tree.QuerySpecification;
import io.prestosql.sql.tree.Relation;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.SampledRelation;
import io.prestosql.sql.tree.SetOperation;
import io.prestosql.sql.tree.SymbolReference;
import io.prestosql.sql.tree.Table;
import io.prestosql.sql.tree.TableSubquery;
import io.prestosql.sql.tree.Union;
import io.prestosql.sql.tree.Unnest;
import io.prestosql.sql.tree.Values;
import io.prestosql.type.TypeCoercion;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getCteMaxQueueSize;
import static io.prestosql.SystemSessionProperties.getExecutionPolicy;
import static io.prestosql.SystemSessionProperties.getTaskConcurrency;
import static io.prestosql.SystemSessionProperties.isCTEReuseEnabled;
import static io.prestosql.metadata.MetadataUtil.createQualifiedObjectName;
import static io.prestosql.spi.plan.AggregationNode.singleGroupingSet;
import static io.prestosql.sql.analyzer.SemanticExceptions.notSupportedException;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static io.prestosql.sql.tree.Join.Type.INNER;
import static java.util.Objects.requireNonNull;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private static final Logger LOG = Logger.get(RelationPlanner.class);

    private final Analysis analysis;
    private final PlanSymbolAllocator planSymbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;
    private final Metadata metadata;
    private final TypeCoercion typeCoercion;
    private final Session session;
    private final SubqueryPlanner subqueryPlanner;
    private final Map<QualifiedName, Integer> namedSubPlan;
    private final UniqueIdAllocator uniqueIdAllocator;

    RelationPlanner(
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
        this.typeCoercion = new TypeCoercion(metadata::getType);
        this.session = session;
        this.subqueryPlanner = new SubqueryPlanner(analysis, planSymbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session);
        this.namedSubPlan = namedSubPlan;
        this.uniqueIdAllocator = uniqueIdAllocator;
    }

    @Override
    protected RelationPlan visitTable(Table node, Void context)
    {
        Scope scope = analysis.getScope(node);
        Query namedQuery = analysis.getNamedQuery(node);
        if (namedQuery != null) {
            RelationPlan subPlan = process(namedQuery, null);

            // Add implicit coercions if view query produces types that don't match the declared output types
            // of the view (e.g., if the underlying tables referenced by the view changed)
            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
            RelationPlan withCoercions = addCoercions(subPlan, types);
            if ((!isCTEReuseEnabled(session) || getExecutionPolicy(session).equals("phased"))
                            || (getCteMaxQueueSize(session) < getTaskConcurrency(session) * 2)) {
                if (getCteMaxQueueSize(session) < getTaskConcurrency(session) * 2) {
                    LOG.info("Main queue size " + getCteMaxQueueSize(session) + "should be more than 2 times of concurrent task " + getTaskConcurrency(session));
                }
                return new RelationPlan(withCoercions.getRoot(), scope, withCoercions.getFieldMappings());
            }

            Integer commonCTERefNum;
            if (namedSubPlan.containsKey(node.getName())) {
                commonCTERefNum = namedSubPlan.get(node.getName());
            }
            else {
                commonCTERefNum = uniqueIdAllocator.getNextId();
                namedSubPlan.put(node.getName(), commonCTERefNum);
            }

            PlanNode cteNode = new CTEScanNode(idAllocator.getNextId(),
                    withCoercions.getRoot(),
                    withCoercions.getFieldMappings(),
                    Optional.empty(),
                    node.getName().toString(),
                    new HashSet<>(),
                    commonCTERefNum);
            subPlan = new RelationPlan(cteNode, scope, withCoercions.getFieldMappings());
            return subPlan;
        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelationType().getAllFields()) {
            Symbol symbol = planSymbolAllocator.newSymbol(field.getName().get(), field.getType());

            outputSymbolsBuilder.add(symbol);
            columns.put(symbol, analysis.getColumn(field));
        }

        List<Symbol> outputSymbols = outputSymbolsBuilder.build();
        boolean isDeleteTarget = analysis.isDeleteTarget(createQualifiedObjectName(session, node, node.getName()));
        PlanNode root = TableScanNode.newInstance(idAllocator.getNextId(), handle, outputSymbols, columns.build(), ReuseExchangeOperator.STRATEGY.REUSE_STRATEGY_DEFAULT, 0, 0, isDeleteTarget);
        RelationPlan tableScan = new RelationPlan(root, scope, outputSymbols);
        tableScan = addRowFilters(node, tableScan);
        tableScan = addColumnMasks(node, tableScan);
        return tableScan;
    }

    private RelationPlan addRowFilters(Table node, RelationPlan plan)
    {
        PlanBuilder planBuilder = initializePlanBuilder(plan);
        for (Expression filter : analysis.getRowFilters(node)) {
            planBuilder = subqueryPlanner.handleSubqueries(planBuilder, filter, filter);

            planBuilder = planBuilder.withNewRoot(new FilterNode(
                    idAllocator.getNextId(),
                    planBuilder.getRoot(),
                    castToRowExpression(planBuilder.rewrite(filter))));
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), plan.getFieldMappings());
    }

    private RelationPlan addColumnMasks(Table table, RelationPlan plan)
    {
        Map<String, List<Expression>> columnMasks = analysis.getColumnMasks(table);

        PlanNode root = plan.getRoot();
        List<Symbol> mappings = plan.getFieldMappings();

        TranslationMap translations = new TranslationMap(plan, analysis, lambdaDeclarationToSymbolMap);
        translations.setFieldMappings(mappings);

        PlanBuilder planBuilder = new PlanBuilder(translations, root, analysis.getParameters());

        for (int i = 0; i < plan.getDescriptor().getAllFieldCount(); i++) {
            Field field = plan.getDescriptor().getFieldByIndex(i);

            for (Expression mask : columnMasks.getOrDefault(field.getName().get(), ImmutableList.of())) {
                planBuilder = subqueryPlanner.handleSubqueries(planBuilder, mask, mask);

                Map<Symbol, RowExpression> assignments = new LinkedHashMap<>();
                for (Symbol symbol : root.getOutputSymbols()) {
                    assignments.put(symbol, castToRowExpression(toSymbolReference(symbol)));
                }
                assignments.put(mappings.get(i), castToRowExpression(translations.rewrite(mask)));

                planBuilder = planBuilder.withNewRoot(new ProjectNode(
                        idAllocator.getNextId(),
                        planBuilder.getRoot(),
                        Assignments.copyOf(assignments)));
            }
        }

        return new RelationPlan(planBuilder.getRoot(), plan.getScope(), mappings);
    }

    @Override
    protected RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        PlanNode root = subPlan.getRoot();
        List<Symbol> mappings = subPlan.getFieldMappings();

        if (node.getColumnNames() != null) {
            ImmutableList.Builder<Symbol> newMappings = ImmutableList.builder();
            Assignments.Builder assignments = Assignments.builder();

            // project only the visible columns from the underlying relation
            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
                Field field = subPlan.getDescriptor().getFieldByIndex(i);
                if (!field.isHidden()) {
                    Symbol aliasedColumn = planSymbolAllocator.newSymbol(field);
                    assignments.put(aliasedColumn, castToRowExpression(toSymbolReference(subPlan.getFieldMappings().get(i))));
                    newMappings.add(aliasedColumn);
                }
            }

            root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build());
            mappings = newMappings.build();
        }

        return new RelationPlan(root, analysis.getScope(node), mappings);
    }

    @Override
    protected RelationPlan visitSampledRelation(SampledRelation node, Void context)
    {
        RelationPlan subPlan = process(node.getRelation(), context);

        double ratio = analysis.getSampleRatio(node);
        PlanNode planNode = new SampleNode(idAllocator.getNextId(),
                subPlan.getRoot(),
                ratio,
                SampleNode.Type.fromType(node.getType()));
        return new RelationPlan(planNode, analysis.getScope(node), subPlan.getFieldMappings());
    }

    @Override
    protected RelationPlan visitJoin(Join node, Void context)
    {
        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
        RelationPlan leftPlan = process(node.getLeft(), context);

        Optional<Unnest> unnest = getUnnest(node.getRight());
        if (unnest.isPresent()) {
            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
                throw notSupportedException(unnest.get(), "UNNEST on other than the right side of CROSS JOIN");
            }
            return planCrossJoinUnnest(leftPlan, node, unnest.get());
        }

        Optional<Lateral> lateral = getLateral(node.getRight());
        if (lateral.isPresent()) {
            return planLateralJoin(node, leftPlan, lateral.get());
        }

        RelationPlan rightPlan = process(node.getRight(), context);

        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
            return planJoinUsing(node, leftPlan, rightPlan);
        }

        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        // NOTE: symbols must be in the same order as the outputDescriptor
        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
        List<Expression> complexJoinExpressions = new ArrayList<>();
        List<Expression> postInnerJoinConditions = new ArrayList<>();

        if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
            Expression criteria = analysis.getJoinCriteria(node);

            RelationType left = analysis.getOutputDescriptor(node.getLeft());
            RelationType right = analysis.getOutputDescriptor(node.getRight());

            List<Expression> leftComparisonExpressions = new ArrayList<>();
            List<Expression> rightComparisonExpressions = new ArrayList<>();
            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
                conjunct = ExpressionUtils.normalize(conjunct);

                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
                    complexJoinExpressions.add(conjunct);
                    continue;
                }

                Set<QualifiedName> dependencies = SymbolsExtractor.extractNames(conjunct, analysis.getColumnReferences());

                if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve)) {
                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
                    complexJoinExpressions.add(conjunct);
                }
                else if (conjunct instanceof ComparisonExpression) {
                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
                    Set<QualifiedName> firstDependencies = SymbolsExtractor.extractNames(firstExpression, analysis.getColumnReferences());
                    Set<QualifiedName> secondDependencies = SymbolsExtractor.extractNames(secondExpression, analysis.getColumnReferences());

                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
                        leftComparisonExpressions.add(firstExpression);
                        rightComparisonExpressions.add(secondExpression);
                        joinConditionComparisonOperators.add(comparisonOperator);
                    }
                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
                        leftComparisonExpressions.add(secondExpression);
                        rightComparisonExpressions.add(firstExpression);
                        joinConditionComparisonOperators.add(comparisonOperator.flip());
                    }
                    else {
                        // the case when we mix symbols from both left and right join side on either side of condition.
                        complexJoinExpressions.add(conjunct);
                    }
                }
                else {
                    complexJoinExpressions.add(conjunct);
                }
            }

            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

            // Add projections for join criteria
            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, planSymbolAllocator, idAllocator);
            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, planSymbolAllocator, idAllocator);

            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
                    Symbol leftSymbol = leftPlanBuilder.translate(leftComparisonExpressions.get(i));
                    Symbol rightSymbol = rightPlanBuilder.translate(rightComparisonExpressions.get(i));

                    equiClauses.add(new JoinNode.EquiJoinClause(leftSymbol, rightSymbol));
                }
                else {
                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
                }
            }
        }

        PlanNode root = new JoinNode(idAllocator.getNextId(),
                JoinNodeUtils.typeConvert(node.getType()),
                leftPlanBuilder.getRoot(),
                rightPlanBuilder.getRoot(),
                equiClauses.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftPlanBuilder.getRoot().getOutputSymbols())
                        .addAll(rightPlanBuilder.getRoot().getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        if (node.getType() != INNER) {
            for (Expression complexExpression : complexJoinExpressions) {
                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
                if (!inPredicates.isEmpty()) {
                    InPredicate inPredicate = Iterables.getLast(inPredicates);
                    throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
                }
            }

            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
            leftPlanBuilder = subqueryPlanner.handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
        }

        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node), outputSymbols);
        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis, lambdaDeclarationToSymbolMap);
        translationMap.setFieldMappings(outputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
            root = new JoinNode(idAllocator.getNextId(),
                    JoinNodeUtils.typeConvert(node.getType()),
                    leftPlanBuilder.getRoot(),
                    rightPlanBuilder.getRoot(),
                    equiClauses.build(),
                    ImmutableList.<Symbol>builder()
                            .addAll(leftPlanBuilder.getRoot().getOutputSymbols())
                            .addAll(rightPlanBuilder.getRoot().getOutputSymbols())
                            .build(),
                    Optional.of(castToRowExpression(rewrittenFilterCondition)),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        if (node.getType() == INNER) {
            // rewrite all the other conditions using output symbols from left + right plan node.
            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root, analysis.getParameters());
            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

            for (Expression expression : complexJoinExpressions) {
                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
            }
            root = rootPlanBuilder.getRoot();

            Expression postInnerJoinCriteria;
            if (!postInnerJoinConditions.isEmpty()) {
                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
                root = new FilterNode(idAllocator.getNextId(), root, castToRowExpression(postInnerJoinCriteria));
            }
        }

        return new RelationPlan(root, analysis.getScope(node), outputSymbols);
    }

    private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right)
    {
        /* Given: l JOIN r USING (k1, ..., kn)

           produces:

            - project
                    coalesce(l.k1, r.k1)
                    ...,
                    coalesce(l.kn, r.kn)
                    l.v1,
                    ...,
                    l.vn,
                    r.v1,
                    ...,
                    r.vn
              - join (l.k1 = r.k1 and ... l.kn = r.kn)
                    - project
                        cast(l.k1 as commonType(l.k1, r.k1))
                        ...
                    - project
                        cast(rl.k1 as commonType(l.k1, r.k1))

            If casts are redundant (due to column type and common type being equal),
            they will be removed by optimization passes.
        */

        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().get()).getColumns();

        Analysis.JoinUsingAnalysis joinAnalysis = analysis.getJoinUsing(node);

        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();

        Map<Identifier, Symbol> leftJoinColumns = new HashMap<>();
        Map<Identifier, Symbol> rightJoinColumns = new HashMap<>();

        Assignments.Builder leftCoercions = Assignments.builder();
        Assignments.Builder rightCoercions = Assignments.builder();

        leftCoercions.putAll(AssignmentUtils.identityAsSymbolReferences(left.getRoot().getOutputSymbols()));
        rightCoercions.putAll(AssignmentUtils.identityAsSymbolReferences(right.getRoot().getOutputSymbols()));
        for (int i = 0; i < joinColumns.size(); i++) {
            Identifier identifier = joinColumns.get(i);
            Type type = analysis.getType(identifier);

            // compute the coercion for the field on the left to the common supertype of left & right
            Symbol leftOutput = planSymbolAllocator.newSymbol(identifier, type);
            int leftField = joinAnalysis.getLeftJoinFields().get(i);
            leftCoercions.put(leftOutput, castToRowExpression(new Cast(
                    toSymbolReference(left.getSymbol(leftField)),
                    type.getTypeSignature().toString(),
                    false,
                    typeCoercion.isTypeOnlyCoercion(left.getDescriptor().getFieldByIndex(leftField).getType(), type))));
            leftJoinColumns.put(identifier, leftOutput);

            // compute the coercion for the field on the right to the common supertype of left & right
            Symbol rightOutput = planSymbolAllocator.newSymbol(identifier, type);
            int rightField = joinAnalysis.getRightJoinFields().get(i);
            rightCoercions.put(rightOutput, castToRowExpression(new Cast(
                    toSymbolReference(right.getSymbol(rightField)),
                    type.getTypeSignature().toString(),
                    false,
                    typeCoercion.isTypeOnlyCoercion(right.getDescriptor().getFieldByIndex(rightField).getType(), type))));
            rightJoinColumns.put(identifier, rightOutput);

            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
        }

        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());

        JoinNode join = new JoinNode(
                idAllocator.getNextId(),
                JoinNodeUtils.typeConvert(node.getType()),
                leftCoercion,
                rightCoercion,
                clauses.build(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftCoercion.getOutputSymbols())
                        .addAll(rightCoercion.getOutputSymbols())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        // Add a projection to produce the outputs of the columns in the USING clause,
        // which are defined as coalesce(l.k, r.k)
        Assignments.Builder assignments = Assignments.builder();

        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        for (Identifier column : joinColumns) {
            Symbol output = planSymbolAllocator.newSymbol(column, analysis.getType(column));
            outputs.add(output);
            assignments.put(output, castToRowExpression(new CoalesceExpression(
                    toSymbolReference(leftJoinColumns.get(column)),
                    toSymbolReference(rightJoinColumns.get(column)))));
        }

        for (int field : joinAnalysis.getOtherLeftFields()) {
            Symbol symbol = left.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.put(symbol, castToRowExpression(toSymbolReference(symbol)));
        }

        for (int field : joinAnalysis.getOtherRightFields()) {
            Symbol symbol = right.getFieldMappings().get(field);
            outputs.add(symbol);
            assignments.put(symbol, castToRowExpression(toSymbolReference(symbol)));
        }

        return new RelationPlan(
                new ProjectNode(idAllocator.getNextId(), join, assignments.build()),
                analysis.getScope(node),
                outputs.build());
    }

    private Optional<Unnest> getUnnest(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getUnnest(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Unnest) {
            return Optional.of((Unnest) relation);
        }
        return Optional.empty();
    }

    private Optional<Lateral> getLateral(Relation relation)
    {
        if (relation instanceof AliasedRelation) {
            return getLateral(((AliasedRelation) relation).getRelation());
        }
        if (relation instanceof Lateral) {
            return Optional.of((Lateral) relation);
        }
        return Optional.empty();
    }

    private RelationPlan planLateralJoin(Join join, RelationPlan leftPlan, Lateral lateral)
    {
        RelationPlan rightPlan = process(lateral.getQuery(), null);
        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

        Expression filterExpression;
        if (!join.getCriteria().isPresent()) {
            filterExpression = TRUE_LITERAL;
        }
        else {
            JoinCriteria criteria = join.getCriteria().get();
            if (criteria instanceof JoinUsing || criteria instanceof NaturalJoin) {
                throw notSupportedException(join, "Lateral join with criteria other than ON");
            }
            filterExpression = (Expression) getOnlyElement(criteria.getNodes());
        }

        List<Symbol> rewriterOutputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getFieldMappings())
                .addAll(rightPlan.getFieldMappings())
                .build();

        // this node is not used in the plan. It is only used for creating the TranslationMap.
        PlanNode dummy = new ValuesNode(
                idAllocator.getNextId(),
                ImmutableList.<Symbol>builder()
                        .addAll(leftPlanBuilder.getRoot().getOutputSymbols())
                        .addAll(rightPlanBuilder.getRoot().getOutputSymbols())
                        .build(),
                ImmutableList.of());

        RelationPlan intermediateRelationPlan = new RelationPlan(dummy, analysis.getScope(join), rewriterOutputSymbols);
        TranslationMap translationMap = new TranslationMap(intermediateRelationPlan, analysis, lambdaDeclarationToSymbolMap);
        translationMap.setFieldMappings(rewriterOutputSymbols);
        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

        Expression rewrittenFilterCondition = translationMap.rewrite(filterExpression);

        PlanBuilder planBuilder = subqueryPlanner.appendLateralJoin(
                leftPlanBuilder,
                rightPlanBuilder,
                lateral.getQuery(),
                true,
                LateralJoinNode.Type.typeConvert(join.getType()),
                rewrittenFilterCondition);

        List<Symbol> outputSymbols = ImmutableList.<Symbol>builder()
                .addAll(leftPlan.getRoot().getOutputSymbols())
                .addAll(rightPlan.getRoot().getOutputSymbols())
                .build();
        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputSymbols);
    }

    private static boolean isEqualComparisonExpression(Expression conjunct)
    {
        return conjunct instanceof ComparisonExpression && ((ComparisonExpression) conjunct).getOperator() == ComparisonExpression.Operator.EQUAL;
    }

    private RelationPlan planCrossJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
    {
        RelationType unnestOutputDescriptor = analysis.getOutputDescriptor(node);
        // Create symbols for the result of unnesting
        ImmutableList.Builder<Symbol> unnestedSymbolsBuilder = ImmutableList.builder();
        for (Field field : unnestOutputDescriptor.getVisibleFields()) {
            Symbol symbol = planSymbolAllocator.newSymbol(field);
            unnestedSymbolsBuilder.add(symbol);
        }
        ImmutableList<Symbol> unnestedSymbols = unnestedSymbolsBuilder.build();

        // Add a projection for all the unnest arguments
        PlanBuilder planBuilder = initializePlanBuilder(leftPlan);
        planBuilder = planBuilder.appendProjections(node.getExpressions(), planSymbolAllocator, idAllocator);
        TranslationMap translations = planBuilder.getTranslations();
        ProjectNode projectNode = (ProjectNode) planBuilder.getRoot();

        ImmutableMap.Builder<Symbol, List<Symbol>> unnestSymbols = ImmutableMap.builder();
        UnmodifiableIterator<Symbol> unnestedSymbolsIterator = unnestedSymbols.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            Symbol inputSymbol = translations.get(expression);
            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (elementType instanceof RowType) {
                    ImmutableList.Builder<Symbol> unnestSymbolBuilder = ImmutableList.builder();
                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
                        unnestSymbolBuilder.add(unnestedSymbolsIterator.next());
                    }
                    unnestSymbols.put(inputSymbol, unnestSymbolBuilder.build());
                }
                else {
                    unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next()));
                }
            }
            else if (type instanceof MapType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next(), unnestedSymbolsIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<Symbol> ordinalitySymbol = node.isWithOrdinality() ? Optional.of(unnestedSymbolsIterator.next()) : Optional.empty();
        checkState(!unnestedSymbolsIterator.hasNext(), "Not all output symbols were matched with input symbols");

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), projectNode, leftPlan.getFieldMappings(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, analysis.getScope(joinNode), unnestNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitTableSubquery(TableSubquery node, Void context)
    {
        return process(node.getQuery(), context);
    }

    @Override
    protected RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, planSymbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session, namedSubPlan, uniqueIdAllocator)
                .plan(node);
    }

    @Override
    protected RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, planSymbolAllocator, idAllocator, lambdaDeclarationToSymbolMap, metadata, session, namedSubPlan, uniqueIdAllocator)
                .plan(node);
    }

    @Override
    protected RelationPlan visitValues(Values node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            Symbol symbol = planSymbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }

        ImmutableList.Builder<List<RowExpression>> rows = ImmutableList.builder();
        for (Expression row : node.getRows()) {
            ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
            if (row instanceof Row) {
                for (Expression item : ((Row) row).getItems()) {
                    Expression expression = Coercer.addCoercions(item, analysis);
                    values.add(castToRowExpression(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), expression)));
                }
            }
            else {
                Expression expression = Coercer.addCoercions(row, analysis);
                values.add(castToRowExpression(ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), expression)));
            }

            rows.add(values.build());
        }

        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputSymbolsBuilder.build(), rows.build());
        return new RelationPlan(valuesNode, scope, outputSymbolsBuilder.build());
    }

    @Override
    protected RelationPlan visitUnnest(Unnest node, Void context)
    {
        Scope scope = analysis.getScope(node);
        ImmutableList.Builder<Symbol> outputSymbolsBuilder = ImmutableList.builder();
        for (Field field : scope.getRelationType().getVisibleFields()) {
            Symbol symbol = planSymbolAllocator.newSymbol(field);
            outputSymbolsBuilder.add(symbol);
        }
        List<Symbol> unnestedSymbols = outputSymbolsBuilder.build();

        // If we got here, then we must be unnesting a constant, and not be in a join (where there could be column references)
        ImmutableList.Builder<Symbol> argumentSymbols = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
        ImmutableMap.Builder<Symbol, List<Symbol>> unnestSymbols = ImmutableMap.builder();
        Iterator<Symbol> unnestedSymbolsIterator = unnestedSymbols.iterator();
        for (Expression expression : node.getExpressions()) {
            Type type = analysis.getType(expression);
            Expression rewritten = Coercer.addCoercions(expression, analysis);
            rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), rewritten);
            values.add(castToRowExpression(rewritten));
            Symbol inputSymbol = planSymbolAllocator.newSymbol(rewritten, type);
            argumentSymbols.add(inputSymbol);
            if (type instanceof ArrayType) {
                Type elementType = ((ArrayType) type).getElementType();
                if (elementType instanceof RowType) {
                    ImmutableList.Builder<Symbol> unnestSymbolBuilder = ImmutableList.builder();
                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
                        unnestSymbolBuilder.add(unnestedSymbolsIterator.next());
                    }
                    unnestSymbols.put(inputSymbol, unnestSymbolBuilder.build());
                }
                else {
                    unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next()));
                }
            }
            else if (type instanceof MapType) {
                unnestSymbols.put(inputSymbol, ImmutableList.of(unnestedSymbolsIterator.next(), unnestedSymbolsIterator.next()));
            }
            else {
                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
            }
        }
        Optional<Symbol> ordinalitySymbol = node.isWithOrdinality() ? Optional.of(unnestedSymbolsIterator.next()) : Optional.empty();
        checkState(!unnestedSymbolsIterator.hasNext(), "Not all output symbols were matched with input symbols");
        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), argumentSymbols.build(), ImmutableList.of(values.build()));

        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), valuesNode, ImmutableList.of(), unnestSymbols.build(), ordinalitySymbol);
        return new RelationPlan(unnestNode, scope, unnestedSymbols);
    }

    private RelationPlan processAndCoerceIfNecessary(Relation node, Void context)
    {
        Type[] coerceToTypes = analysis.getRelationCoercion(node);

        RelationPlan plan = this.process(node, context);

        if (coerceToTypes == null) {
            return plan;
        }

        return addCoercions(plan, coerceToTypes);
    }

    private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes)
    {
        List<Symbol> oldSymbols = plan.getFieldMappings();
        RelationType oldDescriptor = plan.getDescriptor().withOnlyVisibleFields();
        verify(targetColumnTypes.length == oldSymbols.size());
        ImmutableList.Builder<Symbol> newSymbols = new ImmutableList.Builder<>();
        Field[] newFields = new Field[targetColumnTypes.length];
        Assignments.Builder assignments = Assignments.builder();
        for (int i = 0; i < targetColumnTypes.length; i++) {
            Symbol inputSymbol = oldSymbols.get(i);
            Type inputType = planSymbolAllocator.getTypes().get(inputSymbol);
            Type outputType = targetColumnTypes[i];
            if (!outputType.equals(inputType)) {
                Expression cast = new Cast(toSymbolReference(inputSymbol), outputType.getTypeSignature().toString());
                Symbol outputSymbol = planSymbolAllocator.newSymbol(cast, outputType);
                assignments.put(outputSymbol, castToRowExpression(cast));
                newSymbols.add(outputSymbol);
            }
            else {
                SymbolReference symbolReference = toSymbolReference(inputSymbol);
                Symbol outputSymbol = planSymbolAllocator.newSymbol(symbolReference, outputType);
                assignments.put(outputSymbol, castToRowExpression(symbolReference));
                newSymbols.add(outputSymbol);
            }
            Field oldField = oldDescriptor.getFieldByIndex(i);
            newFields[i] = new Field(
                    oldField.getRelationAlias(),
                    oldField.getName(),
                    targetColumnTypes[i],
                    oldField.isHidden(),
                    oldField.getOriginTable(),
                    oldField.getOriginColumnName(),
                    oldField.isAliased());
        }
        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
        return new RelationPlan(projectNode, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields)).build(), newSymbols.build());
    }

    @Override
    protected RelationPlan visitUnion(Union node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        if (node.isDistinct()) {
            planNode = distinct(planNode);
        }
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitIntersect(Intersect node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    @Override
    protected RelationPlan visitExcept(Except node, Void context)
    {
        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

        SetOperationPlan setOperationPlan = process(node);

        PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getSymbolMapping(), ImmutableList.copyOf(setOperationPlan.getSymbolMapping().keySet()));
        return new RelationPlan(planNode, analysis.getScope(node), planNode.getOutputSymbols());
    }

    private SetOperationPlan process(SetOperation node)
    {
        List<Symbol> outputs = null;
        ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
        ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.builder();

        List<RelationPlan> subPlans = node.getRelations().stream()
                .map(relation -> processAndCoerceIfNecessary(relation, null))
                .collect(toImmutableList());

        for (RelationPlan relationPlan : subPlans) {
            List<Symbol> childOutputSymbols = relationPlan.getFieldMappings();
            if (outputs == null) {
                // Use the first Relation to derive output symbol names
                RelationType descriptor = relationPlan.getDescriptor();
                ImmutableList.Builder<Symbol> outputSymbolBuilder = ImmutableList.builder();
                for (Field field : descriptor.getVisibleFields()) {
                    int fieldIndex = descriptor.indexOf(field);
                    Symbol symbol = childOutputSymbols.get(fieldIndex);
                    outputSymbolBuilder.add(planSymbolAllocator.newSymbol(symbol.getName(), planSymbolAllocator.getTypes().get(symbol)));
                }
                outputs = outputSymbolBuilder.build();
            }

            RelationType descriptor = relationPlan.getDescriptor();
            checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
                    "Expected relation to have %s symbols but has %s symbols",
                    descriptor.getVisibleFieldCount(),
                    outputs.size());

            int fieldId = 0;
            for (Field field : descriptor.getVisibleFields()) {
                int fieldIndex = descriptor.indexOf(field);
                symbolMapping.put(outputs.get(fieldId), childOutputSymbols.get(fieldIndex));
                fieldId++;
            }

            sources.add(relationPlan.getRoot());
        }

        return new SetOperationPlan(sources.build(), symbolMapping.build());
    }

    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
    {
        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToSymbolMap);

        // Make field->symbol mapping from underlying relation plan available for translations
        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
        translations.setFieldMappings(relationPlan.getFieldMappings());

        return new PlanBuilder(translations, relationPlan.getRoot(), analysis.getParameters());
    }

    private PlanNode distinct(PlanNode node)
    {
        return new AggregationNode(idAllocator.getNextId(),
                node,
                ImmutableMap.of(),
                singleGroupingSet(node.getOutputSymbols()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty());
    }

    private static class SetOperationPlan
    {
        private final List<PlanNode> sources;
        private final ListMultimap<Symbol, Symbol> symbolMapping;

        private SetOperationPlan(List<PlanNode> sources, ListMultimap<Symbol, Symbol> symbolMapping)
        {
            this.sources = sources;
            this.symbolMapping = symbolMapping;
        }

        public List<PlanNode> getSources()
        {
            return sources;
        }

        public ListMultimap<Symbol, Symbol> getSymbolMapping()
        {
            return symbolMapping;
        }
    }
}
