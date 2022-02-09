/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.StandardFunctionResolution;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.CTEScanNode;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.GenericLiteral;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.TRANSFORM_SELF_JOIN_TO_GROUPBY;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.empty;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.sql.planner.plan.Patterns.Apply.correlation;
import static io.prestosql.sql.planner.plan.Patterns.Apply.subQuery;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static java.util.Objects.requireNonNull;

/**
 * Transforms the SelfJoin resulting in duplicate rows used for IN predicate to aggregation.
 * For IN predicate, duplicate rows does not have any value. It will be overhead.
 * <p>
 * Ex: TPCDS Q95: following CTE is used only in IN predicates for only one column comparison ({@code ws_order_number}).
 * This results in exponential increase in Joined rows with too many duplicate rows.
 * <pre>
 * WITH ws_wh AS
 * (
 *        SELECT ws1.ws_order_number,
 *               ws1.ws_warehouse_sk wh1,
 *               ws2.ws_warehouse_sk wh2
 *        FROM   web_sales ws1,
 *               web_sales ws2
 *        WHERE  ws1.ws_order_number = ws2.ws_order_number
 *        AND    ws1.ws_warehouse_sk <> ws2.ws_warehouse_sk)
 * </pre>
 * <p>
 * Could be optimized as below:
 * <pre>
 * WITH ws_wh AS
 *     (SELECT ws_order_number
 *       FROM  web_sales
 *       GROUP BY ws_order_number
 *       HAVING COUNT(DISTINCT ws_warehouse_sk) > 1)
 * </pre>
 * Optimized CTE scans table only once and results in unique rows.
 */
public class TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate
        implements Rule<ApplyNode>
{
    private static final Capture<ProjectNode> PROJECT_NODE = newCapture();
    private static final Pattern<ProjectNode> PROJECT_NODE_PATTERN = project()
            .capturedAs(PROJECT_NODE);
    private static final Pattern<ApplyNode> PATTERN = applyNode()
            .with(empty(correlation()))
            .with(subQuery().matching(PROJECT_NODE_PATTERN));
    private final StandardFunctionResolution functionResolution;

    public TransformUnCorrelatedInPredicateSubQuerySelfJoinToAggregate(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager());
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return session.getSystemProperty(TRANSFORM_SELF_JOIN_TO_GROUPBY, Boolean.class);
    }

    @Override
    public Result apply(ApplyNode node, Captures captures, Context context)
    {
        if (node.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        //Only in case of IN predicate this optimization makes sense.
        Expression expression = OriginalExpressionUtils.castToExpression(getOnlyElement(node.getSubqueryAssignments().getExpressions()));
        if (!(expression instanceof InPredicate)) {
            return Result.empty();
        }
        ProjectNode projectNode = captures.get(PROJECT_NODE);
        Optional<ProjectNode> transformed = transformProjectNode(context, projectNode);
        if (transformed.isPresent()) {
            return Result.ofPlanNode(new ApplyNode(node.getId(),
                    node.getInput(),
                    transformed.get(),
                    node.getSubqueryAssignments(),
                    node.getCorrelation(),
                    node.getOriginSubquery()));
        }
        return Result.empty();
    }

    private PlanNode getChildFilterNode(Context context, PlanNode node)
    {
        PlanNode finalNode = context.getLookup().resolve(node);
        if (finalNode instanceof ProjectNode) {
            return getChildFilterNode(context, ((ProjectNode) finalNode).getSource());
        }
        return finalNode;
    }

    private Optional<ProjectNode> transformProjectNode(Context context, ProjectNode projectNode)
    {
        //IN predicate requires only one projection
        if (projectNode.getOutputSymbols().size() > 1) {
            return Optional.empty();
        }
        PlanNode source = context.getLookup().resolve(projectNode.getSource());
        if (source instanceof CTEScanNode) {
            source = getChildFilterNode(context, context.getLookup().resolve(((CTEScanNode) source).getSource()));
        }
        if (!(source instanceof FilterNode &&
                context.getLookup().resolve(((FilterNode) source).getSource()) instanceof JoinNode)) {
            return Optional.empty();
        }

        FilterNode filter = (FilterNode) source;
        Expression predicate = OriginalExpressionUtils.castToExpression(filter.getPredicate());
        List<SymbolReference> allPredicateSymbols = new ArrayList<>();
        getAllSymbols(predicate, allPredicateSymbols);
        JoinNode joinNode = (JoinNode) context.getLookup().resolve(((FilterNode) source).getSource());
        if (!isSelfJoin(projectNode, predicate, joinNode, context.getLookup())) {
            //Check next level for Self Join
            PlanNode left = context.getLookup().resolve(joinNode.getLeft());
            boolean changed = false;
            if (left instanceof ProjectNode) {
                Optional<ProjectNode> transformResult = transformProjectNode(context, (ProjectNode) left);
                if (transformResult.isPresent()) {
                    joinNode = new JoinNode(joinNode.getId(), joinNode.getType(), transformResult.get(), joinNode.getRight(), joinNode.getCriteria(),
                            joinNode.getOutputSymbols(), joinNode.getFilter(), joinNode.getLeftHashSymbol(), joinNode.getRightHashSymbol(), joinNode.getDistributionType(),
                            joinNode.isSpillable(), joinNode.getDynamicFilters());
                    changed = true;
                }
            }
            PlanNode right = context.getLookup().resolve(joinNode.getRight());
            if (right instanceof ProjectNode) {
                Optional<ProjectNode> transformResult = transformProjectNode(context, (ProjectNode) right);
                if (transformResult.isPresent()) {
                    joinNode = new JoinNode(joinNode.getId(), joinNode.getType(), joinNode.getLeft(), transformResult.get(), joinNode.getCriteria(),
                            joinNode.getOutputSymbols(), joinNode.getFilter(), joinNode.getLeftHashSymbol(), joinNode.getRightHashSymbol(), joinNode.getDistributionType(),
                            joinNode.isSpillable(), joinNode.getDynamicFilters());
                    changed = true;
                }
            }
            if (changed) {
                FilterNode transformedFilter = new FilterNode(filter.getId(), joinNode, filter.getPredicate());
                ProjectNode transformedProject = new ProjectNode(projectNode.getId(), transformedFilter, projectNode.getAssignments());
                return Optional.of(transformedProject);
            }
            return Optional.empty();
        }

        //Choose the table to use based on projected output.
        TableScanNode leftTable = (TableScanNode) context.getLookup().resolve(joinNode.getLeft());
        TableScanNode rightTable = (TableScanNode) context.getLookup().resolve(joinNode.getRight());
        TableScanNode tableToUse;
        List<RowExpression> aggregationSymbols;
        AggregationNode.GroupingSetDescriptor groupingSetDescriptor;
        Assignments projectionsForCTE = null;
        Assignments projectionsFromFilter = null;
        //Use non-projected column for aggregation
        if (context.getLookup().resolve(projectNode.getSource()) instanceof CTEScanNode) {
            CTEScanNode cteScanNode = (CTEScanNode) context.getLookup().resolve(projectNode.getSource());
            ProjectNode childProjectOfCte = (ProjectNode) context.getLookup().resolve(cteScanNode.getSource());
            List<Symbol> completeOutputSymbols = new ArrayList<>();
            rightTable.getOutputSymbols().forEach((s) -> completeOutputSymbols.add(s));
            leftTable.getOutputSymbols().forEach((s) -> completeOutputSymbols.add(s));
            List<Symbol> outputSymbols = new ArrayList<>();

            for (int i = 0; i < completeOutputSymbols.size(); i++) {
                Symbol outputSymbol = completeOutputSymbols.get(i);
                for (Symbol symbol : projectNode.getOutputSymbols()) {
                    if (childProjectOfCte.getAssignments().getMap().containsKey(symbol)) {
                        if (((SymbolReference) OriginalExpressionUtils.castToExpression(childProjectOfCte.getAssignments().getMap().get(symbol))).getName().equals(outputSymbol.getName())) {
                            outputSymbols.add(outputSymbol);
                        }
                    }
                }
            }
            Map<Symbol, RowExpression> projectionsForCTEMap = new HashMap<>();
            Map<Symbol, RowExpression> projectionsFromFilterMap = new HashMap<>();

            for (Map.Entry entry : childProjectOfCte.getAssignments().getMap().entrySet()) {
                if (entry.getKey().equals(getOnlyElement(projectNode.getOutputSymbols()))) {
                    projectionsForCTEMap.put((Symbol) entry.getKey(), (RowExpression) entry.getValue());
                }
                if (entry.getKey().equals(getOnlyElement(projectNode.getOutputSymbols()))) {
                    projectionsFromFilterMap.put(getOnlyElement(outputSymbols), (RowExpression) entry.getValue());
                }
            }

            projectionsForCTE = new Assignments(projectionsForCTEMap);
            projectionsFromFilter = new Assignments(projectionsFromFilterMap);
            tableToUse = leftTable.getOutputSymbols().contains(getOnlyElement(outputSymbols)) ?
                    leftTable : rightTable;
            aggregationSymbols = allPredicateSymbols.stream()
                    .filter(s -> tableToUse.getOutputSymbols().contains(SymbolUtils.from(s)))
                    .filter(s -> !outputSymbols.contains(SymbolUtils.from(s)))
                    .map(OriginalExpressionUtils::castToRowExpression)
                    .collect(Collectors.toList());
            //Create aggregation
            groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                    ImmutableList.copyOf(outputSymbols), 1, ImmutableSet.of());
        }
        else {
            tableToUse = leftTable.getOutputSymbols().contains(getOnlyElement(projectNode.getOutputSymbols())) ?
                    leftTable : rightTable;
            aggregationSymbols = allPredicateSymbols.stream()
                    .filter(s -> tableToUse.getOutputSymbols().contains(SymbolUtils.from(s)))
                    .filter(s -> !projectNode.getOutputSymbols().contains(SymbolUtils.from(s)))
                    .map(OriginalExpressionUtils::castToRowExpression)
                    .collect(Collectors.toList());
            //Create aggregation
            groupingSetDescriptor = new AggregationNode.GroupingSetDescriptor(
                    ImmutableList.copyOf(projectNode.getOutputSymbols()), 1, ImmutableSet.of());
        }

        AggregationNode.Aggregation aggregation = new AggregationNode.Aggregation(
                new CallExpression(
                        "count",
                        functionResolution.countFunction(),
                        BIGINT,
                        aggregationSymbols),
                aggregationSymbols,
                true, //mark DISTINCT since NOT_EQUALS predicate
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregationsBuilder = ImmutableMap.builder();
        Symbol countSymbol = context.getSymbolAllocator().newSymbol(aggregation.getFunctionCall().getDisplayName(), BIGINT);
        aggregationsBuilder.put(countSymbol, aggregation);

        AggregationNode aggregationNode = new AggregationNode(context.getIdAllocator().getNextId(),
                tableToUse, aggregationsBuilder.build(),
                groupingSetDescriptor,
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                AggregationNode.AggregationType.HASH,
                Optional.empty());

        //Filter rows with count < 1 from aggregation results to match the NOT_EQUALS clause in original query.
        FilterNode filterNode = new FilterNode(context.getIdAllocator().getNextId(),
                aggregationNode,
                OriginalExpressionUtils.castToRowExpression(new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                        SymbolUtils.toSymbolReference(countSymbol),
                        new GenericLiteral("BIGINT", "1"))));
        //Project the aggregated+filtered rows.
        ProjectNode transformedSubquery = new ProjectNode(projectNode.getId(), filterNode, projectNode.getAssignments());
        if (context.getLookup().resolve(projectNode.getSource()) instanceof CTEScanNode) {
            CTEScanNode cteScanNode = (CTEScanNode) context.getLookup().resolve(projectNode.getSource());
            PlanNode projectNodeForCTE = context.getLookup().resolve(cteScanNode.getSource());
            PlanNode projectNodeFromFilter = context.getLookup().resolve(projectNodeForCTE);
            projectNodeFromFilter = new ProjectNode(projectNodeFromFilter.getId(), filterNode, projectionsFromFilter);
            projectNodeForCTE = new ProjectNode(projectNodeForCTE.getId(), projectNodeFromFilter, projectionsForCTE);
            cteScanNode = (CTEScanNode) cteScanNode.replaceChildren(ImmutableList.of(projectNodeForCTE));
            cteScanNode.setOutputSymbols(projectNode.getOutputSymbols());
            transformedSubquery = new ProjectNode(projectNode.getId(), cteScanNode, projectNode.getAssignments());
        }
        return Optional.of(transformedSubquery);
    }

    private static boolean isSelfJoin(ProjectNode projectNode, Expression predicate, JoinNode joinNode, Lookup lookup)
    {
        PlanNode left = lookup.resolve(joinNode.getLeft());
        PlanNode right = lookup.resolve(joinNode.getRight());
        //For current optimization following conditions should match
        // 1. Join should be INNER
        // 2. Both left and right should be on same Table
        // 3. Filtering should have NOT_EQUALS comparison on non-projected column and EQUALS comparison for projected column
        //TODO FIX FOR framenwork changes
        if (joinNode.getType() == JoinNode.Type.INNER &&
                left instanceof TableScanNode &&
                right instanceof TableScanNode &&
                ((TableScanNode) left).getTable().getFullyQualifiedName()
                        .equals(((TableScanNode) right).getTable().getFullyQualifiedName())) {
            if (!(predicate instanceof LogicalBinaryExpression &&
                    ((LogicalBinaryExpression) predicate).getLeft() instanceof ComparisonExpression &&
                    ((LogicalBinaryExpression) predicate).getRight() instanceof ComparisonExpression)) {
                return false;
            }
            SymbolReference projected = SymbolUtils.toSymbolReference(getOnlyElement(projectNode.getOutputSymbols()));
            ComparisonExpression leftPredicate = (ComparisonExpression) ((LogicalBinaryExpression) predicate).getLeft();
            ComparisonExpression rightPredicate = (ComparisonExpression) ((LogicalBinaryExpression) predicate).getRight();
            if (lookup.resolve(projectNode.getSource()) instanceof CTEScanNode) {
                CTEScanNode cteScanNode = (CTEScanNode) lookup.resolve(projectNode.getSource());
                ProjectNode childProjectOfCte = (ProjectNode) lookup.resolve(cteScanNode.getSource());
                RowExpression projectedExpresssion = (childProjectOfCte.getAssignments().get(getOnlyElement(projectNode.getOutputSymbols())));
                for (Symbol symbol : lookup.resolve(childProjectOfCte.getSource()).getOutputSymbols()) {
                    if (symbol.getName().equals(((SymbolReference) OriginalExpressionUtils.castToExpression(projectedExpresssion)).getName())) {
                        projected = SymbolUtils.toSymbolReference(symbol);
                    }
                }
            }
            if (leftPredicate.getChildren().contains(projected) &&
                    leftPredicate.getOperator() == ComparisonExpression.Operator.EQUAL &&
                    rightPredicate.getOperator() == ComparisonExpression.Operator.NOT_EQUAL) {
                return true;
            }
            else if (rightPredicate.getChildren().contains(projected) &&
                    rightPredicate.getOperator() == ComparisonExpression.Operator.EQUAL &&
                    leftPredicate.getOperator() == ComparisonExpression.Operator.NOT_EQUAL) {
                return true;
            }
        }
        return false;
    }

    private static void getAllSymbols(Expression expression, List<SymbolReference> symbols)
    {
        if (expression instanceof LogicalBinaryExpression) {
            LogicalBinaryExpression logicalBinaryExpression = (LogicalBinaryExpression) expression;
            getAllSymbols(logicalBinaryExpression.getLeft(), symbols);
            getAllSymbols(logicalBinaryExpression.getRight(), symbols);
        }
        else if (expression instanceof ComparisonExpression) {
            ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
            getAllSymbols(comparisonExpression.getLeft(), symbols);
            getAllSymbols(comparisonExpression.getRight(), symbols);
        }
        else if (expression instanceof SymbolReference) {
            symbols.add((SymbolReference) expression);
        }
    }
}
