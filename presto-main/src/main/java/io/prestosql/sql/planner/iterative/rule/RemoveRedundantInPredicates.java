/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.FilterNode;
import io.prestosql.sql.planner.plan.JoinNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.planner.plan.TableScanNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.LogicalBinaryExpression;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.sql.ExpressionUtils.combinePredicates;
import static io.prestosql.sql.ExpressionUtils.extractPredicates;
import static io.prestosql.sql.planner.plan.Patterns.Apply.input;
import static io.prestosql.sql.planner.plan.Patterns.Apply.subQuery;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

/**
 * This rule is to remove unnecessary conjunctive IN predicates, in which one is subset of other.
 * So, keeping IN predicate with only subset will be sufficient.
 * <p>
 * Ex: TPCDS Q95: In following predicates, values of first IN is super set of second IN's values
 * as its a result of INNER Join.
 * <pre>
 *     AND  ws1.ws_order_number IN
 *          (
 *                 SELECT ws_order_number
 *                 FROM   ws_wh)
 *     AND  ws1.ws_order_number IN
 *          (
 *                 SELECT wr_order_number
 *                 FROM   web_returns,
 *                        ws_wh
 *                 WHERE  wr_order_number = ws_wh.ws_order_number)
 * </pre>
 */
public class RemoveRedundantInPredicates
        implements Rule<ProjectNode>
{
    private static final Capture<ApplyNode> APPLY_NODE_1 = Capture.newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE_1 = Capture.newCapture();
    private static final Capture<FilterNode> FILTER_NODE_1 = Capture.newCapture();
    private static final Capture<ApplyNode> APPLY_NODE_2 = Capture.newCapture();
    private static final Capture<ProjectNode> PROJECT_NODE_2 = Capture.newCapture();
    private static final Capture<FilterNode> FILTER_NODE_2 = Capture.newCapture();
    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(filter()
                    .with(source().matching(applyNode().capturedAs(APPLY_NODE_1)
                            .with(subQuery().matching(project().capturedAs(PROJECT_NODE_1)
                                    .with(source().matching(filter()
                                            .capturedAs(FILTER_NODE_1)))))
                            .with(input().matching(applyNode().capturedAs(APPLY_NODE_2)
                                    .with(subQuery().matching(project()
                                            .capturedAs(PROJECT_NODE_2)
                                            .with(source().matching(filter()
                                                    .capturedAs(FILTER_NODE_2)))))))))));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode node, Captures captures, Context context)
    {
        FilterNode filterNode = context.getLookup().resolveGroup(node.getSource())
                .map(FilterNode.class::cast).findFirst().get();
        Expression predicate = filterNode.getPredicate();
        List<Expression> conjuncts = ExpressionUtils.extractConjuncts(predicate);
        if (conjuncts.size() <= 1) {
            //No conjucts to optimize
            return Result.empty();
        }
        ApplyNode applyNode1 = captures.get(APPLY_NODE_1);
        ApplyNode applyNode2 = captures.get(APPLY_NODE_2);
        if (applyNode1.getSubqueryAssignments().size() != 1 || applyNode2.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }
        //Only in case of IN predicate this optimization makes sense.
        Assignments assignments1 = applyNode1.getSubqueryAssignments();
        Assignments assignments2 = applyNode2.getSubqueryAssignments();
        SymbolReference output1 = getOnlyElement(assignments1.getOutputs()).toSymbolReference();
        SymbolReference output2 = getOnlyElement(assignments2.getOutputs()).toSymbolReference();
        if (!(conjuncts.contains(output1) && conjuncts.contains(output2))) {
            //Both outputs should be part of conjuncts
            return Result.empty();
        }
        Expression expression1 = getOnlyElement(assignments1.getExpressions());
        Expression expression2 = getOnlyElement(assignments2.getExpressions());
        if (!(expression1 instanceof InPredicate && expression2 instanceof InPredicate)) {
            //Both expressions should be IN predicates
            return Result.empty();
        }
        InPredicate inPredicate1 = (InPredicate) expression1;
        InPredicate inPredicate2 = (InPredicate) expression2;
        if (!inPredicate1.getValue().equals(inPredicate2.getValue())) {
            //both IN predicates should be on same column
            return Result.empty();
        }
        Optional<InPredicate> superPredicate = identifySuperPredicate(inPredicate1, inPredicate2, captures, context);
        if (superPredicate.isPresent()) {
            if (superPredicate.get() == inPredicate1) {
                Expression transformedPredicate = ExpressionTreeRewriter.rewriteWith(new Visitor(output1), predicate);
                FilterNode transformedFilter = new FilterNode(filterNode.getId(),
                        applyNode1.getInput(), //By-passed first IN predicate
                        transformedPredicate);
                ProjectNode transformedProject = new ProjectNode(node.getId(), transformedFilter, node.getAssignments());
                return Result.ofPlanNode(transformedProject);
            }
            if (superPredicate.get() == inPredicate2) {
                ApplyNode transformedApply = new ApplyNode(applyNode1.getId(),
                        applyNode2.getInput(), //by-passed second IN predicate
                        applyNode1.getSubquery(),
                        applyNode1.getSubqueryAssignments(),
                        applyNode1.getCorrelation(),
                        applyNode1.getOriginSubquery());
                Expression transformedPredicate = ExpressionTreeRewriter.rewriteWith(new Visitor(output2), predicate);
                FilterNode transformedFilter = new FilterNode(filterNode.getId(), transformedApply, transformedPredicate);
                ProjectNode transformedProject = new ProjectNode(node.getId(), transformedFilter, node.getAssignments());
                return Result.ofPlanNode(transformedProject);
            }
        }
        return Result.empty();
    }

    private Optional<InPredicate> identifySuperPredicate(InPredicate inPredicate1, InPredicate inPredicate2,
            Captures captures, Context context)
    {
        //Identify the predicate whose results are superset of other predicate.
        //Superset predicate needs to be removed.
        //Idea: Find the unique tables being joined (inner join).
        //The predicate with lesser tables will be super predicate,
        // provided all these tables are joined in other predicate.
        //If such condition doesn't meet, then nothing can be removed.
        FilterNode filterNode1 = captures.get(FILTER_NODE_1);
        FilterNode filterNode2 = captures.get(FILTER_NODE_2);
        PlanNode source1 = context.getLookup().resolveGroup(filterNode1.getSource()).findFirst().get();
        PlanNode source2 = context.getLookup().resolveGroup(filterNode2.getSource()).findFirst().get();
        Set<String> tables1 = new HashSet<>();
        collectUniqueTables(source1, context.getLookup(), tables1);
        Set<String> tables2 = new HashSet<>();
        collectUniqueTables(source2, context.getLookup(), tables2);
        if (tables1.size() > tables2.size() && tables1.containsAll(tables2)) {
            return Optional.of(inPredicate2);
        }
        if (tables2.size() > tables1.size() && tables2.containsAll(tables1)) {
            return Optional.of(inPredicate1);
        }
        return Optional.empty();
    }

    private void collectUniqueTables(PlanNode node, Lookup lookup, Set<String> tables)
    {
        node.getSources().stream().flatMap(lookup::resolveGroup).forEach(n -> {
            if (n instanceof TableScanNode) {
                tables.add(((TableScanNode) n).getTable().getFullyQualifiedName());
            }
            else if ((n instanceof JoinNode && ((JoinNode) n).getType() == JoinNode.Type.INNER) ||
                    n instanceof ProjectNode ||
                    n instanceof FilterNode) {
                collectUniqueTables(n, lookup, tables);
            }
        });
    }

    private static class Visitor
            extends ExpressionRewriter<Void>
    {
        private Expression toRemove;

        Visitor(Expression toRemove)
        {
            this.toRemove = toRemove;
        }

        @Override
        public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            return combinePredicates(node.getOperator(),
                    extractPredicates(node.getOperator(), node)
                            .stream()
                            .filter(e -> !toRemove.equals(e))
                            .collect(Collectors.toList()));
        }
    }
}
