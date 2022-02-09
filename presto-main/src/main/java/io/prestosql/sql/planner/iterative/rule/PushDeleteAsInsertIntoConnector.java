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
import io.prestosql.Session;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.ExpressionUtils;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.SimplePlanRewriter;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.relational.FunctionResolution;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.tree.ComparisonExpression;
import io.prestosql.sql.tree.Expression;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.DELETE_TRANSACTIONAL_TABLE_DIRECT;
import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.matching.Pattern.typeOf;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.Patterns.TableWriter.target;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.planner.plan.Patterns.tableFinish;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.planner.plan.Patterns.tableWriterNode;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

/**
 * This rule pushes the DeleteAsInsertInto plans of entire partition/table
 * into connectors if enabled.
 */
public class PushDeleteAsInsertIntoConnector
        implements Rule<TableFinishNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Capture<FilterNode> FILTER = newCapture();
    private static final Capture<TableWriterNode> WRITER_NODE = newCapture();
    private static final Pattern<TableFinishNode> PATTERN =
            tableFinish().with(source().matching(
                    tableWriterNode()
                            .with(target().matching(typeOf(TableWriterNode.DeleteAsInsertReference.class)))
                            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)))));
    private static final Pattern<TableFinishNode> WITH_PARTITION_FILTER =
            tableFinish().with(source().matching(
                    tableWriterNode()
                            .capturedAs(WRITER_NODE)
                            .with(target().matching(typeOf(TableWriterNode.DeleteAsInsertReference.class)))
                            .with(source().matching(project()
                                    .with(source().matching(
                                            filter().capturedAs(FILTER)))))));

    private final Metadata metadata;
    private final boolean withFilter;
    private final LogicalRowExpressions logicalRowExpressions;

    public PushDeleteAsInsertIntoConnector(Metadata metadata, boolean withFilter)
    {
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.withFilter = withFilter;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return session.getSystemProperty(DELETE_TRANSACTIONAL_TABLE_DIRECT, Boolean.class);
    }

    @Override
    public Pattern<TableFinishNode> getPattern()
    {
        if (withFilter) {
            return WITH_PARTITION_FILTER;
        }
        return PATTERN;
    }

    @Override
    public Result apply(TableFinishNode node, Captures captures, Context context)
    {
        if (!withFilter) {
            TableScanNode tableScan = captures.get(TABLE_SCAN);
            return metadata.applyDelete(context.getSession(), tableScan.getTable())
                    .map(newHandle -> new TableDeleteNode(
                            context.getIdAllocator().getNextId(),
                            newHandle,
                            getOnlyElement(node.getOutputSymbols())))
                    .map(Result::ofPlanNode)
                    .orElseGet(Result::empty);
        }

        TableWriterNode writerNode = captures.get(WRITER_NODE);
        TableWriterNode.DeleteAsInsertReference deleteTargetRef = (TableWriterNode.DeleteAsInsertReference) writerNode.getTarget();
        if (!deleteTargetRef.getConstraint().isPresent()) {
            //Not expected to reach here.
            return Result.empty();
        }
        Expression predicate = deleteTargetRef.getConstraint().get();
        Expression filtered = ExpressionUtils.filterDeterministicConjuncts(predicate);
        if (!predicate.equals(filtered)) {
            //There were some non-deterministic filters.. so cannot directly delete
            return Result.empty();
        }
        Set<Symbol> allPredicateSymbols = SymbolsExtractor.extractUnique(predicate);
        Map<Symbol, ColumnHandle> columnAssignments = deleteTargetRef.getColumnAssignments();
        Set<Symbol> allColumns = columnAssignments.keySet();
        List<Symbol> predicateColumnSymbols = allPredicateSymbols.stream().filter(allColumns::contains).distinct().collect(Collectors.toList());
        //If all predicate symbols are partitionColumns, then only partition can be deleted directly.
        if (predicateColumnSymbols.isEmpty() || !predicateColumnSymbols.stream().allMatch(
                symbol ->
                {
                    ColumnHandle columnHandle = columnAssignments.get(symbol);
                    return columnHandle != null && columnHandle.isPartitionKey();
                })) {
            return Result.empty();
        }
        FilterNode filterNode = captures.get(FILTER);
        List<Symbol> nonTableSymbols = allPredicateSymbols.stream().filter(symbol -> !allColumns.contains(symbol)).collect(Collectors.toList());
        PredicateContext predicateContext = new PredicateContext();
        PlanNode rewrittenSource = SimplePlanRewriter.rewriteWith(new ReWriter(columnAssignments.keySet(), nonTableSymbols, context.getLookup(), logicalRowExpressions), filterNode, predicateContext);
        /**
         * Create the TableDeleteNode with source to evaluate the predicate subqueries
         */
        TableDeleteNode tableDeleteNode = new TableDeleteNode(context.getIdAllocator().getNextId(),
                rewrittenSource,
                Optional.of(predicateContext.tablePredicate),
                deleteTargetRef.getHandle(),
                deleteTargetRef.getColumnAssignments(),
                getOnlyElement(node.getOutputSymbols()));

        return Result.ofPlanNode(tableDeleteNode);
    }

    private class PredicateContext
    {
        RowExpression tablePredicate = LogicalRowExpressions.TRUE_CONSTANT;
    }

    /**
     * Class to remove the TableScanNode, which have same output symbols present in TableWriterNode
     * and all other projections and filters are removed as well.
     */
    private class ReWriter
            extends SimplePlanRewriter<PredicateContext>
    {
        private final Set<Symbol> tableSymbols;
        private final Set<Symbol> nonTableSymbols;
        private final Lookup lookup;
        private final LogicalRowExpressions logicalRowExpressions;

        ReWriter(Set<Symbol> tableSymbols, List<Symbol> nonTableSymbols, Lookup lookup, LogicalRowExpressions logicalRowExpressions)
        {
            this.logicalRowExpressions = requireNonNull(logicalRowExpressions, "logicalRowExpressions is null");
            this.tableSymbols = tableSymbols;
            this.nonTableSymbols = new HashSet<>(nonTableSymbols);
            this.lookup = lookup;
        }

        @Override
        public PlanNode visitGroupReference(GroupReference node, RewriteContext<PredicateContext> context)
        {
            PlanNode resolvedNode = lookup.resolve(node);
            return context.rewrite(resolvedNode, context.get());
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<PredicateContext> context)
        {
            PlanNode rewritten = context.rewrite(lookup.resolve(node.getSource()), context.get());
            Assignments assignments = node.getAssignments();
            if (!rewritten.getOutputSymbols().equals(node.getSource().getOutputSymbols())) {
                //Filter all assignments which are no longer available in source.
                Assignments.Builder builder = Assignments.builder();
                builder.putAll(assignments.filter(rewritten.getOutputSymbols()));
                //Project out symbols which are part of predicate
                List<Symbol> finalNonTableSymbols = this.nonTableSymbols.stream().filter(rewritten.getOutputSymbols()::contains).collect(Collectors.toList());
                for (Symbol symbol : finalNonTableSymbols) {
                    builder.put(symbol, castToRowExpression(toSymbolReference(symbol)));
                }
                assignments = builder.build();
            }
            return new ProjectNode(node.getId(), rewritten, assignments);
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<PredicateContext> context)
        {
            RowExpression predicate = node.getPredicate();
            Set<Symbol> allSymbols = SymbolsExtractor.extractUnique(predicate);
            allSymbols.stream().filter(symbol -> !tableSymbols.contains(symbol)).forEach(nonTableSymbols::add);

            PlanNode reWritten = context.rewrite(lookup.resolve(node.getSource()), context.get());
            List<Symbol> reWrittenOutputSymbols = reWritten.getOutputSymbols();
            if (!reWrittenOutputSymbols.equals(node.getSource().getOutputSymbols())) {
                /**
                 * extract the conjuncts of the filter predicate and remove the conjuncts
                 * which contains symbols which are no longer part of rewritten output symbols.
                 */
                List<RowExpression> expressions = LogicalRowExpressions.extractConjuncts(predicate);
                List<RowExpression> reWrittenExpressions = expressions.stream()
                        .filter(expression -> reWrittenOutputSymbols.containsAll(SymbolsExtractor.extractUnique(expression)))
                        .collect(Collectors.toList());
                List<RowExpression> tablePredicates = expressions.stream()
                        .filter(expression -> ImmutableList.<Symbol>builder()
                                .addAll(tableSymbols)
                                .addAll(nonTableSymbols)
                                .build().containsAll(SymbolsExtractor.extractUnique(expression)))
                        .collect(Collectors.toList());
                if (!tablePredicates.isEmpty()) {
                    context.get().tablePredicate = logicalRowExpressions.combineConjuncts(context.get().tablePredicate,
                            logicalRowExpressions.combineConjuncts(tablePredicates));
                }
                if (reWrittenExpressions.isEmpty()) {
                    return reWritten;
                }
                predicate = logicalRowExpressions.combineConjuncts(reWrittenExpressions);
            }
            return new FilterNode(node.getId(), reWritten, predicate);
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<PredicateContext> context)
        {
            PlanNode rewrittenLeft = context.rewrite(lookup.resolve(node.getLeft()), context.get());
            PlanNode rewrittenRight = context.rewrite(lookup.resolve(node.getRight()), context.get());
            if (tableSymbols.containsAll(rewrittenLeft.getOutputSymbols())) {
                //All of the left side is from table scan of the deleting table, which needs to be removed from plan.
                return rewrittenRight;
            }
            else if (tableSymbols.containsAll(rewrittenRight.getOutputSymbols())) {
                //All of the Right side is from table scan of the deleting table, which needs to be removed from plan.
                return rewrittenRight;
            }
            //Filter all other parameters for Join.
            ImmutableList<Symbol> rewrittenOutputSymbols = ImmutableList.<Symbol>builder()
                    .addAll(rewrittenLeft.getOutputSymbols())
                    .addAll(rewrittenRight.getOutputSymbols()).build();
            List<JoinNode.EquiJoinClause> criteria = node.getCriteria().stream()
                    .filter(equiJoinClause -> {
                        Set<Symbol> symbols = SymbolsExtractor.extractUnique(
                                new ComparisonExpression(ComparisonExpression.Operator.EQUAL,
                                        toSymbolReference(equiJoinClause.getLeft()),
                                        toSymbolReference(equiJoinClause.getRight())));
                        return rewrittenOutputSymbols.containsAll(symbols);
                    }).collect(Collectors.toList());
            Optional<RowExpression> rewrittenFilter = node.getFilter().map(filter -> {
                List<RowExpression> expressions = LogicalRowExpressions.extractConjuncts(filter);
                List<RowExpression> filteredExpressions = expressions.stream().filter(expression -> {
                    Set<Symbol> symbols = SymbolsExtractor.extractUnique(expression);
                    return rewrittenOutputSymbols.containsAll(symbols);
                }).collect(Collectors.toList());
                return logicalRowExpressions.combineConjuncts(filteredExpressions);
            });
            Optional<Symbol> leftHashSymbol = Optional.empty();
            if (leftHashSymbol.isPresent() && rewrittenOutputSymbols.contains(leftHashSymbol.get())) {
                leftHashSymbol = node.getLeftHashSymbol();
            }
            Optional<Symbol> rightHashSymbol = Optional.empty();
            if (rightHashSymbol.isPresent() && rewrittenOutputSymbols.contains(rightHashSymbol.get())) {
                rightHashSymbol = node.getRightHashSymbol();
            }
            Map<String, Symbol> dynamicFilters = node.getDynamicFilters().entrySet().stream()
                    .filter(entry -> rewrittenOutputSymbols.contains(entry.getValue()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            return new JoinNode(node.getId(),
                    node.getType(),
                    rewrittenLeft,
                    rewrittenRight,
                    criteria,
                    rewrittenOutputSymbols,
                    rewrittenFilter,
                    leftHashSymbol,
                    rightHashSymbol,
                    node.getDistributionType(),
                    node.isSpillable(),
                    dynamicFilters);
        }
    }
}
