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
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.TableScanNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.SpatialJoinNode;
import io.prestosql.sql.planner.plan.StatisticAggregations;
import io.prestosql.sql.planner.plan.TableDeleteNode;
import io.prestosql.sql.planner.plan.TableFinishNode;
import io.prestosql.sql.planner.plan.TableWriterNode;
import io.prestosql.sql.planner.plan.VacuumTableNode;
import io.prestosql.sql.relational.OriginalExpressionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.builder;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.applyNode;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.spatialJoin;
import static io.prestosql.sql.planner.plan.Patterns.tableDeleteNode;
import static io.prestosql.sql.planner.plan.Patterns.tableFinish;
import static io.prestosql.sql.planner.plan.Patterns.tableScan;
import static io.prestosql.sql.planner.plan.Patterns.tableWriterNode;
import static io.prestosql.sql.planner.plan.Patterns.vacuumTableNode;
import static io.prestosql.sql.planner.plan.Patterns.values;
import static io.prestosql.sql.planner.plan.Patterns.window;
import static java.util.Objects.requireNonNull;

public class RowExpressionRewriteRuleSet
{
    public interface PlanRowExpressionRewriter
    {
        RowExpression rewrite(RowExpression expression, Rule.Context context);
    }

    protected final PlanRowExpressionRewriter rewriter;

    public RowExpressionRewriteRuleSet(PlanRowExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules(Metadata metadata)
    {
        return ImmutableSet.of(
                valueRowExpressionRewriteRule(),
                tableScanRowExpressionRewriteRule(),
                filterRowExpressionRewriteRule(),
                projectRowExpressionRewriteRule(),
                applyNodeRowExpressionRewriteRule(),
                windowRowExpressionRewriteRule(metadata),
                joinRowExpressionRewriteRule(),
                spatialJoinRowExpressionRewriteRule(),
                aggregationRowExpressionRewriteRule(),
                tableFinishRowExpressionRewriteRule(),
                tableWriterRowExpressionRewriteRule(),
                vacuumTableRowExpressionRewriteRule(),
                tableDeleteRowExpressionRewriteRule());
    }

    private Rule<TableDeleteNode> tableDeleteRowExpressionRewriteRule()
    {
        return new TableDeleteRowExpressionRewrite();
    }

    public Rule<ValuesNode> valueRowExpressionRewriteRule()
    {
        return new ValuesRowExpressionRewrite();
    }

    public Rule<FilterNode> filterRowExpressionRewriteRule()
    {
        return new FilterRowExpressionRewrite();
    }

    public Rule<TableScanNode> tableScanRowExpressionRewriteRule()
    {
        return new TableScanRowExpressionRewrite();
    }

    public Rule<ProjectNode> projectRowExpressionRewriteRule()
    {
        return new ProjectRowExpressionRewrite();
    }

    public Rule<ApplyNode> applyNodeRowExpressionRewriteRule()
    {
        return new ApplyRowExpressionRewrite();
    }

    public Rule<WindowNode> windowRowExpressionRewriteRule(Metadata metadata)
    {
        return new WindowRowExpressionRewrite(metadata);
    }

    public Rule<JoinNode> joinRowExpressionRewriteRule()
    {
        return new JoinRowExpressionRewrite();
    }

    public Rule<SpatialJoinNode> spatialJoinRowExpressionRewriteRule()
    {
        return new SpatialJoinRowExpressionRewrite();
    }

    public Rule<TableFinishNode> tableFinishRowExpressionRewriteRule()
    {
        return new TableFinishRowExpressionRewrite();
    }

    public Rule<TableWriterNode> tableWriterRowExpressionRewriteRule()
    {
        return new TableWriterRowExpressionRewrite();
    }

    public Rule<VacuumTableNode> vacuumTableRowExpressionRewriteRule()
    {
        return new VacuumTableRowExpressionRewrite();
    }

    public Rule<AggregationNode> aggregationRowExpressionRewriteRule()
    {
        return new AggregationRowExpressionRewrite();
    }

    private final class ProjectRowExpressionRewrite
            implements Rule<ProjectNode>
    {
        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments.Builder builder = Assignments.builder();
            boolean anyRewritten = false;
            for (Map.Entry<Symbol, RowExpression> entry : projectNode.getAssignments().getMap().entrySet()) {
                RowExpression rewritten = rewriter.rewrite(entry.getValue(), context);
                if (!rewritten.equals(entry.getValue())) {
                    anyRewritten = true;
                }
                builder.put(entry.getKey(), rewritten);
            }
            Assignments assignments = builder.build();
            if (anyRewritten) {
                return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
            }
            return Result.empty();
        }
    }

    private final class SpatialJoinRowExpressionRewrite
            implements Rule<SpatialJoinNode>
    {
        @Override
        public Pattern<SpatialJoinNode> getPattern()
        {
            return spatialJoin();
        }

        @Override
        public Result apply(SpatialJoinNode spatialJoinNode, Captures captures, Context context)
        {
            RowExpression filter = spatialJoinNode.getFilter();
            RowExpression rewritten = rewriter.rewrite(filter, context);

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new SpatialJoinNode(
                    spatialJoinNode.getId(),
                    spatialJoinNode.getType(),
                    spatialJoinNode.getLeft(),
                    spatialJoinNode.getRight(),
                    spatialJoinNode.getOutputSymbols(),
                    rewritten,
                    spatialJoinNode.getLeftPartitionSymbol(),
                    spatialJoinNode.getRightPartitionSymbol(),
                    spatialJoinNode.getKdbTree()));
        }
    }

    private final class JoinRowExpressionRewrite
            implements Rule<JoinNode>
    {
        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            if (!joinNode.getFilter().isPresent()) {
                return Result.empty();
            }

            RowExpression filter = joinNode.getFilter().get();
            RowExpression rewritten = rewriter.rewrite(filter, context);

            if (filter.equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    joinNode.getLeft(),
                    joinNode.getRight(),
                    joinNode.getCriteria(),
                    joinNode.getOutputSymbols(),
                    Optional.of(rewritten),
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType(),
                    joinNode.isSpillable(),
                    joinNode.getDynamicFilters()));
        }
    }

    private final class WindowRowExpressionRewrite
            implements Rule<WindowNode>
    {
        private Metadata metadata;

        public WindowRowExpressionRewrite(Metadata metadata)
        {
            this.metadata = metadata;
        }

        @Override
        public Pattern<WindowNode> getPattern()
        {
            return window();
        }

        @Override
        public Result apply(WindowNode windowNode, Captures captures, Context context)
        {
            checkState(windowNode.getSource() != null);
            boolean anyRewritten = false;
            ImmutableMap.Builder<Symbol, WindowNode.Function> functions = builder();
            for (Map.Entry<Symbol, WindowNode.Function> entry : windowNode.getWindowFunctions().entrySet()) {
                ImmutableList.Builder<RowExpression> newArguments = ImmutableList.builder();
                CallExpression callExpression = new CallExpression(entry.getValue().getSignature(),
                        metadata.getType(entry.getValue().getSignature().getReturnType()),
                        entry.getValue().getArguments(), Optional.empty());
                for (RowExpression argument : callExpression.getArguments()) {
                    RowExpression rewritten = rewriter.rewrite(argument, context);
                    if (rewritten != argument) {
                        anyRewritten = true;
                    }
                    newArguments.add(rewritten);
                }
                functions.put(
                        entry.getKey(),
                        new WindowNode.Function(
                                callExpression.getSignature(),
                                newArguments.build(),
                                entry.getValue().getFrame()));
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new WindowNode(
                        windowNode.getId(),
                        windowNode.getSource(),
                        windowNode.getSpecification(),
                        functions.build(),
                        windowNode.getHashSymbol(),
                        windowNode.getPrePartitionedInputs(),
                        windowNode.getPreSortedOrderPrefix()));
            }
            return Result.empty();
        }
    }

    private final class ApplyRowExpressionRewrite
            implements Rule<ApplyNode>
    {
        @Override
        public Pattern<ApplyNode> getPattern()
        {
            return applyNode();
        }

        @Override
        public Result apply(ApplyNode applyNode, Captures captures, Context context)
        {
            Assignments assignments = applyNode.getSubqueryAssignments();
            Optional<Assignments> rewrittenAssignments = translateAssignments(assignments, context);

            if (!rewrittenAssignments.isPresent()) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ApplyNode(
                    applyNode.getId(),
                    applyNode.getInput(),
                    applyNode.getSubquery(),
                    rewrittenAssignments.get(),
                    applyNode.getCorrelation(),
                    applyNode.getOriginSubquery()));
        }
    }

    private Optional<Assignments> translateAssignments(Assignments assignments, Rule.Context context)
    {
        Assignments.Builder builder = Assignments.builder();
        assignments.getMap()
                .entrySet()
                .stream()
                .forEach(entry -> builder.put(entry.getKey(), rewriter.rewrite(entry.getValue(), context)));
        Assignments rewritten = builder.build();
        if (rewritten.equals(assignments)) {
            return Optional.empty();
        }
        return Optional.of(rewritten);
    }

    private final class FilterRowExpressionRewrite
            implements Rule<FilterNode>
    {
        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            checkState(filterNode.getSource() != null);
            RowExpression rewritten = rewriter.rewrite(filterNode.getPredicate(), context);

            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private final class TableScanRowExpressionRewrite
            implements Rule<TableScanNode>
    {
        @Override
        public Pattern<TableScanNode> getPattern()
        {
            return tableScan();
        }

        @Override
        public Result apply(TableScanNode tableScanNode, Captures captures, Context context)
        {
            checkState(tableScanNode != null);
            if (tableScanNode.getPredicate().isPresent()) {
                RowExpression rewritten = rewriter.rewrite(tableScanNode.getPredicate().get(), context);
                if (!tableScanNode.getPredicate().get().equals(rewritten)) {
                    return Result.ofPlanNode(new TableScanNode(tableScanNode.getId(),
                            tableScanNode.getTable(),
                            tableScanNode.getOutputSymbols(),
                            tableScanNode.getAssignments(),
                            tableScanNode.getEnforcedConstraint(),
                            Optional.of(rewritten),
                            tableScanNode.getStrategy(),
                            tableScanNode.getReuseTableScanMappingId(),
                            tableScanNode.getConsumerTableScanNodeCount(),
                            tableScanNode.isForDelete()));
                }
            }

            return Result.empty();
        }
    }

    private final class ValuesRowExpressionRewrite
            implements Rule<ValuesNode>
    {
        @Override
        public Pattern<ValuesNode> getPattern()
        {
            return values();
        }

        @Override
        public Result apply(ValuesNode valuesNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableList.Builder<List<RowExpression>> rows = ImmutableList.builder();
            for (List<RowExpression> row : valuesNode.getRows()) {
                ImmutableList.Builder<RowExpression> newRow = ImmutableList.builder();
                for (RowExpression rowExpression : row) {
                    RowExpression rewritten = rewriter.rewrite(rowExpression, context);
                    if (!rewritten.equals(rowExpression)) {
                        anyRewritten = true;
                    }
                    newRow.add(rewritten);
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Result.empty();
        }
    }

    private final class AggregationRowExpressionRewrite
            implements Rule<AggregationNode>
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            boolean changed = false;
            ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> rewrittenAggregation = builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : node.getAggregations().entrySet()) {
                Type returnType = context.getSymbolAllocator().getSymbols().get(entry.getKey());
                AggregationNode.Aggregation rewritten = rewriteAggregation(entry.getValue(), returnType, context);
                rewrittenAggregation.put(entry.getKey(), rewritten);
                if (!rewritten.equals(entry.getValue())) {
                    changed = true;
                }
            }

            if (changed) {
                AggregationNode aggregationNode = new AggregationNode(
                        node.getId(),
                        node.getSource(),
                        rewrittenAggregation.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedSymbols(),
                        node.getStep(),
                        node.getHashSymbol(),
                        node.getGroupIdSymbol());
                return Result.ofPlanNode(aggregationNode);
            }
            return Result.empty();
        }
    }

    private final class TableFinishRowExpressionRewrite
            implements Rule<TableFinishNode>
    {
        @Override
        public Pattern<TableFinishNode> getPattern()
        {
            return tableFinish();
        }

        @Override
        public Result apply(TableFinishNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new TableFinishNode(
                        node.getId(),
                        node.getSource(),
                        node.getTarget(),
                        node.getRowCountSymbol(),
                        rewrittenStatisticsAggregation,
                        node.getStatisticsAggregationDescriptor()));
            }
            return Result.empty();
        }
    }

    private Optional<StatisticAggregations> translateStatisticAggregation(StatisticAggregations statisticAggregations, Rule.Context context)
    {
        ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> rewrittenAggregation = builder();
        boolean changed = false;
        for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : statisticAggregations.getAggregations().entrySet()) {
            Type returnType = context.getSymbolAllocator().getSymbols().get(entry.getKey());
            AggregationNode.Aggregation rewritten = rewriteAggregation(entry.getValue(), returnType, context);
            rewrittenAggregation.put(entry.getKey(), rewritten);
            if (!rewritten.equals(entry.getValue())) {
                changed = true;
            }
        }
        if (changed) {
            return Optional.of(new StatisticAggregations(rewrittenAggregation.build(), statisticAggregations.getGroupingSymbols()));
        }
        return Optional.empty();
    }

    private final class TableWriterRowExpressionRewrite
            implements Rule<TableWriterNode>
    {
        @Override
        public Pattern<TableWriterNode> getPattern()
        {
            return tableWriterNode();
        }

        @Override
        public Result apply(TableWriterNode node, Captures captures, Context context)
        {
            checkState(node.getSource() != null);

            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new TableWriterNode(
                        node.getId(),
                        node.getSource(),
                        node.getTarget(),
                        node.getRowCountSymbol(),
                        node.getFragmentSymbol(),
                        node.getColumns(),
                        node.getColumnNames(),
                        node.getPartitioningScheme(),
                        rewrittenStatisticsAggregation,
                        node.getStatisticsAggregationDescriptor()));
            }
            return Result.empty();
        }
    }

    private final class VacuumTableRowExpressionRewrite
            implements Rule<VacuumTableNode>
    {
        @Override
        public Pattern<VacuumTableNode> getPattern()
        {
            return vacuumTableNode();
        }

        @Override
        public Result apply(VacuumTableNode node, Captures captures, Context context)
        {
            if (!node.getStatisticsAggregation().isPresent()) {
                return Result.empty();
            }

            Optional<StatisticAggregations> rewrittenStatisticsAggregation = translateStatisticAggregation(node.getStatisticsAggregation().get(), context);

            if (rewrittenStatisticsAggregation.isPresent()) {
                return Result.ofPlanNode(new VacuumTableNode(
                        node.getId(),
                        node.getTable(),
                        node.getTarget(),
                        node.getRowCountSymbol(),
                        node.getFragmentSymbol(),
                        node.getPartition(),
                        node.isFull(),
                        node.getInputSymbols(),
                        rewrittenStatisticsAggregation,
                        node.getStatisticsAggregationDescriptor()));
            }
            return Result.empty();
        }
    }

    private AggregationNode.Aggregation rewriteAggregation(AggregationNode.Aggregation aggregation, Type returnType, Rule.Context context)
    {
        CallExpression callExpression = new CallExpression(aggregation.getSignature(), returnType, aggregation.getArguments(), Optional.empty());
        RowExpression expression = rewriter.rewrite(callExpression, context);
        return new AggregationNode.Aggregation(
                aggregation.getSignature(),
                ((CallExpression) expression).getArguments(),
                aggregation.isDistinct(),
                aggregation.getFilter(),
                aggregation.getOrderingScheme(),
                aggregation.getMask());
    }

    protected static Map<Symbol, Integer> getLayout(Rule.Context context)
    {
        Map<Symbol, Integer> layout = new HashMap<>();
        int inputId = 0;
        for (Map.Entry<Symbol, Type> entry : context.getSymbolAllocator().getSymbols().entrySet()) {
            layout.put(entry.getKey(), inputId);
            inputId++;
        }
        return layout;
    }

    private class TableDeleteRowExpressionRewrite
            implements Rule<TableDeleteNode>
    {
        @Override
        public Pattern<TableDeleteNode> getPattern()
        {
            return tableDeleteNode();
        }

        @Override
        public Result apply(TableDeleteNode node, Captures captures, Context context)
        {
            if (node.getFilter().isPresent() && OriginalExpressionUtils.isExpression(node.getFilter().get())) {
                RowExpression rewritten = rewriter.rewrite(node.getFilter().get(), context);
                return Result.ofPlanNode(new TableDeleteNode(
                        node.getId(),
                        node.getSource(),
                        Optional.of(rewritten),
                        node.getTarget(),
                        node.getAssignments(),
                        node.getOutput()));
            }

            return Result.empty();
        }
    }
}
