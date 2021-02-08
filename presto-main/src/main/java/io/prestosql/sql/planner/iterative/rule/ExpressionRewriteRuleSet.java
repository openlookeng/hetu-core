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
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.OrderingSchemeUtils;
import io.prestosql.sql.planner.SymbolUtils;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AssignmentUtils;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.OrderBy;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SortItem;
import io.prestosql.sql.tree.SortItem.NullOrdering;
import io.prestosql.sql.tree.SymbolReference;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.filter;
import static io.prestosql.sql.planner.plan.Patterns.join;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.values;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static io.prestosql.sql.tree.SortItem.Ordering.ASCENDING;
import static io.prestosql.sql.tree.SortItem.Ordering.DESCENDING;
import static java.util.Objects.requireNonNull;

public class ExpressionRewriteRuleSet
{
    public interface ExpressionRewriter
    {
        Expression rewrite(Expression expression, Rule.Context context);
    }

    private final ExpressionRewriter rewriter;

    public ExpressionRewriteRuleSet(ExpressionRewriter rewriter)
    {
        this.rewriter = requireNonNull(rewriter, "rewriter is null");
    }

    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                aggregationExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite());
    }

    public Rule<?> projectExpressionRewrite()
    {
        return new ProjectExpressionRewrite(rewriter);
    }

    public Rule<?> aggregationExpressionRewrite()
    {
        return new AggregationExpressionRewrite(rewriter);
    }

    public Rule<?> filterExpressionRewrite()
    {
        return new FilterExpressionRewrite(rewriter);
    }

    public Rule<?> joinExpressionRewrite()
    {
        return new JoinExpressionRewrite(rewriter);
    }

    public Rule<?> valuesExpressionRewrite()
    {
        return new ValuesExpressionRewrite(rewriter);
    }

    private static final class ProjectExpressionRewrite
            implements Rule<ProjectNode>
    {
        private final ExpressionRewriter rewriter;

        ProjectExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<ProjectNode> getPattern()
        {
            return project();
        }

        @Override
        public Result apply(ProjectNode projectNode, Captures captures, Context context)
        {
            Assignments assignments = AssignmentUtils.rewrite(projectNode.getAssignments(), x -> rewriter.rewrite(x, context));
            if (projectNode.getAssignments().equals(assignments)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new ProjectNode(projectNode.getId(), projectNode.getSource(), assignments));
        }
    }

    private static final class AggregationExpressionRewrite
            implements Rule<AggregationNode>
    {
        private final ExpressionRewriter rewriter;

        AggregationExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return aggregation();
        }

        @Override
        public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
        {
            boolean anyRewritten = false;
            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : aggregationNode.getAggregations().entrySet()) {
                Aggregation aggregation = entry.getValue();
                FunctionCall call = (FunctionCall) rewriter.rewrite(
                        new FunctionCall(
                                Optional.empty(),
                                QualifiedName.of(aggregation.getSignature().getName()),
                                Optional.empty(),
                                aggregation.getFilter().map(symbol -> new SymbolReference(symbol.getName())),
                                aggregation.getOrderingScheme().map(orderBy -> new OrderBy(orderBy.getOrderBy().stream()
                                        .map(symbol -> new SortItem(
                                                new SymbolReference(symbol.getName()),
                                                orderBy.getOrdering(symbol).isAscending() ? ASCENDING : DESCENDING,
                                                orderBy.getOrdering(symbol).isNullsFirst() ? NullOrdering.FIRST : NullOrdering.LAST))
                                        .collect(toImmutableList()))),
                                aggregation.isDistinct(),
                                aggregation.getArguments().stream().map(OriginalExpressionUtils::castToExpression).collect(toImmutableList())),
                        context);
                verify(call.getName().equals(QualifiedName.of(aggregation.getSignature().getName())), "Aggregation function name changed");
                Aggregation newAggregation = new Aggregation(
                        aggregation.getSignature(),
                        call.getArguments().stream().map(OriginalExpressionUtils::castToRowExpression).collect(toImmutableList()),
                        call.isDistinct(),
                        call.getFilter().map(SymbolUtils::from),
                        call.getOrderBy().map(OrderingSchemeUtils::fromOrderBy),
                        aggregation.getMask());
                aggregations.put(entry.getKey(), newAggregation);
                if (!aggregation.equals(newAggregation)) {
                    anyRewritten = true;
                }
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        aggregations.build(),
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedSymbols(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
            }
            return Result.empty();
        }
    }

    private static final class FilterExpressionRewrite
            implements Rule<FilterNode>
    {
        private final ExpressionRewriter rewriter;

        FilterExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<FilterNode> getPattern()
        {
            return filter();
        }

        @Override
        public Result apply(FilterNode filterNode, Captures captures, Context context)
        {
            RowExpression rewritten;
            if (isExpression(filterNode.getPredicate())) {
                rewritten = castToRowExpression(rewriter.rewrite(castToExpression(filterNode.getPredicate()), context));
            }
            else {
                rewritten = filterNode.getPredicate();
            }
            if (filterNode.getPredicate().equals(rewritten)) {
                return Result.empty();
            }
            return Result.ofPlanNode(new FilterNode(filterNode.getId(), filterNode.getSource(), rewritten));
        }
    }

    private static final class JoinExpressionRewrite
            implements Rule<JoinNode>
    {
        private final ExpressionRewriter rewriter;

        JoinExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

        @Override
        public Pattern<JoinNode> getPattern()
        {
            return join();
        }

        @Override
        public Result apply(JoinNode joinNode, Captures captures, Context context)
        {
            Optional<Expression> filter = joinNode.getFilter().map(x -> rewriter.rewrite(castToExpression(x), context));
            if (!joinNode.getFilter().map(OriginalExpressionUtils::castToExpression).equals(filter)) {
                return Result.ofPlanNode(new JoinNode(
                        joinNode.getId(),
                        joinNode.getType(),
                        joinNode.getLeft(),
                        joinNode.getRight(),
                        joinNode.getCriteria(),
                        joinNode.getOutputSymbols(),
                        filter.map(OriginalExpressionUtils::castToRowExpression),
                        joinNode.getLeftHashSymbol(),
                        joinNode.getRightHashSymbol(),
                        joinNode.getDistributionType(),
                        joinNode.isSpillable(),
                        joinNode.getDynamicFilters()));
            }
            return Result.empty();
        }
    }

    private static final class ValuesExpressionRewrite
            implements Rule<ValuesNode>
    {
        private final ExpressionRewriter rewriter;

        ValuesExpressionRewrite(ExpressionRewriter rewriter)
        {
            this.rewriter = rewriter;
        }

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
                for (RowExpression expression : row) {
                    if (isExpression(expression)) {
                        RowExpression rewritten = castToRowExpression(rewriter.rewrite(castToExpression(expression), context));
                        if (!expression.equals(rewritten)) {
                            anyRewritten = true;
                        }
                        newRow.add(rewritten);
                    }
                    else {
                        newRow.add(expression);
                    }
                }
                rows.add(newRow.build());
            }
            if (anyRewritten) {
                return Result.ofPlanNode(new ValuesNode(valuesNode.getId(), valuesNode.getOutputSymbols(), rows.build()));
            }
            return Result.empty();
        }
    }
}
