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
import io.prestosql.spi.plan.AggregationNode;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.FilterNode;
import io.prestosql.spi.plan.GroupReference;
import io.prestosql.spi.plan.JoinNode;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.ValuesNode;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.ApplyNode;

import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public final class ExpressionExtractor
{
    public static List<RowExpression> extractExpressions(PlanNode plan)
    {
        return extractExpressions(plan, Lookup.noLookup());
    }

    public static List<RowExpression> extractExpressions(PlanNode plan, Lookup lookup)
    {
        requireNonNull(plan, "plan is null");
        requireNonNull(lookup, "lookup is null");

        ImmutableList.Builder<RowExpression> expressionsBuilder = ImmutableList.builder();
        plan.accept(new Visitor(true, lookup), expressionsBuilder::add);
        return expressionsBuilder.build();
    }

    public static List<RowExpression> extractExpressionsNonRecursive(PlanNode plan)
    {
        ImmutableList.Builder<RowExpression> expressionsBuilder = ImmutableList.builder();
        plan.accept(new Visitor(false, Lookup.noLookup()), expressionsBuilder::add);
        return expressionsBuilder.build();
    }

    public static void forEachExpression(PlanNode plan, Consumer<RowExpression> expressionConsumer)
    {
        plan.accept(new Visitor(true, Lookup.noLookup()), expressionConsumer);
    }

    private ExpressionExtractor()
    {
    }

    private static class Visitor
            extends SimplePlanVisitor<Consumer<RowExpression>>
    {
        private final boolean recursive;
        private final Lookup lookup;

        Visitor(boolean recursive, Lookup lookup)
        {
            this.recursive = recursive;
            this.lookup = requireNonNull(lookup, "lookup is null");
        }

        @Override
        public Void visitPlan(PlanNode node, Consumer<RowExpression> context)
        {
            if (recursive) {
                return super.visitPlan(node, context);
            }
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, Consumer<RowExpression> context)
        {
            return lookup.resolve(node).accept(this, context);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Consumer<RowExpression> context)
        {
            for (Aggregation aggregation : node.getAggregations().values()) {
                aggregation.getArguments().forEach(context);
            }
            return super.visitAggregation(node, context);
        }

        @Override
        public Void visitFilter(FilterNode node, Consumer<RowExpression> context)
        {
            context.accept(node.getPredicate());
            return super.visitFilter(node, context);
        }

        @Override
        public Void visitProject(ProjectNode node, Consumer<RowExpression> context)
        {
            node.getAssignments().getExpressions().forEach(context);
            return super.visitProject(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Consumer<RowExpression> context)
        {
            node.getFilter().ifPresent(context);
            return super.visitJoin(node, context);
        }

        @Override
        public Void visitValues(ValuesNode node, Consumer<RowExpression> context)
        {
            node.getRows().forEach(row -> row.forEach(context));
            return super.visitValues(node, context);
        }

        @Override
        public Void visitApply(ApplyNode node, Consumer<RowExpression> context)
        {
            node.getSubqueryAssignments().getExpressions().forEach(context);
            return super.visitApply(node, context);
        }
    }
}
