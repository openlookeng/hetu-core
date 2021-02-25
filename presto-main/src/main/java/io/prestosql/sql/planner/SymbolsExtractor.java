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
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.plan.AggregationNode.Aggregation;
import io.prestosql.spi.plan.PlanNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.plan.WindowNode;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.optimizations.PlanNodeSearcher;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DefaultTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.sql.planner.SymbolUtils.from;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.Objects.requireNonNull;

public final class SymbolsExtractor
{
    private SymbolsExtractor() {}

    public static Set<Symbol> extractUnique(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        ExpressionExtractor.extractExpressions(node).forEach(expression -> {
            if (isExpression(expression)) {
                uniqueSymbols.addAll(extractUnique(castToExpression(expression)));
            }
            else {
                Map<Integer, Symbol> layout = new HashMap<>();
                int channel = 0;
                for (Symbol symbol : node.getOutputSymbols()) {
                    layout.put(channel++, symbol);
                }
                uniqueSymbols.addAll(extractUnique(expression, layout));
            }
        });

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUniqueNonRecursive(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        ExpressionExtractor.extractExpressionsNonRecursive(node).forEach(expression -> uniqueSymbols.addAll(extractUniqueVariableInternal(expression)));

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(PlanNode node, Lookup lookup)
    {
        ImmutableSet.Builder<Symbol> uniqueSymbols = ImmutableSet.builder();
        ExpressionExtractor.extractExpressions(node, lookup).forEach(expression -> {
            if (isExpression(expression)) {
                uniqueSymbols.addAll(extractUnique(castToExpression(expression)));
            }
            else {
                uniqueSymbols.addAll(extractUnique(expression));
            }
        });

        return uniqueSymbols.build();
    }

    public static Set<Symbol> extractUnique(Expression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static Set<Symbol> extractUnique(RowExpression expression)
    {
        if (isExpression(expression)) {
            return extractUnique(castToExpression(expression));
        }
        return ImmutableSet.copyOf(extractAll(expression, new HashMap<>()));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends RowExpression> expressions, List<Map<Integer, Symbol>> layouts)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        if (layouts != null && !layouts.isEmpty()) {
            int pos = 0;
            for (RowExpression expression : expressions) {
                unique.addAll(extractAll(expression, layouts.get(pos++)));
            }
        }
        else {
            for (RowExpression expression : expressions) {
                unique.addAll(extractAll(expression));
            }
        }
        return unique.build();
    }

    public static Set<Symbol> extractUnique(RowExpression expression, Map<Integer, Symbol> layout)
    {
        return ImmutableSet.copyOf(extractAll(expression, layout));
    }

    public static Set<Symbol> extractUnique(Iterable<? extends Expression> expressions)
    {
        ImmutableSet.Builder<Symbol> unique = ImmutableSet.builder();
        for (Expression expression : expressions) {
            unique.addAll(extractAll(expression));
        }
        return unique.build();
    }

    public static Set<Symbol> extractUnique(Aggregation aggregation)
    {
        return ImmutableSet.copyOf(extractAll(aggregation));
    }

    public static Set<Symbol> extractUnique(WindowNode.Function function)
    {
        return ImmutableSet.copyOf(extractAll(function));
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new SymbolBuilderVisitor().process(expression, builder);
        return builder.build();
    }

    public static List<Symbol> extractAll(RowExpression expression)
    {
        if (isExpression(expression)) {
            return extractAll(castToExpression(expression));
        }
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        expression.accept(new SymbolRowExpressionVisitor(new HashMap<>()), builder);
        return builder.build();
    }

    public static List<Symbol> extractAll(RowExpression expression, Map<Integer, Symbol> layout)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        expression.accept(new SymbolRowExpressionVisitor(layout), builder);
        return builder.build();
    }

    public static List<Symbol> extractAll(Aggregation aggregation)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (RowExpression argument : aggregation.getArguments()) {
            if (isExpression(argument)) {
                builder.addAll(extractAll(castToExpression(argument)));
            }
            else {
                builder.addAll(extractAll(argument));
            }
        }
        aggregation.getFilter().ifPresent(builder::add);
        aggregation.getOrderingScheme().ifPresent(orderBy -> builder.addAll(orderBy.getOrderBy()));
        return builder.build();
    }

    public static List<Symbol> extractAll(WindowNode.Function function)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        for (RowExpression argument : function.getArguments()) {
            if (isExpression(argument)) {
                builder.addAll(extractAll(castToExpression(argument)));
            }
            else {
                builder.addAll(extractAll(argument));
            }
        }
        function.getFrame().getEndValue().ifPresent(builder::add);
        function.getFrame().getStartValue().ifPresent(builder::add);
        return builder.build();
    }

    // to extract qualified name with prefix
    public static Set<QualifiedName> extractNames(Expression expression, Set<NodeRef<Expression>> columnReferences)
    {
        ImmutableSet.Builder<QualifiedName> builder = ImmutableSet.builder();
        new QualifiedNameBuilderVisitor(columnReferences).process(expression, builder);
        return builder.build();
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode)
    {
        return extractOutputSymbols(planNode, Lookup.noLookup());
    }

    public static Set<Symbol> extractOutputSymbols(PlanNode planNode, Lookup lookup)
    {
        return PlanNodeSearcher.searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getOutputSymbols().stream())
                .collect(toImmutableSet());
    }

    public static Set<Symbol> extractAllSymbols(PlanNode planNode, Lookup lookup)
    {
        return PlanNodeSearcher.searchFrom(planNode, lookup)
                .findAll()
                .stream()
                .flatMap(node -> node.getAllSymbols().stream())
                .collect(toImmutableSet());
    }

    private static Set<Symbol> extractUniqueVariableInternal(RowExpression expression)
    {
        if (isExpression(expression)) {
            return extractUnique(castToExpression(expression));
        }
        return extractUnique(expression);
    }

    private static class SymbolBuilderVisitor
            extends DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitSymbolReference(SymbolReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(from(node));
            return null;
        }
    }

    private static class SymbolRowExpressionVisitor
            implements RowExpressionVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        private final Map<Integer, Symbol> layout;

        public SymbolRowExpressionVisitor(Map<Integer, Symbol> layout)
        {
            this.layout = layout;
        }

        @Override
        public Void visitInputReference(InputReferenceExpression input, ImmutableList.Builder<Symbol> context)
        {
            context.add(layout.get(input.getField()));
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, ImmutableList.Builder<Symbol> context)
        {
            call.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, ImmutableList.Builder<Symbol> context)
        {
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, ImmutableList.Builder<Symbol> context)
        {
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, ImmutableList.Builder<Symbol> context)
        {
            context.add(new Symbol(reference.getName()));
            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, ImmutableList.Builder<Symbol> context)
        {
            specialForm.getArguments().forEach(argument -> argument.accept(this, context));
            return null;
        }
    }

    private static class QualifiedNameBuilderVisitor
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<QualifiedName>>
    {
        private final Set<NodeRef<Expression>> columnReferences;

        private QualifiedNameBuilderVisitor(Set<NodeRef<Expression>> columnReferences)
        {
            this.columnReferences = requireNonNull(columnReferences, "columnReferences is null");
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableSet.Builder<QualifiedName> builder)
        {
            if (columnReferences.contains(NodeRef.<Expression>of(node))) {
                builder.add(DereferenceExpression.getQualifiedName(node));
            }
            else {
                process(node.getBase(), builder);
            }
            return null;
        }

        @Override
        protected Void visitIdentifier(Identifier node, ImmutableSet.Builder<QualifiedName> builder)
        {
            builder.add(QualifiedName.of(node.getValue()));
            return null;
        }
    }
}
