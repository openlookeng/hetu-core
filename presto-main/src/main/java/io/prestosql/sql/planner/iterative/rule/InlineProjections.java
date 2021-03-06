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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.prestosql.expressions.DefaultRowExpressionTraversalVisitor;
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.spi.plan.Assignments;
import io.prestosql.spi.plan.ProjectNode;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.RowExpressionVariableInliner;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.sql.util.AstUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.sql.planner.ExpressionSymbolInliner.inlineSymbols;
import static io.prestosql.sql.planner.SymbolUtils.toSymbolReference;
import static io.prestosql.sql.planner.plan.AssignmentUtils.isIdentity;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;
import static java.util.stream.Collectors.toSet;

/**
 * Inlines expressions from a child project node into a parent project node
 * as long as they are simple constants, or they are referenced only once (to
 * avoid introducing duplicate computation) and the references don't appear
 * within a TRY block (to avoid changing semantics).
 */
public class InlineProjections
        implements Rule<ProjectNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<ProjectNode> PATTERN = project()
            .with(source().matching(project().capturedAs(CHILD)));

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ProjectNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        Sets.SetView<Symbol> targets = extractInliningTargets(parent, child);
        if (targets.isEmpty()) {
            return Result.empty();
        }

        // inline the expressions
        Assignments assignments = child.getAssignments().filter(targets::contains);
        Map<Symbol, RowExpression> parentAssignments = parent.getAssignments()
                .entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> (inlineReferences(entry.getValue(), assignments))));

        // Synthesize identity assignments for the inputs of expressions that were inlined
        // to place in the child projection.
        // If all assignments end up becoming identity assignments, they'll get pruned by
        // other rules
        Set<Symbol> inputs = child.getAssignments()
                .entrySet().stream()
                .filter(entry -> targets.contains(entry.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(entry -> extractInputs(entry).stream())
                .collect(toSet());

        Assignments.Builder childAssignments = Assignments.builder();
        for (Map.Entry<Symbol, RowExpression> assignment : child.getAssignments().entrySet()) {
            if (!targets.contains(assignment.getKey())) {
                childAssignments.put(assignment);
            }
        }

        boolean allTranslated = child.getAssignments().entrySet()
                .stream()
                .map(Map.Entry::getValue)
                .noneMatch(OriginalExpressionUtils::isExpression);

        for (Symbol input : inputs) {
            if (allTranslated) {
                Type inputType = context.getSymbolAllocator().getSymbols().get(input);
                childAssignments.put(input, new VariableReferenceExpression(input.getName(), inputType));
            }
            else {
                childAssignments.put(input, castToRowExpression(toSymbolReference(input)));
            }
        }

        return Result.ofPlanNode(
                new ProjectNode(
                        parent.getId(),
                        new ProjectNode(
                                child.getId(),
                                child.getSource(),
                                childAssignments.build()),
                        Assignments.copyOf(parentAssignments)));
    }

    private RowExpression inlineReferences(RowExpression expression, Assignments assignments)
    {
        if (isExpression(expression)) {
            Function<Symbol, Expression> mapping = symbol -> {
                if (assignments.get(symbol) == null) {
                    return toSymbolReference(symbol);
                }
                return castToExpression(assignments.get(symbol));
            };
            return castToRowExpression(inlineSymbols(mapping, castToExpression(expression)));
        }

        Map<VariableReferenceExpression, RowExpression> variableMap = assignments.getMap().entrySet().stream()
                .collect(Collectors.toMap(v -> new VariableReferenceExpression(v.getKey().getName(), v.getValue().getType()), v -> v.getValue()));
        return RowExpressionVariableInliner.inlineVariables(variable -> variableMap.getOrDefault(variable, variable), expression);
    }

    private Sets.SetView<Symbol> extractInliningTargets(ProjectNode parent, ProjectNode child)
    {
        // candidates for inlining are
        //   1. references to simple constants
        //   2. references to complex expressions that
        //      a. are not inputs to try() expressions
        //      b. appear only once across all expressions
        //      c. are not identity projections
        // which come from the child, as opposed to an enclosing scope.

        Set<Symbol> childOutputSet = ImmutableSet.copyOf(child.getOutputSymbols());

        Map<Symbol, Long> dependencies = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(expression -> extractInputs(expression).stream())
                .filter(childOutputSet::contains)
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));

        // find references to simple constants
        Set<Symbol> constants = dependencies.keySet().stream()
                .filter(input -> isConstant(child.getAssignments().get(input)))
                .collect(toSet());

        // exclude any complex inputs to TRY expressions. Inlining them would potentially
        // change the semantics of those expressions
        Set<Symbol> tryArguments = parent.getAssignments()
                .getExpressions().stream()
                .flatMap(expression -> extractTryArguments(expression).stream())
                .collect(toSet());

        Set<Symbol> singletons = dependencies.entrySet().stream()
                .filter(entry -> entry.getValue() == 1) // reference appears just once across all expressions in parent project node
                .filter(entry -> !tryArguments.contains(entry.getKey())) // they are not inputs to TRY. Otherwise, inlining might change semantics
                .filter(entry -> !isIdentity(child.getAssignments(), entry.getKey())) // skip identities, otherwise, this rule will keep firing forever
                .map(Map.Entry::getKey)
                .collect(toSet());

        return Sets.union(singletons, constants);
    }

    private Set<Symbol> extractTryArguments(RowExpression expression)
    {
        if (isExpression(expression)) {
            return AstUtils.preOrder(castToExpression(expression))
                    .filter(TryExpression.class::isInstance)
                    .map(TryExpression.class::cast)
                    .flatMap(tryExpression -> SymbolsExtractor.extractAll(tryExpression).stream())
                    .collect(toSet());
        }
        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();
        expression.accept(new DefaultRowExpressionTraversalVisitor<ImmutableSet.Builder<Symbol>>()
        {
            @Override
            public Void visitCall(CallExpression call, ImmutableSet.Builder<Symbol> context)
            {
                if (isTryFunction(call.getSignature().getName())) {
                    context.addAll(SymbolsExtractor.extractAll(call));
                }
                return super.visitCall(call, context);
            }
        }, builder);
        return builder.build();
    }

    private static boolean isConstant(RowExpression expression)
    {
        if (isExpression(expression)) {
            return castToExpression(expression) instanceof Literal;
        }
        return expression instanceof ConstantExpression;
    }

    private static boolean isTryFunction(String functionName)
    {
        if (functionName.equals(TryFunction.NAME)) {
            return true;
        }
        else {
            return false;
        }
    }

    private static List<Symbol> extractInputs(RowExpression expression)
    {
        if (isExpression(expression)) {
            return SymbolsExtractor.extractAll(castToExpression(expression));
        }
        return SymbolsExtractor.extractAll(expression);
    }
}
