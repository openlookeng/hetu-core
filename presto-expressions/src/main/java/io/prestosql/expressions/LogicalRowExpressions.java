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
package io.prestosql.expressions;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.sql.RowExpressionUtils;
import io.prestosql.spi.type.StandardTypes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.spi.sql.RowExpressionUtils.FALSE_CONSTANT;
import static io.prestosql.spi.sql.RowExpressionUtils.TRUE_CONSTANT;
import static io.prestosql.spi.sql.RowExpressionUtils.combinePredicates;
import static io.prestosql.spi.sql.RowExpressionUtils.extractPredicates;
import static io.prestosql.spi.sql.RowExpressionUtils.filterConjuncts;
import static io.prestosql.spi.sql.RowExpressionUtils.isConjunctionOrDisjunction;
import static io.prestosql.spi.sql.RowExpressionUtils.or;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static java.lang.Math.min;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class LogicalRowExpressions
{
    // 10000 is very conservative estimation
    private static final int ELIMINATE_COMMON_SIZE_LIMIT = 10000;
    private final DeterminismEvaluator determinismEvaluator;

    public LogicalRowExpressions(DeterminismEvaluator determinismEvaluator)
    {
        this.determinismEvaluator = determinismEvaluator;
    }

    /**
     * Given a logical expression, the goal is to push negation to the leaf nodes.
     * This only applies to propositional logic and comparison. this utility cannot be applied to high-order logic.
     * Examples of non-applicable cases could be f(a AND b) > 5
     *
     * An applicable example:
     *
     *        NOT
     *         |
     *      ___OR_                          AND
     *     /      \                      /      \
     *    NOT     OR        ==>        AND      AND
     *     |     /  \                 /  \     /   \
     *    AND   c   NOT              a    b   NOT  d
     *   /  \        |                         |
     *  a    b       d                         c
     */
    public RowExpression pushNegationToLeaves(RowExpression expression)
    {
        return expression.accept(new PushNegationVisitor(), null);
    }

    /**
     * Given a logical expression, the goal is to convert to conjuctive normal form (CNF).
     * This requires making a call to `pushNegationToLeaves`. There is no guarantee as to
     * the balance of the resulting expression tree.
     *
     * This only applies to propositional logic. this utility cannot be applied to high-order logic.
     * Examples of non-applicable cases could be f(a AND b) > 5
     *
     * NOTE: This may exponentially increase the number of RowExpressions in the expression.
     *
     * An applicable example:
     *
     *        NOT
     *         |
     *      ___OR_                          AND
     *     /      \                      /      \
     *    NOT     OR        ==>        OR      AND
     *     |     /  \                 /  \     /   \
     *    OR   c   NOT              a    b   NOT  d
     *   /  \        |                         |
     *  a    b       d                         c
     */
    public RowExpression convertToConjunctiveNormalForm(RowExpression expression)
    {
        return convertToNormalForm(expression, AND);
    }

    /**
     * Given a logical expression, the goal is to convert to disjunctive normal form (DNF).
     * The same limitations, format, and risks apply as for converting to conjunctive normal form (CNF).
     *
     * An applicable example:
     *
     *        NOT                                    OR
     *         |                                 /        \
     *      ___OR_                          AND            AND
     *     /      \                        /    \        /     \
     *    NOT     OR        ==>          a     AND      b     AND
     *     |     /  \                         /   \          /   \
     *    OR   c   NOT                       NOT  d         NOT  d
     *   /  \        |                         |              |
     *  a    b       d                         c              c
     */
    public RowExpression convertToDisjunctiveNormalForm(RowExpression expression)
    {
        return convertToNormalForm(expression, OR);
    }

    public RowExpression minimalNormalForm(RowExpression expression)
    {
        RowExpression conjunctiveNormalForm = convertToConjunctiveNormalForm(expression);
        RowExpression disjunctiveNormalForm = convertToDisjunctiveNormalForm(expression);
        return numOfClauses(conjunctiveNormalForm) > numOfClauses(disjunctiveNormalForm) ? disjunctiveNormalForm : conjunctiveNormalForm;
    }

    public RowExpression convertToNormalForm(RowExpression expression, SpecialForm.Form clauseJoiner)
    {
        return pushNegationToLeaves(expression).accept(new ConvertNormalFormVisitor(), rootContext(clauseJoiner));
    }

    public RowExpression filterDeterministicConjuncts(RowExpression expression)
    {
        return filterConjuncts(expression, this.determinismEvaluator::isDeterministic);
    }

    public RowExpression filterNonDeterministicConjuncts(RowExpression expression)
    {
        return filterConjuncts(expression, predicate -> !this.determinismEvaluator.isDeterministic(predicate));
    }

    private final class PushNegationVisitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
        @Override
        public RowExpression visitCall(CallExpression call, Void context)
        {
            if (!isNegationExpression(call)) {
                return call;
            }

            checkArgument(call.getArguments().size() == 1, "Not expression should have exactly one argument");
            RowExpression argument = call.getArguments().get(0);

            // eliminate two consecutive negations
            if (isNegationExpression(argument)) {
                return ((CallExpression) argument).getArguments().get(0).accept(new PushNegationVisitor(), null);
            }

            if (isComparisonExpression(argument)) {
                return negateComparison((CallExpression) argument);
            }

            if (!isConjunctionOrDisjunction(argument)) {
                return call;
            }

            // push negation through conjunction or disjunction
            SpecialForm specialForm = ((SpecialForm) argument);
            RowExpression left = specialForm.getArguments().get(0);
            RowExpression right = specialForm.getArguments().get(1);
            if (specialForm.getForm() == AND) {
                // !(a AND b) ==> !a OR !b
                return or(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(this, null));
            }
            // !(a OR b) ==> !a AND !b
            return and(notCallExpression(left).accept(new PushNegationVisitor(), null), notCallExpression(right).accept(this, null));
        }

        private RowExpression negateComparison(CallExpression expression)
        {
            OperatorType newOperator = negate(getOperator(expression).orElse(null));
            if (newOperator == null) {
                return new CallExpression(new Signature("not",
                                                        SCALAR,
                                                        parseTypeSignature(StandardTypes.BOOLEAN),
                                                        ImmutableList.of(parseTypeSignature(StandardTypes.BOOLEAN))),
                    BOOLEAN,
                    singletonList(expression), Optional.empty());
            }
            checkArgument(expression.getArguments().size() == 2, "Comparison expression must have exactly two arguments");
            RowExpression left = expression.getArguments().get(0).accept(this, null);
            RowExpression right = expression.getArguments().get(1).accept(this, null);
            return new CallExpression(
                    Signature.internalOperator(newOperator, BOOLEAN, asList(left.getType(), right.getType())),
                    BOOLEAN,
                    asList(left, right), Optional.empty());
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            if (!isConjunctionOrDisjunction(specialForm)) {
                return specialForm;
            }

            RowExpression left = specialForm.getArguments().get(0);
            RowExpression right = specialForm.getArguments().get(1);

            if (specialForm.getForm() == AND) {
                return and(left.accept(this, null), right.accept(this, null));
            }
            return or(left.accept(this, null), right.accept(this, null));
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, Void context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, Void context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }
    }

    private static ConvertNormalFormVisitorContext rootContext(SpecialForm.Form clauseJoiner)
    {
        return new ConvertNormalFormVisitorContext(clauseJoiner, 0);
    }

    private static class ConvertNormalFormVisitorContext
    {
        private final SpecialForm.Form expectedClauseJoiner;
        private final int depth;

        public ConvertNormalFormVisitorContext(SpecialForm.Form expectedClauseJoiner, int depth)
        {
            this.expectedClauseJoiner = expectedClauseJoiner;
            this.depth = depth;
        }

        public ConvertNormalFormVisitorContext childContext()
        {
            return new ConvertNormalFormVisitorContext(expectedClauseJoiner, depth + 1);
        }
    }

    private class ConvertNormalFormVisitor
            implements RowExpressionVisitor<RowExpression, ConvertNormalFormVisitorContext>
    {
        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, ConvertNormalFormVisitorContext context)
        {
            if (!isConjunctionOrDisjunction(specialForm)) {
                return specialForm;
            }
            // Attempt to convert sub expression to expected normal form, deduplicate and fold constants.
            RowExpression rewritten = combinePredicates(
                    specialForm.getForm(),
                    extractPredicates(specialForm.getForm(), specialForm).stream()
                            .map(subPredicate -> subPredicate.accept(this, context.childContext()))
                            .collect(toList()));

            if (!isConjunctionOrDisjunction(rewritten)) {
                return rewritten;
            }

            SpecialForm rewrittenSpecialForm = (SpecialForm) rewritten;
            io.prestosql.spi.relation.SpecialForm.Form expressionClauseJoiner = rewrittenSpecialForm.getForm();
            List<List<RowExpression>> groupedClauses = getGroupedClauses(rewrittenSpecialForm);

            if (groupedClauses.stream().mapToInt(List::size).sum() > ELIMINATE_COMMON_SIZE_LIMIT) {
                return rewritten;
            }
            groupedClauses = eliminateCommonPredicates(groupedClauses);

            // extractCommonPredicates can produce opposite expectedClauseJoiner
            List<List<RowExpression>> groupedClausesWithFlippedJoiner = extractCommonPredicates(expressionClauseJoiner, groupedClauses);
            if (groupedClausesWithFlippedJoiner != null) {
                groupedClauses = groupedClausesWithFlippedJoiner;
                expressionClauseJoiner = flip(expressionClauseJoiner);
            }

            int numClauses = groupedClauses.stream().mapToInt(List::size).sum();

            int numClausesProducedByDistributiveLaw = groupedClauses.size();
            for (List<RowExpression> group : groupedClauses) {
                numClausesProducedByDistributiveLaw *= group.size();
                // If distributive rule will produce too many sub expressions, return what we have instead.
                if (context.depth > 0 || numClausesProducedByDistributiveLaw > numClauses * 2) {
                    return combineGroupedClauses(expressionClauseJoiner, groupedClauses);
                }
            }
            // size unchanged means distributive law will not apply, we can save an unnecessary crossProduct call.
            // For example, distributive law cannot apply to (a || b || c).
            if (numClausesProducedByDistributiveLaw == numClauses) {
                return combineGroupedClauses(expressionClauseJoiner, groupedClauses);
            }

            // TODO if the non-deterministic operation only appears in the only sub-predicates that has size >1, we can still expand it.
            // For example: a && b && c && (d || e) can still be expanded if d or e is non-deterministic.
            boolean deterministic = groupedClauses.stream()
                    .flatMap(List::stream)
                    .allMatch(determinismEvaluator::isDeterministic);

            // Do not apply distributive law if there is non-deterministic element or we have already got expected expectedClauseJoiner.
            if (expressionClauseJoiner == context.expectedClauseJoiner || !deterministic) {
                return combineGroupedClauses(expressionClauseJoiner, groupedClauses);
            }

            // else, we apply distributive law and rewrite based on distributive property of Boolean algebra, for example
            // (l1 OR l2) AND (r1 OR r2) <=> (l1 AND r1) OR (l1 AND r2) OR (l2 AND r1) OR (l2 AND r2)
            groupedClauses = crossProduct(groupedClauses);
            return combineGroupedClauses(context.expectedClauseJoiner, groupedClauses);
        }

        @Override
        public RowExpression visitCall(CallExpression call, ConvertNormalFormVisitorContext context)
        {
            return call;
        }

        @Override
        public RowExpression visitInputReference(InputReferenceExpression reference, ConvertNormalFormVisitorContext context)
        {
            return reference;
        }

        @Override
        public RowExpression visitConstant(ConstantExpression literal, ConvertNormalFormVisitorContext context)
        {
            return literal;
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, ConvertNormalFormVisitorContext context)
        {
            return lambda;
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, ConvertNormalFormVisitorContext context)
        {
            return reference;
        }
    }

    private boolean isNegationExpression(RowExpression expression)
    {
        return expression instanceof CallExpression && ((CallExpression) expression).getSignature().getName().equals("not");
    }

    private boolean isComparisonExpression(RowExpression expression)
    {
        if (expression instanceof CallExpression) {
            Signature signature = ((CallExpression) expression).getSignature();
            try {
                OperatorType operatorType = signature.unmangleOperator(signature.getName());
                return operatorType.equals(EQUAL) ||
                        operatorType.equals(NOT_EQUAL) ||
                        operatorType.equals(LESS_THAN) ||
                        operatorType.equals(LESS_THAN_OR_EQUAL) ||
                        operatorType.equals(GREATER_THAN) ||
                        operatorType.equals(GREATER_THAN_OR_EQUAL) ||
                        operatorType.equals(IS_DISTINCT_FROM);
            }
            catch (IllegalArgumentException e) {
                return false;
            }
        }
        return false;
    }

    /**
     * Extract the component predicates as a list of list in which is grouped so that the outer level has same conjunctive/disjunctive joiner as original predicate and
     * inner level has opposite joiner.
     * For example, (a or b) and (a or c) or ( a or c) returns [[a,b], [a,c], [a,c]]
     */
    private List<List<RowExpression>> getGroupedClauses(SpecialForm expression)
    {
        return extractPredicates(expression.getForm(), expression).stream()
                .map(RowExpressionUtils::extractPredicates)
                .collect(toList());
    }

    private int numOfClauses(RowExpression expression)
    {
        if (expression instanceof SpecialForm) {
            return getGroupedClauses((SpecialForm) expression).stream().mapToInt(List::size).sum();
        }
        return 1;
    }

    /**
     * Eliminate a sub predicate if its sub predicates contain its peer.
     * For example: (a || b) && a = a, (a && b) || b = b
     */
    private List<List<RowExpression>> eliminateCommonPredicates(List<List<RowExpression>> groupedClauses)
    {
        if (groupedClauses.size() < 2) {
            return groupedClauses;
        }
        // initialize to self
        int[] reduceTo = IntStream.range(0, groupedClauses.size()).toArray();
        for (int i = 0; i < groupedClauses.size(); i++) {
            // Do not eliminate predicates contain non-deterministic value
            // (a || b) && a should be kept same if a is non-deterministic.
            // TODO We can eliminate (a || b) && a if a is deterministic even b is not.
            if (groupedClauses.get(i).stream().allMatch(determinismEvaluator::isDeterministic)) {
                for (int j = 0; j < groupedClauses.size(); j++) {
                    if (isSuperSet(groupedClauses.get(reduceTo[i]), groupedClauses.get(j))) {
                        reduceTo[i] = j; //prefer smaller set
                    }
                    else if (isSameSet(groupedClauses.get(reduceTo[i]), groupedClauses.get(j))) {
                        reduceTo[i] = min(reduceTo[i], j); //prefer predicates that appears earlier.
                    }
                }
            }
        }

        return unmodifiableList(stream(reduceTo)
                .distinct()
                .boxed()
                .map(groupedClauses::get)
                .collect(toList()));
    }

    /**
     * Eliminate a sub predicate if its component predicates contain its peer. Will return null if cannot extract common predicates otherwise return a nested list with flipped form
     * For example:
     * (a || b || c || d) && (a || b || e || f)  -> a || b || ((c || d) && (e || f))
     * (a || b) && (c || d) -> null
     */
    private List<List<RowExpression>> extractCommonPredicates(SpecialForm.Form rootClauseJoiner, List<List<RowExpression>> groupedPredicates)
    {
        if (groupedPredicates.isEmpty()) {
            return null;
        }
        Set<RowExpression> commonPredicates = new LinkedHashSet<>(groupedPredicates.get(0));
        for (int i = 1; i < groupedPredicates.size(); i++) {
            // remove all non-common predicates
            commonPredicates.retainAll(groupedPredicates.get(i));
        }

        if (commonPredicates.isEmpty()) {
            return null;
        }
        // extract the component predicates that are not in common predicates: [(c || d), (e || f)]
        List<RowExpression> remainingPredicates = new ArrayList<>();
        for (List<RowExpression> group : groupedPredicates) {
            List<RowExpression> remaining = group.stream()
                    .filter(predicate -> !commonPredicates.contains(predicate))
                    .collect(toList());
            remainingPredicates.add(combinePredicates(flip(rootClauseJoiner), remaining));
        }
        // combine common predicates and remaining predicates to flipped nested form. For example: [[a], [b], [ (c || d), (e || f)]
        return Stream.concat(commonPredicates.stream().map(predicate -> singletonList(predicate)), Stream.of(remainingPredicates))
                .collect(toList());
    }

    private RowExpression combineGroupedClauses(SpecialForm.Form clauseJoiner, List<List<RowExpression>> nestedPredicates)
    {
        return combinePredicates(clauseJoiner, nestedPredicates.stream()
                .map(predicate -> combinePredicates(flip(clauseJoiner), predicate))
                .collect(toList()));
    }

    /**
     * Cartesian cross product of List of List.
     * For example, [[a], [b, c], [d]] becomes [[a,b,d], [a,c,d]]
     */
    private static List<List<RowExpression>> crossProduct(List<List<RowExpression>> groupedPredicates)
    {
        checkArgument(groupedPredicates.size() > 0, "Must contains more than one child");
        List<List<RowExpression>> result = groupedPredicates.get(0).stream().map(Collections::singletonList).collect(toList());
        for (int i = 1; i < groupedPredicates.size(); i++) {
            result = crossProduct(result, groupedPredicates.get(i));
        }
        return result;
    }

    private static List<List<RowExpression>> crossProduct(List<List<RowExpression>> previousCrossProduct, List<RowExpression> clauses)
    {
        List<List<RowExpression>> result = new ArrayList<>();
        for (List<RowExpression> previousClauses : previousCrossProduct) {
            for (RowExpression newClause : clauses) {
                List<RowExpression> newClauses = new ArrayList<>(previousClauses);
                newClauses.add(newClause);
                result.add(newClauses);
            }
        }
        return result;
    }

    private static SpecialForm.Form flip(SpecialForm.Form binaryLogicalOperation)
    {
        switch (binaryLogicalOperation) {
            case AND:
                return OR;
            case OR:
                return AND;
        }
        throw new UnsupportedOperationException("Invalid binary logical operation: " + binaryLogicalOperation);
    }

    private Optional<OperatorType> getOperator(RowExpression expression)
    {
        try {
            if (expression instanceof CallExpression) {
                Signature signature = ((CallExpression) expression).getSignature();
                return Optional.of(signature.unmangleOperator(signature.getName()));
            }
        }
        catch (IllegalArgumentException e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    private RowExpression notCallExpression(RowExpression argument)
    {
        return new CallExpression(new Signature("not",
                                        SCALAR,
                                        parseTypeSignature(StandardTypes.BOOLEAN),
                                        ImmutableList.of(parseTypeSignature(StandardTypes.BOOLEAN))),
                BOOLEAN,
                singletonList(argument), Optional.empty());
    }

    private static OperatorType negate(OperatorType operator)
    {
        switch (operator) {
            case EQUAL:
                return NOT_EQUAL;
            case NOT_EQUAL:
                return EQUAL;
            case GREATER_THAN:
                return LESS_THAN_OR_EQUAL;
            case LESS_THAN:
                return GREATER_THAN_OR_EQUAL;
            case LESS_THAN_OR_EQUAL:
                return GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return LESS_THAN;
        }
        return null;
    }

    private static void checkArgument(boolean condition, String message, Object... arguments)
    {
        if (!condition) {
            throw new IllegalArgumentException(String.format(message, arguments));
        }
    }

    private static <T> boolean isSuperSet(Collection<T> a, Collection<T> b)
    {
        // We assumes a, b both are de-duplicated collections.
        return a.size() > b.size() && a.containsAll(b);
    }

    private static <T> boolean isSameSet(Collection<T> a, Collection<T> b)
    {
        // We assumes a, b both are de-duplicated collections.
        return a.size() == b.size() && a.containsAll(b) && b.containsAll(a);
    }

    public static RowExpression and(RowExpression... expressions)
    {
        return and(asList(expressions));
    }

    public static RowExpression and(Collection<RowExpression> expressions)
    {
        return binaryExpression(AND, expressions);
    }

    public static RowExpression binaryExpression(SpecialForm.Form form, Collection<RowExpression> expressions)
    {
        requireNonNull(form, "operator is null");
        requireNonNull(expressions, "expressions is null");

        if (expressions.isEmpty()) {
            switch (form) {
                case AND:
                    return TRUE_CONSTANT;
                case OR:
                    return FALSE_CONSTANT;
                default:
                    throw new IllegalArgumentException("Unsupported binary expression operator");
            }
        }

        // Build balanced tree for efficient recursive processing that
        // preserves the evaluation order of the input expressions.
        //
        // The tree is built bottom up by combining pairs of elements into
        // binary AND expressions.
        //
        // Example:
        //
        // Initial state:
        //  a b c d e
        //
        // First iteration:
        //
        //  /\    /\   e
        // a  b  c  d
        //
        // Second iteration:
        //
        //    / \    e
        //  /\   /\
        // a  b c  d
        //
        //
        // Last iteration:
        //
        //      / \
        //    / \  e
        //  /\   /\
        // a  b c  d

        Queue<RowExpression> queue = new ArrayDeque<>(expressions);
        while (queue.size() > 1) {
            Queue<RowExpression> buffer = new ArrayDeque<>();

            // combine pairs of elements
            while (queue.size() >= 2) {
                List<RowExpression> arguments = asList(queue.remove(), queue.remove());
                buffer.add(new SpecialForm(form, BOOLEAN, arguments));
            }

            // if there's and odd number of elements, just append the last one
            if (!queue.isEmpty()) {
                buffer.add(queue.remove());
            }

            // continue processing the pairs that were just built
            queue = buffer;
        }

        return queue.remove();
    }
}
