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
package io.prestosql.spi.sql;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.DeterminismEvaluator;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Predicate;

import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.Signature.internalOperator;
import static io.prestosql.spi.function.Signature.unmangleOperator;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class RowExpressionUtils
{
    public static final ConstantExpression TRUE_CONSTANT = new ConstantExpression(true, BOOLEAN);
    public static final ConstantExpression FALSE_CONSTANT = new ConstantExpression(false, BOOLEAN);

    public static List<RowExpression> extractConjuncts(RowExpression expression)
    {
        return extractPredicates(AND, expression);
    }

    public static List<RowExpression> extractDisjuncts(RowExpression expression)
    {
        return extractPredicates(OR, expression);
    }

    public static List<RowExpression> extractPredicates(RowExpression expression)
    {
        if (expression instanceof SpecialForm) {
            SpecialForm.Form form = ((SpecialForm) expression).getForm();
            if (form == AND || form == OR) {
                return extractPredicates(form, expression);
            }
        }
        return singletonList(expression);
    }

    public static List<RowExpression> extractPredicates(SpecialForm.Form form, RowExpression expression)
    {
        if (expression instanceof SpecialForm && ((SpecialForm) expression).getForm() == form) {
            SpecialForm specialForm = (SpecialForm) expression;
            if (specialForm.getArguments().size() != 2) {
                throw new IllegalStateException("logical binary expression requires exactly 2 operands");
            }

            List<RowExpression> predicates = new ArrayList<>();
            predicates.addAll(extractPredicates(form, specialForm.getArguments().get(0)));
            predicates.addAll(extractPredicates(form, specialForm.getArguments().get(1)));
            return unmodifiableList(predicates);
        }

        return singletonList(expression);
    }

    public static RowExpression and(RowExpression... expressions)
    {
        return and(asList(expressions));
    }

    public static RowExpression and(Collection<RowExpression> expressions)
    {
        return binaryExpression(AND, expressions);
    }

    public static RowExpression or(RowExpression... expressions)
    {
        return or(asList(expressions));
    }

    public static RowExpression or(Collection<RowExpression> expressions)
    {
        return binaryExpression(OR, expressions);
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

    public static RowExpression combinePredicates(SpecialForm.Form form, RowExpression... expressions)
    {
        return combinePredicates(form, asList(expressions));
    }

    public static RowExpression combinePredicates(SpecialForm.Form form, Collection<RowExpression> expressions)
    {
        if (form == AND) {
            return combineConjuncts(expressions);
        }
        return combineDisjuncts(expressions);
    }

    public static RowExpression combineConjuncts(RowExpression... expressions)
    {
        return combineConjuncts(asList(expressions));
    }

    public static RowExpression combineConjuncts(Collection<RowExpression> expressions)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> conjuncts = expressions.stream()
                .flatMap(e -> extractConjuncts(e).stream())
                .filter(e -> !e.equals(TRUE_CONSTANT))
                .collect(toList());

        conjuncts = removeDuplicates(conjuncts);

        if (conjuncts.contains(FALSE_CONSTANT)) {
            return FALSE_CONSTANT;
        }

        return and(conjuncts);
    }

    public RowExpression combineDisjuncts(RowExpression... expressions)
    {
        return combineDisjuncts(asList(expressions));
    }

    public static RowExpression combineDisjuncts(Collection<RowExpression> expressions)
    {
        return combineDisjunctsWithDefault(expressions, FALSE_CONSTANT);
    }

    public static RowExpression combineDisjunctsWithDefault(Collection<RowExpression> expressions, RowExpression emptyDefault)
    {
        requireNonNull(expressions, "expressions is null");

        List<RowExpression> disjuncts = expressions.stream()
                .flatMap(e -> extractDisjuncts(e).stream())
                .filter(e -> !e.equals(FALSE_CONSTANT))
                .collect(toList());

        disjuncts = removeDuplicates(disjuncts);

        if (disjuncts.contains(TRUE_CONSTANT)) {
            return TRUE_CONSTANT;
        }

        return disjuncts.isEmpty() ? emptyDefault : or(disjuncts);
    }

    public static RowExpression filterConjuncts(RowExpression expression, Predicate<RowExpression> predicate)
    {
        List<RowExpression> conjuncts = extractConjuncts(expression).stream()
                .filter(predicate)
                .collect(toList());

        return combineConjuncts(conjuncts);
    }

    public static boolean isDeterministic(RowExpression call)
    {
        if (call instanceof CallExpression) {
            Set<String> functions = ImmutableSet.of("rand", "random", "shuffle", "uuid");
            if (functions.contains(((CallExpression) call).getSignature().getName().toLowerCase(Locale.ENGLISH))) {
                return false;
            }
        }
        return true;
    }

    public static boolean isDeterministic(DeterminismEvaluator evaluator, RowExpression call)
    {
        if (!evaluator.isDeterministic(call)) {
            return false;
        }
        return true;
    }

    public static RowExpression flip(RowExpression expressions)
    {
        if (expressions instanceof CallExpression) {
            CallExpression call = (CallExpression) expressions;
            String name = call.getSignature().getName();
            if (name.contains("$operator$") && unmangleOperator(name).isComparisonOperator()) {
                if (call.getArguments().get(1) instanceof VariableReferenceExpression &&
                        call.getArguments().get(0) instanceof ConstantExpression) {
                    OperatorType operator;
                    switch (unmangleOperator(name)) {
                        case LESS_THAN:
                            operator = GREATER_THAN;
                            break;
                        case LESS_THAN_OR_EQUAL:
                            operator = GREATER_THAN_OR_EQUAL;
                            break;
                        case GREATER_THAN:
                            operator = LESS_THAN;
                            break;
                        case GREATER_THAN_OR_EQUAL:
                            operator = LESS_THAN_OR_EQUAL;
                            break;
                        case IS_DISTINCT_FROM:
                            operator = IS_DISTINCT_FROM;
                            break;
                        default:
                            operator = unmangleOperator(name);
                    }
                    Signature signature = internalOperator(operator,
                            call.getSignature().getReturnType(),
                            call.getSignature().getArgumentTypes().get(1),
                            call.getSignature().getArgumentTypes().get(0));
                    List<RowExpression> arguments = new ArrayList<>();
                    arguments.add(call.getArguments().get(1));
                    arguments.add(call.getArguments().get(0));
                    return new CallExpression(signature, call.getType(), arguments, Optional.empty());
                }
            }
        }
        return expressions;
    }

    public static CallExpression simplePredicate(OperatorType operatorType, String name, Type type, Object value)
    {
        Signature sig = internalOperator(operatorType, BOOLEAN.getTypeSignature(), type.getTypeSignature());
        VariableReferenceExpression varRef = new VariableReferenceExpression(name, VARCHAR);
        ConstantExpression constantExpression = new ConstantExpression(value, type);
        List<RowExpression> arguments = new ArrayList<>(2);
        arguments.add(varRef);
        arguments.add(constantExpression);
        return new CallExpression(sig, BOOLEAN, arguments, Optional.empty());
    }

    /**
     * Removes duplicate deterministic expressions. Preserves the relative order
     * of the expressions in the list.
     */
    public static List<RowExpression> removeDuplicates(List<RowExpression> expressions)
    {
        Set<RowExpression> seen = new HashSet<>();

        List<RowExpression> result = new ArrayList<>();
        for (RowExpression expression : expressions) {
            if (isDeterministic(expression)) {
                RowExpression expressionFlip = flip(expression);
                if (!seen.contains(expressionFlip)) {
                    result.add(expressionFlip);
                    seen.add(expressionFlip);
                }
            }
            else {
                result.add(expression);
            }
        }

        return unmodifiableList(result);
    }

    public static boolean isConjunctionOrDisjunction(RowExpression expression)
    {
        if (expression instanceof SpecialForm) {
            SpecialForm.Form form = ((SpecialForm) expression).getForm();
            return form == AND || form == OR;
        }
        return false;
    }
}
