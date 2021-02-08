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

import io.prestosql.expressions.DefaultRowExpressionTraversalVisitor;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DefaultExpressionTraversalVisitor;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.IfExpression;
import io.prestosql.sql.tree.InPredicate;
import io.prestosql.sql.tree.NullIfExpression;
import io.prestosql.sql.tree.SearchedCaseExpression;
import io.prestosql.sql.tree.SimpleCaseExpression;
import io.prestosql.sql.tree.SubscriptExpression;
import io.prestosql.sql.tree.TryExpression;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class NullabilityAnalyzer
{
    private NullabilityAnalyzer() {}

    /**
     * TODO: this currently produces a very conservative estimate.
     * We need to narrow down the conditions under which certain constructs
     * can return null (e.g., if(a, b, c) might return null for non-null a
     * only if b or c can be null.
     */
    public static boolean mayReturnNullOnNonNullInput(Expression expression)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        new Visitor().process(expression, result);
        return result.get();
    }

    public static boolean mayReturnNullOnNonNullInput(RowExpression expression, TypeManager typeManager)
    {
        requireNonNull(expression, "expression is null");

        AtomicBoolean result = new AtomicBoolean(false);
        expression.accept(new RowExpressionVisitor(typeManager), result);
        return result.get();
    }

    private static class Visitor
            extends DefaultExpressionTraversalVisitor<Void, AtomicBoolean>
    {
        @Override
        protected Void visitCast(Cast node, AtomicBoolean result)
        {
            // Certain casts (e.g., cast(JSON 'null' AS ...)) can return null, but we know
            // that any "type-only" coercion cannot produce null on non-null input, so
            // take advantage of that fact to produce a more precise result.
            //
            // TODO: need a generic way to determine whether a CAST can produce null on non-null input.
            // This should be part of the metadata associated with the CAST. (N.B. the rules in
            // ISO/IEC 9075-2:2016, section 7.16.21 seems to imply that CAST cannot return NULL
            // except for the CAST(NULL AS x) case -- we should fix this at some point)
            //
            // Also, try_cast (i.e., safe cast) can return null
            result.set(node.isSafe() || !node.isTypeOnly());
            return null;
        }

        @Override
        protected Void visitNullIfExpression(NullIfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitDereferenceExpression(DereferenceExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitInPredicate(InPredicate node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSearchedCaseExpression(SearchedCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSimpleCaseExpression(SimpleCaseExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitTryExpression(TryExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitIfExpression(IfExpression node, AtomicBoolean result)
        {
            result.set(true);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, AtomicBoolean result)
        {
            // TODO: this should look at whether the return type of the function is annotated with @SqlNullable
            result.set(true);
            return null;
        }
    }

    private static class RowExpressionVisitor
            extends DefaultRowExpressionTraversalVisitor<AtomicBoolean>
    {
        private final TypeManager typeManager;

        public RowExpressionVisitor(TypeManager typeManager)
        {
            this.typeManager = typeManager;
        }

        @Override
        public Void visitCall(CallExpression call, AtomicBoolean result)
        {
            Signature signature = call.getSignature();

            Optional<OperatorType> operator = Signature.getOperatorType(signature.getName());
            if (operator.isPresent()) {
                switch (operator.get()) {
                    case SATURATED_FLOOR_CAST:
                    case CAST: {
                        checkArgument(call.getArguments().size() == 1);
                        Type sourceType = call.getArguments().get(0).getType();
                        Type targetType = call.getType();
                        if (!typeManager.isTypeOnlyCoercion(sourceType, targetType)) {
                            result.set(true);
                        }
                    }
                    case SUBSCRIPT:
                        result.set(true);
                }
            }
            else if (!functionReturnsNullForNotNullInput(signature)) {
                result.set(true);
            }

            call.getArguments().forEach(argument -> argument.accept(this, result));
            return null;
        }

        private boolean functionReturnsNullForNotNullInput(Signature signature)
        {
            return (signature.getName().equalsIgnoreCase("like"));
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, AtomicBoolean result)
        {
            switch (specialForm.getForm()) {
                case IN:
                case IF:
                case SWITCH:
                case WHEN:
                case NULL_IF:
                case DEREFERENCE:
                    result.set(true);
            }
            specialForm.getArguments().forEach(argument -> argument.accept(this, result));
            return null;
        }
    }
}
