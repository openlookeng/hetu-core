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
package io.prestosql.sql.gen;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;

import java.util.List;

public class LambdaExpressionExtractor
{
    private LambdaExpressionExtractor()
    {
    }

    public static List<LambdaDefinitionExpression> extractLambdaExpressions(RowExpression expression)
    {
        Visitor visitor = new Visitor();
        expression.accept(visitor, new Context(false));
        return visitor.getLambdaExpressionsPostOrder();
    }

    private static class Visitor
            implements RowExpressionVisitor<Void, Context>
    {
        private final ImmutableList.Builder<LambdaDefinitionExpression> lambdaExpressions = ImmutableList.builder();

        @Override
        public Void visitInputReference(InputReferenceExpression node, Context context)
        {
            // TODO: change such that CallExpressions only capture the inputs they actually depend on
            return null;
        }

        @Override
        public Void visitCall(CallExpression call, Context context)
        {
            for (RowExpression rowExpression : call.getArguments()) {
                rowExpression.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitSpecialForm(SpecialForm specialForm, Context context)
        {
            for (RowExpression rowExpression : specialForm.getArguments()) {
                rowExpression.accept(this, context);
            }

            return null;
        }

        @Override
        public Void visitConstant(ConstantExpression literal, Context context)
        {
            return null;
        }

        @Override
        public Void visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            lambda.getBody().accept(this, new Context(true));
            lambdaExpressions.add(lambda);
            return null;
        }

        @Override
        public Void visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            return null;
        }

        private List<LambdaDefinitionExpression> getLambdaExpressionsPostOrder()
        {
            return lambdaExpressions.build();
        }
    }

    private static class Context
    {
        private final boolean inLambda;

        public Context(boolean inLambda)
        {
            this.inLambda = inLambda;
        }

        public boolean isInLambda()
        {
            return inLambda;
        }
    }
}
