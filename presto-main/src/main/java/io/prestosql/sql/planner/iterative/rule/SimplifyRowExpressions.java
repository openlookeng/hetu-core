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

import com.google.common.annotations.VisibleForTesting;
import io.prestosql.expressions.LogicalRowExpressions;
import io.prestosql.expressions.RowExpressionRewriter;
import io.prestosql.expressions.RowExpressionTreeRewriter;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.RowExpressionDeterminismEvaluator;
import io.prestosql.sql.relational.RowExpressionOptimizer;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.spi.relation.SpecialForm.Form.AND;
import static io.prestosql.spi.relation.SpecialForm.Form.OR;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.SERIALIZABLE;
import static java.util.Objects.requireNonNull;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata)
    {
        super(new Rewriter(metadata));
    }

    private static class Rewriter
            implements PlanRowExpressionRewriter
    {
        private final RowExpressionOptimizer optimizer;
        private final LogicalExpressionRewriter logicalExpressionRewriter;

        public Rewriter(Metadata metadata)
        {
            requireNonNull(metadata, "metadata is null");
            this.optimizer = new RowExpressionOptimizer(metadata);
            this.logicalExpressionRewriter = new LogicalExpressionRewriter(metadata);
        }

        @Override
        public RowExpression rewrite(RowExpression expression, Rule.Context context)
        {
            return rewrite(expression, context.getSession().toConnectorSession());
        }

        private RowExpression rewrite(RowExpression expression, ConnectorSession session)
        {
            RowExpression optimizedRowExpression = optimizer.optimize(expression, SERIALIZABLE, session);
            if (optimizedRowExpression instanceof ConstantExpression || !BooleanType.BOOLEAN.equals(optimizedRowExpression.getType())) {
                return optimizedRowExpression;
            }
            return RowExpressionTreeRewriter.rewriteWith(logicalExpressionRewriter, optimizedRowExpression, true);
        }
    }

    @VisibleForTesting
    public static RowExpression rewrite(RowExpression expression, Metadata metadata, ConnectorSession session)
    {
        return new Rewriter(metadata).rewrite(expression, session);
    }

    private static class LogicalExpressionRewriter
            extends RowExpressionRewriter<Boolean>
    {
        private final LogicalRowExpressions logicalRowExpressions;
        private final Metadata metadata;

        public LogicalExpressionRewriter(Metadata metadata)
        {
            this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata));
            this.metadata = metadata;
        }

        @Override
        public RowExpression rewriteCall(CallExpression node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (node.getSignature().getName().equals("not")) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "NOT must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        @Override
        public RowExpression rewriteSpecialForm(SpecialForm node, Boolean isRoot, RowExpressionTreeRewriter<Boolean> treeRewriter)
        {
            if (isConjunctiveDisjunctive(node.getForm())) {
                checkState(BooleanType.BOOLEAN.equals(node.getType()), "AND/OR must be boolean function");
                return rewriteBooleanExpression(node, isRoot);
            }
            if (isRoot) {
                return treeRewriter.rewrite(node, false);
            }
            return null;
        }

        private boolean isConjunctiveDisjunctive(SpecialForm.Form form)
        {
            return form == AND || form == OR;
        }

        private RowExpression rewriteBooleanExpression(RowExpression expression, boolean isRoot)
        {
            if (isRoot) {
                return logicalRowExpressions.convertToConjunctiveNormalForm(expression);
            }
            return logicalRowExpressions.minimalNormalForm(expression);
        }
    }
}
