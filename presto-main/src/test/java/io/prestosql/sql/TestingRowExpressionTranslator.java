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
package io.prestosql.sql;

import com.google.common.collect.ImmutableMap;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.MetadataManager;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.ExpressionAnalyzer;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionInterpreter;
import io.prestosql.sql.planner.LiteralEncoder;
import io.prestosql.sql.planner.NoOpSymbolResolver;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.relational.RowExpressionOptimizer;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.NodeRef;

import java.util.Map;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.sql.planner.RowExpressionInterpreter.Level.OPTIMIZED;
import static java.util.Collections.emptyList;

public class TestingRowExpressionTranslator
{
    private final Metadata metadata;
    private final LiteralEncoder literalEncoder;

    public TestingRowExpressionTranslator(Metadata metadata)
    {
        this.metadata = metadata;
        this.literalEncoder = new LiteralEncoder(metadata);
    }

    public TestingRowExpressionTranslator()
    {
        this(MetadataManager.createTestMetadataManager());
    }

    public RowExpression translateAndOptimize(Expression expression)
    {
        return translateAndOptimize(expression, getExpressionTypes(expression, TypeProvider.empty()));
    }

    public RowExpression translateAndOptimize(Expression expression, TypeProvider typeProvider)
    {
        return translateAndOptimize(expression, getExpressionTypes(expression, typeProvider));
    }

    public RowExpression translate(String sql, Map<Symbol, Type> types)
    {
        return translate(ExpressionUtils.rewriteIdentifiersToSymbolReferences(new SqlParser().createExpression(sql)), TypeProvider.viewOf(types));
    }

    public RowExpression translate(Expression expression, TypeProvider typeProvider)
    {
        return SqlToRowExpressionTranslator.translate(
                expression,
                SCALAR,
                getExpressionTypes(expression, typeProvider),
                ImmutableMap.of(),
                metadata.getFunctionAndTypeManager(),
                TEST_SESSION,
                false);
    }

    public RowExpression translateAndOptimize(Expression expression, Map<NodeRef<Expression>, Type> types)
    {
        RowExpression rowExpression = SqlToRowExpressionTranslator.translate(expression, SCALAR, types, ImmutableMap.of(), metadata.getFunctionAndTypeManager(), TEST_SESSION, false);
        RowExpressionOptimizer optimizer = new RowExpressionOptimizer(metadata);
        return optimizer.optimize(rowExpression, OPTIMIZED, TEST_SESSION.toConnectorSession());
    }

    Expression simplifyExpression(Expression expression)
    {
        // Testing simplified expressions is important, since simplification may create CASTs or function calls that cannot be simplified by the ExpressionOptimizer

        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(expression, TypeProvider.empty());
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, TEST_SESSION, expressionTypes);
        Object value = interpreter.optimize(NoOpSymbolResolver.INSTANCE);
        return literalEncoder.toExpression(value, expressionTypes.get(NodeRef.of(expression)));
    }

    private Map<NodeRef<Expression>, Type> getExpressionTypes(Expression expression, TypeProvider typeProvider)
    {
        ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata,
                TEST_SESSION,
                typeProvider,
                emptyList(),
                node -> new IllegalStateException("Unexpected node: %s" + node),
                WarningCollector.NOOP,
                false);
        expressionAnalyzer.analyze(expression, Scope.create());
        return expressionAnalyzer.getExpressionTypes();
    }
}
