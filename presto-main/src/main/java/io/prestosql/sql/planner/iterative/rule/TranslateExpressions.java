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

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.function.FunctionKind;
import io.prestosql.spi.plan.Symbol;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.type.FunctionType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.PlanSymbolAllocator;
import io.prestosql.sql.planner.TypeAnalyzer;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.relational.OriginalExpressionUtils;
import io.prestosql.sql.relational.SqlToRowExpressionTranslator;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.NodeRef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.sql.relational.OriginalExpressionUtils.castToExpression;
import static io.prestosql.sql.relational.OriginalExpressionUtils.isExpression;

public class TranslateExpressions
        extends RowExpressionRewriteRuleSet
{
    public TranslateExpressions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewriter(metadata, sqlParser));
    }

    private static PlanRowExpressionRewriter createRewriter(Metadata metadata, SqlParser sqlParser)
    {
        return new PlanRowExpressionRewriter()
        {
            @Override
            public RowExpression rewrite(RowExpression expression, Rule.Context context)
            {
                // special treatment of the CallExpression in Aggregation
                if (expression instanceof CallExpression && ((CallExpression) expression).getArguments().stream().anyMatch(OriginalExpressionUtils::isExpression)) {
                    return removeOriginalExpressionArguments((CallExpression) expression, context.getSession(), context.getSymbolAllocator(), context);
                }
                return removeOriginalExpression(expression, context, new HashMap<>());
            }

            private RowExpression removeOriginalExpressionArguments(CallExpression callExpression, Session session, PlanSymbolAllocator planSymbolAllocator, Rule.Context context)
            {
                Map<NodeRef<Expression>, Type> types = analyzeCallExpressionTypes(callExpression, session, planSymbolAllocator.getTypes());
                return new CallExpression(
                        callExpression.getSignature(),
                        callExpression.getType(),
                        callExpression.getArguments().stream()
                                .map(expression -> removeOriginalExpression(expression, session, types, context))
                                .collect(toImmutableList()),
                        Optional.empty());
            }

            private Map<NodeRef<Expression>, Type> analyzeCallExpressionTypes(CallExpression callExpression, Session session, TypeProvider typeProvider)
            {
                List<LambdaExpression> lambdaExpressions = callExpression.getArguments().stream()
                        .filter(OriginalExpressionUtils::isExpression)
                        .map(OriginalExpressionUtils::castToExpression)
                        .filter(LambdaExpression.class::isInstance)
                        .map(LambdaExpression.class::cast)
                        .collect(toImmutableList());
                ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
                TypeAnalyzer typeAnalyzer = new TypeAnalyzer(sqlParser, metadata);
                if (!lambdaExpressions.isEmpty()) {
                    List<FunctionType> functionTypes = callExpression.getSignature().getArgumentTypes().stream()
                            .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                            .map(metadata::getType)
                            .map(FunctionType.class::cast)
                            .collect(toImmutableList());
                    InternalAggregationFunction internalAggregationFunction = metadata.getAggregateFunctionImplementation(callExpression.getSignature());
                    List<Class<?>> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                    verify(lambdaExpressions.size() == functionTypes.size());
                    verify(lambdaExpressions.size() == lambdaInterfaces.size());

                    for (int i = 0; i < lambdaExpressions.size(); i++) {
                        LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                        FunctionType functionType = functionTypes.get(i);

                        // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                        // which requires the types of all sub-expressions.
                        //
                        // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                        // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                        //
                        // This does not work here since the function call representation in final aggregation node
                        // is currently a hack: it takes intermediate type as input, and may not be a valid
                        // function call in Presto.
                        //
                        // TODO: Once the final aggregation function call representation is fixed,
                        // the same mechanism in project and filter expression should be used here.
                        verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                        Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                        Map<Symbol, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                        for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                            LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                            Type type = functionType.getArgumentTypes().get(j);
                            lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                            lambdaArgumentSymbolTypes.put(new Symbol(argument.getName().getValue()), type);
                        }

                        // the lambda expression itself
                        builder.put(NodeRef.of(lambdaExpression), functionType)
                                // expressions from lambda arguments
                                .putAll(lambdaArgumentExpressionTypes)
                                // expressions from lambda body
                                .putAll(typeAnalyzer.getTypes(session, TypeProvider.copyOf(lambdaArgumentSymbolTypes), lambdaExpression.getBody()));
                    }
                }
                for (RowExpression argument : callExpression.getArguments()) {
                    if (!isExpression(argument) || castToExpression(argument) instanceof LambdaExpression) {
                        continue;
                    }
                    builder.putAll(typeAnalyzer.getTypes(session, typeProvider, castToExpression(argument)));
                }
                return builder.build();
            }

            private RowExpression toRowExpression(Expression expression, Map<NodeRef<Expression>, Type> types, Map<Symbol, Integer> layout, Session session)
            {
                return SqlToRowExpressionTranslator.translate(expression, FunctionKind.SCALAR, types, layout, metadata, session, false);
            }

            private RowExpression removeOriginalExpression(RowExpression expression, Rule.Context context, Map<Symbol, Integer> layout)
            {
                if (isExpression(expression)) {
                    TypeAnalyzer typeAnalyzer = new TypeAnalyzer(sqlParser, metadata);
                    return toRowExpression(
                            castToExpression(expression),
                            typeAnalyzer.getTypes(context.getSession(), context.getSymbolAllocator().getTypes(), castToExpression(expression)),
                            layout,
                            context.getSession());
                }
                return expression;
            }

            private RowExpression removeOriginalExpression(RowExpression rowExpression, Session session, Map<NodeRef<Expression>, Type> types, Rule.Context context)
            {
                if (isExpression(rowExpression)) {
                    Expression expression = castToExpression(rowExpression);
                    return toRowExpression(expression, types, new HashMap<>(), session);
                }
                return rowExpression;
            }
        };
    }
}
