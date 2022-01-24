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
package io.prestosql.sql.relational.optimizer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.Session;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.relational.FunctionResolution;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.CastType.CAST;
import static io.prestosql.metadata.CastType.JSON_TO_ARRAY_CAST;
import static io.prestosql.metadata.CastType.JSON_TO_MAP_CAST;
import static io.prestosql.metadata.CastType.JSON_TO_ROW_CAST;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.relation.SpecialForm.Form.BIND;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.StandardTypes.ARRAY;
import static io.prestosql.spi.type.StandardTypes.MAP;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.relational.Expressions.call;
import static io.prestosql.sql.relational.Expressions.constant;
import static io.prestosql.sql.relational.Expressions.constantNull;
import static io.prestosql.type.JsonType.JSON;

public class ExpressionOptimizer
{
    private final FunctionAndTypeManager functionAndTypeManager;
    private final ConnectorSession session;
    private final FunctionResolution functionResolution;

    public ExpressionOptimizer(FunctionAndTypeManager functionAndTypeManager, Session session)
    {
        this.functionAndTypeManager = functionAndTypeManager;
        this.session = session.toConnectorSession();
        this.functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    public RowExpression optimize(RowExpression expression)
    {
        return expression.accept(new Visitor(), null);
    }

    private class Visitor
            implements RowExpressionVisitor<RowExpression, Void>
    {
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
        public RowExpression visitCall(CallExpression inputCall, Void context)
        {
            CallExpression call = inputCall;
            if (functionResolution.isCastFunction(call.getFunctionHandle())) {
                call = rewriteCast(call);
            }

            BuiltInScalarFunctionImplementation function = functionAndTypeManager.getBuiltInScalarFunctionImplementation(call.getFunctionHandle());
            List<RowExpression> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, context))
                    .collect(toImmutableList());

            // TODO: optimize function calls with lambda arguments. For example, apply(x -> x + 2, 1)
            if (Iterables.all(arguments, instanceOf(ConstantExpression.class)) && functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).isDeterministic()) {
                MethodHandle method = function.getMethodHandle();

                if (method.type().parameterCount() > 0 && method.type().parameterType(0) == ConnectorSession.class) {
                    method = method.bindTo(session);
                }

                int index = 0;
                List<Object> constantArguments = new ArrayList<>();
                for (RowExpression argument : arguments) {
                    Object value = ((ConstantExpression) argument).getValue();
                    // if any argument is null, return null
                    if (value == null && function.getArgumentProperty(index).getNullConvention() == RETURN_NULL_ON_NULL) {
                        return constantNull(call.getType());
                    }
                    constantArguments.add(value);
                    index++;
                }

                try {
                    return constant(method.invokeWithArguments(constantArguments), call.getType());
                }
                catch (Throwable e) {
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    // Do nothing. As a result, this specific tree will be left untouched. But irrelevant expressions will continue to get evaluated and optimized.
                }
            }

            return call(call.getDisplayName(), call.getFunctionHandle(), functionAndTypeManager.getType(call.getType().getTypeSignature()), arguments);
        }

        @Override
        public RowExpression visitSpecialForm(SpecialForm specialForm, Void context)
        {
            switch (specialForm.getForm()) {
                // TODO: optimize these special forms
                case IF: {
                    checkState(specialForm.getArguments().size() == 3, "IF function should have 3 arguments. Get " + specialForm.getArguments().size());
                    RowExpression optimizedOperand = specialForm.getArguments().get(0).accept(this, context);
                    if (optimizedOperand instanceof ConstantExpression) {
                        ConstantExpression constantOperand = (ConstantExpression) optimizedOperand;
                        checkState(constantOperand.getType().equals(BOOLEAN), "Operand of IF function should be BOOLEAN type. Get type " + constantOperand.getType().getDisplayName());
                        if (Boolean.TRUE.equals(constantOperand.getValue())) {
                            return specialForm.getArguments().get(1).accept(this, context);
                        }
                        // FALSE and NULL
                        else {
                            return specialForm.getArguments().get(2).accept(this, context);
                        }
                    }
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), arguments);
                }
                case BIND: {
                    checkState(specialForm.getArguments().size() >= 1, BIND + " function should have at least 1 argument. Got " + specialForm.getArguments().size());

                    boolean allConstantExpression = true;
                    ImmutableList.Builder<RowExpression> optimizedArgumentsBuilder = ImmutableList.builder();
                    for (RowExpression argument : specialForm.getArguments()) {
                        RowExpression optimizedArgument = argument.accept(this, context);
                        if (!(optimizedArgument instanceof ConstantExpression)) {
                            allConstantExpression = false;
                        }
                        optimizedArgumentsBuilder.add(optimizedArgument);
                    }
                    if (allConstantExpression) {
                        // Here, optimizedArguments should be merged together into a new ConstantExpression.
                        // It's not implemented because it would be dead code anyways because visitLambda does not produce ConstantExpression.
                        throw new UnsupportedOperationException();
                    }
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), optimizedArgumentsBuilder.build());
                }
                case NULL_IF:
                case SWITCH:
                case WHEN:
                case BETWEEN:
                case IS_NULL:
                case COALESCE:
                case AND:
                case OR:
                case IN:
                case DEREFERENCE:
                case ROW_CONSTRUCTOR: {
                    List<RowExpression> arguments = specialForm.getArguments().stream()
                            .map(argument -> argument.accept(this, null))
                            .collect(toImmutableList());
                    return new SpecialForm(specialForm.getForm(), specialForm.getType(), arguments);
                }
                default:
                    throw new IllegalArgumentException("Unsupported special form " + specialForm.getForm());
            }
        }

        @Override
        public RowExpression visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return new LambdaDefinitionExpression(lambda.getArgumentTypes(), lambda.getArguments(), lambda.getBody().accept(this, context));
        }

        @Override
        public RowExpression visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return reference;
        }

        private CallExpression rewriteCast(CallExpression call)
        {
            if (call.getArguments().get(0) instanceof CallExpression) {
                // Optimization for CAST(JSON_PARSE(...) AS ARRAY/MAP/ROW)
                CallExpression innerCall = (CallExpression) call.getArguments().get(0);
                if (functionAndTypeManager.getFunctionMetadata(innerCall.getFunctionHandle()).getName().getObjectName().equals("json_parse")) {
                    checkArgument(innerCall.getType().equals(JSON));
                    checkArgument(innerCall.getArguments().size() == 1);
                    TypeSignature returnType = call.getType().getTypeSignature();
                    if (returnType.getBase().equals(ARRAY)) {
                        return call(
                                JSON_TO_ARRAY_CAST.name(),
                                functionAndTypeManager.lookupCast(
                                        JSON_TO_ARRAY_CAST,
                                        parseTypeSignature(StandardTypes.VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(MAP)) {
                        return call(
                                JSON_TO_MAP_CAST.name(),
                                functionAndTypeManager.lookupCast(
                                        JSON_TO_MAP_CAST,
                                        parseTypeSignature(StandardTypes.VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                    if (returnType.getBase().equals(ROW)) {
                        return call(
                                JSON_TO_ROW_CAST.name(),
                                functionAndTypeManager.lookupCast(
                                        JSON_TO_ROW_CAST,
                                        parseTypeSignature(StandardTypes.VARCHAR),
                                        returnType),
                                call.getType(),
                                innerCall.getArguments());
                    }
                }
            }

            return call(
                    call.getDisplayName(),
                    functionAndTypeManager.lookupCast(CAST, call.getArguments().get(0).getType().getTypeSignature(), call.getType().getTypeSignature()),
                    call.getType(),
                    call.getArguments());
        }
    }
}
