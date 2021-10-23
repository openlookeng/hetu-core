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

import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.BytecodeNode;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.relation.CallExpression;
import io.prestosql.spi.relation.ConstantExpression;
import io.prestosql.spi.relation.InputReferenceExpression;
import io.prestosql.spi.relation.LambdaDefinitionExpression;
import io.prestosql.spi.relation.RowExpression;
import io.prestosql.spi.relation.RowExpressionVisitor;
import io.prestosql.spi.relation.SpecialForm;
import io.prestosql.spi.relation.VariableReferenceExpression;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.LambdaBytecodeGenerator.CompiledLambda;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantTrue;
import static io.airlift.bytecode.instruction.Constant.loadBoolean;
import static io.airlift.bytecode.instruction.Constant.loadDouble;
import static io.airlift.bytecode.instruction.Constant.loadFloat;
import static io.airlift.bytecode.instruction.Constant.loadInt;
import static io.airlift.bytecode.instruction.Constant.loadLong;
import static io.airlift.bytecode.instruction.Constant.loadString;
import static io.prestosql.sql.gen.BytecodeUtils.loadConstant;
import static io.prestosql.sql.gen.ClassContext.SPLIT_EXPRESSION_DEPTH_THRESHOLD;
import static io.prestosql.sql.gen.LambdaBytecodeGenerator.generateLambda;
import static java.lang.String.format;

public class RowExpressionCompiler
{
    private final CallSiteBinder callSiteBinder;
    private final CachedInstanceBinder cachedInstanceBinder;
    private final RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler;
    private final Metadata metadata;
    private final Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap;
    private final ClassContext functionContext;

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            Metadata metadata,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap)
    {
        this(callSiteBinder, cachedInstanceBinder, fieldReferenceCompiler, metadata, compiledLambdaMap, null);
    }

    RowExpressionCompiler(
            CallSiteBinder callSiteBinder,
            CachedInstanceBinder cachedInstanceBinder,
            RowExpressionVisitor<BytecodeNode, Scope> fieldReferenceCompiler,
            Metadata metadata,
            Map<LambdaDefinitionExpression, CompiledLambda> compiledLambdaMap,
            ClassContext functionContext)
    {
        this.callSiteBinder = callSiteBinder;
        this.cachedInstanceBinder = cachedInstanceBinder;
        this.fieldReferenceCompiler = fieldReferenceCompiler;
        this.metadata = metadata;
        this.compiledLambdaMap = compiledLambdaMap;
        this.functionContext = functionContext;
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope)
    {
        return compile(rowExpression, scope, Optional.empty());
    }

    public BytecodeNode compile(RowExpression rowExpression, Scope scope, Optional<Class<?>> lambdaInterface)
    {
        return rowExpression.accept(new Visitor(), new Context(scope, lambdaInterface));
    }

    private class Visitor
            implements RowExpressionVisitor<BytecodeNode, Context>
    {
        @Override
        public BytecodeNode visitCall(CallExpression call, Context context)
        {
            FunctionAndTypeManager functionAndTypeManager = metadata.getFunctionAndTypeManager();
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            BytecodeGeneratorContext generatorContext;
            switch (functionMetadata.getImplementationType()) {
                case BUILTIN:
                    generatorContext = new BytecodeGeneratorContext(
                            RowExpressionCompiler.this,
                            context.getScope(),
                            callSiteBinder,
                            cachedInstanceBinder,
                            functionAndTypeManager);
                    return (new FunctionCallCodeGenerator()).generateExpression(call.getFunctionHandle(), generatorContext, call.getType(), call.getArguments());
                default:
                    throw new IllegalArgumentException(format("Unsupported function implementation type in RowExpressionCompiler: %s. For the external function, we will support to execute them in the future release.", functionMetadata.getImplementationType()));
            }
        }

        @Override
        public BytecodeNode visitSpecialForm(SpecialForm specialForm, Context context)
        {
            BytecodeGenerator generator;
            ClassContext classContext = null;
            // special-cased in function registry
            switch (specialForm.getForm()) {
                // lazy evaluation
                case IF:
                    generator = new IfCodeGenerator();
                    break;
                case NULL_IF:
                    generator = new NullIfCodeGenerator();
                    break;
                case SWITCH:
                    // (SWITCH <expr> (WHEN <expr> <expr>) (WHEN <expr> <expr>) <expr>)
                    generator = new SwitchCodeGenerator();
                    break;
                case BETWEEN:
                    generator = new BetweenCodeGenerator();
                    break;
                // functions that take null as input
                case IS_NULL:
                    generator = new IsNullCodeGenerator();
                    break;
                case COALESCE:
                    generator = new CoalesceCodeGenerator();
                    break;
                // functions that require varargs and/or complex types (e.g., lists)
                case IN:
                    generator = new InCodeGenerator(metadata);
                    break;
                // optimized implementations (shortcircuiting behavior)
                case AND:
                    classContext = functionContext;
                    if (classContext != null) {
                        classContext.incrementDepth();
                        classContext.addWeight(1);
                    }
                    generator = new AndCodeGenerator();
                    break;
                case BETWEEN_AND:
                    generator = new AndCodeGenerator();
                    break;
                case OR:
                    classContext = functionContext;
                    if (classContext != null) {
                        classContext.incrementDepth();
                        classContext.addWeight(1);
                    }
                    generator = new OrCodeGenerator();
                    break;
                case DEREFERENCE:
                    generator = new DereferenceCodeGenerator();
                    break;
                case ROW_CONSTRUCTOR:
                    generator = new RowConstructorCodeGenerator();
                    break;
                case BIND:
                    generator = new BindCodeGenerator(compiledLambdaMap, context.getLambdaInterface().get());
                    break;
                default:
                    throw new IllegalStateException("Can not compile special form: " + specialForm.getForm());
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    metadata.getFunctionAndTypeManager());

            BytecodeNode bytecodeNode = null;
            if (classContext != null) {
                /* if (classContext.getWeight() >= SPLIT_EXPRESSION_WEIGHT) */
                if (classContext.getWeight() > SPLIT_EXPRESSION_DEPTH_THRESHOLD) {
                    /* Generate a function and change the Scope variable */
                    classContext.incrementFuncSeq();
                    String newFunctionName = "filterInternal" + classContext.getFuncSeq();

                    classContext.resetWeight();

                    /* call the new method and return the expression */
                    bytecodeNode = classContext.getFilterMethodGenerator()
                            .generateNewFilterMethod(newFunctionName, RowExpressionCompiler.this,
                                    generator, specialForm, context.getScope());
                }
                else {
                    bytecodeNode = generator.generateExpression(null, generatorContext, specialForm.getType(), specialForm.getArguments());
                }
                classContext.decrementDepth();
            }
            else {
                bytecodeNode = generator.generateExpression(null, generatorContext, specialForm.getType(), specialForm.getArguments());
            }

            return bytecodeNode;
        }

        @Override
        public BytecodeNode visitConstant(ConstantExpression constant, Context context)
        {
            Object value = constant.getValue();
            Class<?> javaType = constant.getType().getJavaType();

            BytecodeBlock block = new BytecodeBlock();
            if (value == null) {
                return block.comment("constant null")
                        .append(context.getScope().getVariable("wasNull").set(constantTrue()))
                        .pushJavaDefault(javaType);
            }

            // use LDC for primitives (boolean, short, int, long, float, double)
            block.comment("constant " + constant.getType().getTypeSignature());
            if (javaType == boolean.class) {
                return block.append(loadBoolean((Boolean) value));
            }
            if (javaType == byte.class || javaType == short.class || javaType == int.class) {
                return block.append(loadInt(((Number) value).intValue()));
            }
            if (javaType == long.class) {
                return block.append(loadLong((Long) value));
            }
            if (javaType == float.class) {
                return block.append(loadFloat((Float) value));
            }
            if (javaType == double.class) {
                return block.append(loadDouble((Double) value));
            }
            if (javaType == String.class) {
                return block.append(loadString((String) value));
            }

            // bind constant object directly into the call-site using invoke dynamic
            Binding binding = callSiteBinder.bind(value, constant.getType().getJavaType());

            return new BytecodeBlock()
                    .setDescription("constant " + constant.getType())
                    .comment(constant.toString())
                    .append(loadConstant(binding));
        }

        @Override
        public BytecodeNode visitInputReference(InputReferenceExpression node, Context context)
        {
            return fieldReferenceCompiler.visitInputReference(node, context.getScope());
        }

        @Override
        public BytecodeNode visitLambda(LambdaDefinitionExpression lambda, Context context)
        {
            checkState(compiledLambdaMap.containsKey(lambda), "lambda expressions map does not contain this lambda definition");
            if (!context.lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class)) {
                // lambdaInterface is checked to be annotated with FunctionalInterface when generating ScalarFunctionImplementation
                throw new VerifyException("lambda should be generated as class annotated with FunctionalInterface");
            }

            BytecodeGeneratorContext generatorContext = new BytecodeGeneratorContext(
                    RowExpressionCompiler.this,
                    context.getScope(),
                    callSiteBinder,
                    cachedInstanceBinder,
                    metadata.getFunctionAndTypeManager());

            return generateLambda(
                    generatorContext,
                    ImmutableList.of(),
                    compiledLambdaMap.get(lambda),
                    context.getLambdaInterface().get());
        }

        @Override
        public BytecodeNode visitVariableReference(VariableReferenceExpression reference, Context context)
        {
            if (reference.getName().startsWith(TEMP_PREFIX)) {
                return context.getScope().getTempVariable(reference.getName().substring(TEMP_PREFIX.length()));
            }
            return fieldReferenceCompiler.visitVariableReference(reference, context.getScope());
        }
    }

    private static final String TEMP_PREFIX = "$$TEMP$$";

    public static VariableReferenceExpression createTempVariableReferenceExpression(Variable variable, Type type)
    {
        return new VariableReferenceExpression(TEMP_PREFIX + variable.getName(), type);
    }

    private static class Context
    {
        private final Scope scope;
        private final Optional<Class<?>> lambdaInterface;

        public Context(Scope scope, Optional<Class<?>> lambdaInterface)
        {
            this.scope = scope;
            this.lambdaInterface = lambdaInterface;
        }

        public Scope getScope()
        {
            return scope;
        }

        public Optional<Class<?>> getLambdaInterface()
        {
            return lambdaInterface;
        }
    }
}
