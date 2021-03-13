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
package io.prestosql.operator.scalar;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.airlift.bytecode.BytecodeBlock;
import io.airlift.bytecode.ClassDefinition;
import io.airlift.bytecode.MethodDefinition;
import io.airlift.bytecode.Parameter;
import io.airlift.bytecode.Scope;
import io.airlift.bytecode.Variable;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.CastType;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.FunctionMetadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.gen.ArrayGeneratorUtils;
import io.prestosql.sql.gen.ArrayMapBytecodeExpression;
import io.prestosql.sql.gen.CachedInstanceBinder;
import io.prestosql.sql.gen.CallSiteBinder;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.bytecode.Access.FINAL;
import static io.airlift.bytecode.Access.PUBLIC;
import static io.airlift.bytecode.Access.STATIC;
import static io.airlift.bytecode.Access.a;
import static io.airlift.bytecode.Parameter.arg;
import static io.airlift.bytecode.ParameterizedType.type;
import static io.airlift.bytecode.expression.BytecodeExpressions.constantBoolean;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.Signature.typeVariable;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.util.Reflection.methodHandle;
import static io.prestosql.util.CompilerUtils.defineClass;
import static io.prestosql.util.CompilerUtils.makeClassName;

public class ArrayToArrayCast
        extends SqlOperator
{
    public static final ArrayToArrayCast ARRAY_TO_ARRAY_CAST = new ArrayToArrayCast();

    private ArrayToArrayCast()
    {
        super(CAST,
                ImmutableList.of(typeVariable("F"), typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature("array(F)")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromType = boundVariables.getTypeVariable("F");
        Type toType = boundVariables.getTypeVariable("T");

        FunctionHandle functionHandle = functionAndTypeManager.lookupCast(CastType.CAST, fromType.getTypeSignature(), toType.getTypeSignature());
        BuiltInScalarFunctionImplementation function = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionHandle);
        Class<?> castOperatorClass = generateArrayCast(functionAndTypeManager, functionAndTypeManager.getFunctionMetadata(functionHandle), function);

        MethodHandle methodHandle = methodHandle(castOperatorClass, "castArray", ConnectorSession.class, Block.class);
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    private static Class<?> generateArrayCast(TypeManager typeManager, FunctionMetadata elementCastFunctionMetadata, BuiltInScalarFunctionImplementation elementCast)
    {
        CallSiteBinder binder = new CallSiteBinder();

        ClassDefinition definition = new ClassDefinition(
                a(PUBLIC, FINAL),
                makeClassName(Joiner.on("$").join("ArrayCast", elementCastFunctionMetadata.getArgumentTypes().get(0), elementCastFunctionMetadata.getReturnType())),
                type(Object.class));

        Parameter session = arg("session", ConnectorSession.class);
        Parameter value = arg("value", Block.class);

        MethodDefinition method = definition.declareMethod(
                a(PUBLIC, STATIC),
                "castArray",
                type(Block.class),
                session,
                value);

        Scope scope = method.getScope();
        BytecodeBlock body = method.getBody();

        Variable wasNull = scope.declareVariable(boolean.class, "wasNull");
        body.append(wasNull.set(constantBoolean(false)));

        // cast map elements
        Type fromElementType = typeManager.getType(elementCastFunctionMetadata.getArgumentTypes().get(0));
        Type toElementType = typeManager.getType(elementCastFunctionMetadata.getReturnType());
        CachedInstanceBinder cachedInstanceBinder = new CachedInstanceBinder(definition, binder);
        ArrayMapBytecodeExpression newArray = ArrayGeneratorUtils.map(scope, cachedInstanceBinder, fromElementType, toElementType, value, elementCastFunctionMetadata.getName().getObjectName(), elementCast);

        // return the block
        body.append(newArray.ret());

        MethodDefinition constructorDefinition = definition.declareConstructor(a(PUBLIC));
        BytecodeBlock constructorBody = constructorDefinition.getBody();
        Variable thisVariable = constructorDefinition.getThis();
        constructorBody.comment("super();")
                .append(thisVariable)
                .invokeConstructor(Object.class);
        cachedInstanceBinder.generateInitializations(thisVariable, constructorBody);
        constructorBody.ret();

        return defineClass(definition, Object.class, binder.getBindings(), ArrayToArrayCast.class.getClassLoader());
    }
}
