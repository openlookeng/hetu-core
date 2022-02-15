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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.annotation.UsedByGeneratedCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.SingleMapBlock;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.FunctionHandle;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.InterpretedFunctionInvoker;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static io.prestosql.metadata.CastType.CAST;
import static io.prestosql.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.SUBSCRIPT;
import static io.prestosql.spi.function.Signature.typeVariable;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.TypeUtils.readNativeValue;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.spi.util.Reflection.methodHandle;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.lang.String.format;

public class MapSubscriptOperator
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE_BOOLEAN = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, ConnectorSession.class, Block.class, boolean.class);
    private static final MethodHandle METHOD_HANDLE_LONG = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, ConnectorSession.class, Block.class, long.class);
    private static final MethodHandle METHOD_HANDLE_DOUBLE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, ConnectorSession.class, Block.class, double.class);
    private static final MethodHandle METHOD_HANDLE_SLICE = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, ConnectorSession.class, Block.class, Slice.class);
    private static final MethodHandle METHOD_HANDLE_OBJECT = methodHandle(MapSubscriptOperator.class, "subscript", boolean.class, MissingKeyExceptionFactory.class, MethodHandle.class, MethodHandle.class, MethodHandle.class, Type.class, ConnectorSession.class, Block.class, Object.class);

    private final boolean legacyMissingKey;

    public MapSubscriptOperator(boolean legacyMissingKey)
    {
        super(SUBSCRIPT,
                ImmutableList.of(typeVariable("K"), typeVariable("V")),
                ImmutableList.of(),
                parseTypeSignature("V"),
                ImmutableList.of(parseTypeSignature("map(K,V)"), parseTypeSignature("K")));
        this.legacyMissingKey = legacyMissingKey;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type keyType = boundVariables.getTypeVariable("K");
        Type valueType = boundVariables.getTypeVariable("V");

        MethodHandle keyNativeHashCode = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperatorFunctionHandle(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        MethodHandle keyNativeEquals = functionAndTypeManager.getBuiltInScalarFunctionImplementation(functionAndTypeManager.resolveOperatorFunctionHandle(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));

        MethodHandle methodHandle;
        if (keyType.getJavaType() == boolean.class) {
            methodHandle = METHOD_HANDLE_BOOLEAN;
        }
        else if (keyType.getJavaType() == long.class) {
            methodHandle = METHOD_HANDLE_LONG;
        }
        else if (keyType.getJavaType() == double.class) {
            methodHandle = METHOD_HANDLE_DOUBLE;
        }
        else if (keyType.getJavaType() == Slice.class) {
            methodHandle = METHOD_HANDLE_SLICE;
        }
        else {
            methodHandle = METHOD_HANDLE_OBJECT;
        }
        methodHandle = MethodHandles.insertArguments(methodHandle, 0, legacyMissingKey);
        MissingKeyExceptionFactory missingKeyExceptionFactory = new MissingKeyExceptionFactory(functionAndTypeManager, keyType);
        methodHandle = methodHandle.bindTo(missingKeyExceptionFactory).bindTo(keyNativeHashCode).bindTo(keyBlockNativeEquals).bindTo(keyBlockHashCode).bindTo(valueType);
        methodHandle = methodHandle.asType(methodHandle.type().changeReturnType(Primitives.wrap(valueType.getJavaType())));

        return new BuiltInScalarFunctionImplementation(
                true,
                ImmutableList.of(
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL),
                        valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, ConnectorSession session, Block map, boolean key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, ConnectorSession session, Block map, long key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, ConnectorSession session, Block map, double key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, ConnectorSession session, Block map, Slice key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact(key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    @UsedByGeneratedCode
    public static Object subscript(boolean legacyMissingKey, MissingKeyExceptionFactory missingKeyExceptionFactory, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode, Type valueType, ConnectorSession session, Block map, Object key)
    {
        SingleMapBlock mapBlock = (SingleMapBlock) map;
        int valuePosition = mapBlock.seekKeyExact((Block) key);
        if (valuePosition == -1) {
            if (legacyMissingKey) {
                return null;
            }
            throw missingKeyExceptionFactory.create(session, key);
        }
        return readNativeValue(valueType, mapBlock, valuePosition);
    }

    private static class MissingKeyExceptionFactory
    {
        private final InterpretedFunctionInvoker functionInvoker;
        private final FunctionHandle castFunction;

        public MissingKeyExceptionFactory(FunctionAndTypeManager functionAndTypeManager, Type keyType)
        {
            functionInvoker = new InterpretedFunctionInvoker(functionAndTypeManager);

            FunctionHandle castFunctionHandle = null;
            try {
                castFunctionHandle = functionAndTypeManager.lookupCast(CAST, keyType.getTypeSignature(), VARCHAR.getTypeSignature());
            }
            catch (PrestoException ignored) {
                // the exception could be ignored
            }
            this.castFunction = castFunctionHandle;
        }

        public PrestoException create(ConnectorSession session, Object value)
        {
            if (castFunction != null) {
                try {
                    Slice varcharValue = (Slice) functionInvoker.invoke(castFunction, session, value);
                    return new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Key not present in map: %s", varcharValue.toStringUtf8()));
                }
                catch (RuntimeException ignored) {
                    // the exception could be ignored
                }
            }
            return new PrestoException(INVALID_FUNCTION_ARGUMENT, "Key not present in map");
        }
    }
}
