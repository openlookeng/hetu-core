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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;
import io.hetu.core.transport.block.BlockSerdeUtil;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.QualifiedObjectName;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.FunctionType;
import io.prestosql.spi.type.MapType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.VarcharType;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.connector.CatalogSchemaName.DEFAULT_NAMESPACE;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.FunctionKind.SCALAR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class LiteralFunction
        extends SqlScalarFunction
{
    static final String LITERAL_FUNCTION_NAME = "$literal$";
    private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = ImmutableSet.of(long.class, double.class, Slice.class, boolean.class);

    public LiteralFunction()
    {
        super(new Signature(QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, LITERAL_FUNCTION_NAME), SCALAR, parseTypeSignature("R"), parseTypeSignature("T")));
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "literal";
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type parameterType = boundVariables.getTypeVariable("T");
        Type type = boundVariables.getTypeVariable("R");

        MethodHandle methodHandle = null;
        if (parameterType.getJavaType() == type.getJavaType()) {
            methodHandle = MethodHandles.identity(parameterType.getJavaType());
        }

        if (parameterType.getJavaType() == Slice.class) {
            if (type.getJavaType() == Block.class) {
                methodHandle = BlockSerdeUtil.READ_BLOCK.bindTo(functionAndTypeManager.getBlockEncodingSerde());
            }
        }

        checkArgument(methodHandle != null,
                "Expected type %s to use (or can be converted into) Java type %s, but Java type is %s",
                type,
                parameterType.getJavaType(),
                type.getJavaType());

        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                methodHandle);
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        if (type instanceof FunctionType) {
            // FunctionType contains compiled lambda thus not serializable.
            return false;
        }
        if (type instanceof ArrayType) {
            return isSupportedLiteralType(((ArrayType) type).getElementType());
        }
        else if (type instanceof RowType) {
            RowType rowType = (RowType) type;
            return rowType.getTypeParameters().stream()
                    .allMatch(LiteralFunction::isSupportedLiteralType);
        }
        else if (type instanceof MapType) {
            MapType mapType = (MapType) type;
            return isSupportedLiteralType(mapType.getKeyType()) && isSupportedLiteralType(mapType.getValueType());
        }
        return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
    }

    public static long estimatedSizeInBytes(Object object)
    {
        if (object == null) {
            return 1;
        }
        Class<?> javaType = object.getClass();
        if (javaType == Long.class) {
            return Long.BYTES;
        }
        else if (javaType == Double.class) {
            return Double.BYTES;
        }
        else if (javaType == Boolean.class) {
            return 1;
        }
        else if (object instanceof Block) {
            return ((Block) object).getSizeInBytes();
        }
        else if (object instanceof Slice) {
            return ((Slice) object).length();
        }
        // unknown for rest of types
        return Integer.MAX_VALUE;
    }

    public static Signature getLiteralFunctionSignature(Type type)
    {
        TypeSignature argumentType = typeForLiteralFunctionArgument(type).getTypeSignature();

        return new Signature(
                QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, LITERAL_FUNCTION_NAME + type.getTypeSignature()),
                SCALAR,
                type.getTypeSignature(),
                argumentType);
    }

    public static Type typeForLiteralFunctionArgument(Type type)
    {
        Class<?> clazz = type.getJavaType();
        clazz = Primitives.unwrap(clazz);

        if (clazz == long.class) {
            return BIGINT;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (!clazz.isPrimitive()) {
            if (type instanceof VarcharType) {
                return type;
            }
            else {
                return VARBINARY;
            }
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }
}
