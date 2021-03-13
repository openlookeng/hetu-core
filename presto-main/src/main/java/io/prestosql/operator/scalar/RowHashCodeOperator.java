package io.prestosql.operator.scalar;
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

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionAndTypeManager;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.spi.annotation.UsedByGeneratedCode;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.BuiltInScalarFunctionImplementation;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.spi.function.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.Signature.comparableWithVariadicBound;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.util.Reflection.methodHandle;

public class RowHashCodeOperator
        extends SqlOperator
{
    public static final RowHashCodeOperator ROW_HASH_CODE = new RowHashCodeOperator();
    private static final MethodHandle METHOD_HANDLE = methodHandle(RowHashCodeOperator.class, "hash", Type.class, Block.class);

    private RowHashCodeOperator()
    {
        super(HASH_CODE,
                ImmutableList.of(comparableWithVariadicBound("T", "row")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.BIGINT),
                ImmutableList.of(parseTypeSignature("T")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                METHOD_HANDLE.bindTo(type));
    }

    @UsedByGeneratedCode
    public static long hash(Type rowType, Block block)
    {
        BlockBuilder blockBuilder = rowType.createBlockBuilder(null, 1);
        blockBuilder.appendStructure(block);
        return rowType.hash(blockBuilder.build(), 0);
    }
}
