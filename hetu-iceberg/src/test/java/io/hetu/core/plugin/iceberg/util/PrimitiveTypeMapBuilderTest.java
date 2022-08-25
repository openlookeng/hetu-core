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
package io.hetu.core.plugin.iceberg.util;

import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.BooleanType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Optional;

public class PrimitiveTypeMapBuilderTest
{
    private PrimitiveTypeMapBuilder primitiveTypeMapBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        primitiveTypeMapBuilderUnderTest = null /* TODO: construct the instance */;
    }

    @Test
    public void testMakeTypeMap()
    {
        // Run the test
        RowType from = RowType.from(Arrays.asList(new RowType.Field(Optional.of("row"), BooleanType.BOOLEAN)));
        PrimitiveTypeMapBuilder.makeTypeMap(Arrays.asList(from), Arrays.asList("value"));

//        Type type = new Type() {
//            @Override
//            public TypeSignature getTypeSignature()
//            {
//                return new TypeSignature("map");
//            }
//
//            @Override
//            public String getDisplayName()
//            {
//                return null;
//            }
//
//            @Override
//            public boolean isComparable()
//            {
//                return true;
//            }
//
//            @Override
//            public boolean isOrderable()
//            {
//                return false;
//            }
//
//            @Override
//            public Class<?> getJavaType()
//            {
//                return null;
//            }
//
//            @Override
//            public List<Type> getTypeParameters()
//            {
//                return null;
//            }
//
//            @Override
//            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
//            {
//                return null;
//            }
//
//            @Override
//            public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
//            {
//                return null;
//            }
//
//            @Override
//            public <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position)
//            {
//                return null;
//            }
//
//            @Override
//            public boolean getBoolean(Block block, int position)
//            {
//                return false;
//            }
//
//            @Override
//            public long getLong(Block block, int position)
//            {
//                return 0;
//            }
//
//            @Override
//            public double getDouble(Block block, int position)
//            {
//                return 0;
//            }
//
//            @Override
//            public Slice getSlice(Block block, int position)
//            {
//                return null;
//            }
//
//            @Override
//            public <T> Object getObject(Block<T> block, int position)
//            {
//                return null;
//            }
//
//            @Override
//            public void writeBoolean(BlockBuilder blockBuilder, boolean value)
//            {
//
//            }
//
//            @Override
//            public void writeLong(BlockBuilder blockBuilder, long value)
//            {
//
//            }
//
//            @Override
//            public void writeDouble(BlockBuilder blockBuilder, double value)
//            {
//
//            }
//
//            @Override
//            public void writeSlice(BlockBuilder blockBuilder, Slice value)
//            {
//
//            }
//
//            @Override
//            public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
//            {
//
//            }
//
//            @Override
//            public void writeObject(BlockBuilder blockBuilder, Object value)
//            {
//
//            }
//
//            @Override
//            public void appendTo(Block block, int position, BlockBuilder blockBuilder)
//            {
//
//            }
//
//            @Override
//            public <T> boolean equalTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
//            {
//                return false;
//            }
//
//            @Override
//            public <T> long hash(Block<T> block, int position)
//            {
//                return 0;
//            }
//
//            @Override
//            public <T> int compareTo(Block<T> leftBlock, int leftPosition, Block<T> rightBlock, int rightPosition)
//            {
//                return 0;
//            }
//        };
//        TypeOperators typeOperators = new TypeOperators();
//        MapType<Type, Type> typeTypeMapType = new MapType<Type, Type>(type,type,typeOperators);
//        PrimitiveTypeMapBuilder.makeTypeMap(Arrays.asList(typeTypeMapType),Arrays.asList("value"));
        ArrayType<Type> typeArrayType = new ArrayType<>(BooleanType.BOOLEAN);
        PrimitiveTypeMapBuilder.makeTypeMap(Arrays.asList(typeArrayType), Arrays.asList("value"));

        // Verify the results
    }
}
