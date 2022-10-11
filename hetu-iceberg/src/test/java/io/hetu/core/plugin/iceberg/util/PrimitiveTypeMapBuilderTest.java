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

        ArrayType<Type> typeArrayType = new ArrayType<>(BooleanType.BOOLEAN);
        PrimitiveTypeMapBuilder.makeTypeMap(Arrays.asList(typeArrayType), Arrays.asList("value"));

        // Verify the results
    }
}
