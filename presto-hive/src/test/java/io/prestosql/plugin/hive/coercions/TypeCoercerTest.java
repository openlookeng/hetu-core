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
package io.prestosql.plugin.hive.coercions;

import io.prestosql.plugin.hive.ReaderProjectionsAdapterTest;
import io.prestosql.plugin.hive.util.FieldSetterFactoryTest;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TypeCoercerTest<F extends Type, T extends Type>
{
    private TypeCoercer<F, T> typeCoercerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        typeCoercerUnderTest = new TypeCoercer(new FieldSetterFactoryTest.Type(), new FieldSetterFactoryTest.Type())
        {
        };
    }

    @Test
    public void testApply()
    {
        // Setup
        final Block block = new ReaderProjectionsAdapterTest.Block();

        // Run the test
        final Block result = typeCoercerUnderTest.apply(block);

        // Verify the results
    }
}
