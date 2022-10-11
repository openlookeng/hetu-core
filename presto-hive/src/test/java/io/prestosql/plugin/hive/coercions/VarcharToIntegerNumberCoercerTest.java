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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockListBlock;
import io.prestosql.spi.type.BigintType;
import io.prestosql.spi.type.IntegerType;
import io.prestosql.spi.type.SmallintType;
import io.prestosql.spi.type.TinyintType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VarcharToIntegerNumberCoercerTest<T extends Type>
{
    private VarcharToIntegerNumberCoercer<T> varcharToIntegerNumberCoercerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        varcharToIntegerNumberCoercerUnderTest = new VarcharToIntegerNumberCoercer(VarcharType.createVarcharType(0), TinyintType.TINYINT);
    }

    @Test
    public void test()
    {
        VarcharToIntegerNumberCoercer varcharToIntegerNumberCoercer = new VarcharToIntegerNumberCoercer(VarcharType.createVarcharType(0), SmallintType.SMALLINT);
        VarcharToIntegerNumberCoercer varcharToIntegerNumberCoercer1 = new VarcharToIntegerNumberCoercer(VarcharType.createVarcharType(0), IntegerType.INTEGER);
        VarcharToIntegerNumberCoercer varcharToIntegerNumberCoercer2 = new VarcharToIntegerNumberCoercer(VarcharType.createVarcharType(0), BigintType.BIGINT);
    }

    @Test
    public void testApplyCoercedValue()
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        ReaderProjectionsAdapterTest.Block readerBlock = new ReaderProjectionsAdapterTest.Block();
        Block[] blocks1 = {readerBlock};
        BlockListBlock blockListBlock1 = new BlockListBlock(blocks1, 1, 1);
        Block[] blocks = {blockListBlock1};

        final Block block = new BlockListBlock(blocks, 1, 1);

        // Run the test
        varcharToIntegerNumberCoercerUnderTest.applyCoercedValue(blockBuilder, block, 0);

        // Verify the results
    }
}
