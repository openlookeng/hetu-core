/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
import io.prestosql.spi.type.VarcharType;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class VarcharToVarcharCoercerTest
{
    private VarcharToVarcharCoercer varcharToVarcharCoercerUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        varcharToVarcharCoercerUnderTest = new VarcharToVarcharCoercer(VarcharType.createVarcharType(0), VarcharType.createVarcharType(0));
    }

    @Test
    public void testApplyCoercedValue()
    {
        // Setups
        final BlockBuilder blockBuilder = null;
        ReaderProjectionsAdapterTest.Block block = new ReaderProjectionsAdapterTest.Block();
        Block[] blocks1 = {block, block};
        BlockListBlock blockListBlock = new BlockListBlock(blocks1, 1, 1);

        // Run the test
        varcharToVarcharCoercerUnderTest.applyCoercedValue((BlockBuilder) blockListBlock, block, 0);
        // Verify the results
    }
}
