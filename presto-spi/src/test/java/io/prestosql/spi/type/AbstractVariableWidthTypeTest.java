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
package io.prestosql.spi.type;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;

public class AbstractVariableWidthTypeTest
{
    @Mock
    private TypeSignature mockSignature;

    private AbstractVariableWidthType abstractVariableWidthTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractVariableWidthTypeUnderTest = new AbstractVariableWidthType(mockSignature, Object.class) {
            @Override
            public <T> T get(Block<T> block, int position)
            {
                return null;
            }

            @Override
            public <T> Object getObjectValue(ConnectorSession session, Block<T> block, int position)
            {
                return null;
            }

            @Override
            public void appendTo(Block block, int position, BlockBuilder blockBuilder)
            {
            }

            @Override
            public Optional<Range> getRange()
            {
                return null;
            }

            @Override
            public <T> T read(RawInput<T> input)
            {
                return null;
            }

            @Override
            public <T> void write(BlockBuilder<T> blockBuilder, T value)
            {
            }

            @Override
            public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
            {
                return null;
            }
        };
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = abstractVariableWidthTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = abstractVariableWidthTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0);

        // Verify the results
    }
}
