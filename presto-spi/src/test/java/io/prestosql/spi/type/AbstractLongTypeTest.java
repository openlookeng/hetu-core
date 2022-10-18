/*
 * Copyright (C) 2018-2021. Huawei Technologies Co., Ltd. All rights reserved.
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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockBuilderStatus;
import io.prestosql.spi.connector.ConnectorSession;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AbstractLongTypeTest
{
    @Mock
    private TypeSignature mockSignature;

    private AbstractLongType abstractLongTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        abstractLongTypeUnderTest = new AbstractLongType(mockSignature) {
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
    public void testGetFixedSize() throws Exception
    {
        assertEquals(0, abstractLongTypeUnderTest.getFixedSize());
    }

    @Test
    public void testIsComparable() throws Exception
    {
        assertTrue(abstractLongTypeUnderTest.isComparable());
    }

    @Test
    public void testIsOrderable() throws Exception
    {
        assertTrue(abstractLongTypeUnderTest.isOrderable());
    }

    @Test
    public void testGetLong() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = abstractLongTypeUnderTest.getLong(block, 0);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Block block = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = abstractLongTypeUnderTest.getSlice(block, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteLong() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractLongTypeUnderTest.writeLong(blockBuilder, 0L);

        // Verify the results
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        final Block block = null;
        final BlockBuilder blockBuilder = null;

        // Run the test
        abstractLongTypeUnderTest.appendTo(block, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testEqualTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final boolean result = abstractLongTypeUnderTest.equalTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testHash1()
    {
        // Setup
        final Block block = null;

        // Run the test
        final long result = abstractLongTypeUnderTest.hash(block, 0);

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testCompareTo() throws Exception
    {
        // Setup
        final Block leftBlock = null;
        final Block rightBlock = null;

        // Run the test
        final int result = abstractLongTypeUnderTest.compareTo(leftBlock, 0, rightBlock, 0);

        // Verify the results
        assertEquals(0, result);
    }

    @Test
    public void testCreateBlockBuilder1() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = abstractLongTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0, 0);

        // Verify the results
    }

    @Test
    public void testCreateBlockBuilder2() throws Exception
    {
        // Setup
        final BlockBuilderStatus blockBuilderStatus = null;

        // Run the test
        final BlockBuilder result = abstractLongTypeUnderTest.createBlockBuilder(blockBuilderStatus, 0);

        // Verify the results
    }

    @Test
    public void testCreateFixedSizeBlockBuilder() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = abstractLongTypeUnderTest.createFixedSizeBlockBuilder(0);

        // Verify the results
    }

    @Test
    public void testHash2() throws Exception
    {
        assertEquals(0L, AbstractLongType.hash(0L));
    }
}
