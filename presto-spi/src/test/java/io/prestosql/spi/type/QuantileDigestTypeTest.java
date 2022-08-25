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

import io.airlift.slice.Slice;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class QuantileDigestTypeTest
{
    @Mock
    private Type mockType;

    private QuantileDigestType quantileDigestTypeUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        quantileDigestTypeUnderTest = new QuantileDigestType(mockType);
    }

    @Test
    public void testAppendTo() throws Exception
    {
        // Setup
        final Block block = null;
        final BlockBuilder blockBuilder = null;

        // Run the test
        quantileDigestTypeUnderTest.appendTo(block, 0, blockBuilder);

        // Verify the results
    }

    @Test
    public void testGetSlice() throws Exception
    {
        // Setup
        final Block block = null;
        final Slice expectedResult = null;

        // Run the test
        final Slice result = quantileDigestTypeUnderTest.getSlice(block, 0);

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testWriteSlice1()
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        final Slice value = null;

        // Run the test
        quantileDigestTypeUnderTest.writeSlice(blockBuilder, value);

        // Verify the results
    }

    @Test
    public void testWriteSlice2() throws Exception
    {
        // Setup
        final BlockBuilder blockBuilder = null;
        final Slice value = null;

        // Run the test
        quantileDigestTypeUnderTest.writeSlice(blockBuilder, value, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetObjectValue() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Object result = quantileDigestTypeUnderTest.getObjectValue(null, block, 0);

        // Verify the results
    }

    @Test
    public void testGetTypeParameters() throws Exception
    {
        // Setup
        // Run the test
        final List<Type> result = quantileDigestTypeUnderTest.getTypeParameters();

        // Verify the results
    }
}
