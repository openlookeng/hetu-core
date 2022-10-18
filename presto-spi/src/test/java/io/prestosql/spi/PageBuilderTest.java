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
package io.prestosql.spi;

import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.snapshot.BlockEncodingSerdeProvider;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PageBuilderTest
{
    private PageBuilder pageBuilderUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        pageBuilderUnderTest = new PageBuilder(0, Arrays.asList());
    }

    @Test
    public void testReset() throws Exception
    {
        // Setup
        // Run the test
        pageBuilderUnderTest.reset();

        // Verify the results
    }

    @Test
    public void testNewPageBuilderLike() throws Exception
    {
        // Setup
        // Run the test
        final PageBuilder result = pageBuilderUnderTest.newPageBuilderLike();

        // Verify the results
    }

    @Test
    public void testGetBlockBuilder() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilder result = pageBuilderUnderTest.getBlockBuilder(0);

        // Verify the results
    }

    @Test
    public void testGetType() throws Exception
    {
        // Setup
        // Run the test
        final Type result = pageBuilderUnderTest.getType(0);

        // Verify the results
    }

    @Test
    public void testDeclarePosition() throws Exception
    {
        // Setup
        // Run the test
        pageBuilderUnderTest.declarePosition();

        // Verify the results
    }

    @Test
    public void testDeclarePositions() throws Exception
    {
        // Setup
        // Run the test
        pageBuilderUnderTest.declarePositions(0);

        // Verify the results
    }

    @Test
    public void testIsFull() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = pageBuilderUnderTest.isFull();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        assertTrue(pageBuilderUnderTest.isEmpty());
    }

    @Test
    public void testGetPositionCount() throws Exception
    {
        assertEquals(0, pageBuilderUnderTest.getPositionCount());
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = pageBuilderUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        assertEquals(0L, pageBuilderUnderTest.getRetainedSizeInBytes());
    }

    @Test
    public void testBuild() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageBuilderUnderTest.build();

        // Verify the results
    }

    @Test
    public void testCapture() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        final Object result = pageBuilderUnderTest.capture(serdeProvider);

        // Verify the results
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        final BlockEncodingSerdeProvider serdeProvider = null;

        // Run the test
        pageBuilderUnderTest.restore("state", serdeProvider);

        // Verify the results
    }

    @Test
    public void testWithMaxPageSize() throws Exception
    {
        // Setup
        final List<? extends Type> types = Arrays.asList();

        // Run the test
        final PageBuilder result = PageBuilder.withMaxPageSize(0, types);
        assertEquals(new PageBuilder(0, Arrays.asList()), result.newPageBuilderLike());
        assertEquals(null, result.getBlockBuilder(0));
        assertEquals(null, result.getType(0));
        assertTrue(result.isFull());
        assertTrue(result.isEmpty());
        assertEquals(0, result.getPositionCount());
        assertEquals(0L, result.getSizeInBytes());
        assertEquals(0L, result.getRetainedSizeInBytes());
        assertEquals(new Page(0, new Properties(), null), result.build());
        final BlockEncodingSerdeProvider serdeProvider = null;
        assertEquals("result", result.capture(serdeProvider));
        assertTrue(result.supportsConsolidatedWrites());
        assertEquals(0L, result.getUsedMemory());
    }
}
