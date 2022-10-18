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

import io.prestosql.spi.block.Block;
import org.mockito.Mock;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.MockitoAnnotations.initMocks;
import static org.testng.Assert.assertEquals;

public class PageTest
{
    @Mock
    private Block mockBlocks;

    private Page pageUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        initMocks(this);
        pageUnderTest = new Page(0, new Properties(), mockBlocks);
    }

    @Test
    public void testGetChannelCount() throws Exception
    {
        assertEquals(0, pageUnderTest.getChannelCount());
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = pageUnderTest.getSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetLogicalSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = pageUnderTest.getLogicalSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetRetainedSizeInBytes() throws Exception
    {
        // Setup
        // Run the test
        final long result = pageUnderTest.getRetainedSizeInBytes();

        // Verify the results
        assertEquals(0L, result);
    }

    @Test
    public void testGetBlock()
    {
        // Setup
        // Run the test
        final Block result = pageUnderTest.getBlock(0);

        // Verify the results
    }

    @Test
    public void testGetSingleValuePage() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageUnderTest.getSingleValuePage(0);

        // Verify the results
    }

    @Test
    public void testGetRegion() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageUnderTest.getRegion(0, 0);

        // Verify the results
    }

    @Test
    public void testAppendColumn() throws Exception
    {
        // Setup
        final Block block = null;

        // Run the test
        final Page result = pageUnderTest.appendColumn(block);

        // Verify the results
    }

    @Test
    public void testCompact() throws Exception
    {
        // Setup
        // Run the test
        pageUnderTest.compact();

        // Verify the results
    }

    @Test
    public void testGetLoadedPage() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageUnderTest.getLoadedPage();

        // Verify the results
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", pageUnderTest.toString());
    }

    @Test
    public void testGetPositions() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageUnderTest.getPositions(new int[]{0}, 0, 0);

        // Verify the results
    }

    @Test
    public void testGetColumns() throws Exception
    {
        // Setup
        // Run the test
        final Page result = pageUnderTest.getColumns(0);

        // Verify the results
    }

    @Test
    public void testPrependColumn() throws Exception
    {
        // Setup
        final Block column = null;

        // Run the test
        final Page result = pageUnderTest.prependColumn(column);

        // Verify the results
    }

    @Test
    public void testSetPageMetadata() throws Exception
    {
        // Setup
        // Run the test
        pageUnderTest.setPageMetadata("key", "value");

        // Verify the results
    }

    @Test
    public void testWrapBlocksWithoutCopy()
    {
        // Setup
        final Block[] blocks = new Block[]{};

        // Run the test
        final Page result = Page.wrapBlocksWithoutCopy(0, blocks);
        assertEquals(0, result.getChannelCount());
        assertEquals(0, result.getPositionCount());
        assertEquals(0L, result.getSizeInBytes());
        assertEquals(0L, result.getLogicalSizeInBytes());
        assertEquals(0L, result.getRetainedSizeInBytes());
        assertEquals(null, result.getBlock(0));
        assertEquals(new Page(0, new Properties(), null), result.getSingleValuePage(0));
        assertEquals(new Page(0, new Properties(), null), result.getRegion(0, 0));
        final Block block = null;
        assertEquals(new Page(0, new Properties(), null), result.appendColumn(block));
        assertEquals(new Page(0, new Properties(), null), result.getLoadedPage());
        assertEquals("result", result.toString());
        assertEquals(new Page(0, new Properties(), null), result.getPositions(new int[]{0}, 0, 0));
        assertEquals(new Page(0, new Properties(), null), result.getColumns(0));
        final Block column = null;
        assertEquals(new Page(0, new Properties(), null), result.prependColumn(column));
        assertEquals(new Properties(), result.getPageMetadata());
    }
}
