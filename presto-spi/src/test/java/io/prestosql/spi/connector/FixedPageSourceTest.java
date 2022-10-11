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
package io.prestosql.spi.connector;

import io.prestosql.spi.Page;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class FixedPageSourceTest
{
    private FixedPageSource fixedPageSourceUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        fixedPageSourceUnderTest = new FixedPageSource(
                Arrays.asList(new Page(0, new Properties(), null)));
    }

    @Test
    public void testClose() throws Exception
    {
        // Setup
        // Run the test
        fixedPageSourceUnderTest.close();

        // Verify the results
    }

    @Test
    public void testGetMemoryUsage() throws Exception
    {
        assertEquals(0L, fixedPageSourceUnderTest.getMemoryUsage());
    }

    @Test
    public void testGetReadTimeNanos() throws Exception
    {
        assertEquals(0L, fixedPageSourceUnderTest.getReadTimeNanos());
    }

    @Test
    public void testIsFinished() throws Exception
    {
        // Setup
        // Run the test
        final boolean result = fixedPageSourceUnderTest.isFinished();

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testGetNextPage() throws Exception
    {
        // Setup
        // Run the test
        final Page result = fixedPageSourceUnderTest.getNextPage();

        // Verify the results
    }

    @Test
    public void testGetSystemMemoryUsage() throws Exception
    {
        assertEquals(0L, fixedPageSourceUnderTest.getSystemMemoryUsage());
    }
}
