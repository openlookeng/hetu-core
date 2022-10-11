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
package io.prestosql.spi.block;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class PageBuilderStatusTest
{
    private PageBuilderStatus pageBuilderStatusUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        pageBuilderStatusUnderTest = new PageBuilderStatus(0);
    }

    @Test
    public void testCreateBlockBuilderStatus() throws Exception
    {
        // Setup
        // Run the test
        final BlockBuilderStatus result = pageBuilderStatusUnderTest.createBlockBuilderStatus();

        // Verify the results
    }

    @Test
    public void testIsEmpty() throws Exception
    {
        assertTrue(pageBuilderStatusUnderTest.isEmpty());
    }

    @Test
    public void testIsFull() throws Exception
    {
        assertTrue(pageBuilderStatusUnderTest.isFull());
    }

    @Test
    public void testAddBytes() throws Exception
    {
        // Setup
        // Run the test
        pageBuilderStatusUnderTest.addBytes(0);

        // Verify the results
    }

    @Test
    public void testGetSizeInBytes() throws Exception
    {
        assertEquals(0L, pageBuilderStatusUnderTest.getSizeInBytes());
    }

    @Test
    public void testToString() throws Exception
    {
        assertEquals("result", pageBuilderStatusUnderTest.toString());
    }

    @Test
    public void testCapture() throws Exception
    {
        assertEquals("currentSize", pageBuilderStatusUnderTest.capture(null));
    }

    @Test
    public void testRestore() throws Exception
    {
        // Setup
        // Run the test
        pageBuilderStatusUnderTest.restore("state", null);

        // Verify the results
    }
}
