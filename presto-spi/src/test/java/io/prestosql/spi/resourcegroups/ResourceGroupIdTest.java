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
package io.prestosql.spi.resourcegroups;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ResourceGroupIdTest
{
    private ResourceGroupId resourceGroupIdUnderTest;

    @BeforeMethod
    public void setUp() throws Exception
    {
        resourceGroupIdUnderTest = new ResourceGroupId(new ResourceGroupId("name"), "name");
    }

    @Test
    public void testGetLastSegment() throws Exception
    {
        // Setup
        // Run the test
        final String result = resourceGroupIdUnderTest.getLastSegment();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testGetRoot() throws Exception
    {
        // Setup
        final ResourceGroupId expectedResult = new ResourceGroupId("name");

        // Run the test
        final ResourceGroupId result = resourceGroupIdUnderTest.getRoot();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testGetParent() throws Exception
    {
        // Setup
        final Optional<ResourceGroupId> expectedResult = Optional.of(new ResourceGroupId("name"));

        // Run the test
        final Optional<ResourceGroupId> result = resourceGroupIdUnderTest.getParent();

        // Verify the results
        assertEquals(expectedResult, result);
    }

    @Test
    public void testIsAncestorOf() throws Exception
    {
        // Setup
        final ResourceGroupId descendant = new ResourceGroupId("name");

        // Run the test
        final boolean result = resourceGroupIdUnderTest.isAncestorOf(descendant);

        // Verify the results
        assertTrue(result);
    }

    @Test
    public void testToString() throws Exception
    {
        // Setup
        // Run the test
        final String result = resourceGroupIdUnderTest.toString();

        // Verify the results
        assertEquals("result", result);
    }

    @Test
    public void testEquals() throws Exception
    {
        assertTrue(resourceGroupIdUnderTest.equals("o"));
    }

    @Test
    public void testHashCode() throws Exception
    {
        assertEquals(0, resourceGroupIdUnderTest.hashCode());
    }
}
